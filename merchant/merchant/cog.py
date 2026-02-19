from __future__ import annotations

import asyncio
import logging
import random
from datetime import timedelta
from typing import TYPE_CHECKING, List, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks
from django.db import transaction
from django.utils import timezone
from asgiref.sync import sync_to_async

from bd_models.models import BallInstance, Player
from settings.models import settings

from merchant.models import MerchantItem, MerchantPurchase, MerchantRotation, MerchantRotationItem, MerchantSettings
if TYPE_CHECKING:
    from ballsdex.core.bot import BallsDexBot

log = logging.getLogger(__name__)
Interaction = discord.Interaction["BallsDexBot"]

class Merchant(commands.GroupCog, name="merchant"):
    """Traveling merchant system (BallsDex v3 compatible)."""

    def __init__(self, bot: "BallsDexBot"):
        self.bot = bot
        self._rotation_lock = asyncio.Lock()
        self._rotation_refresher.start()

    async def cog_unload(self) -> None:
        self._rotation_refresher.cancel()

    @tasks.loop(minutes=5)
    async def _rotation_refresher(self) -> None:
        await self.ensure_rotation()

    @_rotation_refresher.before_loop
    async def _before_rotation_loop(self) -> None:
        await self.bot.wait_until_ready()

    async def ensure_rotation(self) -> Optional[MerchantRotation]:
        async with self._rotation_lock:
            config = await MerchantSettings.load()
            if not config.enabled:
                return None

            now = timezone.now()
            rotation = await self._get_active_rotation()
            if rotation and rotation.ends_at > now:
                return rotation

            return await self._create_rotation(config)

    async def _get_active_rotation(self) -> Optional[MerchantRotation]:
        return await MerchantRotation.objects.filter(
            ends_at__gt=timezone.now()
        ).order_by("-starts_at").afirst()

    async def _create_rotation(self, config: MerchantSettings) -> Optional[MerchantRotation]:
        qs = (
            MerchantItem.objects.filter(enabled=True)
            .select_related("ball", "special")
            .order_by("id")
        )
        items = [item async for item in qs]
        if not items:
            log.warning("Merchant rotation skipped: no enabled items found in database.")
            return None

        count = min(config.items_per_rotation, len(items))
        selection = self._weighted_sample(items, count)

        now = timezone.now()
        rotation = await MerchantRotation.objects.acreate(
            starts_at=now,
            ends_at=now + timedelta(minutes=config.rotation_minutes),
        )

        await MerchantRotationItem.objects.abulk_create(
            [
                MerchantRotationItem(
                    rotation=rotation,
                    item=item,
                    price_snapshot=item.price,
                )
                for item in selection
            ]
        )

        await MerchantSettings.objects.filter(pk=config.pk).aupdate(
            last_rotation_at=now
        )

        log.info("Merchant rotation created with %s items.", len(selection))
        return rotation

    @staticmethod
    def _weighted_sample(items: List[MerchantItem], k: int) -> List[MerchantItem]:
        pool = list(items)
        chosen: List[MerchantItem] = []
        while pool and len(chosen) < k:
            weights = [max(1, i.weight) for i in pool]
            pick = random.choices(pool, weights=weights, k=1)[0]
            chosen.append(pick)
            pool.remove(pick)
        return chosen

    async def _get_rotation_items(self, rotation: MerchantRotation) -> List[MerchantRotationItem]:
        qs = rotation.rotation_items.select_related("item__ball", "item__special")
        return [entry async for entry in qs]

    @staticmethod
    def _format_price(price: int, currency: str) -> str:
        return f"{price:,} {currency}"

    @staticmethod
    def _rarity_tag(weight: int) -> str:
        if weight <= 5:
            return "🌌 Legendary"
        if weight <= 15:
            return "💎 Rare"
        if weight <= 35:
            return "✨ Uncommon"
        return "📦 Common"

    @app_commands.command(name="view", description="View the current merchant rotation.")
    async def view(self, interaction: Interaction) -> None:
        rotation = await self.ensure_rotation()
        if not rotation:
            await interaction.response.send_message("The merchant is currently unavailable.", ephemeral=True)
            return

        entries = await self._get_rotation_items(rotation)
        currency = settings.currency_name or "coins"

        embed = discord.Embed(
            title="🧳 The Traveling Merchant",
            description=(
                "Step right up - a fresh selection of curios is available now.\n"
                f"⏳ **Next refresh:** {discord.utils.format_dt(rotation.ends_at, style='R')}"
            ),
            colour=discord.Colour.gold(),
        )
        embed.set_footer(text="Use `/merchant buy <item_id>` to claim an offer")

        if not entries:
            embed.description = "The merchant has nothing to sell right now."
        else:
            lines = []
            for entry in entries:
                special = f" • {entry.item.special.name}" if entry.item.special else ""
                rarity = self._rarity_tag(entry.item.weight)
                lines.append(
                    f"`#{entry.id}` **{entry.item.label}**{special}\n"
                    f"└ {rarity} • 💰 {self._format_price(entry.price_snapshot, currency)}"
                )
            embed.add_field(name="✨ Current Stock", value="\n\n".join(lines), inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="buy", description="Buy an item from the merchant.")
    async def buy(self, interaction: Interaction, item_id: int) -> None:
        config = await MerchantSettings.load()
        if not config.enabled:
            await interaction.response.send_message("The merchant is currently closed.", ephemeral=True)
            return

        rotation = await self.ensure_rotation()
        if not rotation:
            await interaction.response.send_message("No active rotation.", ephemeral=True)
            return

        entries = await self._get_rotation_items(rotation)
        entry = next((e for e in entries if e.id == item_id), None)
        if not entry:
            await interaction.response.send_message("Invalid item ID. Check `/merchant view`.", ephemeral=True)
            return
        currency = settings.currency_name or "coins"

        player, _ = await Player.objects.aget_or_create(discord_id=interaction.user.id)

        last_purchase = await MerchantPurchase.objects.filter(player=player).order_by("-created_at").afirst()
        if last_purchase:
            cooldown = timedelta(seconds=config.purchase_cooldown_seconds)
            if timezone.now() < last_purchase.created_at + cooldown:
                ready_at = last_purchase.created_at + cooldown
                await interaction.response.send_message(
                    f"You're on cooldown! You can buy again {discord.utils.format_dt(ready_at, 'R')}.",
                    ephemeral=True
                )
                return

        if not player.can_afford(entry.price_snapshot):
            await interaction.response.send_message(
                f"You need **{self._format_price(entry.price_snapshot, currency)}** for this item.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        def process_purchase():
            with transaction.atomic():
                p = Player.objects.select_for_update().get(pk=player.pk)
                if not p.can_afford(entry.price_snapshot):
                    return None, "Insufficient funds.", None
                
                p.money -= entry.price_snapshot
                p.save()

                inst = BallInstance.objects.create(
                    ball=entry.item.ball,
                    player=p,
                    special=entry.item.special,
                    server_id=interaction.guild_id,
                    tradeable=True,
                    attack_bonus=random.randint(-settings.max_attack_bonus, settings.max_attack_bonus),
                    health_bonus=random.randint(-settings.max_health_bonus, settings.max_health_bonus),
                )
                MerchantPurchase.objects.create(player=p, rotation_item=entry)
                return inst, None, p.money

        instance, error, remaining_balance = await sync_to_async(process_purchase)()

        if error:
            await interaction.followup.send(error, ephemeral=True)
        else:
            purchase_embed = discord.Embed(
                title="✅ Purchase Complete",
                description=f"You purchased **{instance.description(include_emoji=True, bot=self.bot)}**.",
                colour=discord.Colour.green(),
            )
            purchase_embed.add_field(
                name="Cost",
                value=self._format_price(entry.price_snapshot, currency),
                inline=True,
            )
            purchase_embed.add_field(
                name="Remaining Balance",
                value=self._format_price(remaining_balance, currency),
                inline=True,
            )
            purchase_embed.set_footer(text="Thanks for shopping with the Traveling Merchant")
            await interaction.followup.send(embed=purchase_embed, ephemeral=True)

    @buy.autocomplete("item_id")
    async def autocomplete_item(self, interaction: Interaction, current: str):
        rotation = await self._get_active_rotation()
        if not rotation: return []
        entries = await self._get_rotation_items(rotation)
        return [
            app_commands.Choice(name=f"{e.item.label} ({e.price_snapshot})", value=e.id)
            for e in entries if current.lower() in e.item.label.lower()
        ][:25]
