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

from merchant.models import MerchantItem, MerchantPurchase, MerchantRotation, MerchantRotationItem, MerchantSettings, ActiveMerchant

if TYPE_CHECKING:
    from ballsdex.core.bot import BallsDexBot

log = logging.getLogger(__name__)
Interaction = discord.Interaction["BallsDexBot"]

class MerchantView(discord.ui.View):
    def __init__(self, entries: List[MerchantRotationItem], sale_percentage: int):
        super().__init__(timeout=None)
        for entry in entries:
            self.add_item(discord.ui.Button(
                label=f"Buy {entry.item.label}",
                style=discord.ButtonStyle.green,
                custom_id=f"merchant:buy:{entry.id}"
            ))

class Merchant(commands.GroupCog, name="merchant"):
    """Traveling Merchant system."""

    def __init__(self, bot: "BallsDexBot"):
        self.bot = bot
        self._rotation_lock = asyncio.Lock()
        self._rotation_refresher.start()

    async def cog_unload(self) -> None:
        self._rotation_refresher.cancel()

    @tasks.loop(minutes=5)
    async def _rotation_refresher(self) -> None:
        rotation = await self.ensure_rotation()
        if rotation:
            await self.update_all_merchants(rotation)

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
            return "Legendary"
        if weight <= 15:
            return "Rare"
        if weight <= 35:
            return "Uncommon"
        return "Common"

    def _get_embed(self, rotation: MerchantRotation, entries: List[MerchantRotationItem], sale_percentage: int) -> discord.Embed:
        currency = settings.currency_name or "coins"
        embed = discord.Embed(
            title="✨ Traveling Merchant ✨",
            description=f"The merchant has arrived with new wares!\n⏳ **Refreshes:** {discord.utils.format_dt(rotation.ends_at, style='R')}",
            colour=discord.Colour.gold(),
        )
        if sale_percentage > 0:
            embed.title = f"✨ Traveling Merchant - {sale_percentage}% OFF SALE! ✨"
            embed.colour = discord.Colour.red()

        if not entries:
            embed.description = "The merchant is currently out of stock."
        else:
            for entry in entries:
                price = entry.get_price(sale_percentage)
                original_price = entry.price_snapshot
                special = f" ({entry.item.special.name})" if entry.item.special else ""
                rarity = self._rarity_tag(entry.item.weight)

                price_text = f"**{price:,}** {currency}"
                if sale_percentage > 0:
                    price_text = f"~~{original_price:,}~~ " + price_text

                embed.add_field(
                    name=f"{entry.item.label}{special}",
                    value=f"Rarity: {rarity}\nPrice: {price_text}",
                    inline=True
                )

        embed.set_footer(text="Click the buttons below to purchase!")
        return embed

    async def update_all_merchants(self, rotation: MerchantRotation):
        config = await MerchantSettings.load()
        items = await self._get_rotation_items(rotation)

        async for active in ActiveMerchant.objects.all():
            guild = self.bot.get_guild(active.guild_id)
            if not guild:
                continue
            channel = guild.get_channel(active.channel_id)
            if not channel:
                continue

            try:
                message = await channel.fetch_message(active.message_id)
                embed = self._get_embed(rotation, items, config.sale_percentage)
                view = MerchantView(items, config.sale_percentage)
                await message.edit(embed=embed, view=view)
            except discord.NotFound:
                await active.adelete()
            except Exception:
                log.exception(f"Failed to update merchant message {active.message_id}")

    @app_commands.command(name="send", description="Send the merchant message to this channel.")
    @app_commands.checks.has_permissions(administrator=True)
    async def send(self, interaction: Interaction) -> None:
        rotation = await self.ensure_rotation()
        if not rotation:
            await interaction.response.send_message("The merchant is currently unavailable.", ephemeral=True)
            return

        config = await MerchantSettings.load()
        entries = await self._get_rotation_items(rotation)

        embed = self._get_embed(rotation, entries, config.sale_percentage)
        view = MerchantView(entries, config.sale_percentage)

        await interaction.response.send_message("Merchant message sent!", ephemeral=True)
        message = await interaction.channel.send(embed=embed, view=view)

        await ActiveMerchant.objects.acreate(
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
            message_id=message.id
        )

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type != discord.InteractionType.component:
            return
        custom_id = interaction.data.get("custom_id")
        if not custom_id or not custom_id.startswith("merchant:buy:"):
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            item_id = int(custom_id.split(":")[-1])
        except ValueError:
            return

        config = await MerchantSettings.load()
        if not config.enabled:
            await interaction.followup.send("The merchant is currently closed.", ephemeral=True)
            return

        rotation = await self._get_active_rotation()
        if not rotation:
            await interaction.followup.send("There is no active rotation.", ephemeral=True)
            return

        entry = await MerchantRotationItem.objects.filter(rotation=rotation, id=item_id).select_related("item__ball", "item__special").afirst()
        if not entry:
            await interaction.followup.send("This item is no longer available.", ephemeral=True)
            return
            
        currency = settings.currency_name or "coins"
        price = entry.get_price(config.sale_percentage)

        player, _ = await Player.objects.aget_or_create(discord_id=interaction.user.id)

        last_purchase = await MerchantPurchase.objects.filter(player=player).order_by("-created_at").afirst()
        if last_purchase:
            cooldown = timedelta(seconds=config.purchase_cooldown_seconds)
            if timezone.now() < last_purchase.created_at + cooldown:
                ready_at = last_purchase.created_at + cooldown
                await interaction.followup.send(
                    f"Purchase on cooldown. Try again {discord.utils.format_dt(ready_at, 'R')}.",
                    ephemeral=True
                )
                return

        if not player.can_afford(price):
            await interaction.followup.send(
                f"Insufficient funds. You need **{self._format_price(price, currency)}**.",
                ephemeral=True,
            )
            return

        def process_purchase():
            with transaction.atomic():
                p = Player.objects.select_for_update().get(pk=player.pk)
                if not p.can_afford(price):
                    return None, "Insufficient funds.", None
                
                p.money -= price
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
                title="Purchase Successful",
                description=f"Acquired **{instance.description(include_emoji=True, bot=self.bot)}**.",
                colour=discord.Colour.green(),
            )
            purchase_embed.add_field(
                name="Price Paid",
                value=self._format_price(price, currency),
                inline=True,
            )
            purchase_embed.add_field(
                name="New Balance",
                value=self._format_price(remaining_balance, currency),
                inline=True,
            )
            await interaction.followup.send(embed=purchase_embed, ephemeral=True)
