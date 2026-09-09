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

from merchant.models import (
    ActiveMerchant,
    Merchant as MerchantModel,
    MerchantItem,
    MerchantPurchase,
    MerchantRotation,
    MerchantRotationItem,
)

if TYPE_CHECKING:
    from ballsdex.core.bot import BallsDexBot

log = logging.getLogger(__name__)
Interaction = discord.Interaction["BallsDexBot"]


class MerchantView(discord.ui.View):
    def __init__(self, entries: List[MerchantRotationItem], sale_percentage: int = 0):
        super().__init__(timeout=None)
        for entry in entries:
            self.add_item(
                discord.ui.Button(
                    label=f"Buy {entry.item.label}",
                    style=discord.ButtonStyle.green,
                    custom_id=f"merchant:buy:{entry.id}",
                )
            )


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
        async for merchant_inst in MerchantModel.objects.filter(enabled=True):
            rotation = await self.ensure_rotation(merchant_inst)
            if rotation:
                await self.update_merchant_instances(merchant_inst, rotation)

    @_rotation_refresher.before_loop
    async def _before_rotation_loop(self) -> None:
        await self.bot.wait_until_ready()

    async def ensure_rotation(self, merchant: MerchantModel) -> Optional[MerchantRotation]:
        async with self._rotation_lock:
            if not merchant.enabled:
                return None

            now = timezone.now()
            rotation = await self._get_active_rotation(merchant)
            if rotation and rotation.ends_at > now:
                return rotation

            return await self._create_rotation(merchant)

    async def _get_active_rotation(self, merchant: MerchantModel) -> Optional[MerchantRotation]:
        return (
            await MerchantRotation.objects.filter(
                merchant=merchant, ends_at__gt=timezone.now()
            )
            .order_by("-starts_at")
            .afirst()
        )

    async def _create_rotation(self, merchant: MerchantModel) -> Optional[MerchantRotation]:
        item_qs = (
            merchant.items.filter(enabled=True)
            .select_related("ball", "special")
            .order_by("id")
        )
        items = [item async for item in item_qs]

        if not items:
            all_qs = (
                MerchantItem.objects.filter(enabled=True)
                .select_related("ball", "special")
                .order_by("id")
            )
            items = [item async for item in all_qs]

        if not items:
            log.warning(
                "Merchant '%s' (ID %s) rotation skipped: no enabled items found.",
                merchant.name,
                merchant.pk,
            )
            return None

        count = min(merchant.items_per_rotation, len(items))
        selection = self._weighted_sample(items, count)

        now = timezone.now()
        rotation = await MerchantRotation.objects.acreate(
            merchant=merchant,
            starts_at=now,
            ends_at=now + timedelta(minutes=merchant.rotation_minutes),
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

        await MerchantModel.objects.filter(pk=merchant.pk).aupdate(last_rotation_at=now)

        log.info(
            "Merchant '%s' rotation created with %s items.", merchant.name, len(selection)
        )
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

    async def _get_rotation_items(
        self, rotation: MerchantRotation
    ) -> List[MerchantRotationItem]:
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

    def _get_embed(
        self,
        merchant: MerchantModel,
        rotation: MerchantRotation,
        entries: List[MerchantRotationItem],
    ) -> discord.Embed:
        currency = settings.currency_name or "coins"
        sale_percentage = merchant.sale_percentage

        embed = discord.Embed(
            title=f"✨ {merchant.name} ✨",
            description=(
                f"The merchant has arrived with new wares!\n"
                f"⏳ **Refreshes:** {discord.utils.format_dt(rotation.ends_at, style='R')}"
            ),
            colour=discord.Colour.gold(),
        )
        if sale_percentage > 0:
            embed.title = f"✨ {merchant.name} - {sale_percentage}% OFF SALE! ✨"
            embed.colour = discord.Colour.red()

        if not entries:
            embed.description = f"The {merchant.name} is currently out of stock."
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
                    inline=True,
                )

        embed.set_footer(text="Click the buttons below to purchase!")
        return embed

    async def update_merchant_instances(
        self, merchant: MerchantModel, rotation: MerchantRotation
    ):
        items = await self._get_rotation_items(rotation)

        async for active in ActiveMerchant.objects.filter(merchant=merchant):
            guild = self.bot.get_guild(active.guild_id)
            if not guild:
                continue
            channel = guild.get_channel(active.channel_id)
            if not channel:
                continue

            try:
                message = await channel.fetch_message(active.message_id)
                embed = self._get_embed(merchant, rotation, items)
                view = MerchantView(items, merchant.sale_percentage)
                await message.edit(embed=embed, view=view)
            except discord.NotFound:
                await active.adelete()
            except Exception:
                log.exception(f"Failed to update merchant message {active.message_id}")

    async def update_all_merchants(self, rotation: MerchantRotation):
        """Helper maintaining compatibility with single-rotation calls."""
        merchant = rotation.merchant
        await self.update_merchant_instances(merchant, rotation)

    async def merchant_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> List[app_commands.Choice[int]]:
        choices = []
        async for m in MerchantModel.objects.filter(enabled=True):
            if current.lower() in m.name.lower():
                choices.append(app_commands.Choice(name=m.name, value=m.pk))
        return choices[:25]

    @app_commands.command(name="send", description="Send a merchant message to this channel.")
    @app_commands.describe(merchant="Select which merchant to send")
    @app_commands.autocomplete(merchant=merchant_autocomplete)
    @app_commands.checks.has_permissions(administrator=True)
    async def send(self, interaction: Interaction, merchant: Optional[int] = None) -> None:
        enabled_merchants = [m async for m in MerchantModel.objects.filter(enabled=True)]
        if not enabled_merchants:
            await interaction.response.send_message(
                "No enabled merchants available.", ephemeral=True
            )
            return

        selected_merchant: Optional[MerchantModel] = None
        if merchant is not None:
            selected_merchant = await MerchantModel.objects.filter(
                pk=merchant, enabled=True
            ).afirst()
            if not selected_merchant:
                await interaction.response.send_message(
                    "Selected merchant not found or is disabled.", ephemeral=True
                )
                return
        else:
            if len(enabled_merchants) == 1:
                selected_merchant = enabled_merchants[0]
            else:
                await interaction.response.send_message(
                    "Multiple merchants exist. Please specify the merchant parameter.",
                    ephemeral=True,
                )
                return

        rotation = await self.ensure_rotation(selected_merchant)
        if not rotation:
            await interaction.response.send_message(
                f"Merchant '{selected_merchant.name}' is currently unavailable.", ephemeral=True
            )
            return

        entries = await self._get_rotation_items(rotation)
        embed = self._get_embed(selected_merchant, rotation, entries)
        view = MerchantView(entries, selected_merchant.sale_percentage)

        await interaction.response.send_message(
            f"Merchant '{selected_merchant.name}' message sent!", ephemeral=True
        )
        message = await interaction.channel.send(embed=embed, view=view)

        await ActiveMerchant.objects.filter(
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
            merchant=selected_merchant,
        ).adelete()

        await ActiveMerchant.objects.acreate(
            merchant=selected_merchant,
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
            message_id=message.id,
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

        entry = (
            await MerchantRotationItem.objects.filter(id=item_id)
            .select_related("rotation__merchant", "item__ball", "item__special")
            .afirst()
        )
        if not entry:
            await interaction.followup.send("This item is no longer available.", ephemeral=True)
            return

        merchant = entry.rotation.merchant
        if not merchant.enabled:
            await interaction.followup.send("This merchant is currently closed.", ephemeral=True)
            return

        if not entry.rotation.is_active():
            await interaction.followup.send("This offer has expired.", ephemeral=True)
            return

        currency = settings.currency_name or "coins"
        price = entry.get_price(merchant.sale_percentage)

        player, _ = await Player.objects.aget_or_create(discord_id=interaction.user.id)

        last_purchase = (
            await MerchantPurchase.objects.filter(
                player=player, rotation_item__rotation__merchant=merchant
            )
            .order_by("-created_at")
            .afirst()
        )
        if last_purchase:
            cooldown = timedelta(seconds=merchant.purchase_cooldown_seconds)
            if timezone.now() < last_purchase.created_at + cooldown:
                ready_at = last_purchase.created_at + cooldown
                await interaction.followup.send(
                    f"Purchase on cooldown. Try again {discord.utils.format_dt(ready_at, 'R')}.",
                    ephemeral=True,
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
                    attack_bonus=random.randint(
                        -settings.max_attack_bonus, settings.max_attack_bonus
                    ),
                    health_bonus=random.randint(
                        -settings.max_health_bonus, settings.max_health_bonus
                    ),
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
