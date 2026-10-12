from __future__ import annotations

import asyncio
import logging
import random
import re
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
    MerchantItem,
    MerchantPurchase,
    MerchantRotation,
    MerchantRotationItem,
    MerchantSettings,
    ActiveMerchant,
)

if TYPE_CHECKING:
    from ballsdex.core.bot import BallsDexBot

log = logging.getLogger(__name__)
Interaction = discord.Interaction["BallsDexBot"]


class MerchantView(discord.ui.View):
    def __init__(
        self,
        entries: List[MerchantRotationItem],
        sale_percentage: int,
        bot: Optional[BallsDexBot] = None,
    ):
        super().__init__(timeout=None)
        symbol = settings.currency_symbol or "\U0001fa99"
        for entry in entries:
            price = entry.get_price(sale_percentage)
            btn_emoji = None
            if bot and entry.item.ball and entry.item.ball.emoji_id:
                btn_emoji = bot.get_emoji(entry.item.ball.emoji_id)
                if not btn_emoji:
                    clean_name = re.sub(r"[^a-zA-Z0-9]", "", entry.item.ball.country)
                    btn_emoji = discord.PartialEmoji(name=clean_name, id=entry.item.ball.emoji_id)

            self.add_item(
                discord.ui.Button(
                    label=f"Buy {entry.item.label} ({price:,} {symbol})",
                    emoji=btn_emoji,
                    style=discord.ButtonStyle.primary if sale_percentage > 0 else discord.ButtonStyle.green,
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
        await self.update_all_merchants()

    @_rotation_refresher.before_loop
    async def _before_rotation_loop(self) -> None:
        await self.bot.wait_until_ready()

    async def ensure_rotation(self, config: Optional[MerchantSettings] = None) -> Optional[MerchantRotation]:
        async with self._rotation_lock:
            if config is None:
                config = await MerchantSettings.load()
            if not config.enabled:
                return None

            now = timezone.now()
            rotation = await self._get_active_rotation(config)
            if rotation and rotation.ends_at > now:
                return rotation

            return await self._create_rotation(config)

    async def _get_active_rotation(self, config: Optional[MerchantSettings] = None) -> Optional[MerchantRotation]:
        qs = MerchantRotation.objects.filter(ends_at__gt=timezone.now()).select_related("merchant").order_by("-starts_at")
        if config:
            qs = qs.filter(merchant=config)
        return await qs.afirst()

    async def _create_rotation(self, config: MerchantSettings) -> Optional[MerchantRotation]:
        qs = config.items.filter(enabled=True).select_related("ball", "special").order_by("id")
        items = [item async for item in qs]
        if not items:
            qs = (
                MerchantItem.objects.filter(enabled=True)
                .select_related("ball", "special")
                .order_by("id")
            )
            items = [item async for item in qs]

        if not items:
            log.warning("Merchant rotation skipped: no enabled items found in database for %s.", config.name)
            return None

        count = min(config.items_per_rotation, len(items))
        selection = self._weighted_sample(items, count)

        now = timezone.now()
        rotation = await MerchantRotation.objects.acreate(
            merchant=config,
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

        log.info("Merchant %s rotation created with %s items.", config.name, len(selection))
        return rotation

    @staticmethod
    def _weighted_sample(items: List[MerchantItem], k: int) -> List[MerchantItem]:
        pool = list(items)
        chosen: List[MerchantItem] = []
        while pool and len(chosen) < k:
            weights = [max(item.weight, 1) for item in pool]
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

    def _get_embed(
        self,
        rotation: MerchantRotation,
        entries: List[MerchantRotationItem],
        sale_percentage: int,
        merchant_name: str = "Traveling Merchant",
    ) -> discord.Embed:
        currency = settings.currency_name or "coins"
        symbol = settings.currency_symbol or "\U0001fa99"

        is_vip = "VIP" in merchant_name.upper()
        if is_vip:
            title = f"\U0001f48e {merchant_name} \u2014 {sale_percentage}% VIP Discount! \U0001f48e" if sale_percentage > 0 else f"\U0001f48e {merchant_name} \U0001f48e"
            color = discord.Colour.purple()
        elif sale_percentage > 0:
            title = f"\u2728 {merchant_name} \u2014 {sale_percentage}% OFF SALE! \u2728"
            color = discord.Colour.red()
        else:
            title = f"\u2728 {merchant_name} \u2728"
            color = discord.Colour.gold()

        desc = (
            f"The merchant has arrived with exclusive wares!\n"
            f"\u23f3 **Refreshes:** {discord.utils.format_dt(rotation.ends_at, style='R')}"
        )
        if sale_percentage > 0:
            desc += f"\n\U0001f3f7\ufe0f **Special Deal:** All items currently discounted by **{sale_percentage}%**!"

        embed = discord.Embed(
            title=title,
            description=desc,
            colour=color,
        )

        if not entries:
            embed.description = "The merchant is currently out of stock."
        else:
            for entry in entries:
                price = entry.get_price(sale_percentage)
                original_price = entry.price_snapshot
                special = f" ({entry.item.special.name})" if entry.item.special else ""
                rarity = self._rarity_tag(entry.item.weight)

                ball_emoji = ""
                if entry.item.ball and entry.item.ball.emoji_id:
                    emoji_obj = self.bot.get_emoji(entry.item.ball.emoji_id)
                    if emoji_obj:
                        ball_emoji = f"{emoji_obj} "
                    else:
                        clean_name = re.sub(r"[^a-zA-Z0-9]", "", entry.item.ball.country)
                        ball_emoji = f"<:{clean_name}:{entry.item.ball.emoji_id}> "

                price_text = f"**{price:,}** {symbol} {currency}"
                if sale_percentage > 0:
                    price_text = f"~~{original_price:,}~~ \u2794 " + price_text

                embed.add_field(
                    name=f"{ball_emoji}{entry.item.label}{special}",
                    value=f"> Rarity: `{rarity}`\n> Price: {price_text}",
                    inline=True,
                )

        embed.set_footer(text="Click the buttons below to purchase!")
        return embed

    async def update_all_merchants(self, specific_rotation: Optional[MerchantRotation] = None):
        async for active in ActiveMerchant.objects.select_related("merchant").all():
            merchant = active.merchant
            if not merchant.enabled:
                continue

            rotation = await self.ensure_rotation(merchant)
            if not rotation:
                continue

            guild = self.bot.get_guild(active.guild_id)
            if not guild:
                continue
            channel = guild.get_channel(active.channel_id)
            if not channel:
                try:
                    channel = await self.bot.fetch_channel(active.channel_id)
                except Exception:
                    continue
            if not channel:
                continue

            items = await self._get_rotation_items(rotation)
            embed = self._get_embed(rotation, items, merchant.sale_percentage, merchant_name=merchant.name)
            view = MerchantView(items, merchant.sale_percentage, bot=self.bot)

            try:
                if active.message_id and active.message_id > 0:
                    try:
                        message = await channel.fetch_message(active.message_id)
                        await message.edit(embed=embed, view=view)
                    except discord.NotFound:
                        message = await channel.send(embed=embed, view=view)
                        active.message_id = message.id
                        await active.asave(update_fields=("message_id",))
                else:
                    message = await channel.send(embed=embed, view=view)
                    active.message_id = message.id
                    await active.asave(update_fields=("message_id",))
            except Exception:
                log.exception(f"Failed to update or send merchant message in channel {active.channel_id}")

    async def merchant_autocomplete(
        self, interaction: discord.Interaction, current: str
    ) -> List[app_commands.Choice[int]]:
        choices = []
        async for m in MerchantSettings.objects.filter(enabled=True):
            if current.lower() in m.name.lower():
                choices.append(app_commands.Choice(name=m.name, value=m.pk))
        return choices[:25]

    @app_commands.command(name="send", description="Send the merchant message to this channel.")
    @app_commands.describe(merchant_id="Select which merchant to send (optional)")
    @app_commands.autocomplete(merchant_id=merchant_autocomplete)
    @app_commands.checks.has_permissions(administrator=True)
    async def send(self, interaction: Interaction, merchant_id: Optional[int] = None) -> None:
        if merchant_id is not None:
            config = await MerchantSettings.objects.filter(pk=merchant_id).afirst()
            if not config:
                await interaction.response.send_message(f"Merchant ID {merchant_id} not found.", ephemeral=True)
                return
        else:
            if interaction.channel_id == 1558263625360220203:
                config = await MerchantSettings.objects.filter(name__icontains="VIP").afirst() or await MerchantSettings.load()
            else:
                config = await MerchantSettings.load()

        rotation = await self.ensure_rotation(config)
        if not rotation:
            await interaction.response.send_message("The merchant is currently unavailable.", ephemeral=True)
            return

        entries = await self._get_rotation_items(rotation)

        embed = self._get_embed(rotation, entries, config.sale_percentage, merchant_name=config.name)
        view = MerchantView(entries, config.sale_percentage, bot=self.bot)

        await interaction.response.send_message(f"{config.name} message sent!", ephemeral=True)
        message = await interaction.channel.send(embed=embed, view=view)

        await ActiveMerchant.objects.aupdate_or_create(
            channel_id=interaction.channel_id,
            defaults={
                "merchant": config,
                "guild_id": interaction.guild_id,
                "message_id": message.id,
            },
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

        active = (
            await ActiveMerchant.objects.filter(channel_id=interaction.channel_id)
            .select_related("merchant")
            .afirst()
        )
        merchant = active.merchant if active else entry.rotation.merchant

        if not merchant.enabled:
            await interaction.followup.send("The merchant is currently closed.", ephemeral=True)
            return

        if not entry.rotation.is_active():
            await interaction.followup.send("This merchant rotation has expired.", ephemeral=True)
            return

        currency = settings.currency_name or "coins"
        symbol = settings.currency_symbol or "\U0001fa99"
        price = entry.get_price(merchant.sale_percentage)

        player, _ = await Player.objects.aget_or_create(discord_id=interaction.user.id)

        last_purchase = (
            await MerchantPurchase.objects.filter(player=player)
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
                f"Insufficient funds. You need **{self._format_price(price, currency)}** ({symbol}).",
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
                value=f"{self._format_price(price, currency)} ({symbol})",
                inline=True,
            )
            purchase_embed.add_field(
                name="New Balance",
                value=f"{self._format_price(remaining_balance, currency)} ({symbol})",
                inline=True,
            )
            await interaction.followup.send(embed=purchase_embed, ephemeral=True)
