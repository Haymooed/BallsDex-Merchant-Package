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

from bd_models.models import BallInstance, Player
from settings.models import settings

# Corrected relative import for your folder structure
from ..models import MerchantItem, MerchantPurchase, MerchantRotation, MerchantRotationItem, MerchantSettings

if TYPE_CHECKING:
    from ballsdex.core.bot import BallsDexBot

log = logging.getLogger(__name__)
Interaction = discord.Interaction["BallsDexBot"]

class Merchant(commands.GroupCog, name="merchant"):
    """Traveling merchant system for BallsDex."""

    def __init__(self, bot: "BallsDexBot"):
        self.bot = bot
        self._rotation_lock = asyncio.Lock()
        self._rotation_refresher.start()

    async def cog_unload(self) -> None:
        self._rotation_refresher.cancel()

    # --- Internal Utilities ---

    def _get_currency_name(self) -> str:
        return settings.currency_name or "coins"

    @tasks.loop(minutes=5)
    async def _rotation_refresher(self) -> None:
        await self.ensure_rotation()

    @_rotation_refresher.before_loop
    async def _before_rotation_loop(self) -> None:
        await self.bot.wait_until_ready()

    async def ensure_rotation(self) -> Optional[MerchantRotation]:
        """Ensures a rotation exists if enabled, creating one if expired."""
        async with self._rotation_lock:
            config = await MerchantSettings.load()
            if not config.enabled:
                return None

            now = timezone.now()
            rotation = await self._get_active_rotation()
            
            if rotation and rotation.ends_at > now:
                return rotation

            return await self._create_new_rotation(config)

    async def _get_active_rotation(self) -> Optional[MerchantRotation]:
        """Fetch the most recent rotation that hasn't ended."""
        return await MerchantRotation.objects.filter(
            ends_at__gt=timezone.now()
        ).order_by("-starts_at").afirst()

    async def _create_new_rotation(self, config: MerchantSettings) -> Optional[MerchantRotation]:
        """Generates a new set of items and saves the rotation."""
        qs = MerchantItem.objects.filter(enabled=True).select_related("ball", "special")
        available_items = [item async for item in qs]

        if not available_items:
            log.warning("Merchant: No enabled items found in database. Skipping rotation.")
            return None

        # Determine how many items to pick
        k = min(config.items_per_rotation, len(available_items))
        
        # Weighted random selection without replacement
        selected_items = []
        pool = list(available_items)
        for _ in range(k):
            weights = [max(1, i.weight) for i in pool]
            choice = random.choices(pool, weights=weights, k=1)[0]
            selected_items.append(choice)
            pool.remove(choice)

        now = timezone.now()
        new_rotation = await MerchantRotation.objects.acreate(
            starts_at=now,
            ends_at=now + timedelta(minutes=config.rotation_minutes)
        )

        await MerchantRotationItem.objects.abulk_create([
            MerchantRotationItem(
                rotation=new_rotation,
                item=item,
                price_snapshot=item.price
            ) for item in selected_items
        ])

        await MerchantSettings.objects.filter(pk=config.pk).aupdate(last_rotation_at=now)
        log.info(f"Merchant: Created new rotation with {len(selected_items)} items.")
        return new_rotation

    async def _get_rotation_entries(self, rotation: MerchantRotation) -> List[MerchantRotationItem]:
        """Returns entries for a specific rotation with joined item data."""
        qs = rotation.rotation_items.select_related("item__ball", "item__special")
        return [entry async for entry in qs]

    async def _get_cooldown_end(self, player: Player, cooldown_seconds: int) -> Optional[timezone.datetime]:
        """Returns the timestamp when the player can buy again."""
        last_purchase = await MerchantPurchase.objects.filter(player=player).order_by("-created_at").afirst()
        if not last_purchase:
            return None
        
        ready_at = last_purchase.created_at + timedelta(seconds=cooldown_seconds)
        return ready_at if ready_at > timezone.now() else None

    # --- Slash Commands ---

    @app_commands.command(name="view", description="Check what the merchant is currently selling.")
    async def view(self, interaction: Interaction):
        rotation = await self.ensure_rotation()
        if not rotation:
            await interaction.response.send_message("The merchant is currently away.", ephemeral=True)
            return

        entries = await self._get_rotation_entries(rotation)
        currency = self._get_currency_name()

        embed = discord.Embed(
            title="🛒 Traveling Merchant",
            description=f"Leaves {discord.utils.format_dt(rotation.ends_at, style='R')}",
            color=discord.Color.gold()
        )

        if not entries:
            embed.description = "The merchant is sold out!"
        else:
            item_list = []
            for entry in entries:
                special_text = f" ({entry.item.special.name})" if entry.item.special else ""
                item_list.append(
                    f"**ID: `{entry.id}`** — {entry.item.label}{special_text}\n"
                    f"└ Price: {entry.price_snapshot:,} {currency}"
                )
            embed.add_field(name="Current Stock", value="\n\n".join(item_list), inline=False)

        embed.set_footer(text="Use /merchant buy <id> to purchase an item.")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="buy", description="Purchase an item using your ID.")
    @app_commands.describe(item_id="The numeric ID of the item from the /merchant view list")
    async def buy(self, interaction: Interaction, item_id: int):
        config = await MerchantSettings.load()
        if not config.enabled:
            return await interaction.response.send_message("The merchant is disabled.", ephemeral=True)

        rotation = await self._get_active_rotation()
        if not rotation:
            return await interaction.response.send_message("There is no active merchant rotation.", ephemeral=True)

        # Validate Item
        entries = await self._get_rotation_entries(rotation)
        entry = next((e for e in entries if e.id == item_id), None)
        if not entry:
            return await interaction.response.send_message("That item ID is not in the current rotation.", ephemeral=True)

        player, _ = await Player.objects.aget_or_create(discord_id=interaction.user.id)

        # Cooldown check
        cooldown_end = await self._get_cooldown_end(player, config.purchase_cooldown_seconds)
        if cooldown_end:
            return await interaction.response.send_message(
                f"You're on cooldown! You can buy again {discord.utils.format_dt(cooldown_end, 'R')}.",
                ephemeral=True
            )

        if not player.can_afford(entry.price_snapshot):
            return await interaction.response.send_message(
                f"You don't have enough {self._get_currency_name()}!", ephemeral=True
            )

        # Defer because database transactions can take a moment
        await interaction.response.defer(ephemeral=True)

        try:
            # Atomic transaction to prevent race conditions
            with transaction.atomic():
                # Re-fetch player with lock
                locked_player = Player.objects.select_for_update().get(pk=player.pk)
                
                if not locked_player.can_afford(entry.price_snapshot):
                    await interaction.followup.send("Transaction failed: Insufficient funds.")
                    return

                # Deduct and Give
                locked_player.money -= entry.price_snapshot
                locked_player.save()

                instance = BallInstance.objects.create(
                    ball=entry.item.ball,
                    player=locked_player,
                    special=entry.item.special,
                    server_id=interaction.guild_id,
                    tradeable=True
                )
                
                MerchantPurchase.objects.create(player=locked_player, rotation_item=entry)

            await interaction.followup.send(
                f"✅ Success! You bought **{instance.description(include_emoji=True, bot=self.bot)}**."
            )
        except Exception:
            log.exception("Error processing merchant purchase")
            await interaction.followup.send("An error occurred during the purchase. Please try again.")

    @buy.autocomplete("item_id")
    async def buy_autocomplete(self, interaction: Interaction, current: str):
        rotation = await self._get_active_rotation()
        if not rotation:
            return []

        entries = await self._get_rotation_entries(rotation)
        choices = []
        for e in entries:
            label = f"{e.item.label} ({e.price_snapshot} {self._get_currency_name()})"
            if current.lower() in label.lower():
                choices.append(app_commands.Choice(name=label, value=e.id))
        
        return choices[:25]