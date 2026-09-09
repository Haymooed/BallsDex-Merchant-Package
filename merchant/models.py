from __future__ import annotations

from datetime import timedelta
from typing import Iterable

from django.db import models
from django.utils import timezone

from bd_models.models import Ball, BallInstance, Player, Special


class Merchant(models.Model):
    """
    Model holding merchant configuration managed from the admin panel.
    Multiple merchants can be created and managed independently.
    """

    name = models.CharField(
        max_length=64, default="Traveling Merchant", help_text="Merchant name displayed in Discord and Admin."
    )
    enabled = models.BooleanField(default=True, help_text="Disable to hide this merchant entirely.")
    rotation_minutes = models.PositiveIntegerField(
        default=24 * 60, help_text="How long a rotation lasts, in minutes. Default: 24h."
    )
    items_per_rotation = models.PositiveSmallIntegerField(
        default=3, help_text="How many offers are selected for each rotation."
    )
    purchase_cooldown_seconds = models.PositiveIntegerField(
        default=3600, help_text="Cooldown between purchases for the same player, in seconds."
    )
    last_rotation_at = models.DateTimeField(null=True, blank=True)
    sale_percentage = models.PositiveSmallIntegerField(
        default=0, help_text="Sale/discount percentage (0-100). Increases the attractiveness of offers."
    )
    items = models.ManyToManyField(
        "MerchantItem",
        blank=True,
        related_name="merchants",
        help_text="Items assigned to this merchant. If none selected, all enabled items are available.",
    )

    class Meta:
        verbose_name = "Merchant"
        verbose_name_plural = "Merchants"

    def __str__(self) -> str:
        return self.name

    @classmethod
    async def load(cls) -> "Merchant":
        """
        Retrieve the default/first merchant instance, creating it with defaults if missing.
        Maintained for backward compatibility.
        """
        instance = await cls.objects.afirst()
        if not instance:
            instance = await cls.objects.acreate()
        return instance

    @property
    def rotation_delta(self) -> timedelta:
        return timedelta(minutes=self.rotation_minutes)

    @property
    def purchase_cooldown(self) -> timedelta:
        return timedelta(seconds=self.purchase_cooldown_seconds)


# Alias for backward compatibility
MerchantSettings = Merchant


class MerchantItem(models.Model):
    """
    Configurable item pool entry for merchant rotations.
    """

    display_name = models.CharField(
        max_length=64, blank=True, help_text="Optional override name. Defaults to the ball name."
    )
    description = models.CharField(max_length=200, blank=True)
    price = models.PositiveBigIntegerField(default=1000, help_text="Price charged to the player.")
    weight = models.PositiveIntegerField(default=1, help_text="Relative selection weight for rotations.")
    enabled = models.BooleanField(default=True)
    ball = models.ForeignKey(Ball, on_delete=models.CASCADE)
    special = models.ForeignKey(Special, null=True, blank=True, on_delete=models.SET_NULL)

    class Meta:
        ordering = ("id",)

    def __str__(self) -> str:
        return self.label

    @property
    def label(self) -> str:
        return self.display_name or self.ball.country


class MerchantRotation(models.Model):
    """
    Stores generated rotations for a specific merchant to persist across restarts.
    """

    merchant = models.ForeignKey(Merchant, on_delete=models.CASCADE, related_name="rotations")
    starts_at = models.DateTimeField()
    ends_at = models.DateTimeField()

    class Meta:
        ordering = ("-starts_at",)

    def __str__(self) -> str:
        return f"{self.merchant.name} rotation ({self.starts_at.strftime('%Y-%m-%d %H:%M')})"

    def is_active(self) -> bool:
        return self.ends_at > timezone.now()

    def remaining(self) -> timedelta:
        return max(self.ends_at - timezone.now(), timedelta())


class MerchantRotationItem(models.Model):
    """
    Snapshot of a merchant offer for a given rotation.
    """

    rotation = models.ForeignKey(MerchantRotation, on_delete=models.CASCADE, related_name="rotation_items")
    item = models.ForeignKey(MerchantItem, on_delete=models.CASCADE, related_name="rotation_entries")
    price_snapshot = models.PositiveBigIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("id",)

    def __str__(self) -> str:
        return f"{self.item.label} ({self.price_snapshot})"

    def get_price(self, sale_percentage: int | None = None) -> int:
        if sale_percentage is None:
            sale_percentage = self.rotation.merchant.sale_percentage
        if sale_percentage <= 0:
            return self.price_snapshot
        return int(self.price_snapshot * (1 - min(sale_percentage, 100) / 100))

    def as_line(self, currency_name: str, collectible_name: str, sale_percentage: int | None = None) -> str:
        special = f" ({self.item.special})" if self.item.special else ""
        price = self.get_price(sale_percentage)
        return f"{self.item.label}{special} — {price} {currency_name} ({collectible_name})"


class ActiveMerchant(models.Model):
    """
    Tracks active merchant messages in channels.
    """

    merchant = models.ForeignKey(Merchant, on_delete=models.CASCADE, related_name="active_instances")
    guild_id = models.BigIntegerField()
    channel_id = models.BigIntegerField()
    message_id = models.BigIntegerField(unique=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Active merchant"
        verbose_name_plural = "Active merchants"

    def __str__(self) -> str:
        return f"{self.merchant.name} in channel {self.channel_id}"


class MerchantPurchase(models.Model):
    """
    Tracks purchases to enforce per-player cooldowns and provide simple auditability.
    """

    player = models.ForeignKey(Player, on_delete=models.CASCADE, related_name="merchant_purchases")
    rotation_item = models.ForeignKey(
        MerchantRotationItem, on_delete=models.CASCADE, related_name="purchases"
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("-created_at",)
        indexes = (models.Index(fields=("player", "created_at")),)

    def __str__(self) -> str:
        return f"{self.player_id} -> {self.rotation_item_id}"
