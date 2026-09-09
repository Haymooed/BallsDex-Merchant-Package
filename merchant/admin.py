from django.contrib import admin

from .models import (
    ActiveMerchant,
    Merchant,
    MerchantItem,
    MerchantPurchase,
    MerchantRotation,
    MerchantRotationItem,
)


@admin.register(Merchant)
class MerchantAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "enabled",
        "rotation_minutes",
        "items_per_rotation",
        "purchase_cooldown_seconds",
        "sale_percentage",
        "last_rotation_at",
    )
    list_filter = ("enabled",)
    search_fields = ("name",)
    filter_horizontal = ("items",)
    readonly_fields = ("last_rotation_at",)


@admin.register(ActiveMerchant)
class ActiveMerchantAdmin(admin.ModelAdmin):
    list_display = ("merchant", "guild_id", "channel_id", "message_id", "created_at")
    readonly_fields = ("merchant", "guild_id", "channel_id", "message_id", "created_at")
    list_filter = ("merchant",)

    def has_add_permission(self, request):
        return False


@admin.register(MerchantItem)
class MerchantItemAdmin(admin.ModelAdmin):
    list_display = ("label", "price", "weight", "enabled", "ball", "special")
    list_filter = ("enabled", "special")
    search_fields = ("display_name", "ball__country")


class MerchantRotationItemInline(admin.TabularInline):
    model = MerchantRotationItem
    extra = 0
    readonly_fields = ("item", "price_snapshot", "created_at")
    can_delete = False


@admin.register(MerchantRotation)
class MerchantRotationAdmin(admin.ModelAdmin):
    list_display = ("merchant", "starts_at", "ends_at")
    readonly_fields = ("merchant", "starts_at", "ends_at")
    list_filter = ("merchant",)
    inlines = (MerchantRotationItemInline,)

    def has_add_permission(self, request):
        return False


@admin.register(MerchantPurchase)
class MerchantPurchaseAdmin(admin.ModelAdmin):
    list_display = ("player", "rotation_item", "created_at")
    search_fields = ("player__discord_id",)
    readonly_fields = ("player", "rotation_item", "created_at")

    def has_add_permission(self, request):
        return False
