from django.apps import AppConfig

class MerchantConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "merchant"
    verbose_name = "merchant"
    dpy_package = "merchant.merchant.cog"
