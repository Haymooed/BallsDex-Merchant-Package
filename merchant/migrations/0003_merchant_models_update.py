from django.db import migrations, models
import django.db.models.deletion


def migrate_settings_to_merchant(apps, schema_editor):
    Merchant = apps.get_model("merchant", "Merchant")
    MerchantSettings = apps.get_model("merchant", "MerchantSettings")
    MerchantRotation = apps.get_model("merchant", "MerchantRotation")
    ActiveMerchant = apps.get_model("merchant", "ActiveMerchant")

    settings_obj = MerchantSettings.objects.first()
    if settings_obj:
        merchant_inst = Merchant.objects.create(
            name="Traveling Merchant",
            enabled=settings_obj.enabled,
            rotation_minutes=settings_obj.rotation_minutes,
            items_per_rotation=settings_obj.items_per_rotation,
            purchase_cooldown_seconds=settings_obj.purchase_cooldown_seconds,
            last_rotation_at=settings_obj.last_rotation_at,
            sale_percentage=settings_obj.sale_percentage,
        )
    else:
        merchant_inst = Merchant.objects.create(name="Traveling Merchant")

    MerchantRotation.objects.filter(merchant__isnull=True).update(merchant=merchant_inst)
    ActiveMerchant.objects.filter(merchant__isnull=True).update(merchant=merchant_inst)


class Migration(migrations.Migration):

    dependencies = [
        ("merchant", "0002_activemerchant_merchantsettings_sale_percentage"),
    ]

    operations = [
        migrations.CreateModel(
            name="Merchant",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "name",
                    models.CharField(
                        default="Traveling Merchant",
                        help_text="Merchant name displayed in Discord and Admin.",
                        max_length=64,
                    ),
                ),
                ("enabled", models.BooleanField(default=True, help_text="Disable to hide this merchant entirely.")),
                (
                    "rotation_minutes",
                    models.PositiveIntegerField(
                        default=1440, help_text="How long a rotation lasts, in minutes. Default: 24h."
                    ),
                ),
                (
                    "items_per_rotation",
                    models.PositiveSmallIntegerField(
                        default=3, help_text="How many offers are selected for each rotation."
                    ),
                ),
                (
                    "purchase_cooldown_seconds",
                    models.PositiveIntegerField(
                        default=3600, help_text="Cooldown between purchases for the same player, in seconds."
                    ),
                ),
                ("last_rotation_at", models.DateTimeField(blank=True, null=True)),
                (
                    "sale_percentage",
                    models.PositiveSmallIntegerField(
                        default=0,
                        help_text="Sale/discount percentage (0-100). Increases the attractiveness of offers.",
                    ),
                ),
                (
                    "items",
                    models.ManyToManyField(
                        blank=True,
                        help_text="Items assigned to this merchant. If none selected, all enabled items are available.",
                        related_name="merchants",
                        to="merchant.merchantitem",
                    ),
                ),
            ],
            options={
                "verbose_name": "Merchant",
                "verbose_name_plural": "Merchants",
            },
        ),
        migrations.AddField(
            model_name="activemerchant",
            name="merchant",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="active_instances",
                to="merchant.merchant",
            ),
        ),
        migrations.AddField(
            model_name="merchantrotation",
            name="merchant",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="rotations",
                to="merchant.merchant",
            ),
        ),
        migrations.RunPython(migrate_settings_to_merchant, reverse_code=migrations.RunPython.noop),
        migrations.AlterField(
            model_name="activemerchant",
            name="merchant",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="active_instances",
                to="merchant.merchant",
            ),
        ),
        migrations.AlterField(
            model_name="merchantrotation",
            name="merchant",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="rotations",
                to="merchant.merchant",
            ),
        ),
        migrations.DeleteModel(
            name="MerchantSettings",
        ),
        migrations.AlterModelOptions(
            name="activemerchant",
            options={"verbose_name": "Active merchant", "verbose_name_plural": "Active merchants"},
        ),
    ]
