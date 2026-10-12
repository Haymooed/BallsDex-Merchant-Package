# Generated manually for configurable Wicked Spell merchant offers.

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("merchant", "0001_initial"),
        ("bd_models", "0020_auction_channel_id_auction_guild_id_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="merchantitem",
            name="item_type",
            field=models.CharField(
                choices=[("ball", "Collectible card"), ("wicked_spell", "Wicked Spell")],
                default="ball",
                max_length=16,
            ),
        ),
        migrations.AddField(
            model_name="merchantitem",
            name="spell_effect",
            field=models.CharField(
                blank=True,
                choices=[
                    ("double_stats", "Double the next caught card's stats"),
                    ("special_variant", "Apply a special variant to the next caught card"),
                ],
                max_length=32,
            ),
        ),
        migrations.AddField(
            model_name="merchantitem",
            name="spell_special",
            field=models.ForeignKey(
                blank=True,
                help_text="Required for a special-variant spell; selects the guaranteed next-card background.",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="merchant_spell_items",
                to="bd_models.special",
            ),
        ),
        migrations.AddField(
            model_name="merchantitem",
            name="stat_multiplier",
            field=models.PositiveSmallIntegerField(
                default=2,
                help_text="Stat multiplier for a double-stats spell. The default is 2×.",
            ),
        ),
        migrations.AlterField(
            model_name="merchantitem",
            name="ball",
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to="bd_models.ball"),
        ),
        migrations.AlterField(
            model_name="merchantitem",
            name="special",
            field=models.ForeignKey(
                blank=True,
                help_text="Optional special background for a collectible-card offer.",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="merchant_card_items",
                to="bd_models.special",
            ),
        ),
    ]
