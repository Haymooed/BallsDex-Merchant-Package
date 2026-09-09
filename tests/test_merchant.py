import os
import sys
from unittest.mock import AsyncMock, MagicMock

mock_pkgs_dir = "/home/jules/self_created_tools/mock_pkgs"
if os.path.exists(mock_pkgs_dir) and mock_pkgs_dir not in sys.path:
    sys.path.insert(0, mock_pkgs_dir)

import django
from django.conf import settings

if not settings.configured:
    settings.configure(
        SECRET_KEY="test_key",
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
            "django.contrib.auth",
            "django.contrib.admin",
            "bd_models",
            "merchant",
        ],
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": ":memory:",
            }
        },
        USE_TZ=True,
    )
    django.setup()

import pytest
from django.core.management import call_command
from bd_models.models import Ball, Special, Player
from merchant.models import Merchant, MerchantItem, MerchantRotation, MerchantRotationItem, ActiveMerchant
from merchant.merchant.cog import Merchant as MerchantCog


@pytest.fixture(autouse=True)
def setup_db(db):
    call_command("migrate", interactive=False)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_multiple_merchants_and_items():
    # 1. Create Balls
    ball1 = await Ball.objects.acreate(country="USA")
    ball2 = await Ball.objects.acreate(country="UK")
    ball3 = await Ball.objects.acreate(country="Japan")

    # 2. Create Items
    item1 = await MerchantItem.objects.acreate(ball=ball1, price=1000)
    item2 = await MerchantItem.objects.acreate(ball=ball2, price=2000)
    item3 = await MerchantItem.objects.acreate(ball=ball3, price=3000)

    # 3. Create two distinct merchants with different discounts and item sets
    merchant_a = await Merchant.objects.acreate(
        name="Standard Merchant",
        sale_percentage=0,
        items_per_rotation=2,
    )
    await merchant_a.items.aadd(item1, item2)

    merchant_b = await Merchant.objects.acreate(
        name="Discount Merchant",
        sale_percentage=50,
        items_per_rotation=1,
    )
    await merchant_b.items.aadd(item3)

    # 4. Instantiate cog
    bot = MagicMock()
    bot.wait_until_ready = AsyncMock()
    cog = MerchantCog(bot)

    # 5. Ensure rotation for Merchant A
    rot_a = await cog.ensure_rotation(merchant_a)
    assert rot_a is not None
    assert rot_a.merchant == merchant_a

    items_a = await cog._get_rotation_items(rot_a)
    assert len(items_a) == 2
    item_labels_a = {rot_item.item.label for rot_item in items_a}
    assert item_labels_a == {"USA", "UK"}
    for rot_item in items_a:
        assert rot_item.get_price() == rot_item.price_snapshot

    # 6. Ensure rotation for Merchant B
    rot_b = await cog.ensure_rotation(merchant_b)
    assert rot_b is not None
    assert rot_b.merchant == merchant_b

    items_b = await cog._get_rotation_items(rot_b)
    assert len(items_b) == 1
    assert items_b[0].item.label == "Japan"
    assert items_b[0].get_price() == 1500

    await cog.cog_unload()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_merchant_autocomplete():
    await Merchant.objects.acreate(name="Black Market", enabled=True)
    await Merchant.objects.acreate(name="Traveling Trader", enabled=True)
    await Merchant.objects.acreate(name="Closed Shop", enabled=False)

    bot = MagicMock()
    cog = MerchantCog(bot)

    interaction = MagicMock()
    choices = await cog.merchant_autocomplete(interaction, "trader")
    assert len(choices) == 1
    assert choices[0].name == "Traveling Trader"

    choices_all = await cog.merchant_autocomplete(interaction, "")
    assert len(choices_all) == 2
    names = {c.name for c in choices_all}
    assert names == {"Black Market", "Traveling Trader"}

    await cog.cog_unload()
