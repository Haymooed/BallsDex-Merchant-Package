# BallsDex V3 Merchant Package

Traveling merchant package for **BallsDex V3**. Provides rotating offers, multiple admin-managed merchant configurations, custom discounts, and interactive Discord embeds for browsing and purchasing collectibles.

## Installation (extra.toml)

Add this entry to `config/extra.toml` so BallsDex installs the package automatically:

```toml
[[ballsdex.packages]]
location = "git+https://github.com/Haymooed/MarketDex-PackagesV3.git"
path = "merchant"
enabled = true
editable = false
```

> The package is distributed as a standard Python package; no manual file copying is required.

## Enabling & configuring

All configuration is handled through the Django admin panel (no hardcoded settings):

- `Merchants` (multiple merchant configurations):
  - **Name**: Custom display name for the merchant (e.g., "Black Market", "Holiday Merchant")
  - **Enabled**: Enable/disable this specific merchant
  - **Sale/Discount percentage**: Sale percentage (0-100%) applied to items for this merchant
  - **Item selection**: Select specific `MerchantItem` entries available to this merchant (if none selected, defaults to all enabled items)
  - **Rotation duration (minutes)**: How long each rotation lasts
  - **Items per rotation**: Number of items randomly sampled per rotation
  - **Purchase cooldown (seconds)**: Per-player cooldown between purchases for this merchant
- `Merchant items`:
  - Selectable pool with price, weight, ball, and optional special
- Rotations & purchases are recorded for visibility and audit.

## Commands (slash, app_commands)

- `/merchant send [merchant]` — Send the interactive merchant message for a selected merchant to the current channel (administrator permission required). Autocomplete allows searching merchants by name.

## Notes

- Rotations are created automatically for enabled merchants when item pools are non-empty.
- Uses BallsDex models (`Ball`, `BallInstance`, `Player`, `Special`) and follows the V3 extra package loading flow.
- Async `setup(bot)` and modern `app_commands`; no legacy decorators or manual loaders.
