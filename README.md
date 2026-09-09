# BallsDex V3 Merchant Package
[![Support me on Patreon](https://img.shields.io/badge/Patreon-F96854?style=for-the-badge&logo=patreon&logoColor=white)](https://www.patreon.com/MarketDexOfficial?utm_campaign=creatorshare_creator)

Traveling merchant package for **BallsDex V3**. Provides rotating offers, multiple admin-managed merchant configurations, custom discounts, and interactive Discord embeds for browsing and purchasing collectibles.

## Installation (extra.toml)

Add this entry to `config/extra.toml` so BallsDex installs the package automatically:

```toml
[[ballsdex.packages]]
location = "git+https://github.com/Haymooed/BallsDex-Merchant-Package.git"
path = "merchant"
enabled = true
editable = false
```

## Enabling & configuring

All configuration is handled through the Django admin panel:

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
