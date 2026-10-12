# 🛒 BallsDex V3 — Traveling Merchant Package

[![Support me on Patreon](https://img.shields.io/badge/Patreon-F96854?style=for-the-badge&logo=patreon&logoColor=white)](https://www.patreon.com/MarketDexOfficial?utm_campaign=creatorshare_creator)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)

A feature-rich **Traveling Merchant** package for **BallsDex V3**, created by **Haymooed**. Bring dynamic, auto-rotating shops directly into your Discord channels with custom merchant profiles, VIP & sale discounts, custom collectible emojis on buttons, and Wicked Spell buffs!

---

## ✨ What's New & Key Features

- 🏪 **Multiple Merchant Profiles (`Merchant` / `MerchantSettings`)** — Create and manage multiple independent merchants (e.g., *Traveling Merchant*, *VIP Merchant*, *Black Market*, *Holiday Merchant*), each with their own item pool, rotation timer, and discount rate.
- 💎 **VIP & Sale Discount Styling** — Automatic visual themes for discounted rotations (`% OFF SALE!` in crimson or `💎 VIP Discount!` in royal purple), complete with strikethrough original prices (`~~1,000~~ ➔ **750**`) and highlighted buy buttons.
- 🎨 **Live Collectible Emojis & Pricing on Buttons** — Interactive purchase buttons automatically display each collectible's custom Discord emoji and live discounted price (`Buy Ball (750 🪙)`).
- 🪄 **Collectible Cards & Wicked Spells (`MerchantItem`)** — Offer standard/special collectible cards or configurable **Wicked Spells** (e.g., *Double next caught card's stats* or *Apply a guaranteed special variant to the next catch*).
- 🔄 **Persistent Auto-Refreshing Embeds (`ActiveMerchant`)** — Background task automatically rotates expired stock every 5 minutes and edits active merchant messages in-place across all servers (or re-sends if the message was deleted).
- ⏱️ **Per-Player Purchase Cooldowns & Audit Log (`MerchantPurchase`)** — Full atomic transaction safety with balance checks, randomized stat bonuses, per-player cooldowns, and purchase history in Django Admin.

---

## 📦 Installation (`extra.toml`)

Add the package to your BallsDex `config/extra.toml`:

```toml
[[ballsdex.packages]]
location = "git+https://github.com/Haymooed/BallsDex-Merchant-Package.git"
path = "merchant"
enabled = true
editable = false
```

Then run database migrations and restart your BallsDex bot and admin panel containers.

---

## ⚙️ Django Admin Configuration

Everything is configurable live through the **Django Admin Panel** under the **Merchant** section:

1. **Merchants (`Merchant`)**
   - **Name**: Display title shown on the Discord embed (include `VIP` in the name to trigger the royal purple VIP theme).
   - **Enabled**: Toggle this merchant on or off.
   - **Sale percentage (`0–100%`)**: Global discount percentage applied to all items in this merchant's rotation.
   - **Items**: Optional curated subset of `MerchantItem` entries for this merchant (if left empty, uses all enabled items).
   - **Rotation minutes**: Duration of each shop rotation before new wares are rolled (default: `1440` = 24 hours).
   - **Items per rotation**: Number of offers sampled per rotation (default: `3`).
   - **Purchase cooldown seconds**: Minimum wait time between purchases per player (default: `900` = 15 minutes).

2. **Merchant Items (`MerchantItem`)**
   - **Display name & Description**: Optional custom override name and flavor text.
   - **Price & Weight**: Base price in your bot's currency and relative rarity weight (`Common`, `Uncommon`, `Rare`, `Mythic`).
   - **Item type**: Choose between a **Collectible card** (`ball` + optional `special`) or a **Wicked Spell** (`double_stats` / `special_variant`).

3. **Active Merchants, Rotations & Purchases**
   - View active Discord channels (`ActiveMerchant`), inspect current/past rotations (`MerchantRotation`), and audit player purchases (`MerchantPurchase`).

---

## 💬 Slash Commands

| Command | Parameters | Permissions | Description |
| :--- | :--- | :--- | :--- |
| `/merchant send` | `[merchant_id]` *(optional, with autocomplete)* | Administrator | Posts and tracks the live auto-updating Merchant shop embed in the current channel. |

---

## 📜 License

Licensed under the [MIT License](LICENSE) — Created by **Haymooed**.
