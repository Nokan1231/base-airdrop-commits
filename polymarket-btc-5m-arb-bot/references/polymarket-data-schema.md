# Polymarket Data Schema Reference

## Gamma API — Market Object

`GET https://gamma-api.polymarket.com/markets`

```json
{
  "conditionId": "0xabc123...",
  "question": "Will BTC be above $65000 at 3:05pm ET?",
  "description": "...",
  "active": true,
  "closed": false,
  "endDateIso": "2024-01-15T20:05:00Z",
  "tokens": [
    {
      "token_id": "123456789",
      "outcome": "YES",
      "price": 0.47
    },
    {
      "token_id": "987654321",
      "outcome": "NO",
      "price": 0.55
    }
  ],
  "volume": 12450.50,
  "liquidity": 3200.00
}
```

Key fields used by the bot:
- `conditionId` → `market_id` (primary key)
- `tokens[].token_id` → used for orderbook and order placement
- `tokens[].outcome` → identifies Up (YES) and Down (NO) sides
- `endDateIso` → used to detect market expiry during settlement
- `active` + `closed` → both must be true/false to filter live markets

---

## CLOB API — Order Book

`GET https://clob.polymarket.com/book?token_id=<token_id>`

```json
{
  "market": "0xabc123...",
  "asset_id": "123456789",
  "bids": [
    { "price": "0.45", "size": "120.50" },
    { "price": "0.44", "size": "85.00" }
  ],
  "asks": [
    { "price": "0.47", "size": "200.00" },
    { "price": "0.48", "size": "150.00" }
  ],
  "hash": "..."
}
```

Bot logic: `best_ask = min(asks, key=price)`; `depth_usdc = price * size`

---

## CLOB API — Trades

`GET https://clob.polymarket.com/trades?market=<conditionId>&limit=200`

```json
[
  {
    "transactionHash": "0xdef456...",
    "timestamp": "1705350000",
    "price": "0.47",
    "size": "50.00",
    "side": "BUY",
    "makerAddress": "0x111...",
    "takerAddress": "0x222...",
    "asset_id": "123456789"
  }
]
```

Bot logic: filtered by `asset_id` to separate Up/Down fills; used by `cluster_fills.py`

---

## Normalized Pair Dict (internal format)

Output of `parse_activity.py`, consumed by all downstream stages:

```json
{
  "market_id": "0xabc123...",
  "question": "Will BTC be above $65000 at 3:05pm ET?",
  "end_date_iso": "2024-01-15T20:05:00Z",
  "up_token_id": "123456789",
  "up_outcome": "YES",
  "down_token_id": "987654321",
  "down_outcome": "NO",
  "fetched_at": "2024-01-15T20:04:30Z",
  "up_orderbook": { "bids": [...], "asks": [...] },
  "down_orderbook": { "bids": [...], "asks": [...] },
  "recent_trades": [...]
}
```

---

## Key URL Slugs

| Resource | URL |
|---|---|
| Active markets | `https://gamma-api.polymarket.com/markets?tag_slug=crypto&closed=false` |
| Order book | `https://clob.polymarket.com/book?token_id=<id>` |
| Recent trades | `https://clob.polymarket.com/trades?market=<conditionId>` |
| Place order (POST) | `https://clob.polymarket.com/order` |
| Cancel order (DELETE) | `https://clob.polymarket.com/order/<orderId>` |
