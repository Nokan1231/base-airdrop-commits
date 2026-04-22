# BTC 5m Arbitrage Patterns

## Core Arbitrage Condition

A Polymarket binary market has exactly two outcomes: Up (YES) and Down (NO).
Exactly one resolves to **$1.00 USDC**. Therefore:

```
Guaranteed payout per pair: $1.00

Entry cost = ask(Up) + ask(Down)
Gross edge = 1.00 - Entry cost
Net edge   = 1.00 - Entry cost - fees
```

**Trade is valid when:** `net_edge > MIN_EDGE (0.005)`

---

## When Opportunities Arise

### Pattern A — Stale quotes after BTC price move
When BTC moves sharply in the last 30 seconds of a 5m window, market makers
pull their quotes. The remaining resting orders create temporary imbalances
where both sides can be bought below $0.98 combined.

### Pattern B — Thin liquidity at market open
New 5m markets open with sparse order books. In the first ~30 seconds,
ask prices on both sides have not yet converged to efficient levels.
Combined cost is frequently $0.93–$0.97.

### Pattern C — Partial fill residue
A large directional trader fills one side partially and moves on, leaving
a stale resting order at an off-market price. The other side may still
be efficiently priced, creating an imbalance window of 5–15 seconds.

---

## Both-Sides Entry Filter

The `detect_pair_arb.py` scanner applies these checks before flagging an opportunity:

| Check | Threshold | Reason |
|---|---|---|
| `ask(Up) + ask(Down)` | < 0.98 (before fee) | Ensures gross margin exists |
| `net_cost` after 2% fee | < 0.995 | Final profitability gate |
| Depth (Up side) | ≥ 5 USDC | Avoids unfillable micro-liquidity |
| Depth (Down side) | ≥ 5 USDC | Same |
| `net_edge` | ≥ 0.005 | Minimum 0.5¢ per dollar (noise floor) |

---

## Position Sizing

```
max_size_shares = min(depth_up, depth_down) / max(ask_up, ask_down)
position_size   = min(POSITION_SIZE_USDC, max_size_shares * net_cost)
```

Never size larger than the thinner side of the book to avoid partial fills
that leave a naked directional exposure.

---

## Exit / Settlement

All positions are held to market expiry — no early exit needed.
One side resolves to $1.00; the other resolves to $0.00.
Net payout per share pair = $1.00 regardless of BTC direction.

Settlement detection in `paper_trade_btc_5m.py` / `live_executor_btc_5m.py`:
- On each cycle, compare open positions against `active_market_ids`
- Positions whose `market_id` is no longer active → settled at `expected_pnl`
