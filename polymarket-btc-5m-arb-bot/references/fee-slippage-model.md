# Fee & Slippage Model

## Polymarket Taker Fee

As of 2024, Polymarket charges a **2% taker fee** on the notional value of each leg.

```
fee = (ask_up * size_up * 0.02) + (ask_down * size_down * 0.02)
    = (ask_up + ask_down) * size * 0.02     # when size_up == size_down
```

The fee is deducted from payout at settlement, not at entry.
The bot models it as an upfront cost to be conservative.

---

## Slippage Model

In backtesting (`backtest_btc_5m.py`), both legs are assumed to fill at:

```
fill_price = best_ask + SLIPPAGE   # SLIPPAGE = 0.005 (0.5 cents)
```

This is conservative for a 5m market with ≥$5 depth at best ask.
In practice, GTC limit orders at `best_ask` often fill at exactly that price.

### Slippage sources on Polymarket:
1. **Queue position** — another bot fills ahead of you at the best ask
2. **Market impact** — your order exhausts the best level, next fill is at ask+1
3. **Latency** — order book moves between your fetch and order submission (~100ms)

---

## Break-Even Analysis

For a $10 USDC position:

| Scenario | Up ask | Down ask | Gross | Fee (2%) | Net cost | Edge |
|---|---|---|---|---|---|---|
| Strong opportunity | 0.46 | 0.50 | 0.96 | 0.0192 | 0.9792 | 0.0208 |
| Marginal (model threshold) | 0.48 | 0.495 | 0.975 | 0.0195 | 0.9945 | 0.0055 |
| Break-even | 0.49 | 0.49 | 0.98 | 0.0196 | 0.9996 | 0.0004 |
| Not profitable | 0.50 | 0.50 | 1.00 | 0.0200 | 1.0200 | -0.0200 |

---

## Incomplete Fill Risk

If only one leg fills (network error, order rejected), the bot holds a naked
directional position. Mitigation in `live_executor_btc_5m.py`:

1. Place both orders within the same cycle iteration
2. If either `place_limit_order()` returns `None`, immediately cancel the other
3. GTC orders that don't fill within the market window expire worthlessly —
   track via `end_date_iso` and cancel before expiry if unfilled

---

## Lag Risk

If the CLOB fetch-to-order latency exceeds ~2 seconds, the arbitrage window
may close. Monitor by logging `time.time() - fetch_start` each cycle.
If consistently > 1.5s, reduce `POLL_INTERVAL_S` and consider running the
bot on a server close to Polymarket's infrastructure (US East Coast).
