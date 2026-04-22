# Risk Rules

All risk parameters are configurable in `config.yaml` and overridable via `.env`.

---

## Position-Level Rules

| Rule | Default | Enforced in |
|---|---|---|
| Max position size | $100 USDC | `backtest_btc_5m.py`, `live_executor_btc_5m.py` |
| Never exceed book depth | min(depth_up, depth_down) | `detect_pair_arb.py` |
| Cancel orphaned leg on partial fill | immediate | `live_executor_btc_5m.py` |

---

## Portfolio-Level Rules

| Rule | Default | Enforced in |
|---|---|---|
| Max open positions | 3 | `RiskManager.can_trade()` |
| Max daily loss | $50 USDC | `RiskManager.daily_loss` |
| Circuit breaker (consecutive losses) | 5 trades | `RiskManager.consecutive_losses` |

---

## Circuit Breaker Logic

```python
if consecutive_losses >= CIRCUIT_BREAKER_LOSSES:
    # Stop opening new positions
    # Existing open positions are still held to expiry (arb guarantee)
    # Manual review required before resuming
```

The circuit breaker fires when the strategy has been wrong 5 times in a row.
This should be rare for a pure arbitrage strategy — if it fires, it usually
signals a bug (e.g. fees changed, market structure changed) rather than
normal variance.

---

## Market Imbalance Guard

If `ask(Up) > 0.55` AND `ask(Down) > 0.55`:
- Combined cost already > $1.10 — no arb possible
- This condition filters out markets where a resolution outcome is already
  strongly priced in (e.g. BTC moved dramatically, one side is near $0.95)

---

## Daily Loss Accounting

```
daily_loss tracks: sum of negative PnL realised today
                   (unrealised open positions NOT counted)
```

Reset manually or schedule `risk.reset_day()` at midnight UTC.

---

## Pre-Live Checklist

Before switching from `paper` to `live`:

- [ ] Paper trader ran for ≥ 1 full trading session (≥ 3 hours)
- [ ] Paper P&L is positive after fees
- [ ] `data/paper_trades.json` shows ≥ 10 settled trades
- [ ] Backtest `expectancy_per_dollar` > 0.003
- [ ] `.env` credentials tested with a $1 manual order on Polymarket UI
- [ ] `POSITION_SIZE_USDC` set to minimum ($5) for first live session
- [ ] `MAX_DAILY_LOSS_USDC` set to 2× `POSITION_SIZE_USDC` for first live session
