"""
Backtests the BTC 5m Up/Down arbitrage strategy on historical opportunity snapshots.

Simulation assumptions:
  - Both legs fill at best_ask + SLIPPAGE (conservative)
  - One leg always resolves to $1.00 at expiry (guaranteed by market design)
  - Daily loss limit and circuit breaker are enforced

Usage:
  python -m scripts.backtest_btc_5m
  python -m scripts.backtest_btc_5m --input data/opportunities.json --size 25

Pipeline position: FOURTH — consumes opportunities.json, produces backtest_report.json.
"""
import argparse
import json
import logging
import os
import statistics
from dataclasses import asdict, dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# --- Simulation parameters ---
FEE_RATE = 0.02
SLIPPAGE = 0.005        # per-side price slippage on fill
MIN_EDGE_AFTER_SLIP = 0.003
MAX_POSITION_USDC = 100.0
MAX_DAILY_LOSS_USDC = 50.0
CIRCUIT_BREAKER_LOSSES = 5


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BacktestTrade:
    market_id: str
    question: str
    entry_up: float       # fill price Up side
    entry_down: float     # fill price Down side
    shares: float         # number of share pairs bought
    gross_cost: float     # (entry_up + entry_down) * shares
    fee_paid: float
    net_cost: float       # gross_cost + fee_paid
    payout: float         # always 1.0 * shares (one side resolves $1)
    pnl: float            # payout - net_cost
    edge_at_entry: float  # pnl / net_cost


@dataclass
class BacktestResult:
    trades: list[BacktestTrade] = field(default_factory=list)

    @property
    def total_pnl(self) -> float:
        return sum(t.pnl for t in self.trades)

    @property
    def total_notional(self) -> float:
        return sum(t.net_cost for t in self.trades)

    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        return sum(1 for t in self.trades if t.pnl > 0) / len(self.trades)

    @property
    def expectancy_per_dollar(self) -> float:
        if self.total_notional == 0:
            return 0.0
        return self.total_pnl / self.total_notional

    @property
    def avg_edge(self) -> float:
        if not self.trades:
            return 0.0
        return statistics.mean(t.edge_at_entry for t in self.trades)

    @property
    def sharpe_approx(self) -> float:
        if len(self.trades) < 2:
            return 0.0
        pnls = [t.pnl for t in self.trades]
        mean = statistics.mean(pnls)
        stdev = statistics.stdev(pnls)
        return mean / stdev if stdev > 0 else 0.0

    def summary(self) -> dict:
        return {
            "num_trades": len(self.trades),
            "total_pnl_usdc": round(self.total_pnl, 4),
            "total_notional_usdc": round(self.total_notional, 2),
            "win_rate": round(self.win_rate, 4),
            "avg_edge": round(self.avg_edge, 6),
            "expectancy_per_dollar": round(self.expectancy_per_dollar, 6),
            "sharpe_approx": round(self.sharpe_approx, 4),
        }


# ---------------------------------------------------------------------------
# Simulation helpers
# ---------------------------------------------------------------------------

def _simulate_trade(opp: dict, size_usdc: float) -> Optional[BacktestTrade]:
    """
    Simulates entering one arbitrage opportunity.
    Applies slippage to both legs before calculating edge.
    Returns None if the trade is no longer profitable after slippage.
    """
    up_fill = opp["up_ask"] + SLIPPAGE
    down_fill = opp["down_ask"] + SLIPPAGE

    gross_per_share = up_fill + down_fill
    if gross_per_share >= 1.0:
        return None

    shares = min(size_usdc, MAX_POSITION_USDC) / gross_per_share
    gross_cost = gross_per_share * shares
    fee_paid = (up_fill + down_fill) * FEE_RATE * shares
    net_cost = gross_cost + fee_paid
    payout = 1.0 * shares
    pnl = payout - net_cost
    edge = pnl / net_cost if net_cost > 0 else 0.0

    if edge < MIN_EDGE_AFTER_SLIP:
        return None

    return BacktestTrade(
        market_id=opp["market_id"],
        question=opp.get("question", "")[:80],
        entry_up=round(up_fill, 6),
        entry_down=round(down_fill, 6),
        shares=round(shares, 6),
        gross_cost=round(gross_cost, 6),
        fee_paid=round(fee_paid, 6),
        net_cost=round(net_cost, 6),
        payout=round(payout, 6),
        pnl=round(pnl, 6),
        edge_at_entry=round(edge, 6),
    )


def run_backtest(
    opportunities: list[dict],
    size_usdc: float = 10.0,
) -> BacktestResult:
    """
    Iterates over historical opportunity snapshots and simulates each trade.
    Enforces daily loss limit and circuit breaker.
    """
    result = BacktestResult()
    daily_loss = 0.0
    consecutive_losses = 0

    for opp in opportunities:
        # Risk guards
        if daily_loss <= -MAX_DAILY_LOSS_USDC:
            logger.warning("Daily loss limit hit — skipping remaining opportunities")
            break
        if consecutive_losses >= CIRCUIT_BREAKER_LOSSES:
            logger.warning(f"Circuit breaker: {consecutive_losses} consecutive losses")
            break

        position_size = min(size_usdc, opp.get("max_size", size_usdc) * opp.get("net_cost", 1.0))
        trade = _simulate_trade(opp, position_size)

        if not trade:
            continue

        result.trades.append(trade)

        if trade.pnl < 0:
            daily_loss += trade.pnl
            consecutive_losses += 1
        else:
            consecutive_losses = 0

        logger.debug(
            f"  Trade: pnl={trade.pnl:+.4f}  edge={trade.edge_at_entry:.4f}  "
            f"{trade.question[:50]}"
        )

    return result


def run(
    input_path: str = "data/opportunities.json",
    output_path: str = "data/backtest_report.json",
    size_usdc: float = 10.0,
) -> BacktestResult:
    with open(input_path) as f:
        opps = json.load(f)

    logger.info(f"Running backtest on {len(opps)} opportunities | size={size_usdc} USDC/trade")
    result = run_backtest(opps, size_usdc)
    summary = result.summary()

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(
            {
                "summary": summary,
                "parameters": {
                    "fee_rate": FEE_RATE,
                    "slippage": SLIPPAGE,
                    "size_usdc": size_usdc,
                    "max_position_usdc": MAX_POSITION_USDC,
                    "max_daily_loss_usdc": MAX_DAILY_LOSS_USDC,
                },
                "trades": [asdict(t) for t in result.trades],
            },
            f,
            indent=2,
        )

    logger.info(f"Backtest complete → {output_path}")
    logger.info(json.dumps(summary, indent=2))
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="BTC 5m Arb Backtest")
    parser.add_argument("--input", default="data/opportunities.json")
    parser.add_argument("--output", default="data/backtest_report.json")
    parser.add_argument("--size", type=float, default=10.0, help="USDC per trade")
    args = parser.parse_args()

    result = run(args.input, args.output, args.size)
    print(json.dumps(result.summary(), indent=2))
