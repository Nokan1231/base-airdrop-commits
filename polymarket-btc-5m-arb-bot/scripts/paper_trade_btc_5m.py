"""
Paper-trading mode: runs the full arb pipeline in real-time but NEVER submits
real orders.  Tracks a virtual portfolio to validate strategy before going live.

Lifecycle per cycle:
  1. fetch_and_normalize()   → fresh order books
  2. scan_all()              → find arb opportunities
  3. on_opportunity()        → simulate entry into virtual position
  4. settle_expired()        → close positions for markets that have ended
  5. save()                  → persist virtual P&L to disk

Run this for at least a full trading session before using live_executor_btc_5m.py.

Pipeline position: FIFTH — prerequisite before live execution.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from scripts.parse_activity import fetch_and_normalize
from scripts.detect_pair_arb import scan_all, ArbOpportunity
from scripts.backtest_btc_5m import _simulate_trade

logger = logging.getLogger(__name__)

POLL_INTERVAL_S = 15
VIRTUAL_BALANCE_USDC = 1000.0
POSITION_SIZE_USDC = 10.0
LOG_PATH = Path("data/paper_trades.json")


class PaperTrader:
    """
    Maintains a virtual portfolio of open and closed arbitrage positions.
    Thread-safe for single-threaded polling loop.
    """

    def __init__(self, balance: float = VIRTUAL_BALANCE_USDC):
        self.balance = balance
        self.initial_balance = balance
        self.open_positions: list[dict] = []
        self.closed_trades: list[dict] = []
        self.cycle = 0
        self.opportunities_seen = 0

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    @property
    def total_pnl(self) -> float:
        realised = sum(t.get("pnl", 0.0) for t in self.closed_trades)
        return (self.balance - self.initial_balance) + realised

    @property
    def open_exposure(self) -> float:
        return sum(p["net_cost"] for p in self.open_positions)

    # ------------------------------------------------------------------
    # Position management
    # ------------------------------------------------------------------

    def on_opportunity(self, opp: ArbOpportunity) -> bool:
        """Simulates entering one arb position. Returns True if entered."""
        if self.balance < POSITION_SIZE_USDC:
            logger.warning("[PAPER] Insufficient virtual balance, skipping")
            return False

        position_size = min(
            POSITION_SIZE_USDC,
            opp.max_size * opp.net_cost,
        )
        trade = _simulate_trade(opp.to_dict(), position_size)
        if not trade:
            return False

        self.balance -= trade.net_cost
        self.open_positions.append({
            "market_id": opp.market_id,
            "question": opp.question[:80],
            "end_date_iso": opp.end_date_iso,
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "net_cost": round(trade.net_cost, 4),
            "expected_pnl": round(trade.pnl, 4),
            "edge": round(trade.edge_at_entry, 6),
            "shares": round(trade.shares, 6),
            "status": "OPEN",
        })
        self.opportunities_seen += 1

        logger.info(
            f"[PAPER] ENTER | edge={trade.edge_at_entry:.4f} | "
            f"cost={trade.net_cost:.2f} USDC | pnl_exp={trade.pnl:.4f} | "
            f"{opp.question[:55]}"
        )
        return True

    def settle_expired(self, active_market_ids: set[str]) -> int:
        """
        Settles positions for markets that are no longer active.
        Assumes expected_pnl is realised (arb guarantees payout).
        Returns number of positions settled.
        """
        still_open: list[dict] = []
        settled = 0
        for pos in self.open_positions:
            if pos["market_id"] not in active_market_ids:
                pos["status"] = "SETTLED"
                pos["settled_time"] = datetime.now(timezone.utc).isoformat()
                pos["pnl"] = pos["expected_pnl"]
                # Return capital + profit
                self.balance += pos["net_cost"] + pos["expected_pnl"]
                self.closed_trades.append(pos)
                settled += 1
                logger.info(
                    f"[PAPER] SETTLE | pnl={pos['expected_pnl']:+.4f} USDC | "
                    f"{pos['question'][:55]}"
                )
            else:
                still_open.append(pos)
        self.open_positions = still_open
        return settled

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self):
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_PATH, "w") as f:
            json.dump(
                {
                    "balance_usdc": round(self.balance, 4),
                    "initial_balance_usdc": self.initial_balance,
                    "total_pnl_usdc": round(self.total_pnl, 4),
                    "open_exposure_usdc": round(self.open_exposure, 4),
                    "cycles": self.cycle,
                    "opportunities_seen": self.opportunities_seen,
                    "open_positions": self.open_positions,
                    "closed_trades": self.closed_trades,
                },
                f,
                indent=2,
            )

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    def run_cycle(self):
        self.cycle += 1
        logger.info(
            f"--- Cycle {self.cycle} | Balance: {self.balance:.2f} USDC | "
            f"Open: {len(self.open_positions)} | PnL: {self.total_pnl:+.4f} ---"
        )

        try:
            pairs = fetch_and_normalize(output_path="data/activity.json")
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            return

        active_ids = {p["market_id"] for p in pairs}
        settled = self.settle_expired(active_ids)
        if settled:
            logger.info(f"Settled {settled} expired positions")

        opps = scan_all(pairs)
        logger.info(f"Opportunities this cycle: {len(opps)}")

        for opp in opps:
            self.on_opportunity(opp)

        self.save()


def run(duration_minutes: int = 60):
    """
    Runs the paper trader for the specified number of minutes.
    Ctrl-C exits gracefully.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    trader = PaperTrader(balance=VIRTUAL_BALANCE_USDC)
    logger.info(
        f"Paper trading started | "
        f"duration={duration_minutes}m | balance={VIRTUAL_BALANCE_USDC} USDC"
    )

    end_time = time.time() + duration_minutes * 60
    try:
        while time.time() < end_time:
            trader.run_cycle()
            time.sleep(POLL_INTERVAL_S)
    except KeyboardInterrupt:
        logger.info("Paper trading stopped by user")

    logger.info(
        f"Paper trading ended | "
        f"pnl={trader.total_pnl:+.4f} USDC | cycles={trader.cycle} | "
        f"trades={len(trader.closed_trades)}"
    )
    trader.save()
    return trader


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="BTC 5m Arb Paper Trader")
    parser.add_argument("--duration", type=int, default=60, help="Minutes to run")
    args = parser.parse_args()

    run(duration_minutes=args.duration)
