"""
Live execution engine for BTC 5m Up/Down arbitrage on Polymarket.

IMPORTANT: Only run this after paper_trade_btc_5m.py has validated the strategy
with positive expected P&L over at least one full trading session.

Required environment variables (copy .env.template → .env):
  POLY_PRIVATE_KEY      Ethereum private key (hex, no 0x prefix)
  POLY_API_KEY          Polymarket CLOB API key
  POLY_API_SECRET       Polymarket CLOB API secret
  POLY_API_PASSPHRASE   Polymarket CLOB API passphrase

Execution flow per cycle:
  1. fetch_and_normalize() — fresh order books from CLOB
  2. scan_all()            — rank arb opportunities by edge
  3. RiskManager.can_trade() — check daily loss / circuit breaker / max positions
  4. execute_arb()         — place both legs as GTC limit orders
  5. monitor & settle      — track fills, release position slot on expiry

Pipeline position: SIXTH (FINAL) — requires paper validation first.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from scripts.parse_activity import fetch_and_normalize
from scripts.detect_pair_arb import scan_all, ArbOpportunity

load_dotenv()
logger = logging.getLogger(__name__)

# --- Runtime parameters (override via .env) ---
POLL_INTERVAL_S = int(os.getenv("POLL_INTERVAL_S", "10"))
POSITION_SIZE_USDC = float(os.getenv("POSITION_SIZE_USDC", "5"))
MAX_DAILY_LOSS_USDC = float(os.getenv("MAX_DAILY_LOSS_USDC", "50"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
CIRCUIT_BREAKER_LOSSES = int(os.getenv("CIRCUIT_BREAKER_LOSSES", "5"))
STATE_PATH = Path("data/live_state.json")


# ---------------------------------------------------------------------------
# Risk manager
# ---------------------------------------------------------------------------

class RiskManager:
    """
    Gate-keeps all trade attempts.  All limits are cumulative within a day;
    call reset_day() at the start of each calendar day if running continuously.
    """

    def __init__(self):
        self.daily_loss = 0.0
        self.consecutive_losses = 0
        self.open_count = 0
        self.total_trades = 0

    def can_trade(self) -> tuple[bool, str]:
        if self.daily_loss <= -MAX_DAILY_LOSS_USDC:
            return False, f"Daily loss limit reached ({self.daily_loss:.2f} USDC)"
        if self.consecutive_losses >= CIRCUIT_BREAKER_LOSSES:
            return False, f"Circuit breaker: {self.consecutive_losses} consecutive losses"
        if self.open_count >= MAX_OPEN_POSITIONS:
            return False, f"Max open positions reached ({self.open_count}/{MAX_OPEN_POSITIONS})"
        return True, ""

    def on_trade_opened(self):
        self.open_count += 1
        self.total_trades += 1

    def on_trade_closed(self, pnl: float):
        self.open_count = max(0, self.open_count - 1)
        if pnl < 0:
            self.daily_loss += pnl
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

    def reset_day(self):
        self.daily_loss = 0.0
        self.consecutive_losses = 0
        logger.info("Risk manager: daily counters reset")


# ---------------------------------------------------------------------------
# CLOB client wrapper
# ---------------------------------------------------------------------------

class ClobClientWrapper:
    """
    Thin wrapper around py-clob-client that handles auth and order placement.
    Raises RuntimeError on init if credentials are missing or invalid.
    """

    CLOB_HOST = "https://clob.polymarket.com"

    def __init__(self):
        self._client = self._init()

    def _init(self):
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.constants import POLYGON
        except ImportError as e:
            raise RuntimeError(
                "py-clob-client not installed. Run: pip install py-clob-client"
            ) from e

        required = ["POLY_PRIVATE_KEY", "POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE"]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

        client = ClobClient(
            host=self.CLOB_HOST,
            chain_id=POLYGON,
            key=os.environ["POLY_PRIVATE_KEY"],
            creds={
                "apiKey": os.environ["POLY_API_KEY"],
                "secret": os.environ["POLY_API_SECRET"],
                "passphrase": os.environ["POLY_API_PASSPHRASE"],
            },
        )
        logger.info("CLOB client initialised (Polygon mainnet)")
        return client

    def place_limit_order(
        self,
        token_id: str,
        price: float,
        size: float,
    ) -> Optional[str]:
        """
        Places a GTC (Good-Till-Cancelled) BUY limit order.
        Returns order ID string on success, None on failure.
        """
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY

            args = OrderArgs(token_id=token_id, price=price, size=size, side=BUY)
            signed = self._client.create_order(args)
            resp = self._client.post_order(signed, OrderType.GTC)
            order_id = resp.get("orderID") or resp.get("id")
            logger.info(f"  Order placed | token={token_id[:16]}… price={price} size={size:.4f} id={order_id}")
            return order_id
        except Exception as e:
            logger.error(f"  Order failed: {e}")
            return None

    def cancel_order(self, order_id: str) -> bool:
        try:
            self._client.cancel(order_id)
            logger.info(f"  Cancelled order {order_id}")
            return True
        except Exception as e:
            logger.warning(f"  Cancel failed for {order_id}: {e}")
            return False


# ---------------------------------------------------------------------------
# Live executor
# ---------------------------------------------------------------------------

class LiveExecutor:
    def __init__(self):
        self.risk = RiskManager()
        self.clob = ClobClientWrapper()
        self.open_positions: list[dict] = []
        self.closed_trades: list[dict] = []
        self.cycle = 0
        self._load_state()

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    def execute_arb(self, opp: ArbOpportunity) -> bool:
        """
        Enters both legs of an arb trade simultaneously.
        Both legs use GTC limit orders at best_ask (no chasing).
        Returns True if both orders were accepted.
        """
        ok, reason = self.risk.can_trade()
        if not ok:
            logger.info(f"[RISK BLOCK] {reason}")
            return False

        size_per_side = POSITION_SIZE_USDC / opp.net_cost

        logger.info(
            f"[LIVE] Entering arb | edge={opp.edge:.4f} | "
            f"up={opp.up_ask:.4f} down={opp.down_ask:.4f} | "
            f"size={size_per_side:.4f} shares | {opp.question[:55]}"
        )

        up_order_id = self.clob.place_limit_order(opp.up_token_id, opp.up_ask, size_per_side)
        down_order_id = self.clob.place_limit_order(opp.down_token_id, opp.down_ask, size_per_side)

        if not up_order_id or not down_order_id:
            # One leg failed — cancel the other to avoid a naked position
            logger.error("[LIVE] Partial fill — cancelling surviving leg")
            if up_order_id:
                self.clob.cancel_order(up_order_id)
            if down_order_id:
                self.clob.cancel_order(down_order_id)
            return False

        self.risk.on_trade_opened()
        self.open_positions.append({
            "market_id": opp.market_id,
            "question": opp.question[:80],
            "end_date_iso": opp.end_date_iso,
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "up_order_id": up_order_id,
            "down_order_id": down_order_id,
            "edge": round(opp.edge, 6),
            "net_cost": round(opp.net_cost * size_per_side, 4),
            "expected_pnl": round(opp.edge * opp.net_cost * size_per_side, 4),
            "status": "OPEN",
        })
        self._save_state()
        return True

    def settle_expired(self, active_market_ids: set[str]):
        """Marks positions for dead markets as settled and updates risk stats."""
        still_open: list[dict] = []
        for pos in self.open_positions:
            if pos["market_id"] not in active_market_ids:
                pos["status"] = "SETTLED"
                pos["settled_time"] = datetime.now(timezone.utc).isoformat()
                pos["pnl"] = pos["expected_pnl"]
                self.risk.on_trade_closed(pos["pnl"])
                self.closed_trades.append(pos)
                logger.info(
                    f"[LIVE] SETTLED | pnl={pos['pnl']:+.4f} USDC | {pos['question'][:55]}"
                )
            else:
                still_open.append(pos)
        self.open_positions = still_open

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self):
        """Polling loop. Exits cleanly on Ctrl-C."""
        logger.info(
            f"Live executor started | "
            f"size={POSITION_SIZE_USDC} USDC | "
            f"max_loss={MAX_DAILY_LOSS_USDC} USDC/day | "
            f"max_positions={MAX_OPEN_POSITIONS}"
        )

        while True:
            self.cycle += 1
            logger.info(
                f"--- Cycle {self.cycle} | "
                f"open={len(self.open_positions)} | "
                f"daily_loss={self.risk.daily_loss:.2f} ---"
            )

            try:
                pairs = fetch_and_normalize(output_path="data/activity_live.json")
                active_ids = {p["market_id"] for p in pairs}

                self.settle_expired(active_ids)

                ok, _ = self.risk.can_trade()
                if ok:
                    opps = scan_all(pairs)
                    logger.info(f"Opportunities: {len(opps)}")
                    for opp in opps:
                        self.execute_arb(opp)
                else:
                    logger.warning(f"Trading paused: {self.risk.can_trade()[1]}")

            except KeyboardInterrupt:
                logger.info("Graceful shutdown requested")
                break
            except Exception as e:
                logger.error(f"Cycle error: {e}", exc_info=True)

            self._save_state()
            time.sleep(POLL_INTERVAL_S)

        logger.info(
            f"Executor stopped | "
            f"total_trades={self.risk.total_trades} | "
            f"closed={len(self.closed_trades)}"
        )

    # ------------------------------------------------------------------
    # State persistence (survives restarts)
    # ------------------------------------------------------------------

    def _save_state(self):
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_PATH, "w") as f:
            json.dump(
                {
                    "cycle": self.cycle,
                    "open_positions": self.open_positions,
                    "closed_trades": self.closed_trades,
                    "risk": {
                        "daily_loss": self.risk.daily_loss,
                        "consecutive_losses": self.risk.consecutive_losses,
                        "open_count": self.risk.open_count,
                        "total_trades": self.risk.total_trades,
                    },
                },
                f,
                indent=2,
            )

    def _load_state(self):
        if STATE_PATH.exists():
            try:
                with open(STATE_PATH) as f:
                    state = json.load(f)
                self.open_positions = state.get("open_positions", [])
                self.closed_trades = state.get("closed_trades", [])
                risk_state = state.get("risk", {})
                self.risk.daily_loss = risk_state.get("daily_loss", 0.0)
                self.risk.consecutive_losses = risk_state.get("consecutive_losses", 0)
                self.risk.open_count = risk_state.get("open_count", 0)
                self.risk.total_trades = risk_state.get("total_trades", 0)
                logger.info(f"Resumed from state: {len(self.open_positions)} open positions")
            except Exception as e:
                logger.warning(f"Could not load previous state: {e}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    executor = LiveExecutor()
    executor.run()
