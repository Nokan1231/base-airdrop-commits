"""
Detects Up/Down arbitrage opportunities in BTC 5m Polymarket markets.

Core insight: each BTC 5m market has exactly two outcomes (Up / Down).
One of them MUST resolve to $1.00. Therefore if you can buy both sides
for less than $1.00 (net of fees), you have a risk-free profit.

Opportunity condition:
  best_ask(Up) + best_ask(Down) + fees < 1.00

Pipeline position: THIRD — consumes activity.json, produces opportunities.json.
"""
import json
import logging
import os
from dataclasses import asdict, dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# --- Strategy parameters (overridable via config.yaml) ---
FEE_RATE = 0.02        # Polymarket taker fee per side: 2 %
MIN_EDGE = 0.005       # Minimum net profit per dollar risked (0.5 ¢)
MIN_LIQUIDITY = 5.0    # Both sides must have at least this many USDC available


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ArbOpportunity:
    market_id: str
    question: str
    end_date_iso: str
    up_token_id: str
    down_token_id: str
    up_ask: float          # best ask price for Up side
    down_ask: float        # best ask price for Down side
    up_liquidity: float    # USDC depth at best ask (Up)
    down_liquidity: float  # USDC depth at best ask (Down)
    gross_cost: float      # up_ask + down_ask
    fee: float             # total fee paid on both legs
    net_cost: float        # gross_cost + fee
    edge: float            # 1.0 - net_cost  (profit per share pair)
    max_size: float        # maximum shares limited by available liquidity

    def to_dict(self) -> dict:
        d = asdict(self)
        for k in ("gross_cost", "fee", "net_cost", "edge", "up_ask",
                  "down_ask", "up_liquidity", "down_liquidity", "max_size"):
            d[k] = round(d[k], 6)
        return d


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _best_ask(orderbook: dict) -> tuple[float, float]:
    """
    Returns (best_ask_price, usdc_depth_at_best_ask).
    Falls back to (1.0, 0.0) when the book is empty.
    """
    asks = orderbook.get("asks", [])
    if not asks:
        return 1.0, 0.0
    best = min(asks, key=lambda x: float(x.get("price", 1.0)))
    price = float(best.get("price", 1.0))
    size = float(best.get("size", 0.0))
    return price, price * size   # depth in USDC


def _calc_fee(up_ask: float, down_ask: float, fee_rate: float = FEE_RATE) -> float:
    """Fee is charged on notional value of each leg separately."""
    return (up_ask * fee_rate) + (down_ask * fee_rate)


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

def detect_arb(
    pair: dict,
    fee_rate: float = FEE_RATE,
    min_edge: float = MIN_EDGE,
    min_liquidity: float = MIN_LIQUIDITY,
) -> Optional[ArbOpportunity]:
    """
    Checks a single market pair for an arbitrage opportunity.
    Returns an ArbOpportunity when edge > min_edge, else None.
    """
    up_book = pair.get("up_orderbook", {})
    down_book = pair.get("down_orderbook", {})

    up_ask, up_liq = _best_ask(up_book)
    down_ask, down_liq = _best_ask(down_book)

    # Reject if either side lacks meaningful liquidity
    if up_liq < min_liquidity or down_liq < min_liquidity:
        logger.debug(f"Low liquidity skip: {pair.get('market_id', '')[:16]} up={up_liq:.2f} down={down_liq:.2f}")
        return None

    gross_cost = up_ask + down_ask
    fee = _calc_fee(up_ask, down_ask, fee_rate)
    net_cost = gross_cost + fee
    edge = 1.0 - net_cost

    if edge < min_edge:
        return None

    # Maximum size is constrained by the thinner side of the book
    max_size = min(up_liq, down_liq) / max(up_ask, down_ask)

    return ArbOpportunity(
        market_id=pair["market_id"],
        question=pair.get("question", ""),
        end_date_iso=pair.get("end_date_iso", ""),
        up_token_id=pair["up_token_id"],
        down_token_id=pair["down_token_id"],
        up_ask=up_ask,
        down_ask=down_ask,
        up_liquidity=up_liq,
        down_liquidity=down_liq,
        gross_cost=gross_cost,
        fee=fee,
        net_cost=net_cost,
        edge=edge,
        max_size=max_size,
    )


def scan_all(
    pairs: list[dict],
    fee_rate: float = FEE_RATE,
    min_edge: float = MIN_EDGE,
    min_liquidity: float = MIN_LIQUIDITY,
) -> list[ArbOpportunity]:
    """
    Scans every market pair and returns all opportunities sorted best-edge first.
    Called by paper_trade_btc_5m.py and live_executor_btc_5m.py each cycle.
    """
    opportunities: list[ArbOpportunity] = []
    for pair in pairs:
        opp = detect_arb(pair, fee_rate, min_edge, min_liquidity)
        if opp:
            opportunities.append(opp)
            logger.info(
                f"ARB | {opp.question[:55]:<55} | "
                f"edge={opp.edge:.4f}  up={opp.up_ask:.4f}  down={opp.down_ask:.4f}  "
                f"max_size={opp.max_size:.2f}"
            )

    opportunities.sort(key=lambda o: o.edge, reverse=True)
    return opportunities


def run(
    input_path: str = "data/activity.json",
    output_path: str = "data/opportunities.json",
) -> list[ArbOpportunity]:
    with open(input_path) as f:
        pairs = json.load(f)

    opps = scan_all(pairs)

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump([o.to_dict() for o in opps], f, indent=2)

    logger.info(f"Found {len(opps)} opportunities → {output_path}")
    return opps


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    opps = run()
    if opps:
        print(f"\nTop opportunity:")
        print(json.dumps(opps[0].to_dict(), indent=2))
    else:
        print("No arbitrage opportunities found in current snapshot.")
