"""
Detects Up/Down arbitrage opportunities in BTC 5m Polymarket markets.

Core insight: each BTC 5m market has exactly two outcomes (Up / Down).
One of them MUST resolve to $1.00. Therefore if you can buy both sides
for less than $1.00 (net of fees), you have a risk-free profit.

Opportunity condition:
  WAP(Up, target_size) + WAP(Down, target_size) + fees < 1.00

WAP = Weighted Average Price computed by walking down the orderbook
      until target_size USDC is filled. This is more accurate than
      reading only the best ask, which understates true fill cost when
      the position size exceeds the depth of a single level.

Pipeline position: THIRD — consumes activity.json, produces opportunities.json.
"""
import json
import logging
import os
from dataclasses import asdict, dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# --- Strategy parameters (overridable via config.yaml) ---
FEE_RATE      = 0.02    # Polymarket taker fee per side: 2%
MIN_EDGE      = 0.005   # Minimum net profit per dollar risked (0.5¢)
MIN_LIQUIDITY = 5.0     # Both sides must have this many USDC available


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BookFill:
    """Result of a simulated walk-down fill for one side."""
    wap: float           # weighted average fill price
    filled_usdc: float   # total USDC spent to acquire shares
    filled_shares: float # total shares acquired
    levels_touched: int  # how many price levels were consumed
    fully_filled: bool   # True if target_usdc was fully satisfied


@dataclass
class ArbOpportunity:
    market_id: str
    question: str
    end_date_iso: str
    up_token_id: str
    down_token_id: str
    up_fill: BookFill       # simulated fill for Up side
    down_fill: BookFill     # simulated fill for Down side
    gross_cost: float       # up_wap + down_wap (per share pair)
    fee: float              # fee on both legs
    net_cost: float         # gross_cost + fee
    edge: float             # 1.0 - net_cost
    max_size_usdc: float    # max position constrained by available liquidity

    def to_dict(self) -> dict:
        d = {
            "market_id":    self.market_id,
            "question":     self.question,
            "end_date_iso": self.end_date_iso,
            "up_token_id":  self.up_token_id,
            "down_token_id": self.down_token_id,
            "up_wap":       round(self.up_fill.wap, 6),
            "down_wap":     round(self.down_fill.wap, 6),
            "up_liquidity_usdc":   round(self.up_fill.filled_usdc, 4),
            "down_liquidity_usdc": round(self.down_fill.filled_usdc, 4),
            "up_levels_touched":   self.up_fill.levels_touched,
            "down_levels_touched": self.down_fill.levels_touched,
            "gross_cost":   round(self.gross_cost, 6),
            "fee":          round(self.fee, 6),
            "net_cost":     round(self.net_cost, 6),
            "edge":         round(self.edge, 6),
            "max_size_usdc": round(self.max_size_usdc, 4),
        }
        return d


# ---------------------------------------------------------------------------
# Core: WAP walk-down
# ---------------------------------------------------------------------------

def _wap_fill(asks: list[dict], target_usdc: float) -> BookFill:
    """
    Simulates filling `target_usdc` worth of shares by walking down
    the ask side of the orderbook from best to worst price.

    Returns a BookFill with the weighted average price (WAP) across
    all levels consumed, and the actual USDC filled (may be less than
    target_usdc if the book is too thin).

    Example:
      asks = [{price: 0.46, size: 100}, {price: 0.47, size: 200}]
      target_usdc = 80  →  fills 80/0.46 = 173.9 shares at 0.46
      wap = 0.46  (single level, didn't need to go deeper)

      target_usdc = 60  →  fills all of level 1 (0.46 * 100 = $46)
                            then $14 more at 0.47
      wap = (46 + 14) / (100 + 14/0.47) = 60 / 129.8 ≈ 0.4622
    """
    # Sort ascending by price (best ask first)
    sorted_asks = sorted(asks, key=lambda x: float(x.get("price", 1.0)))

    remaining_usdc  = target_usdc
    total_cost      = 0.0
    total_shares    = 0.0
    levels_touched  = 0

    for level in sorted_asks:
        if remaining_usdc <= 0:
            break

        price     = float(level.get("price", 1.0))
        size      = float(level.get("size",  0.0))
        level_usdc = price * size          # USDC depth at this level

        fill_usdc   = min(remaining_usdc, level_usdc)
        fill_shares = fill_usdc / price

        total_cost    += fill_usdc
        total_shares  += fill_shares
        remaining_usdc -= fill_usdc
        levels_touched += 1

    if total_shares == 0:
        return BookFill(wap=1.0, filled_usdc=0.0, filled_shares=0.0,
                        levels_touched=0, fully_filled=False)

    wap = total_cost / total_shares

    return BookFill(
        wap=wap,
        filled_usdc=total_cost,
        filled_shares=total_shares,
        levels_touched=levels_touched,
        fully_filled=(remaining_usdc <= 0),
    )


def _total_book_depth_usdc(asks: list[dict]) -> float:
    """Sum of USDC depth across all ask levels."""
    return sum(float(a.get("price", 0)) * float(a.get("size", 0)) for a in asks)


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

def detect_arb(
    pair: dict,
    target_usdc: float = 10.0,
    fee_rate: float = FEE_RATE,
    min_edge: float = MIN_EDGE,
    min_liquidity: float = MIN_LIQUIDITY,
) -> Optional[ArbOpportunity]:
    """
    Checks a single market pair for an arbitrage opportunity using WAP fill.

    target_usdc: the position size to simulate (uses WAP for this exact size).
                 Set to your intended POSITION_SIZE_USDC from config.yaml.

    Returns ArbOpportunity when net_edge > min_edge, else None.
    """
    up_asks   = pair.get("up_orderbook",   {}).get("asks", [])
    down_asks = pair.get("down_orderbook", {}).get("asks", [])

    # Quick depth check before expensive walk-down
    up_depth   = _total_book_depth_usdc(up_asks)
    down_depth = _total_book_depth_usdc(down_asks)

    if up_depth < min_liquidity or down_depth < min_liquidity:
        logger.debug(
            f"Low depth skip: {pair.get('market_id', '')[:16]} "
            f"up={up_depth:.2f} down={down_depth:.2f}"
        )
        return None

    # Walk down both sides for target_usdc
    up_fill   = _wap_fill(up_asks,   target_usdc)
    down_fill = _wap_fill(down_asks, target_usdc)

    # Reject if even the full book can't satisfy our minimum size
    if up_fill.filled_usdc < min_liquidity or down_fill.filled_usdc < min_liquidity:
        return None

    gross_cost = up_fill.wap + down_fill.wap
    fee        = (up_fill.wap + down_fill.wap) * fee_rate
    net_cost   = gross_cost + fee
    edge       = 1.0 - net_cost

    if edge < min_edge:
        return None

    # Max position: constrained by whichever side has less total depth
    max_size_usdc = min(up_depth, down_depth)

    opp = ArbOpportunity(
        market_id=pair["market_id"],
        question=pair.get("question", ""),
        end_date_iso=pair.get("end_date_iso", ""),
        up_token_id=pair["up_token_id"],
        down_token_id=pair["down_token_id"],
        up_fill=up_fill,
        down_fill=down_fill,
        gross_cost=gross_cost,
        fee=fee,
        net_cost=net_cost,
        edge=edge,
        max_size_usdc=max_size_usdc,
    )

    logger.info(
        f"ARB | {opp.question[:50]:<50} | "
        f"edge={edge:.4f}  "
        f"up_wap={up_fill.wap:.4f}(L{up_fill.levels_touched})  "
        f"down_wap={down_fill.wap:.4f}(L{down_fill.levels_touched})  "
        f"max={max_size_usdc:.0f}$"
    )
    return opp


def scan_all(
    pairs: list[dict],
    target_usdc: float = 10.0,
    fee_rate: float = FEE_RATE,
    min_edge: float = MIN_EDGE,
    min_liquidity: float = MIN_LIQUIDITY,
) -> list[ArbOpportunity]:
    """
    Scans every market pair and returns all opportunities sorted best-edge first.
    target_usdc should match POSITION_SIZE_USDC from config / live_executor.
    """
    opportunities: list[ArbOpportunity] = []
    for pair in pairs:
        opp = detect_arb(pair, target_usdc, fee_rate, min_edge, min_liquidity)
        if opp:
            opportunities.append(opp)

    opportunities.sort(key=lambda o: o.edge, reverse=True)
    return opportunities


def run(
    input_path: str = "data/activity.json",
    output_path: str = "data/opportunities.json",
    target_usdc: float = 10.0,
) -> list[ArbOpportunity]:
    with open(input_path) as f:
        pairs = json.load(f)

    opps = scan_all(pairs, target_usdc=target_usdc)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump([o.to_dict() for o in opps], f, indent=2)

    logger.info(f"Found {len(opps)} opportunities → {output_path}")
    return opps


# ---------------------------------------------------------------------------
# Standalone WAP calculator (for quick inspection)
# ---------------------------------------------------------------------------

def explain_fill(asks: list[dict], target_usdc: float) -> str:
    """
    Returns a human-readable breakdown of a simulated fill.
    Useful for debugging / logging.

    Example output:
      Fill $10.00 across 2 levels:
        Level 1: price=0.4600  size=15.00  cost=$6.90  shares=15.00
        Level 2: price=0.4700  size=50.00  cost=$3.10  shares=6.60
      WAP = 0.4631  total_shares = 21.60
    """
    sorted_asks = sorted(asks, key=lambda x: float(x.get("price", 1.0)))
    lines = [f"Fill ${target_usdc:.2f} across orderbook:"]
    remaining = target_usdc
    total_shares = 0.0
    total_cost = 0.0

    for i, level in enumerate(sorted_asks, 1):
        if remaining <= 0:
            break
        price = float(level.get("price", 1.0))
        size  = float(level.get("size",  0.0))
        level_usdc = price * size
        fill_usdc  = min(remaining, level_usdc)
        fill_shares = fill_usdc / price
        total_shares += fill_shares
        total_cost   += fill_usdc
        remaining    -= fill_usdc
        lines.append(
            f"  Level {i}: price={price:.4f}  size={size:.2f}  "
            f"cost=${fill_usdc:.4f}  shares={fill_shares:.4f}"
        )

    wap = total_cost / total_shares if total_shares else 1.0
    lines.append(f"WAP = {wap:.6f}  total_shares = {total_shares:.4f}")
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=float, default=10.0, help="Target USDC per side")
    args = parser.parse_args()

    opps = run(target_usdc=args.size)
    if opps:
        best = opps[0]
        print(f"\nTop opportunity (WAP-adjusted, size=${args.size}):")
        print(json.dumps(best.to_dict(), indent=2))
        print(f"\nUp side fill breakdown:")
        import json as _json
        with open("data/activity.json") as f:
            pairs = _json.load(f)
        matched = next((p for p in pairs if p["market_id"] == best.market_id), None)
        if matched:
            print(explain_fill(matched["up_orderbook"].get("asks", []), args.size))
    else:
        print("No arbitrage opportunities in current snapshot.")
