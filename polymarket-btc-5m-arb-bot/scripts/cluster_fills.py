"""
Clusters partial fills from recent_trades into reconstructed parent orders.

Two fills belong to the same parent order when:
  - They share the same maker address
  - They occur within CLUSTER_WINDOW_S seconds of each other
  - They are on the same side (BUY / SELL)

Pipeline position: SECOND — consumes activity.json, produces clusters.json.
"""
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

CLUSTER_WINDOW_S = 2.0   # max gap between fills to merge into one order
MIN_FILL_SIZE = 0.01     # ignore dust fills smaller than this


@dataclass
class Fill:
    timestamp: float
    price: float
    size: float
    side: str        # "BUY" | "SELL"
    maker: str
    taker: str
    market_id: str
    token_id: str
    tx_hash: str


@dataclass
class ClusteredOrder:
    fills: list[Fill] = field(default_factory=list)

    @property
    def avg_price(self) -> float:
        total = sum(f.size for f in self.fills)
        if total == 0:
            return 0.0
        return sum(f.price * f.size for f in self.fills) / total

    @property
    def total_size(self) -> float:
        return sum(f.size for f in self.fills)

    @property
    def cost(self) -> float:
        return self.avg_price * self.total_size

    @property
    def side(self) -> str:
        return self.fills[0].side if self.fills else ""

    @property
    def maker(self) -> str:
        return self.fills[0].maker if self.fills else ""

    def to_dict(self) -> dict:
        return {
            "avg_price": round(self.avg_price, 6),
            "total_size": round(self.total_size, 6),
            "cost": round(self.cost, 6),
            "side": self.side,
            "fill_count": len(self.fills),
            "first_ts": self.fills[0].timestamp if self.fills else None,
            "last_ts": self.fills[-1].timestamp if self.fills else None,
            "maker": self.maker,
            "market_id": self.fills[0].market_id if self.fills else None,
            "token_id": self.fills[0].token_id if self.fills else None,
        }


def _parse_fills(raw_trades: list[dict], market_id: str, token_id: str) -> list[Fill]:
    """Converts raw trade dicts to Fill objects, dropping dust."""
    fills: list[Fill] = []
    for t in raw_trades:
        # Only include fills that match this specific token
        if t.get("asset_id") and t["asset_id"] != token_id:
            continue
        size = float(t.get("size", 0))
        if size < MIN_FILL_SIZE:
            continue
        fills.append(Fill(
            timestamp=float(t.get("timestamp", 0)),
            price=float(t.get("price", 0)),
            size=size,
            side=t.get("side", "BUY").upper(),
            maker=t.get("makerAddress", t.get("maker_address", "")),
            taker=t.get("takerAddress", t.get("taker_address", "")),
            market_id=market_id,
            token_id=token_id,
            tx_hash=t.get("transactionHash", t.get("transaction_hash", "")),
        ))
    return sorted(fills, key=lambda f: f.timestamp)


def cluster_fills(fills: list[Fill]) -> list[ClusteredOrder]:
    """
    Merges fills into parent orders using a sliding time + maker window.
    Output list is sorted by first-fill timestamp.
    """
    if not fills:
        return []

    clusters: list[ClusteredOrder] = [ClusteredOrder(fills=[fills[0]])]

    for fill in fills[1:]:
        last_fill = clusters[-1].fills[-1]
        same_maker = fill.maker == last_fill.maker and fill.maker != ""
        same_side = fill.side == last_fill.side
        within_window = (fill.timestamp - last_fill.timestamp) <= CLUSTER_WINDOW_S

        if same_maker and same_side and within_window:
            clusters[-1].fills.append(fill)
        else:
            clusters.append(ClusteredOrder(fills=[fill]))

    return clusters


def cluster_market_pair(pair: dict) -> dict:
    """
    Clusters fills for both the Up and Down sides of one market pair.
    Uses recent_trades from activity.json; separates them by token_id.
    """
    market_id = pair["market_id"]
    trades = pair.get("recent_trades", [])

    up_fills = _parse_fills(trades, market_id, pair["up_token_id"])
    down_fills = _parse_fills(trades, market_id, pair["down_token_id"])

    # If token_id is not present in trades, fall back to splitting by index
    if not up_fills and not down_fills and trades:
        mid = len(trades) // 2
        up_fills = _parse_fills(trades[:mid], market_id, pair["up_token_id"])
        down_fills = _parse_fills(trades[mid:], market_id, pair["down_token_id"])

    up_clusters = cluster_fills(up_fills)
    down_clusters = cluster_fills(down_fills)

    logger.debug(
        f"{market_id[:12]}… → "
        f"up: {len(up_fills)} fills → {len(up_clusters)} orders | "
        f"down: {len(down_fills)} fills → {len(down_clusters)} orders"
    )

    return {
        "market_id": market_id,
        "question": pair.get("question", ""),
        "up_clusters": [c.to_dict() for c in up_clusters],
        "down_clusters": [c.to_dict() for c in down_clusters],
        "up_fill_count": len(up_fills),
        "down_fill_count": len(down_fills),
    }


def run(
    input_path: str = "data/activity.json",
    output_path: str = "data/clusters.json",
) -> list[dict]:
    with open(input_path) as f:
        pairs = json.load(f)

    results = [cluster_market_pair(p) for p in pairs]

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    total_up = sum(r["up_fill_count"] for r in results)
    total_down = sum(r["down_fill_count"] for r in results)
    logger.info(
        f"Clustered {len(results)} markets | "
        f"{total_up} up fills, {total_down} down fills → {output_path}"
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
