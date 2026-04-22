"""
Fetches BTC 5m market data from Polymarket using two transport modes:

MODE 1 — Async REST (default, used by paper/backtest)
  Uses aiohttp to fire all orderbook requests concurrently instead of
  sequentially. Fetching 20 markets takes ~0.3s instead of ~2s.

MODE 2 — WebSocket Streamer (used by live_executor for real-time ticks)
  Subscribes to Polymarket's CLOB WebSocket feed and keeps an in-memory
  order book that is always current without polling.
  Call:  streamer = OrderBookStreamer(token_ids)
         await streamer.start()
         book = streamer.get_book(token_id)   # always fresh

Pipeline position: FIRST — all other scripts consume the output of this one.
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

CLOB_BASE  = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_WS    = "wss://ws-subscriptions-clob.polymarket.com/ws/"

# Max simultaneous HTTP connections to the CLOB API
_CONNECTOR_LIMIT = 20


# ---------------------------------------------------------------------------
# Market filtering
# ---------------------------------------------------------------------------

def _is_btc_5m(market: dict) -> bool:
    q = market.get("question", "").upper()
    return (
        "BTC" in q
        and ("5-MINUTE" in q or "5 MINUTE" in q or "5MIN" in q or "5M" in q)
        and market.get("active", False)
        and not market.get("closed", True)
        and len(market.get("tokens", [])) == 2
    )


def _find_token(tokens: list[dict], labels: tuple) -> Optional[dict]:
    for t in tokens:
        if t.get("outcome", "").upper() in labels:
            return t
    return None


def parse_market_pair(market: dict) -> Optional[dict]:
    """Normalizes a raw Gamma market into a structured Up/Down pair dict."""
    tokens = market.get("tokens", [])
    if len(tokens) != 2:
        return None

    up_token   = _find_token(tokens, ("YES", "UP", "HIGHER", "ABOVE"))
    down_token = _find_token(tokens, ("NO",  "DOWN", "LOWER", "BELOW"))
    if not up_token or not down_token:
        up_token, down_token = tokens[0], tokens[1]

    return {
        "market_id":    market["conditionId"],
        "question":     market.get("question", ""),
        "end_date_iso": market.get("endDateIso", ""),
        "up_token_id":  up_token["token_id"],
        "up_outcome":   up_token.get("outcome", "YES"),
        "down_token_id":  down_token["token_id"],
        "down_outcome":   down_token.get("outcome", "NO"),
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# MODE 1 — Async REST fetch (concurrent aiohttp)
# ---------------------------------------------------------------------------

async def _get_json(session: aiohttp.ClientSession, url: str, params: dict) -> dict | list:
    async with session.get(url, params=params) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)


async def _fetch_markets(session: aiohttp.ClientSession) -> list[dict]:
    data = await _get_json(
        session,
        f"{GAMMA_BASE}/markets",
        {"tag_slug": "crypto", "closed": "false", "limit": 500},
    )
    return [m for m in data if _is_btc_5m(m)]


async def _fetch_orderbook(session: aiohttp.ClientSession, token_id: str) -> dict:
    return await _get_json(session, f"{CLOB_BASE}/book", {"token_id": token_id})


async def _fetch_trades(session: aiohttp.ClientSession, market_id: str) -> list[dict]:
    data = await _get_json(
        session,
        f"{CLOB_BASE}/trades",
        {"market": market_id, "limit": 200},
    )
    return data if isinstance(data, list) else data.get("data", [])


async def _fetch_pair(session: aiohttp.ClientSession, pair: dict) -> Optional[dict]:
    """
    Fires 3 concurrent requests per pair: up book, down book, trades.
    A single pair that fails is skipped without stopping the others.
    """
    try:
        up_book, down_book, trades = await asyncio.gather(
            _fetch_orderbook(session, pair["up_token_id"]),
            _fetch_orderbook(session, pair["down_token_id"]),
            _fetch_trades(session, pair["market_id"]),
        )
        return {**pair,
                "up_orderbook":   up_book,
                "down_orderbook": down_book,
                "recent_trades":  trades}
    except aiohttp.ClientError as e:
        logger.warning(f"Skipping {pair['market_id'][:16]}…: {e}")
        return None


async def fetch_and_normalize_async(
    output_path: str = "data/activity.json",
) -> list[dict]:
    """
    Async core: fetches all markets in parallel and returns normalized pairs.
    Compared to the old sequential version, this is 5-10× faster for ≥5 markets.
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    connector = aiohttp.TCPConnector(limit=_CONNECTOR_LIMIT)
    timeout   = aiohttp.ClientTimeout(total=20, connect=5)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        markets = await _fetch_markets(session)
        logger.info(f"Found {len(markets)} active BTC 5m markets")

        pairs = [parse_market_pair(m) for m in markets]
        pairs = [p for p in pairs if p]

        # All pairs fetched concurrently — no sleep needed
        results_raw = await asyncio.gather(*[_fetch_pair(session, p) for p in pairs])
        results = [r for r in results_raw if r]

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Saved {len(results)} pairs → {output_path}")
    return results


def fetch_and_normalize(output_path: str = "data/activity.json") -> list[dict]:
    """
    Synchronous drop-in replacement for the old requests-based version.
    Internally runs the async pipeline via asyncio.run().
    """
    return asyncio.run(fetch_and_normalize_async(output_path))


# ---------------------------------------------------------------------------
# MODE 2 — WebSocket Streamer (real-time order book, used by live executor)
# ---------------------------------------------------------------------------

class OrderBookStreamer:
    """
    Subscribes to Polymarket's CLOB WebSocket feed for a set of token IDs.
    Maintains an always-current in-memory order book per token.

    Usage (inside an async context):
        streamer = OrderBookStreamer(["token_id_up", "token_id_down"])
        asyncio.create_task(streamer.run())
        await streamer.wait_ready()
        book = streamer.get_book("token_id_up")  # {"bids": [...], "asks": [...]}
        streamer.stop()

    The live executor should prefer get_book() over HTTP fetches to eliminate
    the round-trip latency between snapshot and order placement.
    """

    def __init__(self, token_ids: list[str], reconnect_delay: float = 2.0):
        self._token_ids = token_ids
        self._reconnect_delay = reconnect_delay
        self._books: dict[str, dict] = {tid: {"bids": [], "asks": []} for tid in token_ids}
        self._ready = asyncio.Event()
        self._stop  = asyncio.Event()
        self._initial_snapshot_count = 0

    def get_book(self, token_id: str) -> dict:
        """Returns the current order book for token_id. Thread-safe read."""
        return self._books.get(token_id, {"bids": [], "asks": []})

    def stop(self):
        self._stop.set()

    async def wait_ready(self, timeout: float = 10.0):
        """Waits until initial snapshots have been received for all tokens."""
        await asyncio.wait_for(self._ready.wait(), timeout=timeout)

    async def run(self):
        """Main loop — reconnects automatically on disconnect."""
        while not self._stop.is_set():
            try:
                await self._connect_and_stream()
            except Exception as e:
                logger.warning(f"[WS] Disconnected: {e} — reconnecting in {self._reconnect_delay}s")
                await asyncio.sleep(self._reconnect_delay)

    async def _connect_and_stream(self):
        import websockets

        async with websockets.connect(CLOB_WS, ping_interval=20) as ws:
            logger.info(f"[WS] Connected → subscribing to {len(self._token_ids)} tokens")

            # Subscribe to order book channel for each token
            for token_id in self._token_ids:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "channel": "book",
                    "market": token_id,
                }))

            async for raw in ws:
                if self._stop.is_set():
                    break
                self._handle_message(json.loads(raw))

    def _handle_message(self, msg: dict):
        event_type = msg.get("event_type", msg.get("type", ""))
        token_id   = msg.get("asset_id", msg.get("market", ""))

        if token_id not in self._books:
            return

        if event_type in ("book", "price_change"):
            # Full snapshot
            self._books[token_id] = {
                "bids": msg.get("bids", []),
                "asks": msg.get("asks", []),
            }
            self._initial_snapshot_count += 1
            if self._initial_snapshot_count >= len(self._token_ids):
                self._ready.set()

        elif event_type in ("tick_size_change",):
            pass  # metadata only, ignore

        logger.debug(f"[WS] {event_type} | token={token_id[:16]}… | "
                     f"bids={len(self._books[token_id]['bids'])} "
                     f"asks={len(self._books[token_id]['asks'])}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    data = fetch_and_normalize()
    if data:
        print(f"\nFetched {len(data)} market pairs. First result:")
        print(json.dumps({
            k: v for k, v in data[0].items()
            if k not in ("up_orderbook", "down_orderbook", "recent_trades")
        }, indent=2))
    else:
        print("No BTC 5m markets found.")
