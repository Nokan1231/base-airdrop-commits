"""
Fetches and parses BTC 5-minute market activity from the Polymarket CLOB API.
Outputs a normalized list of market-pair dicts for downstream pipeline stages.

Pipeline position: FIRST — all other scripts consume the output of this one.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests

CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

logger = logging.getLogger(__name__)


def fetch_btc_5m_markets() -> list[dict]:
    """Returns active BTC 5-minute Up/Down market pairs from Gamma API."""
    resp = requests.get(
        f"{GAMMA_BASE}/markets",
        params={
            "tag_slug": "crypto",
            "closed": "false",
            "limit": 500,
        },
        timeout=15,
    )
    resp.raise_for_status()
    markets = resp.json()

    btc_5m = [
        m for m in markets
        if _is_btc_5m(m)
    ]
    return btc_5m


def _is_btc_5m(market: dict) -> bool:
    q = market.get("question", "").upper()
    return (
        "BTC" in q
        and ("5-MINUTE" in q or "5 MINUTE" in q or "5MIN" in q or "5M" in q)
        and market.get("active", False)
        and not market.get("closed", True)
        and len(market.get("tokens", [])) == 2
    )


def fetch_orderbook(token_id: str) -> dict:
    """Fetches live order book for a specific outcome token."""
    resp = requests.get(
        f"{CLOB_BASE}/book",
        params={"token_id": token_id},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_trades(market_id: str, limit: int = 200) -> list[dict]:
    """Fetches recent confirmed trades for a market."""
    resp = requests.get(
        f"{CLOB_BASE}/trades",
        params={"market": market_id, "limit": limit},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("data", [])


def parse_market_pair(market: dict) -> Optional[dict]:
    """
    Normalizes a raw Gamma market dict into a structured Up/Down pair.
    Returns None if outcomes cannot be identified.
    """
    tokens = market.get("tokens", [])
    if len(tokens) != 2:
        return None

    # Identify Up/Yes and Down/No tokens by outcome label
    up_token = _find_token(tokens, ("YES", "UP", "HIGHER", "ABOVE"))
    down_token = _find_token(tokens, ("NO", "DOWN", "LOWER", "BELOW"))

    if not up_token or not down_token:
        up_token, down_token = tokens[0], tokens[1]

    return {
        "market_id": market["conditionId"],
        "question": market.get("question", ""),
        "end_date_iso": market.get("endDateIso", ""),
        "up_token_id": up_token["token_id"],
        "up_outcome": up_token.get("outcome", "YES"),
        "down_token_id": down_token["token_id"],
        "down_outcome": down_token.get("outcome", "NO"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def _find_token(tokens: list[dict], labels: tuple) -> Optional[dict]:
    for t in tokens:
        if t.get("outcome", "").upper() in labels:
            return t
    return None


def fetch_and_normalize(output_path: str = "data/activity.json") -> list[dict]:
    """
    Full fetch pipeline:
      1. Get active BTC 5m markets from Gamma API
      2. Fetch order book for each Up/Down token
      3. Fetch recent trades for each market
      4. Normalize into a list of pair dicts
      5. Write to output_path and return

    Downstream consumers: cluster_fills.py, detect_pair_arb.py
    """
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    logger.info("Fetching BTC 5m markets from Gamma API...")
    markets = fetch_btc_5m_markets()
    logger.info(f"Found {len(markets)} active BTC 5m markets")

    results: list[dict] = []
    for market in markets:
        pair = parse_market_pair(market)
        if not pair:
            continue

        try:
            pair["up_orderbook"] = fetch_orderbook(pair["up_token_id"])
            pair["down_orderbook"] = fetch_orderbook(pair["down_token_id"])
            pair["recent_trades"] = fetch_trades(pair["market_id"])
            results.append(pair)
            logger.debug(f"Fetched: {pair['question'][:70]}")
        except requests.RequestException as e:
            logger.warning(f"Skipping {pair['market_id']}: {e}")

        time.sleep(0.1)  # stay under CLOB rate limits

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Saved {len(results)} market pairs → {output_path}")
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    data = fetch_and_normalize()
    if data:
        print(json.dumps(data[0], indent=2, default=str))
    else:
        print("No BTC 5m markets found.")
