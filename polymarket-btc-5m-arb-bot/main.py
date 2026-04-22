"""
Polymarket BTC 5m Arbitrage Bot — CLI entry point.

Final formula (from blueprint):
  parse activity → cluster fills → detect paired cost → backtest → paper trade → live execution

Usage examples:
  python main.py fetch               # Step 1: download fresh market data
  python main.py cluster             # Step 2: cluster partial fills
  python main.py scan                # Step 3: detect arb opportunities
  python main.py backtest            # Step 4: simulate on current snapshot
  python main.py paper               # Step 5: paper trade for 60 min (live data, no orders)
  python main.py paper --duration 30 # Step 5: 30 minutes
  python main.py live                # Step 6: REAL trading (needs .env credentials)
  python main.py pipeline            # Steps 1-4 in sequence (research mode)
"""
import argparse
import logging
import sys
from pathlib import Path


def _setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def cmd_fetch(args):
    from scripts.parse_activity import fetch_and_normalize
    pairs = fetch_and_normalize(output_path="data/activity.json")
    print(f"Fetched {len(pairs)} BTC 5m market pairs → data/activity.json")


def cmd_cluster(args):
    _require_file("data/activity.json")
    from scripts.cluster_fills import run
    run(input_path="data/activity.json", output_path="data/clusters.json")
    print("Clustered fills → data/clusters.json")


def cmd_scan(args):
    _require_file("data/activity.json")
    from scripts.detect_pair_arb import run
    opps = run(input_path="data/activity.json", output_path="data/opportunities.json")
    if opps:
        print(f"\nFound {len(opps)} opportunities. Top result:")
        import json
        print(json.dumps(opps[0].to_dict(), indent=2))
    else:
        print("No arbitrage opportunities in current snapshot.")


def cmd_backtest(args):
    _require_file("data/opportunities.json", hint="Run 'python main.py scan' first")
    from scripts.backtest_btc_5m import run
    import json
    result = run(
        input_path="data/opportunities.json",
        output_path="data/backtest_report.json",
        size_usdc=args.size,
    )
    print("\nBacktest summary:")
    print(json.dumps(result.summary(), indent=2))
    print("Full report → data/backtest_report.json")


def cmd_paper(args):
    from scripts.paper_trade_btc_5m import run
    print(f"Starting paper trader for {args.duration} minutes...")
    print("Press Ctrl-C to stop early.\n")
    trader = run(duration_minutes=args.duration)
    print(f"\nFinal P&L: {trader.total_pnl:+.4f} USDC | Log → data/paper_trades.json")


def cmd_live(args):
    _check_env_credentials()
    print("Starting LIVE executor. Real orders will be placed on Polymarket.")
    print("Press Ctrl-C to stop.\n")
    from scripts.live_executor_btc_5m import LiveExecutor
    executor = LiveExecutor()
    executor.run()


def cmd_pipeline(args):
    """Runs steps 1–4 in sequence for a complete research snapshot."""
    print("=== Step 1/4: Fetch market data ===")
    cmd_fetch(args)

    print("\n=== Step 2/4: Cluster fills ===")
    cmd_cluster(args)

    print("\n=== Step 3/4: Detect opportunities ===")
    cmd_scan(args)

    print("\n=== Step 4/4: Backtest ===")
    cmd_backtest(args)

    print("\nPipeline complete. Review data/backtest_report.json before paper trading.")


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------

def _require_file(path: str, hint: str = ""):
    if not Path(path).exists():
        msg = f"Required file not found: {path}"
        if hint:
            msg += f"\nHint: {hint}"
        print(f"ERROR: {msg}", file=sys.stderr)
        sys.exit(1)


def _check_env_credentials():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    required = ["POLY_PRIVATE_KEY", "POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"ERROR: Missing .env credentials: {', '.join(missing)}", file=sys.stderr)
        print("Copy .env.template → .env and fill in your Polymarket API keys.", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="polymarket-arb-bot",
        description="Polymarket BTC 5m Up/Down Arbitrage Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])

    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("fetch", help="Download fresh BTC 5m market data")
    sub.add_parser("cluster", help="Cluster partial fills into parent orders")
    sub.add_parser("scan", help="Detect Up/Down arbitrage opportunities")

    bt = sub.add_parser("backtest", help="Backtest strategy on current snapshot")
    bt.add_argument("--size", type=float, default=10.0, help="USDC per trade (default: 10)")

    paper = sub.add_parser("paper", help="Paper trade (live data, virtual money)")
    paper.add_argument("--duration", type=int, default=60, help="Minutes to run (default: 60)")

    sub.add_parser("live", help="LIVE trading — requires .env credentials")
    sub.add_parser("pipeline", help="Run fetch → cluster → scan → backtest in sequence")

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    _setup_logging(args.log_level)

    handlers = {
        "fetch": cmd_fetch,
        "cluster": cmd_cluster,
        "scan": cmd_scan,
        "backtest": cmd_backtest,
        "paper": cmd_paper,
        "live": cmd_live,
        "pipeline": cmd_pipeline,
    }
    handlers[args.command](args)


if __name__ == "__main__":
    main()
