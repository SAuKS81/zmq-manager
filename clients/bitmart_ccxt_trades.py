#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timezone

import ccxt.pro as ccxtpro


def format_ts_ms(ts_ms: int | None) -> str:
    if not ts_ms:
        return "-"
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone()
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


async def main() -> int:
    parser = argparse.ArgumentParser(description="Subscribe to BitMart spot trades/orderbooks via ccxt.pro")
    parser.add_argument("--symbol", default="BTC/USDT", help="CCXT symbol")
    parser.add_argument("--data-type", default="trades", choices=["trades", "orderbooks"], help="Stream type")
    parser.add_argument("--depth", type=int, default=100, choices=[5, 20, 50, 100], help="Orderbook depth")
    args = parser.parse_args()

    exchange = ccxtpro.bitmart({"newUpdates": True, "tradesLimit": 1, "options": {"defaultType": "spot"}})

    try:
        while True:
            if args.data_type == "orderbooks":
                params = {"depth": "depth/increase100" if args.depth == 100 else f"depth{args.depth}"}
                book = await exchange.watch_order_book(args.symbol, args.depth, params)
                ts_ms = book.get("timestamp")
                bids = book.get("bids") or []
                asks = book.get("asks") or []
                best_bid = bids[0] if bids else [None, None]
                best_ask = asks[0] if asks else [None, None]
                print(
                    f"{format_ts_ms(ts_ms)} "
                    f"bid={best_bid[0]} bid_volume={best_bid[1]} "
                    f"ask={best_ask[0]} ask_volume={best_ask[1]}"
                )
                continue

            trades = await exchange.watch_trades(args.symbol)
            if not trades:
                continue
            for trade in trades:
                print(f"{format_ts_ms(trade.get('timestamp'))} price={trade.get('price')} volume={trade.get('amount')}")
    finally:
        await exchange.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
