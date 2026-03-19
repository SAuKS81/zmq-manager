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


def top_level(side: list[list[float]] | None) -> tuple[float | None, float | None]:
    if not side:
        return None, None
    level = side[0]
    if len(level) < 2:
        return None, None
    return level[0], level[1]


async def main() -> int:
    parser = argparse.ArgumentParser(description="Subscribe to MEXC spot trades/orderbooks via ccxt.pro")
    parser.add_argument("--symbol", default="BTC/USDT", help="CCXT symbol")
    parser.add_argument("--data-type", default="trades", choices=["trades", "orderbooks"], help="Stream type")
    parser.add_argument("--depth", type=int, default=5, choices=[5, 10, 20], help="Orderbook depth for watch_order_book")
    parser.add_argument("--frequency", default="100ms", choices=["10ms", "100ms"], help="MEXC trade/orderbook update interval")
    args = parser.parse_args()

    exchange = ccxtpro.mexc(
        {
            "newUpdates": True,
            "tradesLimit": 1,
            "options": {
                "defaultType": "spot",
            },
        }
    )

    try:
        while True:
            if args.data_type == "trades":
                trades = await exchange.watch_trades(args.symbol, params={"frequency": args.frequency})
                if not trades:
                    continue
                for trade in trades:
                    ts_ms = trade.get("timestamp")
                    price = trade.get("price")
                    amount = trade.get("amount")
                    print(f"{format_ts_ms(ts_ms)} price={price} volume={amount}")
                continue

            book = await exchange.watch_order_book(args.symbol, limit=args.depth, params={"frequency": args.frequency})
            ts_ms = book.get("timestamp")
            bid_price, bid_amount = top_level(book.get("bids"))
            ask_price, ask_amount = top_level(book.get("asks"))
            print(
                f"{format_ts_ms(ts_ms)} "
                f"bid={bid_price} bid_volume={bid_amount} "
                f"ask={ask_price} ask_volume={ask_amount}"
            )
    finally:
        await exchange.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
