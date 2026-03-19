#!/usr/bin/env python3
import argparse
import json
import signal
import sys
import uuid
from datetime import datetime, timezone

import ccxt
import msgpack
import zmq


def default_broker_address() -> str:
    if sys.platform.startswith("linux"):
        return "ipc:///tmp/feed_broker.ipc"
    return "tcp://127.0.0.1:5555"


def format_ts_ms(ts_ms: int | None) -> str:
    if not ts_ms:
        return "-"
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone()
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def detect_header(frames: list[bytes]) -> bytes:
    if len(frames) < 2:
        return b""
    for frame in reversed(frames[:-1]):
        if len(frame) == 1 and frame in (b"T", b"O", b"J"):
            return frame
    return b""


def normalize_trade_obj(trade) -> dict:
    if isinstance(trade, dict):
        return {
            "timestamp": trade.get("timestamp", trade.get("t")),
            "price": trade.get("price", trade.get("p")),
            "amount": trade.get("amount", trade.get("a")),
            "symbol": trade.get("symbol", trade.get("s")),
            "exchange": trade.get("exchange", trade.get("e")),
        }
    return {}


def normalize_orderbook_obj(ob) -> dict:
    if not isinstance(ob, dict):
        return {}
    return {
        "timestamp": ob.get("timestamp", ob.get("t")),
        "bids": ob.get("bids", ob.get("b")) or [],
        "asks": ob.get("asks", ob.get("a")) or [],
        "symbol": ob.get("symbol", ob.get("s")),
    }


def top_level(levels) -> tuple[float | None, float | None]:
    if not levels:
        return None, None
    level = levels[0]
    if isinstance(level, dict):
        return level.get("price", level.get("p")), level.get("amount", level.get("a"))
    if isinstance(level, (list, tuple)) and len(level) >= 2:
        return level[0], level[1]
    return None, None


def load_active_kucoin_symbols(quote: str, limit: int | None) -> list[str]:
    exchange = ccxt.kucoin({"options": {"defaultType": "spot"}})
    close_fn = getattr(exchange, "close", None)
    try:
        markets = exchange.load_markets()
    finally:
        if callable(close_fn):
            close_fn()

    symbols: list[str] = []
    for market in markets.values():
        if not market.get("active"):
            continue
        if market.get("spot") is not True:
            continue
        if market.get("quote") != quote:
            continue
        symbol = market.get("symbol")
        if symbol:
            symbols.append(symbol)

    symbols = sorted(set(symbols))
    if limit is not None:
        symbols = symbols[:limit]
    return symbols


def build_request(symbols: list[str], encoding: str, data_type: str, depth: int) -> dict:
    req = {
        "action": "subscribe_bulk",
        "exchange": "kucoin_native",
        "symbols": symbols,
        "market_type": "spot",
        "data_type": data_type,
        "encoding": encoding,
    }
    if data_type == "orderbooks":
        req["depth"] = depth
    return req


def build_unsubscribe(symbols: list[str], data_type: str, depth: int) -> dict:
    req = {
        "action": "unsubscribe_bulk",
        "exchange": "kucoin_native",
        "symbols": symbols,
        "market_type": "spot",
        "data_type": data_type,
    }
    if data_type == "orderbooks":
        req["depth"] = depth
    return req


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke client for KuCoin native spot trades/orderbooks via zmq_manager")
    parser.add_argument("--broker", default=default_broker_address(), help="ZMQ broker address")
    parser.add_argument("--quote", default="USDT", help="Quote asset filter")
    parser.add_argument("--limit-symbols", type=int, default=50, help="Number of active symbols to subscribe")
    parser.add_argument("--data-type", default="trades", choices=["trades", "orderbooks"], help="Stream type")
    parser.add_argument("--depth", type=int, default=1, choices=[1, 5, 50], help="Orderbook depth")
    parser.add_argument("--encoding", default="msgpack", choices=["json", "msgpack", "binary"], help="Broker encoding")
    args = parser.parse_args()

    symbols = load_active_kucoin_symbols(args.quote.upper(), args.limit_symbols)
    if not symbols:
        print("[CLIENT] no KuCoin symbols found")
        return 1

    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, f"kucoin-smoke-{uuid.uuid4().hex}".encode())
    sock.connect(args.broker)

    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    sock.send_json(build_request(symbols, args.encoding, args.data_type, args.depth))

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    print(
        f"[CLIENT] broker={args.broker} exchange=kucoin_native "
        f"symbols={len(symbols)} quote={args.quote.upper()} "
        f"data_type={args.data_type} depth={args.depth} encoding={args.encoding}"
    )

    while not stop:
        events = dict(poller.poll(timeout=250))
        if sock not in events or not (events[sock] & zmq.POLLIN):
            continue

        frames = sock.recv_multipart()
        header = detect_header(frames)
        payload = frames[-1]

        if header == b"T":
            obj = msgpack.unpackb(payload, raw=False)
            trades = obj if isinstance(obj, list) else [obj]
            for trade in trades:
                trade = normalize_trade_obj(trade)
                print(
                    f"{format_ts_ms(trade.get('timestamp'))} "
                    f"{trade.get('symbol')} price={trade.get('price')} volume={trade.get('amount')}"
                )
            continue

        if header == b"O":
            obj = msgpack.unpackb(payload, raw=False)
            books = obj if isinstance(obj, list) else [obj]
            for ob in books:
                ob = normalize_orderbook_obj(ob)
                bid_price, bid_amount = top_level(ob.get("bids"))
                ask_price, ask_amount = top_level(ob.get("asks"))
                print(
                    f"{format_ts_ms(ob.get('timestamp'))} {ob.get('symbol')} "
                    f"bid={bid_price} bid_volume={bid_amount} "
                    f"ask={ask_price} ask_volume={ask_amount}"
                )
            continue

        try:
            obj = json.loads(payload.decode("utf-8"))
        except Exception:
            continue

        if isinstance(obj, dict) and obj.get("type") == "ping":
            sock.send_json({"message": "pong"})
            continue

        if isinstance(obj, dict) and obj.get("type") == "error":
            print(f"[CLIENT] error payload={obj}")
            continue

        if isinstance(obj, dict) and str(obj.get("type", "")).startswith("stream_"):
            print(f"[CLIENT] status payload={obj}")
            continue

    try:
        sock.send_json(build_unsubscribe(symbols, args.data_type, args.depth))
    except Exception:
        pass
    finally:
        sock.close(0)
        ctx.term()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
