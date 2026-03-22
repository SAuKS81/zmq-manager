#!/usr/bin/env python3
import argparse
import json
import signal
import sys
import uuid
from datetime import datetime, timezone

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
        }
    return {}


def normalize_orderbook_obj(book) -> dict:
    if isinstance(book, dict):
        return {
            "timestamp": book.get("timestamp", book.get("t")),
            "bids": book.get("bids", book.get("b")) or [],
            "asks": book.get("asks", book.get("a")) or [],
        }
    return {}


def first_level(levels):
    if not levels:
        return (None, None)
    level = levels[0]
    if isinstance(level, dict):
        return (level.get("price", level.get("p")), level.get("amount", level.get("a")))
    if isinstance(level, (list, tuple)) and len(level) >= 2:
        return (level[0], level[1])
    return (None, None)


def build_request(symbol: str, data_type: str, encoding: str, depth: int, orderbook_mode: str) -> dict:
    req = {
        "action": "subscribe_bulk",
        "exchange": "bitmart_native",
        "symbols": [symbol],
        "market_type": "spot",
        "data_type": data_type,
        "encoding": encoding,
    }
    if data_type == "orderbooks":
        req["depth"] = depth
        req["orderbook_mode"] = orderbook_mode
    return req


def build_unsubscribe(symbol: str, data_type: str) -> dict:
    return {
        "action": "unsubscribe_bulk",
        "exchange": "bitmart_native",
        "symbols": [symbol],
        "market_type": "spot",
        "data_type": data_type,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Subscribe to BitMart native trades/orderbooks via zmq_manager")
    parser.add_argument("--broker", default=default_broker_address(), help="ZMQ broker address")
    parser.add_argument("--symbol", default="BTC/USDT", help="CCXT-style symbol")
    parser.add_argument("--data-type", default="trades", choices=["trades", "orderbooks"], help="Stream type")
    parser.add_argument("--depth", type=int, default=100, choices=[100], help="BitMart orderbook level for level100 mode")
    parser.add_argument("--orderbook-mode", default="level100", choices=["level100", "all"], help="BitMart orderbook mode")
    parser.add_argument("--encoding", default="msgpack", choices=["json", "msgpack", "binary"], help="Broker encoding")
    args = parser.parse_args()

    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, f"bitmart-native-{uuid.uuid4().hex}".encode())
    sock.connect(args.broker)

    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    sock.send_json(build_request(args.symbol, args.data_type, args.encoding, args.depth, args.orderbook_mode))

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    print(
        f"[CLIENT] broker={args.broker} exchange=bitmart_native symbol={args.symbol} "
        f"market_type=spot data_type={args.data_type} encoding={args.encoding}"
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
                print(f"{format_ts_ms(trade.get('timestamp'))} price={trade.get('price')} volume={trade.get('amount')}")
            continue

        if header == b"O":
            obj = msgpack.unpackb(payload, raw=False)
            books = obj if isinstance(obj, list) else [obj]
            for book in books:
                book = normalize_orderbook_obj(book)
                best_bid = first_level(book.get("bids") or [])
                best_ask = first_level(book.get("asks") or [])
                print(
                    f"{format_ts_ms(book.get('timestamp'))} "
                    f"bid={best_bid[0]} bid_volume={best_bid[1]} "
                    f"ask={best_ask[0]} ask_volume={best_ask[1]}"
                )
            continue

        try:
            obj = json.loads(payload.decode("utf-8"))
        except Exception:
            continue

        if isinstance(obj, dict) and obj.get("type") == "ping":
            sock.send_json({"message": "pong"})
            continue

        if isinstance(obj, dict) and str(obj.get("type", "")).startswith("stream_"):
            print(
                f"[CLIENT] status type={obj.get('type')} exchange={obj.get('exchange')} "
                f"symbol={obj.get('symbol')} reason={obj.get('reason')}"
            )
            continue

    try:
        sock.send_json(build_unsubscribe(args.symbol, args.data_type))
    except Exception:
        pass
    finally:
        sock.close(0)
        ctx.term()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
