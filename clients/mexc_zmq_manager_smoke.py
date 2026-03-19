#!/usr/bin/env python3
import argparse
import json
import signal
import sys
import uuid
from pathlib import Path
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
    if not isinstance(trade, dict):
        return {}
    return {
        "timestamp": trade.get("timestamp", trade.get("t")),
        "price": trade.get("price", trade.get("p")),
        "amount": trade.get("amount", trade.get("a")),
    }


def normalize_orderbook_obj(ob) -> dict:
    if not isinstance(ob, dict):
        return {}
    return {
        "timestamp": ob.get("timestamp", ob.get("t")),
        "bids": ob.get("bids", ob.get("b")) or [],
        "asks": ob.get("asks", ob.get("a")) or [],
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


def chunked(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def load_symbols_from_file(path: str, limit: int | None) -> list[str]:
    symbols: list[str] = []
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        symbols.append(line)

    symbols = list(dict.fromkeys(symbols))
    if limit and limit > 0:
        symbols = symbols[:limit]
    return symbols


def load_active_mexc_symbols(quote: str, limit: int | None) -> list[str]:
    exchange = ccxt.mexc({"options": {"defaultType": "spot"}})
    try:
        markets = exchange.load_markets()
    finally:
        close_fn = getattr(exchange, "close", None)
        if callable(close_fn):
            close_fn()

    symbols: list[str] = []
    for symbol, market in markets.items():
        if not market.get("spot"):
            continue
        if not market.get("active", True):
            continue
        if market.get("quote") != quote:
            continue
        symbols.append(symbol)

    symbols.sort()
    if limit and limit > 0:
        symbols = symbols[:limit]
    return symbols


def send_subscribe_bulk(sock: zmq.Socket, symbols: list[str], encoding: str, data_type: str, depth: int, frequency: str) -> None:
    req = {
        "action": "subscribe_bulk",
        "exchange": "mexc_native",
        "symbols": symbols,
        "market_type": "spot",
        "data_type": data_type,
        "encoding": encoding,
    }
    if data_type == "orderbooks":
        req["depth"] = depth
    req["frequency"] = frequency
    sock.send_json(req)


def send_unsubscribe_bulk(sock: zmq.Socket, symbols: list[str], data_type: str, depth: int, frequency: str) -> None:
    req = {
        "action": "unsubscribe_bulk",
        "exchange": "mexc_native",
        "symbols": symbols,
        "market_type": "spot",
        "data_type": data_type,
    }
    if data_type == "orderbooks":
        req["depth"] = depth
    req["frequency"] = frequency
    sock.send_json(req)


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke client: load active MEXC symbols via ccxt, consume via zmq_manager")
    parser.add_argument("--broker", default=default_broker_address(), help="ZMQ broker address")
    parser.add_argument("--encoding", default="msgpack", choices=["json", "msgpack", "binary"], help="Broker encoding")
    parser.add_argument("--data-type", default="trades", choices=["trades", "orderbooks"], help="Stream type")
    parser.add_argument("--depth", type=int, default=5, choices=[5, 10, 20], help="Orderbook depth")
    parser.add_argument("--frequency", default="100ms", choices=["10ms", "100ms"], help="MEXC trade/orderbook update interval")
    parser.add_argument("--quote", default="USDT", help="Only include symbols with this quote currency")
    parser.add_argument("--limit-symbols", type=int, default=0, help="Optional cap for number of active symbols")
    parser.add_argument("--bulk-size", type=int, default=200, help="Symbols per subscribe_bulk request")
    parser.add_argument("--symbols", default="", help="Comma-separated explicit symbol list, e.g. BTC/USDT,ETH/USDT")
    parser.add_argument("--symbols-file", default="", help="Optional text file with one symbol per line")
    args = parser.parse_args()

    symbol_limit = args.limit_symbols if args.limit_symbols > 0 else None
    symbols: list[str]
    symbol_source = "ccxt"

    if args.symbols.strip():
        symbols = [part.strip() for part in args.symbols.split(",") if part.strip()]
        if symbol_limit and symbol_limit > 0:
            symbols = symbols[:symbol_limit]
        symbol_source = "cli"
    elif args.symbols_file.strip():
        symbols = load_symbols_from_file(args.symbols_file, symbol_limit)
        symbol_source = args.symbols_file
    else:
        try:
            symbols = load_active_mexc_symbols(args.quote.upper(), symbol_limit)
        except Exception as exc:
            fallback = Path("scripts") / "symbols_example_50.txt"
            if fallback.exists():
                print(f"[CLIENT] ccxt symbol discovery failed: {exc}")
                print(f"[CLIENT] fallback to local symbol file: {fallback}")
                symbols = load_symbols_from_file(str(fallback), symbol_limit)
                symbol_source = str(fallback)
            else:
                raise

    if not symbols:
        raise SystemExit("no active MEXC symbols found")

    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, f"mexc-smoke-{uuid.uuid4().hex}".encode())
    sock.connect(args.broker)

    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    batches = chunked(symbols, max(1, args.bulk_size))
    for batch in batches:
        send_subscribe_bulk(sock, batch, args.encoding, args.data_type, args.depth, args.frequency)

    print(
        f"[CLIENT] broker={args.broker} exchange=mexc_native symbols={len(symbols)} "
        f"quote={args.quote.upper()} data_type={args.data_type} depth={args.depth} frequency={args.frequency} "
        f"encoding={args.encoding} bulk_requests={len(batches)} source={symbol_source}"
    )

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

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
            for ob in books:
                ob = normalize_orderbook_obj(ob)
                bid_price, bid_amount = top_level(ob.get("bids"))
                ask_price, ask_amount = top_level(ob.get("asks"))
                print(
                    f"{format_ts_ms(ob.get('timestamp'))} "
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
        for batch in batches:
            send_unsubscribe_bulk(sock, batch, args.data_type, args.depth, args.frequency)
    except Exception:
        pass
    finally:
        sock.close(0)
        ctx.term()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
