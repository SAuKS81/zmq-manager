#!/usr/bin/env python3
import argparse
import json
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import ccxt
import msgpack
import zmq


ALLOWED_INTERVALS = {"1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"}


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
        if len(frame) == 1 and frame in (b"T", b"O", b"K", b"J"):
            return frame
    return b""


def normalize_ohlcv_obj(item) -> dict:
    if not isinstance(item, dict):
        return {}
    return {
        "exchange": item.get("exchange", item.get("e")),
        "symbol": item.get("symbol", item.get("s")),
        "market_type": item.get("market_type", item.get("m")),
        "interval": item.get("interval", item.get("i")),
        "timestamp": item.get("timestamp", item.get("t")),
        "open": item.get("open", item.get("o")),
        "high": item.get("high", item.get("h")),
        "low": item.get("low", item.get("l")),
        "close": item.get("close", item.get("c")),
        "volume": item.get("volume", item.get("v")),
        "confirm": item.get("confirm", item.get("cf")),
    }


def load_bybit_symbols(market_types: list[str], quote: str, limit_per_market: int | None) -> dict[str, list[str]]:
    exchange = ccxt.bybit({"enableRateLimit": True})
    close_fn = getattr(exchange, "close", None)
    try:
        markets = exchange.load_markets()
    finally:
        if callable(close_fn):
            close_fn()

    result: dict[str, list[str]] = {market_type: [] for market_type in market_types}
    for market in markets.values():
        if not market.get("active", True):
            continue
        symbol = market.get("symbol")
        if not symbol:
            continue

        if "spot" in result:
            if market.get("spot") is True and market.get("quote") == quote:
                result["spot"].append(symbol)

        if "swap" in result:
            if market.get("swap") is True and market.get("linear") is True and market.get("settle") == quote:
                result["swap"].append(symbol)

    for market_type, symbols in result.items():
        unique = sorted(set(symbols))
        if limit_per_market is not None:
            unique = unique[:limit_per_market]
        result[market_type] = unique
    return result


def chunks(items: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        size = len(items) or 1
    return [items[i : i + size] for i in range(0, len(items), size)]


def build_subscribe(symbols: list[str], market_type: str, interval: str, encoding: str) -> dict:
    return {
        "action": "subscribe_bulk",
        "exchange": "bybit_native",
        "symbols": symbols,
        "market_type": market_type,
        "data_type": "ohlcv",
        "interval": interval,
        "encoding": encoding,
    }


def build_unsubscribe(symbols: list[str], market_type: str, interval: str) -> dict:
    return {
        "action": "unsubscribe_bulk",
        "exchange": "bybit_native",
        "symbols": symbols,
        "market_type": market_type,
        "data_type": "ohlcv",
        "interval": interval,
    }


def send_json(sock: zmq.Socket, payload: dict) -> None:
    sock.send_json(payload)


def print_status(obj: dict) -> None:
    reason = obj.get("reason") or ""
    print(
        f"[SMOKE] status type={obj.get('type')} status={obj.get('status')} "
        f"exchange={obj.get('exchange')} market_type={obj.get('market_type')} "
        f"data_type={obj.get('data_type')} interval={obj.get('interval')} "
        f"symbol={obj.get('symbol')} reason={reason}"
    )


def handle_payload(sock: zmq.Socket, frames: list[bytes], print_samples: int) -> tuple[int, int, int, str, int]:
    header = detect_header(frames)
    payload = frames[-1]

    if header == b"K":
        try:
            obj = msgpack.unpackb(payload, raw=False)
        except Exception as exc:
            print(f"[SMOKE] msgpack ohlcv decode failed: {exc}")
            return 0, 0, 1, "", print_samples
        klines = obj if isinstance(obj, list) else [obj]
        if print_samples > 0:
            for raw in klines[:print_samples]:
                item = normalize_ohlcv_obj(raw)
                print(
                    "[SMOKE] kline "
                    f"{format_ts_ms(item.get('timestamp'))} "
                    f"{item.get('exchange')} {item.get('market_type')} {item.get('symbol')} "
                    f"{item.get('interval')} o={item.get('open')} h={item.get('high')} "
                    f"l={item.get('low')} c={item.get('close')} v={item.get('volume')} "
                    f"confirm={item.get('confirm')}"
                )
                print_samples -= 1
                if print_samples <= 0:
                    break
        return len(klines), 0, 0, "", print_samples

    try:
        obj = json.loads(payload.decode("utf-8"))
    except Exception:
        return 0, 0, 1, "", print_samples

    if isinstance(obj, dict) and obj.get("type") == "ping":
        send_json(sock, {"message": "pong"})
        return 0, 0, 0, "", print_samples

    if isinstance(obj, dict) and str(obj.get("type", "")).startswith("stream_"):
        print_status(obj)
        return 0, 1, 0, str(obj.get("type") or ""), print_samples

    if isinstance(obj, list):
        count = 0
        for item in obj:
            if isinstance(item, dict) and item.get("data_type") == "ohlcv":
                count += 1
        return count, 0, 0, "", print_samples

    if isinstance(obj, dict) and obj.get("data_type") == "ohlcv":
        return 1, 0, 0, "", print_samples

    return 0, 0, 0, "", print_samples


def main() -> int:
    parser = argparse.ArgumentParser(description="Bybit native OHLCV smoke client using ccxt for market discovery")
    parser.add_argument("--broker", default=default_broker_address(), help="ZMQ broker address")
    parser.add_argument("--market-types", default="spot,swap", help="Comma-separated: spot,swap")
    parser.add_argument("--quote", default="USDT", help="Quote/settle filter")
    parser.add_argument("--limit-symbols", type=int, default=0, help="Symbols per market type, 0 = all")
    parser.add_argument("--bulk-size", type=int, default=10, help="Symbols per subscribe_bulk request")
    parser.add_argument("--interval", default="1m", choices=sorted(ALLOWED_INTERVALS), help="OHLCV interval")
    parser.add_argument("--encoding", default="msgpack", choices=["json", "msgpack", "binary"], help="Broker encoding")
    parser.add_argument("--duration", type=float, default=60.0, help="Run duration in seconds")
    parser.add_argument("--rate-log", type=float, default=10.0, help="Stats interval in seconds")
    parser.add_argument("--subscribe-pause", type=float, default=0.05, help="Pause between bulk requests in seconds")
    parser.add_argument("--print-samples", type=int, default=5, help="Print first N decoded candles")
    args = parser.parse_args()

    market_types = [part.strip() for part in args.market_types.split(",") if part.strip()]
    invalid_market_types = [mt for mt in market_types if mt not in {"spot", "swap"}]
    if invalid_market_types:
        raise SystemExit(f"invalid --market-types: {','.join(invalid_market_types)}")
    if not market_types:
        raise SystemExit("--market-types must not be empty")
    if args.duration <= 0:
        raise SystemExit("--duration must be > 0")
    if args.rate_log <= 0:
        raise SystemExit("--rate-log must be > 0")
    if args.bulk_size <= 0:
        raise SystemExit("--bulk-size must be > 0")

    limit = args.limit_symbols if args.limit_symbols > 0 else None
    symbols_by_market = load_bybit_symbols(market_types, args.quote.upper(), limit)
    total_symbols = sum(len(symbols) for symbols in symbols_by_market.values())
    if total_symbols == 0:
        raise SystemExit("[SMOKE] no Bybit symbols found")

    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, f"bybit-ohlcv-smoke-{uuid.uuid4().hex}".encode())
    sock.setsockopt(zmq.LINGER, 3000)
    sock.connect(args.broker)

    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    subscribe_batches: list[tuple[str, list[str]]] = []
    for market_type, symbols in symbols_by_market.items():
        for batch in chunks(symbols, args.bulk_size):
            subscribe_batches.append((market_type, batch))
            send_json(sock, build_subscribe(batch, market_type, args.interval, args.encoding))
            if args.subscribe_pause > 0:
                time.sleep(args.subscribe_pause)

    print(
        f"[SMOKE] broker={args.broker} exchange=bybit_native data_type=ohlcv "
        f"interval={args.interval} encoding={args.encoding} total_symbols={total_symbols} "
        f"batches={len(subscribe_batches)} "
        + " ".join(f"{mt}={len(symbols)}" for mt, symbols in symbols_by_market.items())
    )

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    start = time.time()
    last_rate = start
    total_ohlcv = 0
    total_status = 0
    total_decode_errors = 0
    window_ohlcv = 0
    window_decode_errors = 0
    print_samples = max(args.print_samples, 0)

    try:
        while not stop and (time.time() - start) < args.duration:
            events = dict(poller.poll(timeout=250))
            if sock in events and (events[sock] & zmq.POLLIN):
                frames = sock.recv_multipart()
                ohlcv, status, decode_errors, _, print_samples = handle_payload(sock, frames, print_samples)
                total_ohlcv += ohlcv
                window_ohlcv += ohlcv
                total_status += status
                total_decode_errors += decode_errors
                window_decode_errors += decode_errors

            now = time.time()
            elapsed = now - last_rate
            if elapsed >= args.rate_log:
                print(
                    f"[SMOKE] rate ohlcv/s={window_ohlcv / elapsed:.2f} "
                    f"window_decode_errors={window_decode_errors} total_ohlcv={total_ohlcv}"
                )
                last_rate = now
                window_ohlcv = 0
                window_decode_errors = 0
    finally:
        try:
            for market_type, batch in subscribe_batches:
                send_json(sock, build_unsubscribe(batch, market_type, args.interval))
                if args.subscribe_pause > 0:
                    time.sleep(min(args.subscribe_pause, 0.02))
            send_json(sock, {"action": "disconnect"})
            print("[SMOKE] DISCONNECT_SENT")
            drain_until = time.time() + 2.0
            while time.time() < drain_until:
                events = dict(poller.poll(timeout=100))
                if sock not in events or not (events[sock] & zmq.POLLIN):
                    continue
                frames = sock.recv_multipart()
                payload = frames[-1]
                try:
                    obj = json.loads(payload.decode("utf-8"))
                except Exception:
                    continue
                if isinstance(obj, dict) and obj.get("type") == "ping":
                    send_json(sock, {"message": "pong"})
                elif isinstance(obj, dict) and str(obj.get("type", "")).startswith("stream_"):
                    print_status(obj)
        except Exception:
            pass
        sock.close()
        ctx.term()

    print(
        f"[SMOKE] done total_ohlcv={total_ohlcv} total_status={total_status} "
        f"decode_errors={total_decode_errors}"
    )
    return 0 if total_ohlcv > 0 and total_decode_errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
