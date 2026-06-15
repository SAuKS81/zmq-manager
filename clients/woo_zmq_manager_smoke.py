#!/usr/bin/env python3
import argparse
import json
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import msgpack
import zmq


def default_broker_address() -> str:
    if sys.platform.startswith("linux"):
        return "ipc:///tmp/feed_broker.ipc"
    return "tcp://127.0.0.1:5555"


def detect_header(frames: list[bytes]) -> bytes:
    if len(frames) < 2:
        return b""
    for frame in reversed(frames[:-1]):
        if len(frame) == 1 and frame in (b"T", b"J"):
            return frame
    return b""


def format_ts_ms(ts_ms: int | None) -> str:
    if not ts_ms:
        return "-"
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone()
    return dt.strftime("%H:%M:%S.%f")[:-3]


def normalize_trade(trade) -> dict:
    if isinstance(trade, dict):
        return {
            "exchange": trade.get("exchange", trade.get("e")),
            "symbol": trade.get("symbol", trade.get("s")),
            "timestamp": trade.get("timestamp", trade.get("t")),
            "price": trade.get("price", trade.get("p")),
            "amount": trade.get("amount", trade.get("a")),
            "side": trade.get("side"),
        }
    if isinstance(trade, (list, tuple)) and len(trade) >= 8:
        return {
            "exchange": trade[0],
            "symbol": trade[1],
            "timestamp": trade[3],
            "side": trade[5],
            "price": trade[6],
            "amount": trade[7],
        }
    return {}


def print_trade(trade) -> None:
    item = normalize_trade(trade)
    ts = format_ts_ms(item.get("timestamp"))
    exchange = item.get("exchange") or "?"
    symbol = item.get("symbol") or "?"
    side = item.get("side") or "?"
    price = item.get("price") or "?"
    amount = item.get("amount") or "?"
    print(f"[TRADE] {ts} {exchange} {symbol} {side} price={price} amount={amount}")


def build_subscribe(exchange: str, market_type: str, symbols: list[str], encoding: str) -> dict:
    return {
        "action": "subscribe_bulk",
        "exchange": exchange,
        "market_type": market_type,
        "symbols": symbols,
        "data_type": "trades",
        "encoding": encoding,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke client for WOO X native trades via zmq_manager")
    parser.add_argument("--broker", default=default_broker_address(), help="ZMQ broker address")
    parser.add_argument("--exchange", default="woo_native", choices=["woo_native", "woox_native"], help="Native WOO exchange id")
    parser.add_argument("--market-type", default="spot", choices=["spot", "swap"], help="Market type")
    parser.add_argument("--symbols", default="BTC/USDT,ETH/USDT", help="Comma-separated CCXT unified symbols")
    parser.add_argument("--encoding", default="msgpack", choices=["json", "msgpack"], help="Requested broker encoding")
    parser.add_argument("--duration", default="60s", help="Run duration, e.g. 30s, 2m, 0 for until Ctrl+C")
    parser.add_argument("--print-trades", action="store_true", help="Print decoded trade rows")
    parser.add_argument("--debug-frames", type=int, default=3, help="Print first N raw frame previews")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("symbols must not be empty")

    duration_sec = parse_duration(args.duration)
    deadline = time.time() + duration_sec if duration_sec > 0 else None

    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, f"woo-native-smoke-{uuid.uuid4().hex}".encode())
    sock.connect(args.broker)

    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    sock.send_json(build_subscribe(args.exchange, args.market_type, symbols, args.encoding))
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    print(
        f"[SMOKE] broker={args.broker} exchange={args.exchange} market_type={args.market_type} "
        f"symbols={','.join(symbols)} encoding={args.encoding} duration={args.duration}"
    )

    debug_left = max(args.debug_frames, 0)
    window_start = time.time()
    total_trades = 0
    window_trades = 0
    decode_errors = 0
    status_count = 0

    while not stop:
        if deadline is not None and time.time() >= deadline:
            break

        events = dict(poller.poll(timeout=250))
        if sock not in events:
            print_rate_if_due(window_start, window_trades, total_trades, decode_errors)
            now = time.time()
            if now - window_start >= 5:
                window_start = now
                window_trades = 0
            continue

        frames = sock.recv_multipart()
        if debug_left > 0:
            debug_left -= 1
            print("[SMOKE] debug frames=" + str([frame[:100] for frame in frames]))

        header = detect_header(frames)
        payload = frames[-1]

        try:
            if header == b"T" and args.encoding == "msgpack":
                obj = msgpack.unpackb(payload, raw=False)
            else:
                obj = json.loads(payload.decode("utf-8"))
        except Exception as exc:
            decode_errors += 1
            print(f"[SMOKE] decode failed header={header!r} err={exc} payload_prefix={payload[:120]!r}")
            continue

        if isinstance(obj, dict) and obj.get("type") == "ping":
            sock.send_json({"message": "pong"})
            continue

        if isinstance(obj, dict) and str(obj.get("type", "")).startswith("stream_"):
            status_count += 1
            print(
                f"[SMOKE] status type={obj.get('type')} status={obj.get('status')} "
                f"exchange={obj.get('exchange')} market_type={obj.get('market_type')} "
                f"symbol={obj.get('symbol')} reason={obj.get('reason')}"
            )
            continue

        trades = obj if isinstance(obj, list) else [obj]
        total_trades += len(trades)
        window_trades += len(trades)
        if args.print_trades:
            for trade in trades:
                print_trade(trade)

        now = time.time()
        if now - window_start >= 5:
            print_rate(window_start, now, window_trades, total_trades, decode_errors)
            window_start = now
            window_trades = 0

    try:
        sock.send_json({"action": "disconnect"})
        print("[SMOKE] disconnect sent")
    except Exception:
        pass
    finally:
        sock.close(0)
        ctx.term()

    print(f"[SMOKE] done total_trades={total_trades} status={status_count} decode_errors={decode_errors}")
    return 0 if total_trades > 0 and decode_errors == 0 else 1


def parse_duration(value: str) -> float:
    value = value.strip().lower()
    if value in {"0", "0s", "none"}:
        return 0
    multiplier = 1.0
    if value.endswith("ms"):
        multiplier = 0.001
        value = value[:-2]
    elif value.endswith("s"):
        value = value[:-1]
    elif value.endswith("m"):
        multiplier = 60.0
        value = value[:-1]
    return float(value) * multiplier


def print_rate_if_due(window_start: float, window_trades: int, total_trades: int, decode_errors: int) -> None:
    now = time.time()
    if now - window_start >= 5:
        print_rate(window_start, now, window_trades, total_trades, decode_errors)


def print_rate(window_start: float, now: float, window_trades: int, total_trades: int, decode_errors: int) -> None:
    elapsed = max(now - window_start, 0.001)
    print(
        f"[SMOKE] rate trades/s={window_trades / elapsed:.2f} "
        f"total_trades={total_trades} decode_errors={decode_errors}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
