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


def send_json(sock: zmq.Socket, payload: dict) -> None:
    sock.send_json(payload)


def detect_header(frames: list[bytes]) -> bytes:
    if len(frames) < 2:
        return b""
    # ROUTER/DEALER interoperability can introduce an extra empty frame.
    # Use the last one-byte non-empty frame before the payload as header.
    for frame in reversed(frames[:-1]):
        if len(frame) == 1 and frame in (b"T", b"O", b"J"):
            return frame
    return b""


def build_requests(exchanges: list[str], symbols: list[str], market_type: str, encoding: str) -> list[dict]:
    requests = []
    for exchange in exchanges:
        requests.append(
            {
                "action": "subscribe_bulk",
                "exchange": exchange,
                "symbols": symbols,
                "market_type": market_type,
                "data_type": "trades",
                "encoding": encoding,
            }
        )
    return requests


def format_ts_ms(ts_ms: int | None) -> str:
    if not ts_ms:
        return "-"
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone()
    return dt.strftime("%H:%M:%S.%f")[:-3]


def normalize_trade_obj(trade) -> dict:
    if isinstance(trade, dict):
        if "timestamp" in trade or "exchange" in trade or "symbol" in trade or "price" in trade:
            return trade
        # msgpack tags from Go struct:
        # e=exchange, s=symbol, t=timestamp, p=price
        return {
            "timestamp": trade.get("timestamp", trade.get("t")),
            "exchange": trade.get("exchange", trade.get("e")),
            "symbol": trade.get("symbol", trade.get("s")),
            "price": trade.get("price", trade.get("p")),
        }
    if isinstance(trade, (list, tuple)) and len(trade) >= 8:
        # Fallback if msgpack decoder returns positional arrays.
        return {
            "exchange": trade[0],
            "symbol": trade[1],
            "timestamp": trade[3],
            "price": trade[6],
        }
    return {}


def print_trade_row(trade: dict) -> None:
    trade = normalize_trade_obj(trade)
    ts = format_ts_ms(trade.get("timestamp"))
    exchange = trade.get("exchange") or "?"
    symbol = trade.get("symbol") or "?"
    price = trade.get("price") or "?"
    print(f"[TRADE] {ts} {exchange} {symbol} {price}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Simple native ZMQ trade rate client")
    parser.add_argument("--broker", default=default_broker_address(), help="ZMQ broker address")
    parser.add_argument("--market-type", default="spot", choices=["spot", "swap"], help="Market type")
    parser.add_argument("--symbols", default="ETH/USDT", help="Comma-separated CCXT unified symbols")
    parser.add_argument("--exchanges", default="bybit_native", help="Comma-separated exchanges")
    parser.add_argument("--encoding", default="msgpack", choices=["json", "msgpack", "binary"], help="Requested broker encoding")
    parser.add_argument("--rate-interval", type=float, default=1.0, help="Seconds between rate prints")
    parser.add_argument("--debug-frames", type=int, default=5, help="Print first N received frames for debugging")
    parser.add_argument("--print-trades", action="store_true", help="Print each received trade")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    exchanges = [e.strip() for e in args.exchanges.split(",") if e.strip()]
    if not symbols or not exchanges:
        raise SystemExit("symbols and exchanges must not be empty")

    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, f"native-rate-{uuid.uuid4().hex}".encode())
    sock.connect(args.broker)

    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    for req in build_requests(exchanges, symbols, args.market_type, args.encoding):
        send_json(sock, req)

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    print(
        f"[CLIENT] broker={args.broker} exchanges={','.join(exchanges)} symbols={','.join(symbols)} "
        f"market_type={args.market_type} encoding={args.encoding}"
    )

    interval_start = time.time()
    msg_count = 0
    trade_count = 0
    debug_frames_left = max(args.debug_frames, 0)

    while not stop:
        events = dict(poller.poll(timeout=250))
        if sock in events and (events[sock] & zmq.POLLIN):
            frames = sock.recv_multipart()
            if debug_frames_left > 0:
                debug_frames_left -= 1
                print(
                    "[CLIENT] debug frames="
                    + str([frame[:80] for frame in frames])
                )

            header = detect_header(frames)
            payload = frames[-1]

            if len(header) == 1 and header == b"T":
                try:
                    obj = msgpack.unpackb(payload, raw=False)
                except Exception as exc:
                    print(f"[CLIENT] msgpack trade decode failed: {exc}")
                    continue
                if isinstance(obj, list):
                    msg_count += 1
                    trade_count += len(obj)
                    if args.print_trades:
                        for trade in obj:
                            print_trade_row(trade)
                continue

            if len(header) == 1 and header == b"O":
                # Ignore OB payloads for this trade-rate client.
                continue

            try:
                obj = json.loads(payload.decode("utf-8"))
            except Exception as exc:
                print(f"[CLIENT] json decode failed: {exc} header={header!r} payload_prefix={payload[:120]!r}")
                continue

            if isinstance(obj, dict) and obj.get("type") == "ping":
                send_json(sock, {"message": "pong"})
                continue

            if isinstance(obj, dict) and obj.get("type") == "error":
                print(f"[CLIENT] error code={obj.get('code')} message={obj.get('message')}")
                continue

            if isinstance(obj, dict) and str(obj.get("type", "")).startswith("stream_"):
                print(
                    f"[CLIENT] status type={obj.get('type')} exchange={obj.get('exchange')} "
                    f"market_type={obj.get('market_type')} symbol={obj.get('symbol')} reason={obj.get('reason')}"
                )
                continue

            if isinstance(obj, list):
                msg_count += 1
                trade_count += len(obj)
                if args.print_trades:
                    for trade in obj:
                        print_trade_row(trade)
                continue

            if isinstance(obj, dict) and obj.get("data_type") == "trades":
                msg_count += 1
                trade_count += 1
                if args.print_trades:
                    print_trade_row(obj)
                continue

            print(f"[CLIENT] unhandled payload type={type(obj).__name__} header={header!r}")

        now = time.time()
        elapsed = now - interval_start
        if elapsed >= args.rate_interval:
            print(f"[CLIENT] msg/s={msg_count / elapsed:.2f} trades/s={trade_count / elapsed:.2f}")
            interval_start = now
            msg_count = 0
            trade_count = 0

    try:
        send_json(sock, {"action": "disconnect"})
    except Exception:
        pass
    finally:
        sock.close(0)
        ctx.term()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
