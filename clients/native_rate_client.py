#!/usr/bin/env python3
import argparse
import json
import signal
import sys
import time
import uuid

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


def main() -> int:
    parser = argparse.ArgumentParser(description="Simple native ZMQ trade rate client")
    parser.add_argument("--broker", default=default_broker_address(), help="ZMQ broker address")
    parser.add_argument("--market-type", default="spot", choices=["spot", "swap"], help="Market type")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT", help="Comma-separated native symbols")
    parser.add_argument("--exchanges", default="binance_native,bybit_native", help="Comma-separated exchanges")
    parser.add_argument("--encoding", default="msgpack", choices=["json", "msgpack", "binary"], help="Requested broker encoding")
    parser.add_argument("--rate-interval", type=float, default=1.0, help="Seconds between rate prints")
    parser.add_argument("--debug-frames", type=int, default=5, help="Print first N received frames for debugging")
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
        if sock in events and events[sock] == zmq.POLLIN:
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
                continue

            if isinstance(obj, dict) and obj.get("data_type") == "trades":
                msg_count += 1
                trade_count += 1
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
