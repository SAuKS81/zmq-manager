#!/usr/bin/env bash
set -euo pipefail

LIMIT=1000
OUT_FILE="scripts/symbols_binance_spot_top1000.txt"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/gen_symbols_binance_spot.sh [--limit 1000] [--out scripts/symbols_binance_spot_top1000.txt]

Generates a Binance spot symbol list in broker format (e.g. BTC/USDT),
sorted by 24h quoteVolume descending.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --limit)
      LIMIT="${2:-}"; shift 2 ;;
    --out)
      OUT_FILE="${2:-}"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "[GEN] unknown arg: $1" >&2
      usage
      exit 2 ;;
  esac
done

if ! [[ "${LIMIT}" =~ ^[0-9]+$ ]] || [[ "${LIMIT}" -le 0 ]]; then
  echo "[GEN] --limit must be a positive integer" >&2
  exit 2
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "[GEN] curl is required" >&2
  exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "[GEN] python3 is required" >&2
  exit 2
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

exchange_info_json="${tmp_dir}/binance_exchange_info.json"
tickers_json="${tmp_dir}/binance_tickers.json"

curl -fsS "https://api.binance.com/api/v3/exchangeInfo" -o "${exchange_info_json}"
curl -fsS "https://api.binance.com/api/v3/ticker/24hr" -o "${tickers_json}"

mkdir -p "$(dirname "${OUT_FILE}")"

python3 - "${exchange_info_json}" "${tickers_json}" "${LIMIT}" "${OUT_FILE}" <<'PY'
import json
import sys

exchange_info_path, tickers_path, limit_raw, out_file = sys.argv[1:]
limit = int(limit_raw)

with open(exchange_info_path, "r", encoding="utf-8") as f:
    exchange_info = json.load(f)
with open(tickers_path, "r", encoding="utf-8") as f:
    tickers = json.load(f)

quote_volume_by_symbol = {}
for row in tickers:
    sym = row.get("symbol")
    if not sym:
        continue
    try:
        quote_volume = float(row.get("quoteVolume", "0"))
    except Exception:
        quote_volume = 0.0
    quote_volume_by_symbol[sym] = quote_volume

eligible = []
for row in exchange_info.get("symbols", []):
    if row.get("status") != "TRADING":
        continue
    if row.get("quoteAsset") != "USDT":
        continue
    base = row.get("baseAsset")
    symbol = row.get("symbol")
    if not base or not symbol:
        continue
    eligible.append((quote_volume_by_symbol.get(symbol, 0.0), f"{base}/USDT"))

eligible.sort(key=lambda x: x[0], reverse=True)
top = eligible[:limit]

with open(out_file, "w", encoding="utf-8") as f:
    for _, sym in top:
        f.write(sym + "\n")

print(f"[GEN] wrote {len(top)} symbols to {out_file}")
PY
