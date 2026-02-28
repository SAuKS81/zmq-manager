#!/usr/bin/env bash
set -euo pipefail

LIMIT=1000
OUT_FILE="scripts/symbols_bybit_swap_top1000.txt"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/gen_symbols_bybit_swap.sh [--limit 1000] [--out scripts/symbols_bybit_swap_top1000.txt]

Generates a Bybit USDT-perpetual symbol list in broker format (e.g. BTC/USDT:USDT),
sorted by 24h turnover descending.
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

instruments_json="${tmp_dir}/bybit_swap_instruments.json"
tickers_json="${tmp_dir}/bybit_swap_tickers.json"

curl -fsS "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000" -o "${instruments_json}"
curl -fsS "https://api.bybit.com/v5/market/tickers?category=linear" -o "${tickers_json}"

mkdir -p "$(dirname "${OUT_FILE}")"

python3 - "${instruments_json}" "${tickers_json}" "${LIMIT}" "${OUT_FILE}" <<'PY'
import json
import sys

instruments_path, tickers_path, limit_raw, out_file = sys.argv[1:]
limit = int(limit_raw)

with open(instruments_path, "r", encoding="utf-8") as f:
    instruments = json.load(f)
with open(tickers_path, "r", encoding="utf-8") as f:
    tickers = json.load(f)

turnover_by_symbol = {}
for row in tickers.get("result", {}).get("list", []):
    sym = row.get("symbol")
    if not sym:
        continue
    try:
        turnover = float(row.get("turnover24h", "0"))
    except Exception:
        turnover = 0.0
    turnover_by_symbol[sym] = turnover

eligible = []
for row in instruments.get("result", {}).get("list", []):
    if row.get("status") != "Trading":
        continue
    if row.get("contractType") != "LinearPerpetual":
        continue
    quote = row.get("quoteCoin")
    if quote != "USDT":
        continue
    base = row.get("baseCoin")
    symbol = row.get("symbol")
    if not base or not symbol:
        continue
    ccxt_symbol = f"{base}/USDT:USDT"
    eligible.append((turnover_by_symbol.get(symbol, 0.0), ccxt_symbol))

eligible.sort(key=lambda x: x[0], reverse=True)
top = eligible[:limit]

with open(out_file, "w", encoding="utf-8") as f:
    for _, sym in top:
        f.write(sym + "\n")

print(f"[GEN] wrote {len(top)} symbols to {out_file}")
PY

