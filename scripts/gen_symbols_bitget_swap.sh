#!/usr/bin/env bash
set -euo pipefail

LIMIT=1000
OUT_FILE="scripts/symbols_bitget_swap_top1000.txt"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/gen_symbols_bitget_swap.sh [--limit 1000] [--out scripts/symbols_bitget_swap_top1000.txt]

Generates a Bitget USDT perpetual symbol list in broker format (e.g. BTC/USDT:USDT),
sorted by 24h usdtVolume descending.
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

contracts_json="${tmp_dir}/bitget_swap_contracts.json"
tickers_json="${tmp_dir}/bitget_swap_tickers.json"

curl -fsS "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES" -o "${contracts_json}"
curl -fsS "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES" -o "${tickers_json}"

mkdir -p "$(dirname "${OUT_FILE}")"

python3 - "${contracts_json}" "${tickers_json}" "${LIMIT}" "${OUT_FILE}" <<'PY'
import json
import sys

contracts_path, tickers_path, limit_raw, out_file = sys.argv[1:]
limit = int(limit_raw)

with open(contracts_path, "r", encoding="utf-8") as f:
    contracts_doc = json.load(f)
with open(tickers_path, "r", encoding="utf-8") as f:
    tickers_doc = json.load(f)

vol_by_symbol = {}
for row in tickers_doc.get("data", []):
    sym = row.get("symbol")
    if not sym:
        continue
    try:
        vol = float(row.get("usdtVolume", "0"))
    except Exception:
        vol = 0.0
    vol_by_symbol[sym] = vol

eligible = []
for row in contracts_doc.get("data", []):
    if row.get("quoteCoin") != "USDT":
        continue
    base = row.get("baseCoin")
    symbol = row.get("symbol")
    if not base or not symbol:
        continue
    eligible.append((vol_by_symbol.get(symbol, 0.0), f"{base}/USDT:USDT"))

eligible.sort(key=lambda x: x[0], reverse=True)
top = eligible[:limit]

with open(out_file, "w", encoding="utf-8") as f:
    for _, sym in top:
        f.write(sym + "\n")

print(f"[GEN] wrote {len(top)} symbols to {out_file}")
PY

