#!/usr/bin/env bash
set -euo pipefail

RUN_NAME=""
SYMBOLS_FILE=""
SYMBOLS_LIMIT=""
SYMBOLS_FILE_SPOT=""
SYMBOLS_FILE_SWAP=""
SYMBOLS_LIMIT_SPOT=""
SYMBOLS_LIMIT_SWAP=""
EXCHANGES=""
DURATION="60s"
PPROF_ENDPOINT="http://127.0.0.1:6060"
BROKER_ENDPOINT="ipc:///tmp/feed_broker.ipc"
ENCODING="msgpack"
OB_DEPTH="5"
SUBSCRIBE_PAUSE="1ms"
MARKET_TYPE="spot"
MARKET_TYPES=""
BULK_SIZE="100"

RUN_DIR=""
SMOKE_PID=""
BUNDLE_DONE=0

create_bundle() {
  if [[ -z "${RUN_DIR}" || ! -d "${RUN_DIR}" ]]; then
    return
  fi
  local tmp_bundle
  tmp_bundle="${RUN_DIR}.bundle.tmp.tar.gz"
  tar -C "${RUN_DIR}" -czf "${tmp_bundle}" . >/dev/null 2>&1 || return
  mv -f "${tmp_bundle}" "${RUN_DIR}/bundle.tar.gz"
}

usage() {
  cat <<'EOF'
Usage:
  ./scripts/baseline_ingest.sh \
    --run-name <name> \
    [--symbols-file <path>] \
    --symbols-limit <n> \
    [--symbols-file-spot <path>] \
    [--symbols-file-swap <path>] \
    [--symbols-limit-spot <n>] \
    [--symbols-limit-swap <n>] \
    --exchanges <csv> \
    [--duration 60s] \
    [--pprof-endpoint http://127.0.0.1:6060] \
    [--broker-endpoint ipc:///tmp/feed_broker.ipc] \
    [--encoding msgpack] \
    [--ob-depth 5] \
    [--subscribe-pause 1ms] \
    [--market-type spot|swap] \
    [--market-types spot,swap] \
    [--bulk-size 100]
EOF
}

cleanup() {
  if [[ -n "${SMOKE_PID}" ]]; then
    if kill -0 "${SMOKE_PID}" 2>/dev/null; then
      kill "${SMOKE_PID}" 2>/dev/null || true
    fi
    wait "${SMOKE_PID}" 2>/dev/null || true
  fi

  if [[ -n "${RUN_DIR}" && -d "${RUN_DIR}" && "${BUNDLE_DONE}" -eq 0 ]]; then
    create_bundle || true
    BUNDLE_DONE=1
  fi
}
trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-name)
      RUN_NAME="${2:-}"; shift 2 ;;
    --symbols-file)
      SYMBOLS_FILE="${2:-}"; shift 2 ;;
    --symbols-limit)
      SYMBOLS_LIMIT="${2:-}"; shift 2 ;;
    --symbols-file-spot)
      SYMBOLS_FILE_SPOT="${2:-}"; shift 2 ;;
    --symbols-file-swap)
      SYMBOLS_FILE_SWAP="${2:-}"; shift 2 ;;
    --symbols-limit-spot)
      SYMBOLS_LIMIT_SPOT="${2:-}"; shift 2 ;;
    --symbols-limit-swap)
      SYMBOLS_LIMIT_SWAP="${2:-}"; shift 2 ;;
    --exchanges)
      EXCHANGES="${2:-}"; shift 2 ;;
    --duration)
      DURATION="${2:-}"; shift 2 ;;
    --pprof-endpoint)
      PPROF_ENDPOINT="${2:-}"; shift 2 ;;
    --broker-endpoint)
      BROKER_ENDPOINT="${2:-}"; shift 2 ;;
    --encoding)
      ENCODING="${2:-}"; shift 2 ;;
    --ob-depth)
      OB_DEPTH="${2:-}"; shift 2 ;;
    --subscribe-pause)
      SUBSCRIBE_PAUSE="${2:-}"; shift 2 ;;
    --market-type)
      MARKET_TYPE="${2:-}"; shift 2 ;;
    --market-types)
      MARKET_TYPES="${2:-}"; shift 2 ;;
    --bulk-size)
      BULK_SIZE="${2:-}"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "[BASELINE] unknown arg: $1" >&2
      usage
      exit 2 ;;
  esac
done

if [[ -z "${RUN_NAME}" || -z "${SYMBOLS_LIMIT}" || -z "${EXCHANGES}" ]]; then
  echo "[BASELINE] missing required args" >&2
  usage
  exit 2
fi

if [[ "${MARKET_TYPE}" != "spot" && "${MARKET_TYPE}" != "swap" ]]; then
  echo "[BASELINE] invalid --market-type: ${MARKET_TYPE} (allowed: spot|swap)" >&2
  exit 2
fi

if [[ -n "${MARKET_TYPES}" ]]; then
  for mt in $(echo "${MARKET_TYPES}" | tr ',' ' '); do
    if [[ "${mt}" != "spot" && "${mt}" != "swap" ]]; then
      echo "[BASELINE] invalid market type in --market-types: ${mt} (allowed: spot|swap)" >&2
      exit 2
    fi
  done
fi

if [[ -z "${SYMBOLS_FILE}" && -z "${SYMBOLS_FILE_SPOT}" && -z "${SYMBOLS_FILE_SWAP}" ]]; then
  echo "[BASELINE] missing symbols source: provide --symbols-file and/or market-specific files" >&2
  exit 2
fi

if [[ -n "${SYMBOLS_FILE}" && ! -f "${SYMBOLS_FILE}" ]]; then
  echo "[BASELINE] symbols file not found: ${SYMBOLS_FILE}" >&2
  exit 2
fi

if [[ -n "${SYMBOLS_FILE_SPOT}" && ! -f "${SYMBOLS_FILE_SPOT}" ]]; then
  echo "[BASELINE] symbols file not found: ${SYMBOLS_FILE_SPOT}" >&2
  exit 2
fi

if [[ -n "${SYMBOLS_FILE_SWAP}" && ! -f "${SYMBOLS_FILE_SWAP}" ]]; then
  echo "[BASELINE] symbols file not found: ${SYMBOLS_FILE_SWAP}" >&2
  exit 2
fi

PPROF_ENDPOINT="${PPROF_ENDPOINT%/}"

# 1) Validate pprof endpoint reachable (fail fast)
curl -fsS "${PPROF_ENDPOINT}/debug/pprof/" >/dev/null

# 2) Create run directory
RUN_TS="$(date -u +"%Y-%m-%dT%H-%M-%SZ")"
BASE_DIR="${HOME}/pprof_runs"
RUN_DIR="${BASE_DIR}/${RUN_TS}_${RUN_NAME}"
mkdir -p "${RUN_DIR}"

# 3) Snapshot PRE metrics
curl -fsS "${PPROF_ENDPOINT}/metrics" -o "${RUN_DIR}/metrics_pre.txt"

# 4) Start smoke client in background (nohup)
SMOKE_MARKET_FLAG=(--market-type="${MARKET_TYPE}")
if [[ -n "${MARKET_TYPES}" ]]; then
  SMOKE_MARKET_FLAG=(--market-types="${MARKET_TYPES}")
fi

SMOKE_SYMBOL_FLAGS=(--symbols-file="${SYMBOLS_FILE}" --symbols-limit="${SYMBOLS_LIMIT}")
if [[ -n "${SYMBOLS_FILE_SPOT}" ]]; then
  SMOKE_SYMBOL_FLAGS+=(--symbols-file-spot="${SYMBOLS_FILE_SPOT}")
fi
if [[ -n "${SYMBOLS_FILE_SWAP}" ]]; then
  SMOKE_SYMBOL_FLAGS+=(--symbols-file-swap="${SYMBOLS_FILE_SWAP}")
fi
if [[ -n "${SYMBOLS_LIMIT_SPOT}" ]]; then
  SMOKE_SYMBOL_FLAGS+=(--symbols-limit-spot="${SYMBOLS_LIMIT_SPOT}")
fi
if [[ -n "${SYMBOLS_LIMIT_SWAP}" ]]; then
  SMOKE_SYMBOL_FLAGS+=(--symbols-limit-swap="${SYMBOLS_LIMIT_SWAP}")
fi

touch "${RUN_DIR}/smoke.log"

nohup go run ./clients/smoke_client.go \
  --exchanges="${EXCHANGES}" \
  "${SMOKE_SYMBOL_FLAGS[@]}" \
  --trades=true \
  --orderbooks=true \
  --ob-depth="${OB_DEPTH}" \
  --bulk-size="${BULK_SIZE}" \
  --duration="${DURATION}" \
  --encoding="${ENCODING}" \
  --broker="${BROKER_ENDPOINT}" \
  --subscribe-pause="${SUBSCRIBE_PAUSE}" \
  "${SMOKE_MARKET_FLAG[@]}" \
  --rate-log=10s \
  >"${RUN_DIR}/smoke.log" 2>&1 &
SMOKE_PID=$!

# 5) Wait until subscribe phase is complete
for _ in $(seq 1 600); do
  if grep -qs "SUBSCRIBE_DONE" "${RUN_DIR}/smoke.log"; then
    break
  fi
  sleep 1
done

if ! grep -qs "SUBSCRIBE_DONE" "${RUN_DIR}/smoke.log"; then
  echo "[BASELINE] smoke client did not reach SUBSCRIBE_DONE within timeout" >&2
  exit 1
fi

SUBSCRIBE_DONE_LINE="$(grep "SUBSCRIBE_DONE" "${RUN_DIR}/smoke.log" | tail -n1)"
EFFECTIVE_SYMBOLS_TOTAL="$(echo "${SUBSCRIBE_DONE_LINE}" | sed -n 's/.*symbols=\([0-9][0-9]*\).*/\1/p')"
SUBSCRIBE_REQUESTS="$(echo "${SUBSCRIBE_DONE_LINE}" | sed -n 's/.*requests=\([0-9][0-9]*\).*/\1/p')"
if [[ -z "${EFFECTIVE_SYMBOLS_TOTAL}" ]]; then
  EFFECTIVE_SYMBOLS_TOTAL=0
fi
if [[ -z "${SUBSCRIBE_REQUESTS}" ]]; then
  SUBSCRIBE_REQUESTS=0
fi

# 6) Fixed warmup after steady-state subscription setup
sleep 10

# 7) CPU profile 30s
curl -fsS "${PPROF_ENDPOINT}/debug/pprof/profile?seconds=30" -o "${RUN_DIR}/cpu.pprof"

# 8) Heap profile
curl -fsS "${PPROF_ENDPOINT}/debug/pprof/heap" -o "${RUN_DIR}/heap.pprof"

# 9) Allocs profile
curl -fsS "${PPROF_ENDPOINT}/debug/pprof/allocs" -o "${RUN_DIR}/allocs.pprof"

# 10) Snapshot POST metrics
curl -fsS "${PPROF_ENDPOINT}/metrics" -o "${RUN_DIR}/metrics_post.txt"

# 11) Stop smoke client cleanly
if kill -0 "${SMOKE_PID}" 2>/dev/null; then
  kill "${SMOKE_PID}" 2>/dev/null || true
fi
wait "${SMOKE_PID}" 2>/dev/null || true
SMOKE_PID=""

# Gate check: drops delta detected?
DROPS_PRE_TOTAL="$(awk '/^zmq_dropped_messages_total\{/ {sum += $NF} END {if (sum == "") sum = 0; printf "%.0f", sum}' "${RUN_DIR}/metrics_pre.txt")"
DROPS_POST_TOTAL="$(awk '/^zmq_dropped_messages_total\{/ {sum += $NF} END {if (sum == "") sum = 0; printf "%.0f", sum}' "${RUN_DIR}/metrics_post.txt")"
DROPS_TOTAL=$((DROPS_POST_TOTAL - DROPS_PRE_TOTAL))
if [[ "${DROPS_TOTAL}" -lt 0 ]]; then
  DROPS_TOTAL=0
fi

DROPS_TRADE_PRE="$(awk '/^zmq_dropped_messages_total\{.*type="trade"/ {sum += $NF} END {if (sum == "") sum = 0; printf "%.0f", sum}' "${RUN_DIR}/metrics_pre.txt")"
DROPS_TRADE_POST="$(awk '/^zmq_dropped_messages_total\{.*type="trade"/ {sum += $NF} END {if (sum == "") sum = 0; printf "%.0f", sum}' "${RUN_DIR}/metrics_post.txt")"
DROPS_TRADE_DELTA=$((DROPS_TRADE_POST - DROPS_TRADE_PRE))
if [[ "${DROPS_TRADE_DELTA}" -lt 0 ]]; then
  DROPS_TRADE_DELTA=0
fi

DROPS_OB_PRE="$(awk '/^zmq_dropped_messages_total\{.*type="ob_update"/ {sum += $NF} END {if (sum == "") sum = 0; printf "%.0f", sum}' "${RUN_DIR}/metrics_pre.txt")"
DROPS_OB_POST="$(awk '/^zmq_dropped_messages_total\{.*type="ob_update"/ {sum += $NF} END {if (sum == "") sum = 0; printf "%.0f", sum}' "${RUN_DIR}/metrics_post.txt")"
DROPS_OB_DELTA=$((DROPS_OB_POST - DROPS_OB_PRE))
if [[ "${DROPS_OB_DELTA}" -lt 0 ]]; then
  DROPS_OB_DELTA=0
fi

GATE_STATUS="PASS"
if [[ "${DROPS_TOTAL}" -gt 0 ]]; then
  GATE_STATUS="FAIL_DROPS_DETECTED"
fi

QUEUE_HWM_JSON="$(awk '
  BEGIN { first = 1; printf "{\n" }
  /^zmq_queue_high_watermark\{queue="/ {
    key = $1
    sub(/^zmq_queue_high_watermark\{queue="/, "", key)
    sub(/"\}$/, "", key)
    val = $NF + 0
    if (!first) {
      printf ",\n"
    }
    first = 0
    printf "    \"%s\": %.0f", key, val
  }
  END {
    if (first) {
      printf "    "
    }
    printf "\n  }"
  }
' "${RUN_DIR}/metrics_post.txt")"

if GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null)"; then
  :
else
  GIT_COMMIT="unknown"
fi

HOSTNAME_VALUE="$(hostname 2>/dev/null || echo unknown)"
GO_VERSION_VALUE="$(go version 2>/dev/null || echo unknown)"

cat >"${RUN_DIR}/meta.json" <<EOF
{
  "timestamp": "${RUN_TS}",
  "run_name": "${RUN_NAME}",
  "symbols_limit": ${SYMBOLS_LIMIT},
  "exchanges": "${EXCHANGES}",
  "duration": "${DURATION}",
  "market_type": "${MARKET_TYPE}",
  "market_types": "${MARKET_TYPES}",
  "effective_symbols_total": ${EFFECTIVE_SYMBOLS_TOTAL},
  "subscribe_requests": ${SUBSCRIBE_REQUESTS},
  "bulk_size": ${BULK_SIZE},
  "git_commit": "${GIT_COMMIT}",
  "hostname": "${HOSTNAME_VALUE}",
  "go_version": "${GO_VERSION_VALUE}",
  "gate_status": "${GATE_STATUS}",
  "drops_delta_total": ${DROPS_TOTAL},
  "drops_delta_trade": ${DROPS_TRADE_DELTA},
  "drops_delta_ob_update": ${DROPS_OB_DELTA},
  "queue_hwm": ${QUEUE_HWM_JSON}
}
EOF

# 12) Bundle artifacts
create_bundle
BUNDLE_DONE=1

echo "[BASELINE] run complete: ${RUN_DIR}"
echo "[BASELINE] gate_status=${GATE_STATUS} drops_delta_total=${DROPS_TOTAL} trade=${DROPS_TRADE_DELTA} ob_update=${DROPS_OB_DELTA}"
echo "[BASELINE] bundle=${RUN_DIR}/bundle.tar.gz"
