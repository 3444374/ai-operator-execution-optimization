#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
if [[ -n "$CONFIG_FILE" ]]; then
  if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "config file not found: $CONFIG_FILE" >&2
    exit 2
  fi
  set -a
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
  set +a
fi

: "${MODEL_PATH:?set MODEL_PATH to a downloaded model directory}"
: "${COMPLETION_MODEL:?set COMPLETION_MODEL to the served model name}"

VLLM_VENV=${VLLM_VENV:-/root/autodl-tmp/venvs/vllm-4090}
VLLM_LOG_DIR=${VLLM_LOG_DIR:-/root/autodl-tmp/vllm_logs}
GPU_IDS=${GPU_IDS:-0,1}
PORTS=${PORTS:-8000,8001}
VLLM_HOST=${VLLM_HOST:-127.0.0.1}
VLLM_DTYPE=${VLLM_DTYPE:-auto}
VLLM_MAX_MODEL_LEN=${VLLM_MAX_MODEL_LEN:-2048}
VLLM_GPU_MEMORY_UTILIZATION=${VLLM_GPU_MEMORY_UTILIZATION:-0.90}
VLLM_SCHEDULING_POLICY=${VLLM_SCHEDULING_POLICY:-fcfs}
STOP_MANAGED_ENDPOINTS=${STOP_MANAGED_ENDPOINTS:-0}
export PATH="$VLLM_VENV/bin:$PATH"
PYTHON="$VLLM_VENV/bin/python"

if [[ ! -x "$PYTHON" ]]; then
  echo "vLLM Python executable not found: $PYTHON" >&2
  exit 2
fi
if [[ ! -d "$MODEL_PATH" ]]; then
  echo "model directory not found: $MODEL_PATH" >&2
  exit 2
fi
if [[ -n "${CUDA_NVCC_BIN:-}" ]]; then
  if [[ ! -d "$CUDA_NVCC_BIN" ]]; then
    echo "CUDA_NVCC_BIN directory not found: $CUDA_NVCC_BIN" >&2
    exit 2
  fi
  export PATH="$CUDA_NVCC_BIN:$PATH"
fi

IFS=',' read -r -a GPU_ARRAY <<< "$GPU_IDS"
IFS=',' read -r -a PORT_ARRAY <<< "$PORTS"
read -r -a EXTRA_ARGS <<< "${VLLM_EXTRA_ARGS:-}"
if [[ ${#GPU_ARRAY[@]} -ne ${#PORT_ARRAY[@]} ]]; then
  echo "GPU_IDS and PORTS must contain the same number of entries" >&2
  exit 2
fi

mkdir -p "$VLLM_LOG_DIR"
LAUNCHER=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/code/scripts/serving/launch_vllm_with_identity.py
if [[ ! -f "$LAUNCHER" ]]; then
  echo "vLLM identity launcher not found: $LAUNCHER" >&2
  exit 2
fi

stop_managed_endpoint() {
  local port=$1
  local pid_file="$VLLM_LOG_DIR/ep_${port}.pid"
  local identity_file="$VLLM_LOG_DIR/ep_${port}.runtime_identity.json"
  [[ -f "$pid_file" ]] || return 0
  local pid
  pid=$(<"$pid_file")
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    local command
    command=$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)
    if [[
      "$command" == *"vllm.entrypoints.openai.api_server"*
      && "$command" == *"--port $port"*
    ]]; then
      kill "$pid"
      for _ in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
      done
      if kill -0 "$pid" 2>/dev/null; then
        echo "managed endpoint PID $pid did not stop within 30 seconds" >&2
        exit 2
      fi
    else
      echo \
        "refusing to stop PID $pid: command does not match vLLM port $port" \
        >&2
      exit 2
    fi
  fi
  rm -f "$pid_file" "$identity_file"
}

start_endpoint() {
  local gpu=$1
  local port=$2
  local log_file="$VLLM_LOG_DIR/ep_${port}.log"
  local pid_file="$VLLM_LOG_DIR/ep_${port}.pid"
  local identity_file="$VLLM_LOG_DIR/ep_${port}.runtime_identity.json"
  if curl -sf "http://$VLLM_HOST:$port/health" >/dev/null 2>&1; then
    echo "port $port already serves a healthy endpoint; refusing to replace it" >&2
    exit 2
  fi
  CUDA_VISIBLE_DEVICES="$gpu" nohup "$PYTHON" "$LAUNCHER" \
    --identity-output "$identity_file" \
    --port "$port" -- \
    --model "$MODEL_PATH" \
    --served-model-name "$COMPLETION_MODEL" \
    --dtype "$VLLM_DTYPE" \
    --max-model-len "$VLLM_MAX_MODEL_LEN" \
    --gpu-memory-utilization "$VLLM_GPU_MEMORY_UTILIZATION" \
    --scheduling-policy "$VLLM_SCHEDULING_POLICY" \
    --port "$port" \
    --host "$VLLM_HOST" \
    "${EXTRA_ARGS[@]}" \
    </dev/null >"$log_file" 2>&1 &
  local pid=$!
  printf '%s\n' "$pid" >"$pid_file"
  echo "started gpu=$gpu port=$port pid=$pid log=$log_file"
}

if [[ "$STOP_MANAGED_ENDPOINTS" == "1" ]]; then
  for port in "${PORT_ARRAY[@]}"; do
    stop_managed_endpoint "$port"
  done
fi
for index in "${!GPU_ARRAY[@]}"; do
  start_endpoint "${GPU_ARRAY[$index]}" "${PORT_ARRAY[$index]}"
done

echo "waiting for endpoints (up to ~12 min, first run JIT-compiles kernels)..."
failed=0
for port in "${PORT_ARRAY[@]}"; do
  ok=0
  for i in $(seq 1 240); do
    curl -sf "http://$VLLM_HOST:${port}/health" >/dev/null 2>&1 && {
      echo "port ${port} READY (poll $i)"
      ok=1
      break
    }
    sleep 3
  done
  if [[ "$ok" == "0" ]]; then
    echo "port ${port} NOT READY; tail log:" >&2
    tail -20 "$VLLM_LOG_DIR/ep_${port}.log" >&2
    failed=1
  fi
done
if [[ "$failed" == "1" ]]; then
  exit 1
fi

for port in "${PORT_ARRAY[@]}"; do
  echo "=MODELS $port="
  curl -sf "http://$VLLM_HOST:$port/v1/models"
  echo
done
echo "=GPU PROCS="
nvidia-smi \
  --query-compute-apps=pid,gpu_uuid,used_memory \
  --format=csv,noheader
echo "endpoints ready: model=$COMPLETION_MODEL ports=$PORTS"
