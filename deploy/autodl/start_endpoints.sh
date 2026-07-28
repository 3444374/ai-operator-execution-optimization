#!/usr/bin/env bash
set -u
source /root/autodl-tmp/venvs/vllm-4090/bin/activate
NVCC=/root/autodl-tmp/venvs/vllm-4090/lib/python3.12/site-packages/nvidia/cuda_nvcc/bin
export PATH="$NVCC:$PATH"
MODEL=/root/autodl-tmp/models/Qwen2.5-1.5B-Instruct
LOG=/root/autodl-tmp/vllm_logs
mkdir -p "$LOG"
start_ep() {
  CUDA_VISIBLE_DEVICES=$1 nohup python -m vllm.entrypoints.openai.api_server \
    --model "$MODEL" --served-model-name qwen2.5-1.5b --dtype auto \
    --max-model-len 2048 --gpu-memory-utilization 0.9 \
    --no-enable-prefix-caching --enable-mfu-metrics \
    --port $2 --host 127.0.0.1 \
    </dev/null >"$LOG/ep_$2.log" 2>&1 &
  echo "started gpu=$1 port=$2 pid=$!"
}
pkill -f "vllm.entrypoints" 2>/dev/null; sleep 2
start_ep 0 8000
start_ep 1 8001
echo "waiting for endpoints (up to ~12 min, first run JIT-compiles kernels)..."
for port in 8000 8001; do
  ok=0
  for i in $(seq 1 240); do
    curl -sf "http://127.0.0.1:${port}/health" >/dev/null 2>&1 && { echo "port ${port} READY (poll $i)"; ok=1; break; }
    sleep 3
  done
  [ "$ok" = "0" ] && echo "port ${port} NOT READY; tail log:" && tail -20 "$LOG/ep_${port}.log"
done
echo "=SMOKE 8000="; curl -s http://127.0.0.1:8000/v1/completions -H 'Content-Type: application/json' -d '{"model":"qwen2.5-1.5b","prompt":"Say one English word:","max_tokens":8,"temperature":0}'; echo
echo "=SMOKE 8001="; curl -s http://127.0.0.1:8001/v1/completions -H 'Content-Type: application/json' -d '{"model":"qwen2.5-1.5b","prompt":"Say one English word:","max_tokens":8,"temperature":0}'; echo
echo "=GPU PROCS="; nvidia-smi --query-compute-apps=pid,gpu_uuid,used_memory --format=csv,noheader
echo EP_DONE
