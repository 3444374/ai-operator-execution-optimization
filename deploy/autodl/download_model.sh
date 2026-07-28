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

: "${MODEL_ID:?set MODEL_ID, for example Qwen/Qwen2.5-7B-Instruct}"
: "${MODEL_DIR:?set MODEL_DIR on /root/autodl-tmp}"

VLLM_VENV=${VLLM_VENV:-/root/autodl-tmp/venvs/vllm-4090}
HF_HOME=${HF_HOME:-/root/autodl-tmp/huggingface}
TURBO_SCRIPT=${TURBO_SCRIPT:-/etc/network_turbo}
PYTHON=${PYTHON:-"$VLLM_VENV/bin/python"}

if [[ ! -f "$TURBO_SCRIPT" ]]; then
  echo "AutoDL academic acceleration script not found: $TURBO_SCRIPT" >&2
  exit 2
fi
if [[ ! -x "$PYTHON" ]]; then
  echo "Python executable not found: $PYTHON" >&2
  exit 2
fi

# AutoDL academic acceleration is shell-local, so every download invocation
# activates it explicitly instead of relying on a previous interactive session.
# shellcheck disable=SC1090
source "$TURBO_SCRIPT" >/dev/null 2>&1
export HF_HOME
export HF_HUB_DISABLE_XET=1

mkdir -p "$HF_HOME" "$MODEL_DIR"
echo "downloading $MODEL_ID -> $MODEL_DIR"
"$PYTHON" - "$MODEL_ID" "$MODEL_DIR" <<'PY'
from pathlib import Path
import sys

from huggingface_hub import snapshot_download

model_id, model_dir = sys.argv[1:]
snapshot_download(
    repo_id=model_id,
    local_dir=Path(model_dir),
)
PY

if find "$MODEL_DIR" -type f -name '*.incomplete' -print -quit | grep -q .; then
  echo "model download contains incomplete files: $MODEL_DIR" >&2
  exit 1
fi
echo "model ready: $MODEL_DIR"
