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
: "${MODEL_DIR:?set MODEL_DIR to a persistent model directory}"

VENV_ROOT=${VENV_ROOT:-/root/autodl-tmp/venvs}
VLLM_VENV=${VLLM_VENV:-"$VENV_ROOT/vllm-4090"}
HF_HOME=${HF_HOME:-/root/autodl-tmp/huggingface}
TURBO_SCRIPT=${TURBO_SCRIPT:-/etc/network_turbo}
PYTHON=${PYTHON:-"$VLLM_VENV/bin/python"}

if [[ ! -x "$PYTHON" ]]; then
  echo "Python executable not found: $PYTHON" >&2
  exit 2
fi

# AutoDL acceleration is optional. Generic clouds and a local 5070 use their
# normal network path; AutoDL still activates its shell-local proxy when present.
if [[ -f "$TURBO_SCRIPT" ]]; then
  # shellcheck disable=SC1090
  source "$TURBO_SCRIPT" >/dev/null 2>&1
  echo "using optional download accelerator: $TURBO_SCRIPT"
else
  echo "download accelerator not present; using the machine's normal network"
fi
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
