#!/bin/sh
exec "$CASE_MODEL_PYTHON" -u -B "$CASE_ROOT/worker_entry.py" "$@"
