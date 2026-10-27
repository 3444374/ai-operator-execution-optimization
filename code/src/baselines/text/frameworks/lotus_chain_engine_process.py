"""Legacy import and child-process entry point for stage-local generation."""

import sys

from src.serving.backends.staged_generation_process import (
    ChainEngineProcess, EngineProcess, EngineSpec, GenerationSpec, MAX_MESSAGE_BYTES,
    _encode, _prepared, serve,
)


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--serve":
        raise SystemExit("private engine worker requires an inherited result descriptor")
    serve(int(sys.argv[2]))
