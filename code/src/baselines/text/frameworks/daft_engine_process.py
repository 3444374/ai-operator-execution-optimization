"""Legacy import and child-process entry point for the shared synchronous transport.

Keep existing callers and old pickle globals on the same implementation objects.
"""

import sys

from src.serving.backends.vllm_sync_process import (
    CLOSE_CLEANUP_RESERVE_S, CODE_ROOT, DEFAULT_CLOSE_TIMEOUT_S,
    EngineProcess, EngineSpec, GenerationSpec, IDENTITY_FIELDS, MAX_MESSAGE_BYTES,
    RESULT_PROTOCOL_ERROR, _encode, _publish, _verified_terminal, serve,
)


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--serve":
        raise SystemExit("private engine worker requires an inherited result descriptor")
    serve(int(sys.argv[2]))
