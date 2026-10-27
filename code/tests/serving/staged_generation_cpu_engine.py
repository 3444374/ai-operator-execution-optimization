"""Run the production stage IPC server against a finite CPU engine fixture."""

import os
import sys
from unittest.mock import patch

from src.serving.backends import staged_generation_process, vllm_engine
from src.serving.backends.vllm_engine import IDENTITY_FIELDS


class FixtureEngine:
    def __init__(self, engine, generation):
        self.generation = generation

    def ready(self):
        return dict(engine_uid="CPU-stage-pipe", pid=os.getpid())

    def run_prepared(self, rows, *, generation, deadline_monotonic):
        return [dict({key: row[key] for key in IDENTITY_FIELDS}, status="succeeded",
                     text=str(generation.max_tokens), submission_confirmed=True,
                     prompt_tokens=3, completion_tokens=1, total_tokens=4,
                     engine_uid="CPU-stage-pipe") for row in rows]

    def close(self):
        return dict(status="closed", engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None)


if __name__ == "__main__":
    with patch.object(vllm_engine, "VllmEngine", FixtureEngine):
        staged_generation_process.serve(int(sys.argv[1]))
