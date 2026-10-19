"""Explicit no-model child process for opt-in Core cascade library checks."""

import hashlib
import json
import os
import sys
import time
import uuid

from src.baselines.text.frameworks.daft_engine_process import EngineProcess
from tests.semantic_methods.test_lotus_cascade import terminal


class CpuCascadeEngine:
    def __init__(self, spec, generation):
        self.uid, self.calls = "cascade-core-cpu-" + uuid.uuid4().hex, 0
        self.spec, self.generation = spec, generation

    def ready(self):
        assert "lotus" not in sys.modules and "vllm" not in sys.modules
        return dict(engine_uid=self.uid, pid=os.getpid(), physical_gpu_work=False,
                    inference_kind="original differential's fixed response and synthetic usage",
                    generation=self.generation.kwargs())

    def run_prepared(self, rows, *, deadline_monotonic):
        assert time.monotonic() < deadline_monotonic and len(rows) == 1
        self.calls += len(rows)
        assert self.spec.max_model_calls is None or self.calls <= self.spec.max_model_calls
        values = []
        for row in rows:
            assert row["call_id"].startswith("lotus-cascade-v1:")
            assert (self.generation.logprobs == 10) == (row["model_role"] == "helper")
            value = terminal(row, row["model_role"])
            value.update(engine_uid=self.uid, model_fixture_pid=os.getpid(),
                         prepare_worker_pid=row["prepare_worker_pid"],
                         messages_sha256=hashlib.sha256(json.dumps(
                             json.loads(row["messages_json"]), sort_keys=True, ensure_ascii=False,
                             separators=(",", ":"),
                         ).encode()).hexdigest())
            values.append(value)
        return values

    def close(self, timeout=10.0):
        return dict(status="closed", engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None,
                    reserved_model_calls=self.calls)


class CpuCascadeEngineProcess(EngineProcess):
    def _command(self, engine, result_fd):
        return [sys.executable, "-m", __name__, str(result_fd)]


if __name__ == "__main__":
    from src.baselines.text.frameworks.daft_engine_process import serve
    from src.serving.backends import vllm_engine

    vllm_engine.VllmEngine = CpuCascadeEngine
    serve(int(sys.argv[1]))
