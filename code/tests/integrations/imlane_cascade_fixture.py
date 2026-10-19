"""Explicit-interpreter CPU responses for IMLane; no author or model scheduler."""

import hashlib
import json
import math
import os
import sys
import uuid

from src.baselines.text.frameworks.daft_engine_process import EngineProcess


class CpuCascadeProcess(EngineProcess):
    mode = "normal"

    def _command(self, engine, result_fd):
        return [engine.model_python, "-m", __name__, str(result_fd), self.mode]


class FailedRowProcess(CpuCascadeProcess):
    mode = "failed-row"


class BadLogsProcess(CpuCascadeProcess):
    mode = "bad-logs"


class FailedCloseProcess(CpuCascadeProcess):
    mode = "failed-close"


if __name__ == "__main__":
    from src.baselines.text.frameworks.daft_engine_process import serve
    from src.serving.backends import vllm_engine

    mode = sys.argv[2]

    class CpuEngine:
        def __init__(self, engine, generation):
            self.role = "helper" if generation.logprobs is not None else "main"
            self.limit, self.calls = engine.max_model_calls, 0
            self.uid = "imlane-cascade-cpu-" + uuid.uuid4().hex

        def ready(self):
            assert "lotus" not in sys.modules and "vllm" not in sys.modules and "ray" not in sys.modules
            return dict(engine_uid=self.uid, pid=os.getpid(), fixture=True,
                        real_model_requests=0, VLLM_RPC_BASE_PATH=os.environ.get("VLLM_RPC_BASE_PATH"))

        def run_prepared(self, rows, **_):
            self.calls += len(rows)
            assert self.calls <= self.limit
            results = []
            for row in rows:
                position = row["source_position"]
                probability = [0.02,0.05,0.1,0.25,0.4,0.6,0.7,0.85,0.95,0.99][position % 10]
                token = "True" if (position % 3 != 0 if self.role == "main" else probability > .5) else "False"
                failed = mode == "failed-row" and position == 4
                content = [dict(token=token, logprob=math.log(.6), top_logprobs=[
                    dict(token="True",logprob=math.log(probability)),
                    dict(token="False",logprob=math.log(1-probability))])]
                if mode == "bad-logs" and position == 4:
                    content = []
                digest = hashlib.sha256(json.dumps(json.loads(row["messages_json"]), ensure_ascii=False,
                    sort_keys=True, separators=(",",":")).encode()).hexdigest()
                results.append(dict(row, messages_sha256=digest, status="failed" if failed else "succeeded",
                    submission_confirmed=True, text=None if failed else token,
                    finish_reason="abort" if failed else "stop", prompt_tokens=13+position,
                    completion_tokens=1, total_tokens=14+position,
                    prompt_token_ids_json=json.dumps(list(range(13+position))),
                    output_token_ids_json=json.dumps([1 if token == "True" else 2]),
                    logprobs_json=json.dumps(content) if self.role == "helper" else "null",
                    engine_request_id=row["call_id"], engine_uid=self.uid,
                    error_type="DeclaredCPUFailure" if failed else None,
                    usage_source="declared CPU fixture; zero model inference"))
            return results

        def close(self):
            confirmed = mode != "failed-close"
            return dict(status="closed" if confirmed else "unknown", reserved_model_calls=self.calls,
                        engine_api_shutdown_confirmed=confirmed, real_model_requests=0)

    vllm_engine.VllmEngine = CpuEngine
    serve(int(sys.argv[1]))
