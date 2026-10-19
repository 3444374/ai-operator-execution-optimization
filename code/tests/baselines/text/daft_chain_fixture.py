"""Fixed CPU responses for finite-chain graph qualification; never a model."""

import hashlib
import json
import os
import sys
import time
import uuid

from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch
from src.baselines.text.frameworks.lotus_chain_engine_process import ChainEngineProcess


class CpuChainProcess(ChainEngineProcess):
    mode = "normal"

    def _command(self, engine, result_fd):
        return [sys.executable, "-B", "-m", __name__, str(result_fd), self.mode]


class CpuChainBatch(FusedChainBatch):
    _engine_type = CpuChainProcess


class BadExtractProcess(CpuChainProcess):
    mode = "bad-extract"


class BadExtractBatch(CpuChainBatch):
    _engine_type = BadExtractProcess


class UnknownProcess(CpuChainProcess):
    mode = "unknown"


class UnknownBatch(CpuChainBatch):
    _engine_type = UnknownProcess


class StallProcess(CpuChainProcess):
    mode = "stall"


class StallBatch(CpuChainBatch):
    _engine_type = StallProcess


class FailCloseProcess(CpuChainProcess):
    mode = "close-unknown"


class FailCloseBatch(CpuChainBatch):
    _engine_type = FailCloseProcess


def _serve(result_fd, mode):
    from src.baselines.text.frameworks.lotus_chain_engine_process import serve
    from src.serving.backends import vllm_engine

    class CpuEngine:
        def __init__(self, engine, generation):
            self.uid, self.calls = "chain-cpu-" + uuid.uuid4().hex, 0
            self.limit = engine.max_model_calls

        def ready(self):
            assert "lotus" not in sys.modules and "ray" not in sys.modules and "vllm" not in sys.modules
            return dict(engine_uid=self.uid, pid=os.getpid(), fixture=True, physical_gpu_work=False)

        def run_prepared(self, rows, *, generation, deadline_monotonic):
            assert time.monotonic() < deadline_monotonic
            self.calls += len(rows)
            assert self.limit is not None and self.calls <= self.limit
            results = []
            for row in rows:
                stage = row["algorithm_stage"]
                messages = json.loads(row["messages_json"])
                content = json.dumps(messages, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                if stage == "map-first":
                    assert "source-positive" in content or "source-negative" in content
                    text = "DERIVED_NEG_FIRST" if "source-negative" in content else "DERIVED_POS_FIRST"
                elif stage == "map-second":
                    assert "DERIVED_POS_FIRST" in content or "DERIVED_NEG_FIRST" in content
                    assert "source-positive" not in content and "source-negative" not in content
                    text = "DERIVED_NEG_SECOND" if "DERIVED_NEG_FIRST" in content else "DERIVED_POS_SECOND"
                    if mode == "stall":
                        time.sleep(60)
                elif stage == "filter-source":
                    text = "False" if "source-negative" in content else "True"
                elif stage == "filter-again":
                    assert "BIZ_" in content
                    text = "False" if "BIZ_NEG" in content else "True"
                elif stage == "filter-generated":
                    assert "DERIVED_" in content
                    assert "source-positive" not in content and "source-negative" not in content
                    text = "False" if "DERIVED_NEG" in content else "True"
                elif stage == "extract-last":
                    assert "DERIVED_POS_FIRST" in content
                    assert "source-positive" not in content and "source-negative" not in content
                    text = ("[]" if mode == "bad-extract" else
                            json.dumps(dict(entity="entity-" + str(row["source_position"]), verdict="positive")))
                else:
                    raise AssertionError("undeclared CPU fixture stage")
                unknown = mode == "unknown" and stage == "map-second"
                results.append(dict(
                    **{name: value for name, value in row.items() if name != "messages_json"},
                    status="unknown" if unknown else "succeeded", text=None if unknown else text,
                    prompt_tokens=None if unknown else 3, completion_tokens=None if unknown else 1,
                    total_tokens=None if unknown else 4, submission_confirmed=None if unknown else True,
                    prompt_token_ids_json=None if unknown else "[1,2,3]",
                    output_token_ids_json=None if unknown else "[4]", logprobs_json="null",
                    finish_reason=None if unknown else "stop", engine_uid=self.uid,
                    engine_request_id=row["call_id"], model_fixture_pid=os.getpid(),
                    max_tokens=generation.max_tokens, temperature=generation.temperature,
                    top_p=generation.top_p, stop=list(generation.stop), seed=generation.seed,
                    response_format=generation.response_format,
                    messages_sha256=hashlib.sha256(content.encode()).hexdigest(),
                    lotus_imported="lotus" in sys.modules, actual_model_call=False,
                    usage_source="synthetic CPU fixture",
                ))
            return results

        def close(self):
            return dict(status="unknown" if mode == "close-unknown" else "closed",
                        engine_api_shutdown_confirmed=mode != "close-unknown",
                        gpu_exit_confirmed=None, reserved_model_calls=self.calls)

    vllm_engine.VllmEngine = CpuEngine
    serve(result_fd)


if __name__ == "__main__":
    _serve(int(sys.argv[1]), sys.argv[2])
