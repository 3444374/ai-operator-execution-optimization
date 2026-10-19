"""Private CPU process responses for the native Extract relation graph tests."""

import hashlib
import json
import os
import sys
import uuid

from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch
from src.baselines.text.frameworks.lotus_chain_engine_process import ChainEngineProcess


class CpuRelationProcess(ChainEngineProcess):
    mode = "normal"

    def _command(self, engine, result_fd):
        return [sys.executable, "-B", "-m", __name__, str(result_fd), self.mode]


class CpuRelationBatch(FusedChainBatch):
    _engine_type = CpuRelationProcess


class UnknownRelationProcess(CpuRelationProcess):
    mode = "unknown-prefix"


class UnknownRelationBatch(CpuRelationBatch):
    _engine_type = UnknownRelationProcess


def _serve(fd, mode):
    from src.baselines.text.frameworks.lotus_chain_engine_process import serve
    from src.serving.backends import vllm_engine

    class CpuEngine:
        def __init__(self, engine, generation):
            self.uid, self.count, self.limit = "native-relation-cpu-" + uuid.uuid4().hex, 0, engine.max_model_calls

        def ready(self):
            assert all(name not in sys.modules for name in ("lotus", "ray", "vllm"))
            return dict(engine_uid=self.uid, pid=os.getpid(), fixture=True, physical_gpu_work=False)

        def run_prepared(self, rows, *, generation, deadline_monotonic):
            self.count += len(rows)
            assert self.limit is not None and self.count <= self.limit
            results = []
            for index, row in enumerate(rows):
                position, stage = row["source_position"], row["algorithm_stage"]
                messages = json.loads(row["messages_json"])
                actual = json.dumps(messages, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                if stage == "e0":
                    values = (
                        dict(shared_value="drop earlier", retained="first only", row_id="data first", call_id="first business"),
                        dict(shared_value="drop separate"),
                        dict(shared_value="keep broadcast", row_id="data last", call_id="last business"),
                        {},
                    )
                    text = json.dumps(values[position])
                elif stage == "m1":
                    assert "source first review" not in actual and "source later review" not in actual
                    text = "mapped keep broadcast" if "keep broadcast" in actual else "mapped drop"
                elif stage == "f2":
                    assert "mapped " in actual
                    text = "True" if "keep broadcast" in actual else "False"
                else:
                    raise AssertionError("CPU fixture received an undeclared stage")
                unknown = mode == "unknown-prefix" and stage == "e0" and position == 2
                results.append(dict(
                    **{key: value for key, value in row.items() if key != "messages_json"},
                    status="unknown" if unknown else "succeeded", text=None if unknown else text,
                    submission_confirmed=False if unknown else True,
                    prompt_tokens=None if unknown else 3, completion_tokens=None if unknown else 1,
                    total_tokens=None if unknown else 4, prompt_token_ids_json="null" if unknown else "[1,2,3]",
                    output_token_ids_json="null" if unknown else "[4]", logprobs_json="null",
                    finish_reason=None if unknown else "stop", engine_uid=self.uid,
                    engine_request_id=str(self.count - len(rows) + index),
                    messages_sha256=hashlib.sha256(actual.encode()).hexdigest(),
                    actual_model_call=False, usage_source="synthetic CPU fixture",
                    fixture_batch_positions=[item["source_position"] for item in rows],
                ))
            return results

        def close(self, timeout=10):
            return dict(status="closed", engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None,
                        reserved_model_calls=self.count, max_model_calls=self.limit)

    vllm_engine.VllmEngine = CpuEngine
    serve(fd)


if __name__ == "__main__":
    _serve(int(sys.argv[1]), sys.argv[2])
