"""CPU-only model child for IMLane qualification; never imports vLLM or LOTUS."""
import os
import sys
import time

from src.baselines.text.frameworks.daft_engine_process import EngineProcess


class FixedEngineProcess(EngineProcess):
    def _command(self, engine, result_fd):
        return [sys.executable, "-m", __name__, str(result_fd),
                os.environ.get("IMLANE_FILTER_FIXTURE", "normal")]


if __name__ == "__main__":
    from src.baselines.text.frameworks.daft_engine_process import serve
    from src.serving.backends import vllm_engine

    mode = sys.argv[2]

    class FixedEngine:
        def __init__(self, engine, _generation):
            self.budget, self.calls = engine.max_model_calls, 0

        def ready(self):
            if "lotus" in sys.modules or "vllm" in sys.modules:
                raise AssertionError("fixture child imported a method or real model runtime")
            return dict(engine_uid="imlane-cpu-" + str(os.getpid()), pid=os.getpid(), fixture=True,
                        real_model_requests=0, lotus_imported=False)

        def run_prepared(self, rows, **_):
            if mode == "stall":
                time.sleep(30)
            if self.calls + len(rows) > self.budget:
                raise RuntimeError("fixture permanent request allowance exceeded")
            self.calls += len(rows)
            identities, _ = vllm_engine._prepared(rows)
            results = []
            for item in identities:
                failed = mode == "partial" and item["source_position"] == 1
                results.append(dict(item, status="failed" if failed else "succeeded",
                                    text=None if failed else ("Answer: True" if item["source_position"] == 0 else "Answer: False"),
                                    prompt_tokens=5, completion_tokens=2, total_tokens=7,
                                    prompt_token_ids_json="[1, 2, 3, 4, 5]", output_token_ids_json="[6, 7]",
                                    finish_reason="abort" if failed else "stop", logprobs_json=None,
                                    engine_request_id=item["call_id"], engine_uid=self.ready()["engine_uid"],
                                    submission_confirmed=True, error_type="FixtureFinishedError" if failed else None,
                                    usage_source="configured CPU fixture; no model inference"))
            return results

        def close(self):
            return dict(status="closed", engine_api_shutdown_confirmed=mode != "fail-close",
                        reserved_model_calls=self.calls, real_model_requests=0)

    vllm_engine.VllmEngine = FixedEngine
    serve(int(sys.argv[1]))
