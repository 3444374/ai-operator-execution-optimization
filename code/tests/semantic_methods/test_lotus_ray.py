"""Opt-in, bounded LOTUS method plus actual Daft/Arrow/Ray, localhost model fixture."""

from importlib.util import find_spec
import json
import base64
import os
from pathlib import Path
import unittest

from src.execution_provider.adapters.native_tasks import build_native_execution
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.semantic_methods.budget import MethodCapacity, row_reservation
from src.semantic_methods.continuation import MethodLimits
from src.semantic_methods.lotus.batch import LotusBatchExecutor, lotus_executor
from src.semantic_methods.lotus.driver import iter_two_map_rows
from src.semantic_methods.lotus.maps import LotusTwoMapMethod, encode, staged_two_map
from src.scheduling.core.session_contract import SessionTimeouts
from tests.semantic_methods.test_lotus_adapter import STAGES, close_execution, config, fixture_server, model


ENABLED = os.environ.get("SEMLOOM_LOTUS_RAY_TEST") == "1" and all(
    find_spec(name) is not None for name in ("lotus", "ray", "daft", "pyarrow"))


@unittest.skipUnless(ENABLED, "explicit actual-library Ray validation is not enabled")
class LotusRayTests(unittest.TestCase):
    def test_single_map_pair_and_incremental_two_map_use_real_daft_ray(self):
        import lotus
        import pandas as pd
        import ray
        task_root = Path(os.environ["SEMLOOM_LOTUS_RAY_TMP"])
        if len(str(task_root).encode()) > 45:
            raise ValueError("use a short data-disk directory for Ray's Unix sockets")
        task_root.mkdir(parents=True, exist_ok=False)
        if ray.is_initialized():
            raise RuntimeError("LOTUS Ray test requires its own new connection")
        try:
            context = ray.init(num_cpus=2, num_gpus=0, include_dashboard=False, object_store_memory=134217728,
                _temp_dir=str(task_root), _node_ip_address="127.0.0.1",
                runtime_env={"env_vars": {"PYTHONPATH": str(Path(__file__).resolve().parents[2]),
                                           "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}})
        except BaseException:
            ray.shutdown()
            raise
        physical = RayMapConfig(context.address_info["address"], 1, 2, 1048600, 8388608)
        self.assertEqual(physical.payload_backend, "daft")
        self.assertEqual(ray.cluster_resources().get("GPU", 0), 0)
        events = []
        responses = []
        chain_results = []
        calls = None
        try:
            with fixture_server(delay=lambda body: 0.6 if "slow" in body["messages"][-1]["content"]
                                and "Classify" not in body["messages"][-1]["content"] else 0) as (base, calls), \
                 lotus.settings.context(enable_cache=False):
                frame = pd.DataFrame({"text": ["slow", "fast", "duplicate", "duplicate"]})
                native_lm = model(base)
                with lotus.settings.context(lm=native_lm), lotus_executor(native_lm):
                    native = frame.sem_map("Summarize {text}.", return_raw_outputs=True)
                native_calls = list(calls)
                lm = model(base)
                execution = build_native_execution(config(base), physical=physical,
                    max_tasks=2, max_active_requests=2, observer=events.append,
                    timeouts=SessionTimeouts(backend_s=45))
                batch = LotusBatchExecutor(execution, config(base), query_id="ray-map", operator_id="lotus-map",
                    on_response=lambda index, full: responses.append(dict(index=index, status_code=full.status_code,
                        headers=full.headers, body_base64=base64.b64encode(full.body).decode("ascii"))))
                try:
                    with lotus.settings.context(lm=lm), lotus_executor(lm, batch):
                        actual = frame.sem_map("Summarize {text}.", return_raw_outputs=True)
                    self.assertEqual(actual.to_dict(), native.to_dict())
                    self.assertEqual(lm.stats, native_lm.stats)
                    self.assertCountEqual(calls[len(native_calls):], native_calls)
                    self.assertEqual(len(batch.last_responses), 4)
                    self.assertTrue(any(e["event"] == "ray_block_put" for e in events))
                finally:
                    close_execution(execution)
                self.assertTrue(any(e["event"] == "ray_transport_closed" and e["confirmed"] for e in events))
                native_chain_lm = model(base)
                staged = staged_two_map(frame, native_chain_lm, STAGES)
                native_chain_calls = list(calls[-8:])
                lm = model(base)
                method = LotusTwoMapMethod(lm, STAGES, model_config=config(base), max_response_bytes=32768)
                limits = MethodLimits(32768, 65536, 131072, 2)
                capacity = MethodCapacity(2, 2 * row_reservation(limits))
                execution = build_native_execution(config(base), physical=physical,
                    max_tasks=2, max_active_requests=2, observer=events.append,
                    timeouts=SessionTimeouts(backend_s=45))
                start_event = len(calls.events)
                try:
                    results = list(iter_two_map_rows(execution, method,
                        (encode(row) for row in frame.to_dict("records")), query_id="ray-chain",
                        operator_id="lotus-two-map", limits=limits, capacity=capacity))
                    rows = [json.loads(r.value)["row"] for r in sorted(results, key=lambda r:r.row.sequence)]
                    for result in results:
                        value = json.loads(result.value)
                        self.assertEqual(len(value["raw_responses"]), 2)
                        from src.execution_provider.adapters.full_response import decode_full_response
                        for encoded in value["raw_responses"]:
                            full = decode_full_response(base64.b64decode(encoded))
                            self.assertEqual(full.status_code, 200)
                            self.assertEqual(json.loads(full.body)["usage"]["total_tokens"], 10)
                            self.assertEqual(json.loads(full.body)["choices"][0]["logprobs"]["content"][0]["logprob"], -0.25)
                        chain_results.append(dict(row_sequence=result.row.sequence, call_id=result.row.call_id,
                                                  value_base64=base64.b64encode(result.value).decode("ascii")))
                    self.assertEqual(rows, staged.to_dict("records"))
                    self.assertEqual(lm.stats, native_chain_lm.stats)
                    self.assertCountEqual(list(calls[-8:]), native_chain_calls)
                    self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)
                    self.assertEqual(execution.engine.jobs.jobs, {})
                    chain_events = calls.events[start_event:]
                    successor = min(t for event, t, body in chain_events if event == "start"
                                    and "Classify" in body["messages"][-1]["content"]
                                    and "fast" in body["messages"][-1]["content"])
                    slow_end = min(t for event, t, body in chain_events if event == "end"
                                   and "Classify" not in body["messages"][-1]["content"]
                                   and "slow" in body["messages"][-1]["content"])
                    self.assertLess(successor, slow_end)
                    self.assertEqual(len(calls), 24)
                finally:
                    close_execution(execution)
        finally:
            ray.shutdown()
            artifact = os.environ.get("SEMLOOM_LOTUS_TEST_ARTIFACT")
            if artifact:
                from src.baselines.common.redact import redact_text
                with (Path(artifact) / "ray-events.json").open("x") as stream:
                    stream.write(redact_text(json.dumps(dict(events=events, responses=responses, chain_results=chain_results,
                        fixture_calls=list(calls or []), fixture_events=calls.events if calls is not None else [],
                        real_model_requests=0), ensure_ascii=False, indent=2)) + "\n")
        self.assertFalse(ray.is_initialized())


if __name__ == "__main__":
    unittest.main()
