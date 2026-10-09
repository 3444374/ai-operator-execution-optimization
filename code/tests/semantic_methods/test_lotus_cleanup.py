"""Real LOTUS/Core fault injection preserves execution and separate cleanup errors."""

from contextlib import contextmanager
from importlib.util import find_spec
import json
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.native_tasks import NativeTaskSession
from src.scheduling.core.session_contract import Usage
from src.semantic_methods.lotus.batch import LotusBatchExecutor
from src.semantic_methods.lotus.sdk import lotus_response, prepare_call
from src.execution_provider.adapters.full_response import decode_full_response
from tests.execution_provider.test_native_tasks import fixture
from tests.semantic_methods.test_lotus_adapter import completion, config, encode, model


@unittest.skipUnless(find_spec("lotus"), "pinned LOTUS is unavailable")
class LotusCleanupTests(unittest.TestCase):
    @contextmanager
    def scenario(self, *, phase=None, primary=None, release_error=None, close_error=None,
                 rows=1, result_limit=16777216):
        execution, backend, _clock = fixture(held_tasks=2, offer_tasks=2,
            input_bytes=16384, result_bytes=16384, item_input_bytes=8192,
            item_result_bytes=8192, metadata_bytes=8192, active_requests=2, active_work=2)
        state = dict(events=[], raw={}, observed=[], errors={},
                     faults={"release": release_error, "close": close_error})

        class Flow(NativeTaskSession):
            def offer(self, tasks):
                if phase == "offer":
                    raise primary
                return super().offer(tasks)

            def advance(self, max_deliveries=1):
                if phase == "advance":
                    raise primary
                return super().advance(max_deliveries)

            def wait(self, progress, timeout_s=None):
                for key, (_handle, task) in tuple(backend.pending.items()):
                    body = json.loads(task.task.payload)
                    full = FullModelResponse(200, (("x-fixture", "a"), ("x-fixture", "b")),
                        b'{"broken":' if phase == "sdk" else encode(completion(body)))
                    state["raw"][key.sequence] = full
                    payload = b"invalid complete response" if phase == "decode" else encode_full_response(full)
                    backend.complete(key, payload)

            def release(self, leases):
                if leases:
                    state["events"].append(("release", len(leases)))
                    if state["faults"]["release"] is not None:
                        raise state["faults"]["release"]
                return super().release(leases)

            def close(self, *, clean=False):
                state["events"].append(("close", clean))
                report = super().close(clean=clean)
                if state["faults"]["close"] is not None:
                    raise state["faults"]["close"]
                return report

        def prepare(*args):
            if phase == "prepare":
                raise primary
            return prepare_call(*args)

        def decode(payload):
            try:
                return decode_full_response(payload)
            except BaseException as error:
                state["errors"]["decode"] = error
                raise

        def convert(full):
            try:
                return lotus_response(full)
            except BaseException as error:
                state["errors"]["sdk"] = error
                raise

        def observe(index, full):
            state["observed"].append((index, full))
            if phase == "observe" and index == rows - 1:
                raise primary

        base = "http://127.0.0.1:1/v1"
        lm = model(base)
        selected = LotusBatchExecutor(execution, config(base), query_id="cleanup-query",
            operator_id="map", on_response=observe, max_batch_result_bytes=result_limit)

        def run(text="controlled"):
            data = [([{"role": "user", "content": f"{text}-{index}"}], index) for index in range(rows)]
            return selected(lm, data, lm.kwargs, False, "cleanup")

        with patch("src.semantic_methods.lotus.batch.NativeTaskSession", Flow), \
             patch("src.semantic_methods.lotus.batch.prepare_call", prepare), \
             patch("src.semantic_methods.lotus.batch.decode_full_response", decode), \
             patch("src.semantic_methods.lotus.batch.lotus_response", convert):
            yield selected, execution, state, run

    def assert_returned(self, execution):
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(execution.engine.jobs.jobs, {})

    def assert_cleanup(self, selected, expected):
        self.assertEqual(selected.last_cleanup_errors, tuple(expected))

    def test_input_error_survives_close_failure(self):
        for phase in ("prepare", "offer", "advance"):
            with self.subTest(phase=phase):
                primary, closing = ValueError(phase + " failed"), OSError("close failed")
                with self.scenario(phase=phase, primary=primary, close_error=closing) as (selected, execution, state, run):
                    with self.assertRaises(ValueError) as caught:
                        run()
                    self.assertIs(caught.exception, primary)
                    self.assert_cleanup(selected, (("close", closing),))
                    self.assertEqual(primary.__notes__, ["LOTUS flow close also failed: OSError"])
                    self.assertEqual(state["events"], [("close", False)])
                    self.assertEqual(selected.last_responses, (None,))
                    self.assert_returned(execution)

    def test_decode_error_survives_release_and_close_failures(self):
        releasing, closing = OSError("release failed"), RuntimeError("close failed")
        with self.scenario(phase="decode", release_error=releasing, close_error=closing) as (selected, execution, state, run):
            with self.assertRaisesRegex(ValueError, "invalid complete response") as caught:
                run()
            self.assertIs(caught.exception, state["errors"]["decode"])
            self.assert_cleanup(selected, (("release", releasing), ("close", closing)))
            self.assertEqual(caught.exception.__notes__, ["LOTUS result release also failed: OSError",
                                                       "LOTUS flow close also failed: RuntimeError"])
            self.assertEqual(state["events"], [("release", 1), ("close", False)])
            self.assertEqual(selected.last_responses, (None,))
            self.assert_returned(execution)

    def test_observation_error_keeps_previous_full_response_and_closes_all_deliveries(self):
        primary, releasing, closing = ValueError("observer failed"), OSError("release failed"), RuntimeError("close failed")
        with self.scenario(phase="observe", primary=primary, release_error=releasing,
                           close_error=closing, rows=2) as (selected, execution, state, run):
            with self.assertRaises(ValueError) as caught:
                run()
            self.assertIs(caught.exception, primary)
            self.assertEqual(state["observed"], [(0, state["raw"][0]), (1, state["raw"][1])])
            self.assertEqual(selected.last_responses, (state["raw"][0], None))
            self.assert_cleanup(selected, (("release", releasing), ("close", closing)))
            self.assertEqual(state["events"], [("release", 2), ("close", False)])
            self.assert_returned(execution)

    def test_sdk_error_keeps_full_response_and_survives_cleanup_failures(self):
        releasing, closing = OSError("release failed"), RuntimeError("close failed")
        with self.scenario(phase="sdk", release_error=releasing, close_error=closing) as (selected, execution, state, run):
            with self.assertRaisesRegex(ValueError, "could not be parsed") as caught:
                run()
            self.assertIs(caught.exception, state["errors"]["sdk"])
            self.assertIsInstance(caught.exception.__cause__, json.JSONDecodeError)
            self.assertEqual(caught.exception.full_response, state["raw"][0])
            self.assertEqual(selected.last_responses, (state["raw"][0],))
            self.assert_cleanup(selected, (("release", releasing), ("close", closing)))
            self.assert_returned(execution)

    def test_release_failure_without_execution_error_propagates_and_close_runs(self):
        releasing = OSError("release failed")
        with self.scenario(release_error=releasing) as (selected, execution, state, run):
            with self.assertRaises(OSError) as caught:
                run()
            self.assertIs(caught.exception, releasing)
            self.assert_cleanup(selected, (("release", releasing),))
            self.assertEqual(state["events"], [("release", 1), ("close", False)])
            self.assertEqual(selected.last_responses, (state["raw"][0],))
            self.assert_returned(execution)

    def test_first_cleanup_error_survives_later_close_failure(self):
        releasing, closing = OSError("release failed"), RuntimeError("close failed")
        with self.scenario(release_error=releasing, close_error=closing) as (selected, execution, state, run):
            with self.assertRaises(OSError) as caught:
                run()
            self.assertIs(caught.exception, releasing)
            self.assert_cleanup(selected, (("release", releasing), ("close", closing)))
            self.assertEqual(releasing.__notes__, ["LOTUS flow close also failed: RuntimeError"])
            self.assertEqual(state["events"], [("release", 1), ("close", False)])
            self.assert_returned(execution)

    def test_close_failure_after_success_propagates(self):
        closing = OSError("close failed")
        with self.scenario(close_error=closing) as (selected, execution, state, run):
            with self.assertRaises(OSError) as caught:
                run()
            self.assertIs(caught.exception, closing)
            self.assert_cleanup(selected, (("close", closing),))
            self.assertEqual(state["events"], [("release", 1), ("close", True)])
            self.assertEqual(selected.last_responses, (state["raw"][0],))
            self.assert_returned(execution)

    def test_response_limit_error_keeps_full_response_and_survives_cleanup_failures(self):
        releasing, closing = OSError("release failed"), RuntimeError("close failed")
        with self.scenario(result_limit=1, release_error=releasing, close_error=closing) as (selected, execution, state, run):
            with self.assertRaisesRegex(ValueError, "declared byte limit") as caught:
                run()
            self.assertEqual(caught.exception.full_response, state["raw"][0])
            self.assert_cleanup(selected, (("release", releasing), ("close", closing)))
            self.assert_returned(execution)

    def test_normal_success_retains_full_response_sdk_usage_and_returns_core_resources(self):
        with self.scenario(rows=2) as (selected, execution, state, run):
            values = run()
            self.assertEqual([value.usage.total_tokens for value in values], [10, 10])
            self.assertEqual([value.usage.prompt_tokens_details.cached_tokens for value in values], [2, 2])
            self.assertEqual(selected.last_responses, tuple(state["raw"][i] for i in range(2)))
            self.assert_cleanup(selected, ())
            self.assertEqual(state["events"], [("release", 2), ("close", True)])
            self.assert_returned(execution)

    def test_cleanup_diagnostics_reset_for_next_batch(self):
        closing = OSError("close failed")
        with self.scenario(close_error=closing) as (selected, execution, state, run):
            with self.assertRaises(OSError):
                run("first")
            self.assert_cleanup(selected, (("close", closing),))
            self.assert_returned(execution)
            state["faults"]["close"] = None
            values = run("second")
            self.assertEqual(values[0].choices[0].message.content, "summary:second-0")
            self.assert_cleanup(selected, ())
            self.assertEqual(selected.last_responses, (state["raw"][0],))
            self.assert_returned(execution)

    def test_cleanup_error_in_callers_except_block_is_not_suppressed(self):
        handled, closing = ValueError("handled caller error"), OSError("close failed")
        with self.scenario(close_error=closing) as (selected, execution, state, run):
            try:
                raise handled
            except ValueError:
                with self.assertRaises(OSError) as caught:
                    run()
            self.assertIs(caught.exception, closing)
            self.assertFalse(hasattr(handled, "__notes__"))
            self.assert_cleanup(selected, (("close", closing),))
            self.assert_returned(execution)


if __name__ == "__main__":
    unittest.main()
