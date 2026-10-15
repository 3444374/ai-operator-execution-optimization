"""Controlled-seam lifecycle tests using explicit Generator/completion doubles."""

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import threading
import unittest
from unittest.mock import patch

from src.semantic_methods.palimpzest import backend


class Observer:
    def __init__(self):
        self.lock, self.events = threading.Lock(), []

    def __getattr__(self, method):
        def record(call_id, *args, **kwargs):
            with self.lock:
                self.events.append((method, call_id, args, kwargs))
        return record


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.original = SimpleNamespace(completion=lambda **kw: kw["messages"])
        self.module = SimpleNamespace(litellm=self.original)
        module = self.module

        class Generator:
            def _parse_answer(self, answer):
                if answer == "INTERRUPT":
                    raise KeyboardInterrupt("fixture parser interruption")
                if answer not in ("TRUE", "FALSE"):
                    raise ValueError("invalid bool")
                return {"passed_operator": answer == "TRUE"}

            def __call__(self, candidate, fields, right_candidate=None, **kwargs):
                default = {"passed_operator": False}
                try:
                    response = module.litellm.completion(model="fixed", messages=candidate.answer)
                except Exception:
                    return default, None, "original-failure-stats", candidate.answer
                try:
                    parsed = self._parse_answer(response)
                except Exception:
                    parsed = default
                return parsed, None, "original-stats", candidate.answer

        self.module.Generator = Generator
        self.generator = Generator()
        self.observer = Observer()
        for target, value in (("validate_source", None), ("import_module", self.module)):
            mocked = patch.object(backend, target, return_value=value)
            mocked.start()
            self.addCleanup(mocked.stop)

    def candidate(self, index="0", answer="TRUE"):
        return SimpleNamespace(source_indices=("reviews-" + index,), answer=answer)

    def test_success_keeps_native_result_and_only_shadows_pz_module(self):
        baseline = self.generator(self.candidate(), {})
        original_call = self.module.Generator.__call__
        with backend.completion_bridge(completion=self.original.completion, observer=self.observer) as state:
            self.assertEqual(self.generator(self.candidate(), {}), baseline)
            self.assertIsNot(self.module.litellm, self.original)
            self.assertEqual(self.original.completion(messages="other-library"), "other-library")
        self.assertIs(self.module.litellm, self.original)
        self.assertIs(self.module.Generator.__call__, original_call)
        self.assertEqual(state.snapshot()["active_generators"], 0)
        self.assertTrue(state.snapshot()["patches_restored"])
        self.assertEqual([e[0] for e in self.observer.events],
                         ["started", "finished", "parsed", "generator_finished"])

    def test_original_completion_error_keeps_default_and_failure_evidence(self):
        failure = TimeoutError("fixture transport")
        def complete(**kwargs):
            raise failure
        with backend.completion_bridge(completion=complete, observer=self.observer):
            result = self.generator(self.candidate(), {})
        self.assertEqual(result[0], {"passed_operator": False})
        self.assertEqual(result[2], "original-failure-stats")
        self.assertIs(self.observer.events[1][3]["error"], failure)

    def test_first_model_contract_failure_blocks_a_queued_generation(self):
        calls = []
        def complete(**kwargs):
            calls.append(kwargs)
            raise backend.PalimpzestAdapterError("wrong fixture response model")
        with backend.completion_bridge(completion=complete, observer=self.observer) as state:
            with ThreadPoolExecutor(max_workers=1) as executor:
                futures = [executor.submit(self.generator, self.candidate(str(index)), {}) for index in range(2)]
                for future in futures:
                    with self.assertRaises(backend.PalimpzestAdapterError):
                        future.result(timeout=5)
        self.assertEqual(len(calls), 1)
        self.assertEqual(state.snapshot()["completions_admitted"], 1)
        self.assertEqual(state.snapshot()["fatal_error_type"], "PalimpzestAdapterError")

    def test_real_provider_exception_does_not_block_the_next_generation(self):
        calls = []
        def complete(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise TimeoutError("fixture provider failure")
            return "TRUE"
        with backend.completion_bridge(completion=complete, observer=self.observer) as state:
            with ThreadPoolExecutor(max_workers=1) as executor:
                futures = [executor.submit(self.generator, self.candidate(str(index)), {}) for index in range(2)]
                results = [future.result(timeout=5) for future in futures]
        self.assertFalse(results[0][0]["passed_operator"])
        self.assertTrue(results[1][0]["passed_operator"])
        self.assertEqual(len(calls), 2)
        self.assertIsNone(state.snapshot()["fatal_error_type"])

    def test_fatal_blocks_a_preparing_generator_before_it_admits_a_completion(self):
        preparing, release = threading.Event(), threading.Event()
        calls = []
        class PreparingCandidate:
            source_indices = ("reviews-preparing",)
            @property
            def answer(self):
                preparing.set()
                if not release.wait(5):
                    raise TimeoutError("fixture preparation remained blocked")
                return "TRUE"
        def complete(**kwargs):
            calls.append(kwargs)
            if not preparing.wait(5):
                raise TimeoutError("fixture second preparation did not start")
            raise backend.PalimpzestAdapterError("wrong fixture response model")
        with backend.completion_bridge(completion=complete, observer=self.observer) as state:
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(self.generator, self.candidate(), {})
                second = executor.submit(self.generator, PreparingCandidate(), {})
                try:
                    with self.assertRaises(backend.PalimpzestAdapterError):
                        first.result(timeout=5)
                finally:
                    release.set()
                with self.assertRaises(backend.PalimpzestAdapterError):
                    second.result(timeout=5)
        self.assertEqual(len(calls), 1)
        self.assertEqual(state.snapshot()["generators_started"], 2)
        self.assertEqual(state.snapshot()["completions_admitted"], 1)

    def test_parser_failure_keeps_default_but_is_distinct_from_negative(self):
        with backend.completion_bridge(completion=self.original.completion, observer=self.observer):
            result = self.generator(self.candidate(answer="invalid"), {})
        self.assertFalse(result[0]["passed_operator"])
        parsed = next(event for event in self.observer.events if event[0] == "parsed")
        self.assertTrue(parsed[3]["parser_default"])
        self.assertIsInstance(parsed[3]["error"], ValueError)

    def test_parser_interrupt_propagates_without_claiming_a_retained_default(self):
        with backend.completion_bridge(completion=self.original.completion, observer=self.observer):
            with self.assertRaises(KeyboardInterrupt):
                self.generator(self.candidate(answer="INTERRUPT"), {})
        self.assertNotIn("parsed", [event[0] for event in self.observer.events])
        finished = next(event for event in self.observer.events if event[0] == "generator_finished")
        self.assertIsInstance(finished[3]["error"], KeyboardInterrupt)

    def test_model_validation_fails_query_before_submission(self):
        class Completion:
            def validate(self, kwargs):
                raise ValueError("model mismatch")
            def __call__(self, **kwargs):
                raise AssertionError("must not send")
        with backend.completion_bridge(completion=Completion(), observer=self.observer):
            with self.assertRaisesRegex(ValueError, "model mismatch"):
                self.generator(self.candidate(), {})
        self.assertNotIn("started", [event[0] for event in self.observer.events])

    def test_failed_observer_cannot_turn_accounting_error_into_success(self):
        self.observer.started = lambda *a, **k: (_ for _ in ()).throw(ValueError("ledger unavailable"))
        def forbidden(**kwargs):
            self.fail("an unaccounted request was sent")
        with backend.completion_bridge(completion=forbidden, observer=self.observer):
            with self.assertRaisesRegex(ValueError, "ledger unavailable"):
                self.generator(self.candidate(), {})

    def test_concurrent_generators_keep_occurrence_identity(self):
        with backend.completion_bridge(completion=self.original.completion, observer=self.observer):
            with ThreadPoolExecutor(max_workers=4) as executor:
                list(executor.map(lambda n: self.generator(self.candidate(str(n)), {}), range(12)))
        starts = {event[1]: event[3]["row_id"] for event in self.observer.events if event[0] == "started"}
        ends = {event[1]: event[3]["row_id"] for event in self.observer.events if event[0] == "generator_finished"}
        self.assertEqual(starts, ends)
        self.assertEqual(len(starts), 12)

    def test_overlapping_bridge_is_rejected(self):
        with backend.completion_bridge(completion=self.original.completion):
            with self.assertRaisesRegex(RuntimeError, "already owns"):
                with backend.completion_bridge(completion=self.original.completion):
                    self.fail("overlap accepted")

    def test_changed_patch_owner_is_reported_without_overwriting_the_new_owner(self):
        other = SimpleNamespace(completion=lambda **kw: "other-owner")
        with self.assertRaisesRegex(RuntimeError, "ownership changed"):
            with backend.completion_bridge(completion=self.original.completion) as state:
                self.module.litellm = other
        self.assertIs(self.module.litellm, other)
        self.assertFalse(state.snapshot()["patches_restored"])

    def test_live_generator_retains_bridge_until_owner_drains_it(self):
        entered, release = threading.Event(), threading.Event()
        errors = []
        def complete(**kwargs):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("fixture did not release")
            return "TRUE"
        def generate():
            try:
                self.generator(self.candidate(), {})
            except BaseException as error:
                errors.append(error)
        thread = threading.Thread(target=generate)
        try:
            with self.assertRaisesRegex(RuntimeError, "live generators"):
                with backend.completion_bridge(completion=complete, observer=self.observer) as state:
                    thread.start()
                    self.assertTrue(entered.wait(5))
            self.assertIsNot(self.module.litellm, self.original)
            self.assertEqual(state.snapshot()["active_generators"], 1)
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertIs(self.module.litellm, self.original)
        self.assertTrue(state.snapshot()["patches_restored"])


if __name__ == "__main__":
    unittest.main()
