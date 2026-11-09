"""Resident query lifecycle with scripted semantics and a real CPU IPC child."""
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity
from src.semantic_methods.lotus import record_chain as chains
from src.semantic_methods.lotus import record_types
from src.semantic_methods.lotus.record_types import stage_identity
from src.semantic_methods.lotus.records import RecordsResources
from src.scheduling.submission_control.feedback_options import ResourceRequestConfig
from src.serving.backends.vllm_protocol import EngineSpec
from tests.execution_provider import test_lotus_record_local_query as query_fixture
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider.test_records_request_feedback import CPUResourceProcess
from tests.semantic_methods.test_lotus_chain_composition import chain_for, source_rows


class ScriptedMethod:
    """Replace only SDK formatting/parsing; retain production relation handling."""

    def __init__(self, stage):
        self.stage = stage
        self.lm = SimpleNamespace(stats=SimpleNamespace(model_dump=lambda: {}))

    def prepare_rows(self, records, *, query_id, allow_nulls=False):
        return [dict(stage_identity(query_id, row, self.stage.stage_id,
                                   model_role=self.stage.model_role),
                     messages_json=json.dumps([dict(role="user", content=str(row.values))]))
                for row in records]

    def interpret_results(self, results):
        return SimpleNamespace(outputs=[json.loads(row["text"]) if self.stage.kind == "extract"
                                        else row["text"] for row in results])


class Tokenizer:
    def apply_chat_template(self, messages, **options):
        return [0] * len(json.dumps(messages))


class Process(CPUResourceProcess):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ready_report = dict(self.ready_report, pid=self.process.pid)


class ResidentReadyFeedbackTests(unittest.TestCase):
    config_for = query_fixture.QueryOwnerTests.config_for

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="resident-ready-feedback-")
        self.addCleanup(temporary.cleanup)
        self.config = Path(temporary.name) / "responses.json"
        scope = patch.object(chains, "LotusRecordMethod", ScriptedMethod)
        scope.start()
        self.addCleanup(scope.stop)
        binding = patch.object(record_types, "_input_instruction",
                               return_value=(["passage"], "Rewrite {passage}"))
        binding.start()
        self.addCleanup(binding.stop)

    def owner(self, *, feedback=True, **options):
        engine = EngineSpec(str(self.config), sys.executable, sys.executable, max_model_len=4096)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=2, query_timeout_s=12, startup_timeout_s=8)
        owner = SemLoomRecordChainCore(engine, resources, execution="record_local_sections",
            capacity=CoreRecordsCapacity(item_input_bytes=65536, item_result_bytes=65536),
            rolling_refill=True, resident=True, batching_tokenizer=Tokenizer(),
            request_feedback=(ResourceRequestConfig(poll_interval_s=.01, window_s=.04,
                max_sample_age_s=.3, min_samples=1, measurement_windows=2,
                allow_exploration=False) if feedback else None), **options)
        self.addCleanup(owner.close, timeout=3)
        return owner

    def run_two_queries(self, kind):
        plan, rows = chain_for(kind), source_rows(size=2)
        self.config_for(plan, rows)
        with ThreadedRuntime([]) as runtime, patch.object(AsyncCoreModelActor, "_process_type", return_value=Process):
            owner = self.owner()
            feedbacks, sessions, engines = [], [], []
            for _ in range(2):
                stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                values = list(stream)
                feedbacks.append(owner._supply_feedback)
                sessions.append(stream.session.session_id)
                engines.append(owner._actors.model_ready["engine_uid"])
                self.assertEqual([row.source_position for row in values], [0, 1])
                output = "field_0" if kind == "E" else "derived_0"
                self.assertTrue(all(output in row.values for row in values))
                self.assertEqual(len(owner.calls), 2)
                self.assertEqual(feedbacks[-1].prepared, 2)
                self.assertTrue(owner.last_close_report["query_completed"])
                self.assertTrue(owner.last_close_report["model_retained"])
                self.assertEqual(set(owner.last_close_report["core_usage"].values()), {0})
                self.assertEqual(stream.pool.used, (0, 0))
                if kind == "E":
                    self.assertEqual([phase["transition"] for phase in owner.phase_history],
                                     ["complete_extract_relation"])
            self.assertIsNot(feedbacks[0], feedbacks[1])
            self.assertEqual(owner._ready_options, {})
            self.assertEqual(feedbacks[0].prepared, 2)
            self.assertNotEqual(sessions[0], sessions[1])
            self.assertEqual(engines[0], engines[1])
            self.assertEqual(len(runtime.async_actors), 1)
            self.assertEqual(owner.close(timeout=3)["status"], "closed")

    def test_two_Extract_queries_complete_on_the_same_resident_owner(self):
        self.run_two_queries("E")

    def test_Map_ready_events_bind_to_each_query_feedback(self):
        self.run_two_queries("M")

    def test_explicit_ready_observer_still_rejects_Extract_before_source(self):
        plan = chain_for("E")
        with ThreadedRuntime([]) as runtime:
            callback = lambda event: None
            owner = self.owner(feedback=False, on_ready_event=callback)
            with self.assertRaisesRegex(ValueError, "Filter/Map"):
                owner.evaluate(None, plan, expected=None, query_id=plan.query_id)
            self.assertIs(owner._ready_options["on_ready_event"], callback)
            self.assertIsNone(owner._root_run)
            self.assertEqual(runtime.async_actors, [])
