"""The IMLane value carrier consumes the shared finite-chain kernel; no real model."""

from copy import deepcopy
from dataclasses import asdict, replace
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from integrations.imlane_records.chain_host import ChainResults, configuration
from integrations.imlane_records.chain_udf import ChainWorker
from integrations.imlane_records.chain_workload import ChainBatches
from integrations.imlane_records.filter_workload import PROTOCOL
from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusRecordChain, LotusRecordMethod, MapMethod, RecordOccurrence, StageSpec, records_to_frame,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, _prepared


def source():
    return [RecordOccurrence("r" + str(i), i, 17,
        dict(review="Review " + str(i), row_id="business-row", call_id="business-call")) for i in range(2)]


def chain(kinds, query_id="q"):
    methods = dict(filter=lambda i, column: FilterMethod("openai/local-fixture", "Keep {" + column + "}"),
                   map=lambda i, column: MapMethod("openai/local-fixture",
                       "Summarize {" + column + "} with {row_id} and {call_id}", output_column="summary" + str(i)),
                   extract=lambda i, column: ExtractMethod("openai/local-fixture", (column,), {"answer": None}))
    stages, column = [], "review"
    for i, kind in enumerate(kinds):
        stages.append(StageSpec(kind + str(i), methods[kind](i, column),
            GenerationSpec(max_tokens=17 + i * 7, temperature=i / 10, stop=(),
                           response_format={"type": "json_object"} if kind == "extract" else None)))
        if kind == "map":
            column = "summary" + str(i)
    return LotusRecordChain(tuple(stages), query_id=query_id)


class EngineFactory:
    """A CPU response fixture; model/GPU calls and model processes are zero."""

    def __init__(self, *, all_false=False, fail_stage=None):
        self.instances, self.all_false, self.fail_stage = [], all_false, fail_stage

    def __call__(self, spec, generation, *_):
        factory = self

        class Engine:
            def __init__(self):
                self.ready_report = dict(engine_uid="cpu-chain-" + str(len(factory.instances)), fixture=True)
                self.batches, self.responses, self.reserved, self.closes = [], [], 0, 0

            def run_prepared(self, rows, *, generation):
                if self.reserved + len(rows) > spec.max_model_calls:
                    raise RuntimeError("CPU fixture permanent call allowance exceeded")
                self.reserved += len(rows)
                self.batches.append((deepcopy(rows), asdict(generation)))
                values = []
                for row in rows:
                    item = _prepared([row])[0][0]
                    stage = item["algorithm_stage"]
                    text = ("Answer: False" if factory.all_false else "Answer: True") if stage.startswith("filter") else (
                        json.dumps({"answer": "answer-" + item["row_id"]}) if stage.startswith("extract") else
                        "GENERATED-" + stage + "-" + item["row_id"])
                    failed = stage == factory.fail_stage and item["row_id"] == "r1"
                    values.append(dict(item, status="failed" if failed else "succeeded", text=None if failed else text,
                        submission_confirmed=True, prompt_tokens=5, completion_tokens=2, total_tokens=7,
                        prompt_token_ids_json="[1,2,3,4,5]", output_token_ids_json="[6,7]", logprobs_json="null",
                        engine_uid=self.ready_report["engine_uid"], engine_request_id=item["call_id"],
                        finish_reason="abort" if failed else "stop", error_type="CPUFixtureFailure" if failed else None,
                        usage_source="configured CPU fixture; model requests=0"))
                self.responses.append(deepcopy(values))
                return values

            def close(self, **_):
                self.closes += 1
                return dict(status="closed", process_exit_confirmed=True, cleanup_errors=[], gpu_exit_confirmed=None,
                            engine_api=dict(status="closed", engine_api_shutdown_confirmed=True, fixture=True))

        engine = Engine()
        self.instances.append(engine)
        return engine


class LocalAuthor:
    """Only the carrier is simulated here; real IMLane scheduling is a separate qualification."""

    def __init__(self, worker):
        self.worker, self.pending = worker, {}
        self.meta, self._terminal = dict(protocol=PROTOCOL), False
        self.submissions = []

    def begin(self, query, **_):
        self.query_id = query

    def submit(self, block, rows, **_):
        if len(rows) != 1:
            raise AssertionError("native block must invoke the full finite kernel once")
        self.submissions.append(block)
        row = rows[0]
        if json.loads(row["record_json"]) == {"operation": "close"}:
            outcome = self.worker.envelope(row, close=self.worker.close())
        else:
            outcome = self.worker.envelope(row, batch=self.worker.run(row))
        self.pending[block] = [dict(call_id=row["call_id"], row_id=row["row_id"], outcome_json=json.dumps(outcome))]

    def wait_ready(self, **_):
        return list(self.pending)

    def collect(self, block, **_):
        return self.pending.pop(block)

    def end(self, **_):
        pass

    def close(self, **_):
        pass

    def abort(self):
        self._terminal = True
        self.worker.close()


@unittest.skipUnless(importlib.util.find_spec("lotus"), "requires pinned LOTUS CPU environment")
class ChainTests(unittest.TestCase):
    def setUp(self):
        import lotus
        self.context = lotus.settings.context(enable_cache=False)
        self.context.__enter__()
        self.addCleanup(self.context.__exit__, None, None, None)
        self.temp = tempfile.TemporaryDirectory(prefix="imlane-chain-cpu-")
        self.addCleanup(self.temp.cleanup)
        self.resources = RecordsResources(model_batch_rows=16)
        self.engine = EngineSpec(self.temp.name, os.sys.executable, os.sys.executable, max_model_calls=8)
        self.count = 0

    def config(self, method, records):
        self.count += 1
        control = Path(self.temp.name) / str(self.count)
        control.mkdir()
        with patch.object(EngineSpec, "validate_driver_environment", return_value={"fixture": True}):
            return configuration(method, self.engine, self.resources, records, control_dir=control,
                deadline=time.monotonic() + 60, model_environment={"CUDA_VISIBLE_DEVICES": "0"})

    def stream(self, method, records, factory=None, *, cancelled=None):
        factory = factory or EngineFactory()
        config = self.config(method, records)
        worker = ChainWorker(config, engine_type=factory)
        client = LocalAuthor(worker)
        stream = ChainResults(iter(records), client, chain=method, expected=records, resources=self.resources,
            control_dir=config["control_dir"], deadline=config["deadline_monotonic"], cancelled=cancelled,
            close_owner=lambda: {"resources_confirmed": True, "fixture": True})
        return stream, worker, factory, client

    def native(self, method, engine):
        import lotus
        import pandas as pd
        from src.semantic_methods.lotus.engine import engine_response

        frame = pd.DataFrame([row.values for row in source()], index=[17, 17])
        captured, batches = [], iter(zip(engine.batches, engine.responses, strict=True))
        for stage in method.stages:
            if frame.empty:
                break
            (_prepared_rows, generation), responses = next(batches)
            lm = LotusRecordMethod(stage).lm

            def respond(data, options, *_):
                captured.append(([message for message, _ in data], dict(options)))
                return [engine_response(row, lm) for row in responses]

            with patch.object(lm, "_process_uncached_messages", side_effect=respond), lotus.settings.context(lm=lm):
                if stage.kind == "filter":
                    frame = frame.sem_filter(stage.method.instruction, default=stage.method.default)
                elif stage.kind == "map":
                    frame = frame.sem_map(stage.method.instruction, suffix=stage.method.output_column,
                                          system_prompt=stage.method.system_prompt)
                else:
                    frame = frame.sem_extract(list(stage.method.input_columns), stage.method.output_columns)
            self.assertEqual(generation["max_tokens"], captured[-1][1]["max_completion_tokens"])
        return frame, captured

    def test_original_fme_mf_and_repeated_stages_share_one_kernel_and_preserve_generation(self):
        import pandas as pd

        for kinds in (("filter", "map", "extract"), ("map", "filter"),
                      ("map", "map", "filter"), ("filter", "filter", "map")):
            with self.subTest(kinds=kinds):
                method = chain(kinds)
                stream, worker, factory, client = self.stream(method, source())
                records = list(stream)
                self.assertTrue(stream.completed)
                self.assertEqual(len(factory.instances), 1)
                engine = factory.instances[0]
                native, captured = self.native(method, engine)
                pd.testing.assert_frame_equal(records_to_frame(records, columns=stream.columns), native)
                self.assertEqual([json.loads(row["messages_json"]) for rows, _ in engine.batches for row in rows],
                                 [message for messages, _ in captured for message in messages])
                self.assertEqual(len(stream.calls), 2 * len(kinds))
                self.assertEqual(len({call["call_id"] for call in stream.calls}), len(stream.calls))
                self.assertEqual({record.values["row_id"] for record in records}, {"business-row"})
                self.assertEqual({record.values["call_id"] for record in records}, {"business-call"})
                if kinds[0] == "map":
                    self.assertIn("GENERATED-map0", json.dumps(engine.batches[1][0]))
                stream.close()
                self.assertTrue(worker.closed)
                self.assertEqual(client.submissions, ["chain-0", "chain-close"])
                self.assertEqual(list(stream), [])
                self.assertIsNone(stream.last_close_report["gpu_exit_confirmed"])

    def test_false_calls_keep_usage_while_the_final_relation_is_empty(self):
        stream, _worker, factory, _client = self.stream(chain(("filter", "filter", "map")),
                                                       source(), EngineFactory(all_false=True))
        self.assertEqual(list(stream), [])
        self.assertTrue(stream.completed)
        self.assertEqual(len(stream.calls), 2)
        self.assertEqual({call["decision"] for call in stream.calls}, {False})
        self.assertEqual(sum(call["prompt_tokens"] for call in stream.calls), 10)
        self.assertEqual(len(factory.instances[0].batches), 1)
        stream.close()

    def test_empty_source_and_cancel_before_submit_allocate_no_engine(self):
        stream, worker, factory, _client = self.stream(chain(("map", "filter")), [])
        self.assertEqual(list(stream), [])
        self.assertEqual(factory.instances, [])
        stream.close()
        self.assertTrue(worker.closed)
        self.assertTrue(stream.last_close_report["model"]["no_model_process_started"])
        stream, _worker, factory, client = self.stream(chain(("map", "filter")), source(), cancelled=lambda: True)
        with self.assertRaises(InterruptedError):
            next(stream)
        self.assertEqual(factory.instances, [])
        self.assertEqual(client.submissions, [])
        stream.close()

    def test_failed_stage_keeps_prior_and_failed_usage_before_raising_and_audit_recovery(self):
        stream, _worker, factory, _client = self.stream(chain(("filter", "map", "extract")),
                                                       source(), EngineFactory(fail_stage="map1"))
        with self.assertRaises(Exception) as raised:
            list(stream)
        self.assertEqual(len(raised.exception.chain_calls), 4)
        self.assertEqual(sum(call["prompt_tokens"] for call in stream.calls), 20)
        self.assertEqual([call["status"] for call in stream.calls], ["succeeded"] * 3 + ["failed"])
        self.assertEqual(factory.instances[0].reserved, 4)
        self.assertFalse(stream.completed)
        stream.close()
        self.assertEqual(len(stream.calls), 4)
        self.assertTrue(stream.raw_result_batches)

    def test_two_query_owners_use_distinct_engine_uids_and_evaluation_ids(self):
        factory = EngineFactory()
        seen = []
        for query in ("q1", "q2"):
            stream, _worker, _factory, _client = self.stream(chain(("map", "filter"), query), source(), factory)
            list(stream)
            seen.append((stream.calls[0]["engine_uid"], {call["call_id"] for call in stream.calls}))
            stream.close()
        self.assertNotEqual(seen[0][0], seen[1][0])
        self.assertFalse(seen[0][1].intersection(seen[1][1]))
        self.assertEqual(len(factory.instances), 2)

    def test_accepted_later_batch_failure_keeps_the_successful_prefix_and_all_known_usage(self):
        self.resources = RecordsResources(model_batch_rows=1, max_active_batches=2)
        stream, _worker, factory, client = self.stream(chain(("filter", "map", "extract")),
            source(), EngineFactory(fail_stage="map1"))
        delivered = []
        with self.assertRaises(Exception):
            for row in stream:
                delivered.append(row)
        self.assertEqual([row.row_id for row in delivered], ["r0"])
        self.assertEqual(len(stream.calls), 5)
        self.assertEqual(sum(row["prompt_tokens"] for row in stream.calls), 25)
        self.assertEqual(factory.instances[0].reserved, 5)
        stream.close()
        self.assertEqual(client.submissions, ["chain-0", "chain-1", "chain-close"])

    def test_filter_and_chain_aliases_cannot_open_distinct_roles_in_one_interpreter(self):
        from integrations.imlane_records import filter_udf, method_owner
        from integrations.imlane_records import chain_udf
        from unittest.mock import Mock

        self.assertIs(filter_udf._slot, chain_udf._slot)
        first = Mock(query_id="q", broken=False, closed=False)
        factory, second = Mock(return_value=first), Mock()
        with patch.object(method_owner.slot, "worker", None), patch.object(method_owner.slot, "fingerprint", None), \
             patch.object(method_owner.slot, "finished", set()):
            self.assertIs(method_owner.owned_worker({"query_id": "q"}, "filter", factory), first)
            with self.assertRaisesRegex(ValueError, "cannot change workload"):
                method_owner.owned_worker({"query_id": "q"}, "chain", second)
            second.assert_not_called()
            self.assertIs(method_owner.slot.worker, first)

    def test_worker_audit_failure_cannot_be_hidden_by_successful_model_close(self):
        method, records = chain(("map", "filter")), source()
        stream, worker, _factory, _client = self.stream(method, records)
        original = worker.run

        def with_error(row):
            batch = original(row)
            batch["cleanup_errors"].append("self-written audit failure fixture")
            return batch

        with patch.object(worker, "run", side_effect=with_error):
            list(stream)
        with self.assertRaisesRegex(RuntimeError, "audit/cleanup"):
            stream.close()
        self.assertFalse(stream.completed)
        self.assertEqual(sum(row["prompt_tokens"] for row in stream.calls), 20)
        self.assertEqual(stream.last_close_report["status"], "unknown")

    def test_source_binding_transport_repeats_and_multi_deployment_fail_without_engine(self):
        method = chain(("map", "filter"))
        original = source()
        mapping = ChainBatches("q", original)
        encoded = mapping.encode(original)
        self.assertEqual(len(encoded), 1)
        with self.assertRaisesRegex(ValueError, "duplicated"):
            mapping.encode(original)
        changed = [replace(original[0], values={"review": "tampered"})]
        with self.assertRaisesRegex(ValueError, "changed"):
            ChainBatches("q", original).encode(changed)
        different = LotusRecordChain((method.stages[0], replace(method.stages[1],
            method=replace(method.stages[1].method, model_name="another-model"))), query_id="q")
        for candidate, records in ((different, original), (method, [original[0], original[0]])):
            with self.subTest(candidate=candidate), self.assertRaises(ValueError):
                self.config(candidate, records)
        with self.assertRaisesRegex(ValueError, "terminal"):
            chain(("extract", "map"))
        for unsupported in ("JoinMethod", "BranchMethod", "GlobalAggregateMethod"):
            wire = deepcopy(method.describe())
            wire["stages"][0]["method_type"] = unsupported
            with self.subTest(unsupported=unsupported), self.assertRaises(KeyError):
                LotusRecordChain.from_dict(wire)


if __name__ == "__main__":
    unittest.main()
