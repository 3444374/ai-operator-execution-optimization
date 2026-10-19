"""Original LOTUS with a fixture author carrier and real finite CPU children."""

from copy import deepcopy
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from integrations.imlane_records.cascade_host import IMLaneCascadeBackend
from integrations.imlane_records.cascade_udf import CascadeWorker
from integrations.imlane_records.cascade_workload import CascadeOccurrences, input_record
from integrations.imlane_records.filter_workload import PROTOCOL
from integrations.imlane_records.owner import StopBudget
from src.semantic_methods.lotus.cascade import CascadePreparer, cascade_identity, run_cascade
from src.semantic_methods.lotus.engine import bind_filter_method, lm_report
from src.semantic_methods.lotus.records import FilterMethod, MOVIE_Q3_INSTRUCTION, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.baselines.text.daft_cascade_fixture import CascadeCsvFixture
from tests.integrations.imlane_cascade_fixture import CpuCascadeProcess, BadLogsProcess, FailedCloseProcess, FailedRowProcess
from tests.semantic_methods import test_lotus_cascade as native_reference


class LocalAuthor:
    """Author scheduling is a fixture here; native qualification is separate."""

    def __init__(self, worker):
        self.worker, self.pending = worker, {}
        self.meta, self._terminal = dict(protocol=PROTOCOL), False
        self.max_pending = 0

    def begin(self, query_id, **unused):
        self.query_id = query_id

    def submit(self, block, rows, **unused):
        if len(rows) == 1 and json.loads(rows[0]["record_json"]) == {"operation":"close"}:
            outcomes = [self.worker.envelope(rows[0], close=self.worker.close())]
        else:
            outcomes = [self.worker.envelope(source, result=result, attempted=attempted)
                        for source,(result,attempted) in zip(rows,self.worker.run(rows),strict=True)]
        self.pending[block] = [dict(call_id=r["call_id"],row_id=r["row_id"],outcome_json=json.dumps(outcome))
                              for r,outcome in zip(rows,outcomes,strict=True)]
        self.max_pending = max(self.max_pending,len(self.pending))

    def wait_ready(self, **unused):
        return list(reversed(self.pending))

    def collect(self, block, **unused):
        return deepcopy(self.pending.pop(block))

    def end(self, **unused):
        assert not self.pending

    def close(self, **unused):
        pass

    def abort(self):
        self._terminal = True
        self.worker.close()


class FixtureBackend(IMLaneCascadeBackend):
    def __init__(self, *args, engine_type=CpuCascadeProcess, author_confirmed=True,
                 author_cleanup_failure=False, stderr_failure=False, **kwargs):
        super().__init__(*args,**kwargs)
        self.engine_type, self.author_confirmed = engine_type,author_confirmed
        self.author_cleanup_failure, self.stderr_failure = author_cleanup_failure,stderr_failure
        self.workers, self.clients, self.prepared = [],[],{}

    def _open_stage(self, stage, config, control):
        prepared = self.prepared
        class Prepare(CascadePreparer):
            def prepare_rows(self, rows):
                result = super().prepare_rows(rows)
                for row in result:
                    prepared[row["call_id"]] = (stage.name,json.loads(row["messages_json"]),stage.effective_kwargs)
                return result
        worker = CascadeWorker(config,prepare_type=Prepare,engine_type=self.engine_type)
        self.workers.append(worker)
        client = LocalAuthor(worker)
        self.clients.append(client)
        def finalizer():
            if stage.name not in self._author_states:
                self._author_states[stage.name] = dict(resources_confirmed=self.author_confirmed,fixture=True)
                self.author_report["cleanup"].append(dict(namespace=stage.name,passed=not self.author_cleanup_failure))
                if self.stderr_failure:
                    self.author_report.setdefault("stderr_capture_errors", []).append("late fixture writer failure")
                if self.author_cleanup_failure:
                    raise RuntimeError("fixture author abnormal exit after physical cleanup")
            return self._author_states[stage.name]
        return client,finalizer


class IMLaneCascadeTests(unittest.TestCase):
    def setUp(self):
        import lotus

        self.temp = tempfile.TemporaryDirectory(dir="/tmp",prefix="imlane-cascade-cpu-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.context = lotus.settings.context(enable_cache=False)
        self.context.__enter__()
        self.addCleanup(lambda:self.context.__exit__(None,None,None))
        self.environment = patch.object(EngineSpec,"validate_driver_environment",return_value=None)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.main,self.helper = native_reference.models()
        self.engines = {}
        for role,lm in (("main",self.main),("helper",self.helper)):
            path = self.root/role
            path.mkdir()
            lm.tokenizer.save(str(path/"tokenizer.json"))
            self.engines[role] = EngineSpec(str(path),sys.executable,sys.executable,max_model_calls=64)
        self.resources = RecordsResources(prepare_batch_rows=3,model_batch_rows=2,
            max_active_batches=2,query_timeout_s=120,startup_timeout_s=15)

    def setup_query(self, *, generic=True, **options):
        import pandas as pd

        query = self.root/("query-"+str(len(list(self.root.glob("query-*")))))
        query.mkdir()
        instruction = "Do {title} and {content} support the claim?" if generic else MOVIE_Q3_INSTRUCTION
        payload = ("title","content","synopsis") if generic else ("reviewText",)
        values = [dict(row_id="row-"+str(i),source_position=i,selected="yes",**(
            dict(title="title "+str(i)+";",content="content "+str(i)+";",synopsis="summary "+str(i)+";")
            if generic else dict(reviewText="unique review "+str(i)+";"))) for i in range(10)]
        bound = bind_filter_method(FilterMethod(self.main.model,instruction))
        source = CascadeCsvFixture(query,bound,payload_columns=payload,rows=values)
        output = query/"owner"
        output.mkdir(mode=0o700)
        args = SimpleNamespace(output=output,stop_budget=StopBudget(threading.Event()))
        backend = FixtureBackend(source,engines=self.engines,resources=self.resources,
            author_args=args,model_environment={"CUDA_VISIBLE_DEVICES":"-1"},**options)
        frame = pd.DataFrame(source.selected()).drop(columns="selected")
        self.addCleanup(backend.close)
        return frame,backend,instruction

    def args(self, *, generic):
        from lotus.types import CascadeArgs

        return CascadeArgs(sampling_percentage=.5,cascade_IS_random_seed=5,cascade_num_calibration_quantiles=5,
            helper_filter_instruction="Does {synopsis} support the claim?" if generic else None)

    def execute(self, *, generic=True, **options):
        frame,backend,instruction = self.setup_query(generic=generic,**options)
        report = run_cascade(frame,self.main,self.helper,self.args(generic=generic),backend,
            instruction=instruction,query_id="cascade-query",max_rows=20,max_calls=30)
        return report,frame,backend,instruction

    def assert_native(self, *, generic):
        report,frame,backend,instruction = self.execute(generic=generic)
        reference = native_reference.CascadeDifferentialTests(methodName="runTest")
        reference.frame = frame
        native,stats,calls,sampled,main,helper = reference.reference(self.args(generic=generic),instruction=instruction)
        self.assertEqual([r["decision"] for r in report["decisions"]],native["_filter"].tolist())
        self.assertEqual(report["stats"],{k:v.model_dump(mode="json") if hasattr(v,"model_dump") else v for k,v in stats.items()})
        self.assertEqual([backend.prepared[r["call_id"]] for r in report["calls"]["records"]],calls)
        self.assertEqual(lm_report(self.main),lm_report(main))
        self.assertEqual(lm_report(self.helper),lm_report(helper))
        event = next(e for e in report["calls"]["events"] if e["event"]=="sample_selected")
        self.assertEqual(event["indices"],sampled)
        self.assertLess(len(set(sampled)),len(sampled))
        self.assertEqual([r["algorithm_stage"] for r in backend.close_reports],
                         ["helper_score","threshold_sample","fallback"])
        self.assertTrue(all(r["stage_process_exit_confirmed"] for r in backend.close_reports))
        self.assertEqual(len({w.kernel.ready_report["engine_uid"] for w in backend.workers}),3)
        self.assertTrue(all(w.kernel.process.poll() is not None for w in backend.workers))
        self.assertTrue(all(c.max_pending==2 for c in backend.clients if len(c.worker.stage.records)>3))
        self.assertNotIn("positive_review_cnt",report)

    def test_non_q3_multicolumn_helper_method_matches_original_global_control(self):
        self.assert_native(generic=True)

    def test_original_q3_cascade_regression_matches_native(self):
        self.assert_native(generic=False)

    def test_known_failed_helper_usage_is_kept_and_no_main_stage_starts(self):
        frame,backend,instruction = self.setup_query(engine_type=FailedRowProcess)
        with self.assertRaisesRegex(ValueError,"omitted declared evaluations") as caught:
            run_cascade(frame,self.main,self.helper,self.args(generic=True),backend,
                instruction=instruction,query_id="failed-helper",max_rows=20,max_calls=30)
        report = caught.exception.query_report
        failed = [r for r in report["calls"]["records"] if r.get("error_type")=="DeclaredCPUFailure"]
        self.assertEqual(len(failed),1)
        self.assertEqual(failed[0]["prompt_tokens"],17)
        self.assertEqual(len(backend.workers),1)
        self.assertTrue(all(r["model_role"]=="helper" for r in report["calls"]["records"]))
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"],75)
        self.assertEqual(report["calls"]["usage"]["known_completion_tokens"],5)
        self.assertEqual(sum(r["prompt_tokens"] is None for r in report["calls"]["records"]),5)

    def test_bad_helper_logprobs_preserves_usage_and_stops_before_calibration(self):
        frame,backend,instruction = self.setup_query(engine_type=BadLogsProcess)
        with self.assertRaisesRegex(RuntimeError,"token metadata") as caught:
            run_cascade(frame,self.main,self.helper,self.args(generic=True),backend,
                instruction=instruction,query_id="bad-logprobs",max_rows=20,max_calls=30)
        report = caught.exception.query_report
        self.assertEqual(len(backend.workers),1)
        self.assertEqual(report["calls"]["usage"]["prompt_tokens"],sum(13+i for i in range(10)))
        self.assertNotIn("global_calibration_completed",[e["event"] for e in report["calls"]["events"]])

    def test_unconfirmed_api_close_blocks_next_role_with_known_results(self):
        frame,backend,instruction = self.setup_query(engine_type=FailedCloseProcess)
        with self.assertRaises(RuntimeError) as caught:
            run_cascade(frame,self.main,self.helper,self.args(generic=True),backend,
                instruction=instruction,query_id="close-unknown",max_rows=20,max_calls=30)
        self.assertEqual(len(backend.workers),1)
        self.assertEqual(caught.exception.query_report["calls"]["usage"]["prompt_tokens"],sum(13+i for i in range(10)))
        self.assertFalse(backend.close_reports[0]["stage_process_exit_confirmed"])
        self.assertIsNotNone(backend._active)
        self.assertTrue(backend.workers[0].kernel.process.poll() is not None)
        # The unknown API close remains unknown on repeated cleanup.
        self._cleanups = [c for c in self._cleanups if c[0] != backend.close]
        with self.assertRaises(RuntimeError):
            backend.close()

    def test_cancellation_before_preflight_never_allocates_an_author(self):
        frame,backend,instruction = self.setup_query(cancelled=lambda:True)
        with self.assertRaises(InterruptedError):
            run_cascade(frame,self.main,self.helper,self.args(generic=True),backend,
                instruction=instruction,query_id="cancelled",max_rows=20,max_calls=30)
        self.assertEqual(backend.workers,[])

    def assert_author_failure_stays_failed(self, **options):
        frame,backend,instruction = self.setup_query(**options)
        with self.assertRaises(RuntimeError) as caught:
            run_cascade(frame,self.main,self.helper,self.args(generic=True),backend,
                instruction=instruction,query_id="author-failure",max_rows=20,max_calls=30)
        self.assertEqual(len(backend.workers),1)
        self.assertEqual(caught.exception.query_report["calls"]["usage"]["prompt_tokens"],sum(13+i for i in range(10)))
        self.assertTrue(backend.close_reports[0]["author_physical_resources_confirmed"])
        self.assertFalse(backend.close_reports[0]["stage_process_exit_confirmed"])
        self.assertIsNotNone(backend._active)
        self._cleanups = [c for c in self._cleanups if c[0] != backend.close]
        with self.assertRaises(RuntimeError):
            backend.close()
        self.assertFalse(backend.close_reports[0]["stage_process_exit_confirmed"])
        self.assertIsNotNone(backend._active)

    def test_physically_closed_author_abnormal_exit_cannot_be_healthy_on_retry(self):
        self.assert_author_failure_stays_failed(author_cleanup_failure=True)

    def test_late_stderr_capture_failure_rejects_physically_closed_stage(self):
        self.assert_author_failure_stays_failed(stderr_failure=True)

    def test_cross_stage_query_mismatch_never_allocates_second_author(self):
        frame,backend,instruction = self.setup_query()
        def evaluate(stage):
            if stage.name != "helper_score":
                stage = replace(stage,records=tuple(dict(row,**cascade_identity("other-query",row["row_id"],
                    row["source_position"],row["call_index"],model_role=row["model_role"],
                    stage=row["algorithm_stage"])) for row in stage.records))
            return backend(stage)
        evaluate.preflight, evaluate.check_lifecycle = backend.preflight,backend.check_lifecycle
        with self.assertRaisesRegex(ValueError,"belongs to another query"):
            run_cascade(frame,self.main,self.helper,self.args(generic=True),evaluate,
                instruction=instruction,query_id="original-query",max_rows=20,max_calls=30)
        self.assertEqual(len(backend.workers),1)
        self.assertEqual([p.name for p in Path(backend.author_args.output).iterdir()],["cascade-helper_score"])

    def test_wire_repeated_source_and_tampered_role_are_not_merged(self):
        report,frame,backend,instruction = self.execute()
        sample = backend.workers[1].stage
        self.assertLess(len({r["source_position"] for r in sample.records}),len(sample.records))
        mapping = CascadeOccurrences(sample)
        encoded = mapping.encode(list(sample.records))
        self.assertEqual(len({r["call_id"] for r in encoded}),len(sample.records))
        self.assertEqual([input_record(r,sample) for r in encoded],list(sample.records))
        altered = deepcopy(encoded[0])
        payload = json.loads(altered["record_json"])
        identity = json.loads(payload["identity_json"])
        identity["model_role"] = "helper"
        payload["identity_json"] = json.dumps(identity)
        altered["record_json"] = json.dumps(payload)
        with self.assertRaises(ValueError):
            input_record(altered,sample)
        with self.assertRaises(ValueError):
            mapping.encode([sample.records[0]])
        mapping = CascadeOccurrences(sample)
        block = mapping.encode(list(sample.records[:2]))
        results = {r["call_id"]:{k:v for k,v in r.items() if k!="imlane_worker"}
                   for r in report["calls"]["records"]}
        wires = [dict(call_id=r["call_id"],row_id=r["row_id"],outcome_json=json.dumps(
            backend.workers[1].envelope(r,result=results[r["call_id"]],attempted=True))) for r in block]
        bad = json.loads(wires[1]["outcome_json"])
        bad["worker"]["model_role"] = "helper"
        wires[1]["outcome_json"] = json.dumps(bad)
        with self.assertRaises(ValueError) as caught:
            mapping.decode(wires,block)
        self.assertEqual([r["call_id"] for r in caught.exception.partial_results],[block[0]["call_id"]])
        self.assertIsNotNone(caught.exception.partial_results[0]["prompt_tokens"])
        self.assertEqual(len(mapping.raw_result_batches),1)

    def test_explicit_cpu_interpreter_survives_embedded_sys_executable(self):
        spec = replace(self.engines["helper"],model_python="/explicit/cpu/python")
        process = CpuCascadeProcess.__new__(CpuCascadeProcess)
        with patch.object(sys,"executable","/author/embedded-executor"):
            self.assertEqual(process._command(spec,7)[0],spec.model_python)


if __name__ == "__main__":
    unittest.main()
