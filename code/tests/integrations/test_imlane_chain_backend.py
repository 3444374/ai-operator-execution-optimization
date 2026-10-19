"""Original LOTUS differential over an explicit fixture author carrier."""

from copy import deepcopy
from functools import partial
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid

from integrations.imlane_records.chain_backend import IMLaneChainBackend
from integrations.imlane_records.chain_udf import ChainWorker
from integrations.imlane_records.owner import StopBudget
from src.semantic_methods.lotus.engine import engine_response
from src.baselines.text.frameworks.lotus_chain_results import ChainExecutionError
from src.semantic_methods.lotus.record_chain import (
    ExtractMethod,LotusRecordChain,LotusRecordMethod,MapMethod,RecordOccurrence,StageSpec,records_to_frame,
)
from src.semantic_methods.lotus.records import FilterMethod,RecordsResources
from src.serving.backends.vllm_engine import EngineSpec,GenerationSpec
from tests.integrations.imlane_chain_fixture import CpuChainProcess,reply,terminal
from tests.integrations.test_imlane_chain import LocalAuthor


def inputs():
    return [RecordOccurrence("occ-"+str(i),i,(17,17,29)[i],
        dict(title="title-"+str(i),body="doc"+str(i)+(" KEEP" if i==0 else " DROP"),
             row_id="business",call_id="business-call"),source_refs={"source":[i]}) for i in range(3)]


def plan(kinds, query="ordinary-chain"):
    stages, columns = [],("title","body")
    for i,kind in enumerate(kinds):
        if kind == "map":
            method = MapMethod("openai/local-fixture","Summarize "+" and ".join("{"+c+"}" for c in columns),"derived"+str(i))
            columns = (method.output_column,)
        elif kind == "extract":
            method = ExtractMethod("openai/local-fixture",columns,{"field":"signal"})
            columns = ("field",)
        else:
            method = FilterMethod("openai/local-fixture","Keep {"+columns[0]+"}")
        stages.append(StageSpec(kind+"-"+str(i),method,GenerationSpec(max_tokens=32+i,stop=(),
            response_format={"type":"json_object"} if kind=="extract" else None),model_role="operation-"+str(i)))
    return LotusRecordChain(tuple(stages),query_id=query,relation_barriers=True)


class FixtureEngine:
    def __init__(self, spec, generation, *_ , fail=False, unknown_close=False):
        self.ready_report = dict(engine_uid="fixture-"+uuid.uuid4().hex,fixture=True)
        self.calls, self.limit, self.closed = [],spec.max_model_calls,False
        self.fail, self.unknown_close = fail,unknown_close

    def run_prepared(self, rows, *, generation):
        assert not self.closed and len(self.calls)+len(rows)<=self.limit
        self.calls.extend(deepcopy(rows))
        return [terminal(row,reply(row["algorithm_stage"].split("-")[0],json.loads(row["messages_json"])),
            self.ready_report["engine_uid"],failed=self.fail and row["source_position"]==1) for row in rows]

    def close(self, **_):
        self.closed = True
        return dict(status="unknown" if self.unknown_close else "closed",process_exit_confirmed=True,
            cleanup_errors=[],engine_ready=self.ready_report,gpu_exit_confirmed=None,
            engine_api=dict(status="unknown" if self.unknown_close else "closed",
                            engine_api_shutdown_confirmed=not self.unknown_close))


class FixtureBackend(IMLaneChainBackend):
    def __init__(self,*args,engine_type=FixtureEngine,author_fail=False,**kwargs):
        super().__init__(*args,**kwargs)
        self.engine_type, self.author_fail = engine_type,author_fail
        self.workers, self.clients, self.messages = [],[],{}

    def _open_phase(self, segment, config, control):
        self._check_author()
        if self.workers:
            assert self.workers[-1].closed
        worker = ChainWorker(config,engine_type=self.engine_type)
        # Observe original preparation without supplying or changing messages.
        for method in worker.batch.chain.methods:
            prepare = method.prepare_rows
            def capture(rows, _prepare=prepare, **kw):
                prepared = _prepare(rows,**kw)
                self.messages.update({row["call_id"]:json.loads(row["messages_json"]) for row in prepared})
                return prepared
            method.prepare_rows = capture
        self.workers.append(worker)
        class OutOfOrderAuthor(LocalAuthor):
            max_pending = 0
            def submit(self,*args,**kw):
                result=super().submit(*args,**kw)
                self.max_pending=max(self.max_pending,len(self.pending))
                return result
            def wait_ready(self,**kw):
                return list(reversed(self.pending))
        client = OutOfOrderAuthor(worker)
        self.clients.append(client)
        physical = {}
        def close():
            if not physical:
                physical.update(resources_confirmed=True,fixture_author=True)
                self.author_report["cleanup"].append(dict(namespace=str(control),passed=not self.author_fail))
                if self.author_fail:
                    raise RuntimeError("declared abnormal author exit after physical settlement")
            self._check_author(str(control))
            return physical
        return client,close,physical


class IMLaneChainBackendTests(unittest.TestCase):
    def setUp(self):
        import lotus
        import pandas as pd
        self.pd = pd
        self.temp = tempfile.TemporaryDirectory(prefix="imlane-methods-cpu-",dir="/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.context = lotus.settings.context(enable_cache=False)
        self.context.__enter__();self.addCleanup(self.context.__exit__,None,None,None)
        self.preflight = patch.object(EngineSpec,"validate_driver_environment",return_value=None)
        self.preflight.start();self.addCleanup(self.preflight.stop)
        self.records = inputs()
        self.index = pd.Index([17,17,29],dtype="int64",name="document-label")
        self.frame = pd.DataFrame([r.values for r in self.records],index=self.index)

    def backend(self, **kw):
        from types import SimpleNamespace
        out = self.root/("query-"+str(len(list(self.root.glob("query-*")))))
        out.mkdir()
        return FixtureBackend(engine=EngineSpec(str(self.root),sys.executable,sys.executable,max_model_calls=64),
            resources=RecordsResources(model_batch_rows=1,max_active_batches=2,query_timeout_s=90),
            author_args=SimpleNamespace(output=out,stop_budget=StopBudget(threading.Event())),
            model_environment={"CUDA_VISIBLE_DEVICES":"-1"},**kw)

    def original(self, chain, frame):
        import lotus
        messages = {}
        for stage in chain.stages:
            lm = LotusRecordMethod(stage).lm
            captured = []
            def respond(data, options, *_):
                captured.extend(message for message,_ in data)
                values = []
                for message,_ in data:
                    row = dict(status="succeeded",text=reply(stage.kind,message),submission_confirmed=True,
                               prompt_tokens=5,completion_tokens=2,total_tokens=7,finish_reason="stop",
                               engine_request_id="original-"+stage.stage_id+"-"+str(len(values)))
                    values.append(engine_response(row,lm))
                return values
            with patch.object(lm,"_process_uncached_messages",side_effect=respond),lotus.settings.context(lm=lm):
                if stage.kind=="map":
                    frame=frame.sem_map(stage.method.instruction,suffix=stage.method.output_column)
                elif stage.kind=="extract":
                    frame=frame.sem_extract(list(stage.method.input_columns),stage.method.output_columns)
                else:
                    frame=frame.sem_filter(stage.method.instruction)
            messages[stage.stage_id]=captured
        return frame,messages

    def assert_original(self, kinds, *, process=False):
        method = plan(kinds)
        backend = self.backend(engine_type=CpuChainProcess if process else FixtureEngine)
        stream = backend.evaluate(iter(self.records),method,expected=self.records,columns=list(self.frame.columns))
        rows = list(stream)
        native,messages = self.original(method,self.frame.copy())
        self.pd.testing.assert_frame_equal(records_to_frame(rows,columns=stream.columns,source_index=self.index),native)
        for stage in method.stages:
            calls=sorted((r for r in stream.calls if r["algorithm_stage"]==stage.stage_id),key=lambda r:r["source_position"])
            self.assertEqual([backend.messages[r["call_id"]] for r in calls],messages[stage.stage_id])
        self.assertEqual(len({r["call_id"] for r in stream.calls}),len(stream.calls))
        self.assertTrue(stream.completed)
        self.assertEqual(stream.last_close_report["status"],"closed")
        self.assertIsNone(stream.last_close_report["gpu_exit_confirmed"])
        self.assertEqual(len(backend.workers),len(method.relation_segments()))
        self.assertTrue(all(w.closed for w in backend.workers))
        self.assertEqual(backend.clients[0].max_pending,2)
        self.assertFalse(stream.last_close_report["engine_reuse_across_segments"])
        self.assertTrue(all(p["source_exhausted"] for p in stream.phase_history))
        if len(method.relation_segments())>1:
            first={r["call_id"] for r in stream.calls if r["algorithm_stage"]==method.stages[0].stage_id
                   and r["source_position"] in (0,1)}
            later=[r for r in stream.calls if r["algorithm_stage"]==method.relation_segments()[1].stages[0].stage_id
                   and r["source_position"] in (0,1)]
            if method.stages[0].kind=="extract":
                self.assertTrue(all(set(r["parent_call_ids"])==first for r in later))
        return backend,stream,rows

    def test_ordinary_map_extract_and_combinations_match_original_sdk(self):
        for kinds in (("map",),("extract",),("map","extract"),("extract","filter"),
                      ("map","extract","filter","map"),("extract","extract","filter")):
            with self.subTest(kinds=kinds):
                self.assert_original(kinds)

    def test_map_extract_and_extract_filter_with_real_cpu_children(self):
        for kinds in (("map","extract"),("extract","filter")):
            with self.subTest(kinds=kinds):
                backend,stream,_=self.assert_original(kinds,process=True)
                reports=stream.last_close_report["engine_reports"]
                self.assertEqual(len({r["engine_ready"]["engine_uid"] for r in reports}),len(backend.workers))
                self.assertTrue(all(r["process_exit_confirmed"] for r in reports))

    def test_failed_extract_keeps_known_usage_and_never_starts_filter(self):
        backend=self.backend(engine_type=partial(FixtureEngine,fail=True))
        stream=backend.evaluate(self.records,plan(("extract","filter")),expected=self.records)
        with self.assertRaises(ChainExecutionError) as caught:list(stream)
        self.assertEqual(caught.exception.remote_error_type,"RuntimeError")
        self.assertEqual(len(backend.workers),1)
        self.assertEqual(len(caught.exception.chain_calls),2)
        self.assertEqual(sum(r["prompt_tokens"] for r in caught.exception.chain_calls),10)
        self.assertEqual({r["source_position"]:r["status"] for r in stream.calls},{0:"succeeded",1:"failed"})
        failed=next(r for r in stream.calls if r["status"]=="failed")
        self.assertEqual((failed["source_position"],failed["error_type"]),(1,"DeclaredCPUFailure"))
        self.assertFalse(stream.completed)
        self.assertEqual(len(list(Path(backend.args.output).glob("phase-*"))),1)

    def test_abnormal_author_close_stays_failed_on_retry(self):
        backend=self.backend(author_fail=True)
        stream=backend.evaluate(self.records,plan(("extract","filter")),expected=self.records)
        with self.assertRaises(RuntimeError):list(stream)
        self.assertEqual(len(backend.workers),1)
        self.assertTrue(stream._phases[0].author_state["resources_confirmed"])
        self.assertTrue(stream._phases[0].last_close_report["model"]["engine_api"]["engine_api_shutdown_confirmed"])
        self.assertEqual(stream.close()["status"],"unknown")
        self.assertFalse(stream.completed)

    def test_unknown_model_api_close_keeps_owner_and_blocks_successor(self):
        backend=self.backend(engine_type=partial(FixtureEngine,unknown_close=True))
        stream=backend.evaluate(self.records,plan(("extract","filter")),expected=self.records)
        with self.assertRaises(RuntimeError):list(stream)
        self.assertEqual(len(backend.workers),1)
        self.assertFalse(backend.workers[0].closed)
        self.assertIsNotNone(backend.workers[0].batch.engine)
        self.assertEqual(stream.close()["status"],"unknown")

    def test_empty_source_retains_schema_and_index_without_model_allocation(self):
        backend=self.backend()
        stream=backend.evaluate([],plan(("map","extract")),expected=[],columns=list(self.frame.columns))
        rows=list(stream)
        native,_=self.original(plan(("map","extract")),self.frame.iloc[:0].copy())
        actual=records_to_frame(rows,columns=stream.columns,source_index=self.index,empty_prototype=native)
        self.pd.testing.assert_frame_equal(actual,native)
        self.assertIsNone(backend.workers[0].batch.engine)

    def test_empty_filter_relation_preserves_later_map_dtype_without_extra_calls(self):
        class EmptyFilterEngine(FixtureEngine):
            def run_prepared(self, rows, *, generation):
                return [dict(row,text="False") for row in super().run_prepared(rows,generation=generation)]

        backend=self.backend(engine_type=EmptyFilterEngine)
        chain=plan(("filter","map","extract"))
        stream=backend.evaluate(self.records,chain,expected=self.records,columns=list(self.frame.columns))
        rows=list(stream)
        original_reply=reply
        with patch(__name__+".reply",side_effect=lambda kind,messages:
                   "False" if kind=="filter" else original_reply(kind,messages)):
            native,_=self.original(chain,self.frame.copy())
        actual=records_to_frame(rows,columns=stream.columns,source_index=self.index,empty_prototype=native)
        self.pd.testing.assert_frame_equal(actual,native)
        self.assertEqual(rows,[])
        self.assertEqual(len(stream.calls),len(self.records))
        self.assertTrue(all(row["algorithm_stage"]==chain.stages[0].stage_id for row in stream.calls))
        self.assertTrue(stream.completed)
        self.assertEqual(stream.last_close_report["status"],"closed")

    def test_cancel_before_owner_and_query_reuse_are_rejected(self):
        backend=self.backend(cancelled=lambda:True)
        with self.assertRaises(InterruptedError):backend.evaluate(self.records,plan(("map",)),expected=self.records)
        self.assertEqual(backend.workers,[])
        with self.assertRaisesRegex(RuntimeError,"one query"):
            backend.evaluate(self.records,plan(("map",),"another-query"),expected=self.records)


if __name__=="__main__":
    unittest.main()
