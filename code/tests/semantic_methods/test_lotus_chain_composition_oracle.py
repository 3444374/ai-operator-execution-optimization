"""Small relation oracle; this test driver is not a production composition owner."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import time
import unittest

from tests.semantic_methods.test_lotus_chain_composition import (
    ResponseFixture, chain_for, normalized, run_driver, source_rows,
)
from src.semantic_methods.lotus import record_chain as chains
from src.semantic_methods.lotus.record_continuation import LotusChainProgram
from src.semantic_methods.lotus.record_relation import complete_extract_relation
from src.semantic_methods.lotus.record_results import ChainBatchResults, _source_binding
from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch
from src.serving.backends.vllm_engine import GenerationSpec


def relation_plan():
    model = 'openai/cpu-fixture'
    stages = (
        chains.StageSpec('step_0', chains.ExtractMethod(model, ('passage',),
            {'shared': 'text', 'optional': 'text'}), GenerationSpec(max_tokens=48, stop=(), response_format={'type':'json_object'})),
        chains.StageSpec('step_1', chains.MapMethod(model, 'Rewrite {shared}', 'shared'), GenerationSpec(max_tokens=32, stop=())),
        chains.StageSpec('step_2', chains.ExtractMethod(model, ('shared',),
            {'shared': 'text', 'optional': 'text'}), GenerationSpec(max_tokens=48, stop=(), response_format={'type':'json_object'})),
        chains.StageSpec('step_3', chains.MapMethod(model, 'Rewrite {shared}', 'final'), GenerationSpec(max_tokens=32, stop=())),
    )
    return chains.LotusRecordChain(stages, query_id='relation-oracle', relation_barriers=True)


class Replies(ResponseFixture):
    def one(self, row, generation):
        result = super().one(row, generation)
        position, stage = row['source_position'], row['algorithm_stage']
        if stage == 'step_0':
            result['text'] = ('{"shared":"first","optional":null}', '{"shared":"last"}', '{}')[position]
        elif stage == 'step_2':
            result['text'] = ('{"shared":"second"}', '{}', '{"optional":null}')[position]
        return result


def relation_oracle(plan, rows, infer):
    """Reuse the existing test's complete-EOF loop; supply expected values only.

    Production must drive its own plan/iterator and compare to this result. This
    helper proves neither an automatic composition entry nor pipelining across E.
    """
    current, parents, columns, calls = rows, None, None, []
    for segment in plan.relation_segments():
        options = dict(initial_parents=parents, columns=columns, allow_nulls=parents is not None)
        result = segment.run_batch(list(current), infer, **options)
        audit = ChainBatchResults(current, segment, initial_parents=parents, columns=columns)
        audit.accept(dict(result, records=[r.describe() for r in result['records']],
            inputs=[_source_binding(r) for r in current], batch_id=segment.stages[0].stage_id))
        audit.verify_success(source_exhausted=True)
        calls.extend(result['calls'])
        current, columns = result['records'], result['columns']
        if segment.stages[-1].kind == 'extract':
            completed = complete_extract_relation(current, columns=columns,
                extract_stage=segment.stages[-1], query_id=plan.query_id,
                calls=result['calls'], source_exhausted=True)
            current, parents, columns = completed.records, completed.parent_call_ids, completed.columns
    return dict(records=list(current), columns=list(columns), calls=calls)


def local_partition_oracle(plan):
    """Test-side expected binding, not an execution factory or routing feature."""
    if len({s.method.model_name for s in plan.stages}) != 1:
        raise NotImplementedError('cross-model deployment routing is unsupported')
    sections, pending = [], []
    for stage in plan.stages:
        if pending and (stage.kind=='extract' or stage.model_role!=pending[0].model_role):
            sections.append(('record_local', tuple(s.stage_id for s in pending)))
            pending=[]
        if stage.kind=='extract':sections.append(('complete_relation', (stage.stage_id,)))
        else:pending.append(stage)
    if pending:sections.append(('record_local',tuple(s.stage_id for s in pending)))
    return tuple(sections)


class CompositionOracleTests(unittest.TestCase):
    def setUp(self):
        import lotus
        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__(); self.addCleanup(scope.__exit__, None, None, None)

    def test_one_serialized_plan_is_unchanged_across_fused_and_local_FM(self):
        original = chain_for('MMFM')
        wire = json.loads(json.dumps(original.describe()))
        same_plan = chains.LotusRecordChain.from_dict(wire)
        rows = source_rows()
        reference = ResponseFixture(same_plan)
        fused = same_plan.run_batch(rows, reference)
        local, requests, _ = run_driver(same_plan, rows)
        self.assertEqual(normalized(local), normalized(fused))
        self.assertEqual(requests, reference.requests)
        self.assertEqual(wire, json.loads(json.dumps(same_plan.describe())))

    def test_repeated_Extract_oracle_closes_groups_and_preserves_overwrite_and_parents(self):
        plan, rows = relation_plan(), source_rows()
        wire, source = deepcopy(plan.describe()), [r.describe() for r in rows]
        replies = Replies(plan)
        result = relation_oracle(chains.LotusRecordChain.from_dict(wire), rows, replies)
        values = [r.values for r in result['records']]
        self.assertEqual([v['shared'] for v in values], ['second', 'second', 'derived/step_1/occ-2'])
        # Explicit JSON null is stringified by the original parser. Missing keys
        # do not erase the previous writer; absent group keys become schema null.
        self.assertEqual([v['optional'] for v in values], ['None', 'None', 'None'])
        self.assertEqual([v['final'] for v in values], ['derived/step_3/occ-'+str(i) for i in range(3)])
        self.assertEqual(result['columns'], ['passage','row_id','call_id','shared','optional','final'])
        by_stage = {s.stage_id:[c for c in result['calls'] if c['algorithm_stage']==s.stage_id] for s in plan.stages}
        self.assertEqual([len(v) for v in by_stage.values()], [3,3,3,3])
        for stage in ('step_1','step_3'):
            preceding = 'step_0' if stage=='step_1' else 'step_2'
            expected = [c['call_id'] for c in by_stage[preceding] if c['source_position']<2]
            for c in by_stage[stage][:2]:self.assertEqual(c['parent_call_ids'],expected)
        last_call = next(c for c in by_stage['step_3'] if c['source_position']==0)
        last_message = replies.requests[last_call['call_id']][0]
        self.assertIn('second',last_message);self.assertNotIn('derived/step_1/occ-0',last_message)
        self.assertEqual([r.describe() for r in rows],source)
        self.assertEqual(plan.describe(),wire)
        # No claim that the same E plan is a supported local FIFO program.
        with self.assertRaisesRegex(ValueError,'Filter/Map'):
            LotusChainProgram(plan,max_result_bytes=65536)

    def test_unknown_second_Extract_never_generates_the_final_Map(self):
        plan = relation_plan(); replies = Replies(plan)
        def incomplete(prepared, *, generation):
            values = replies(prepared,generation=generation)
            if prepared[0]['algorithm_stage']=='step_2':values[0].update(status='unknown',error_type='FixtureUnknown')
            return values
        with self.assertRaisesRegex(RuntimeError,'unknown') as caught:
            relation_oracle(plan,source_rows(),incomplete)
        self.assertFalse(any('step_3' in call for call in replies.requests))
        self.assertTrue(any(c['status']=='unknown' for c in caught.exception.chain_calls))
        self.assertEqual({c['algorithm_stage'] for c in caught.exception.chain_calls},{'step_1','step_2'})

    def test_empty_relation_after_middle_Filter_preserves_schema_and_no_later_requests(self):
        original=relation_plan()
        middle=chains.StageSpec('step_1',chains.FilterMethod('openai/cpu-fixture','Keep {shared}'),GenerationSpec(max_tokens=8))
        plan=chains.LotusRecordChain((original.stages[0],middle,*original.stages[2:]),
                                    query_id=original.query_id,relation_barriers=True)
        replies=Replies(plan)
        def discard(prepared,*,generation):
            values=replies(prepared,generation=generation)
            if prepared[0]['algorithm_stage']=='step_1':
                for value in values:value['text']='False'
            return values
        result=relation_oracle(plan,source_rows(),discard)
        self.assertEqual(result['records'],[])
        self.assertEqual(result['columns'],['passage','row_id','call_id','shared','optional','final'])
        self.assertEqual(len(result['calls']),6)
        self.assertEqual({c['algorithm_stage'] for c in result['calls']},{'step_0','step_1'})
        last=plan.relation_segments()[-1]
        audit=ChainBatchResults([],last,initial_parents={},columns=result['columns'])
        with self.assertRaises(ValueError):audit.verify_success(source_exhausted=False)
        audit.verify_success(source_exhausted=True)

    def test_deployment_change_after_EOF_is_rejected_without_mutating_resident_phase(self):
        plan = relation_plan(); first,next_phase = plan.relation_segments()[:2]
        changed = replace(next_phase.stages[0],method=replace(next_phase.stages[0].method,model_name='openai/other-deployment'))
        foreign = chains.LotusRecordChain((changed,),query_id=plan.query_id)
        # Exercise the actual pre-allocation transition on minimal completed-owner
        # state. There is no model process, Ray actor or claimed cross-model route.
        worker = object.__new__(FusedChainBatch)
        worker.chain=first;worker.expected={r.row_id:_source_binding(r) for r in source_rows()}
        worker.received=set(worker.expected);worker.failed=False;worker._phase_cleanup_errors=[]
        worker._owner_close_started=False;worker.close_on_source_complete=False
        worker._used_stage_ids={s.stage_id for s in first.stages};worker._model_name=first.stages[0].method.model_name
        worker.engine_spec=SimpleNamespace(max_model_len=4096);worker.deadline=time.monotonic()+10
        before=deepcopy(first.describe());ids=set(worker._used_stage_ids)
        with tempfile.TemporaryDirectory() as directory:
            worker.control=Path(directory)
            with self.assertRaisesRegex(ValueError,'deployment'):
                worker.begin_relation_phase(foreign.describe(),list(worker.expected.values()),
                    initial_parents={(r.row_id,r.source_position):('previous-'+r.row_id,) for r in source_rows()},
                    columns=list(source_rows()[0].values),allow_nulls=True,source_exhausted=True)
        self.assertEqual(worker.chain.describe(),before);self.assertEqual(worker._used_stage_ids,ids)

    def test_partition_reference_exposes_EOF_and_role_boundaries_without_inventing_a_router(self):
        original=chain_for('MEMFEM')
        stages=tuple(replace(s,model_role=r) for s,r in zip(original.stages,
            ('main','main','helper','helper','helper','main'),strict=True))
        plan=chains.LotusRecordChain(stages,query_id='opaque-other-plan',relation_barriers=True)
        wire=deepcopy(plan.describe())
        expected=(('record_local',('step_0',)),('complete_relation',('step_1',)),
                  ('record_local',('step_2','step_3')),('complete_relation',('step_4',)),
                  ('record_local',('step_5',)))
        self.assertEqual(local_partition_oracle(plan),expected)
        basic=tuple(tuple(s.stage_id for s in segment.stages) for segment in plan.relation_segments())
        self.assertEqual(basic,(('step_0','step_1'),('step_2','step_3','step_4'),('step_5',)))
        self.assertEqual([stage for _,part in expected for stage in part],[s.stage_id for s in plan.stages])
        self.assertEqual(plan.describe(),wire)
        changed=replace(stages[-1],method=replace(stages[-1].method,model_name='openai/other-deployment'))
        cross_model=chains.LotusRecordChain((*stages[:-1],changed),query_id=plan.query_id,relation_barriers=True)
        with self.assertRaisesRegex(NotImplementedError,'cross-model'):
            local_partition_oracle(cross_model)
        # A future production binder must be compared to this reference. These
        # assertions do not say that such a binder or mixed-role owner exists.


if __name__=='__main__':
    unittest.main()
