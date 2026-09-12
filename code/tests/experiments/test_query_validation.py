"""Reject altered records and misassociation while retaining model quality errors."""
from contextlib import contextmanager
from dataclasses import asdict, replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import content_digest
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.postgresql.map_direct import request_body
from src.experiments.postgresql.map_query_recording import record_execution
from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.query_evaluation import evaluate
from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.query_workloads import prepare
from src.experiments.query_resources import verify_logical_resources, RESOURCE_KEYS


def response(value):
    return dict(model='fixture',choices=[dict(index=0,message=dict(role='assistant',content=value),finish_reason='stop')],
                usage=dict(prompt_tokens=10,completion_tokens=1,total_tokens=11))


class QueryValidationTests(unittest.TestCase):
    def test_pg_organization_uses_independent_selected_input_count(self):
        from tests.experiments.test_organization_evaluation import evidence
        from tests.experiments.test_window_memory import WindowMemoryTests
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prepare(root/'data', 'movie', [('z', 'other', 'unused', 'NEGATIVE')],
                    {'source': 'fixture empty selection'}, max_rows=1)
            organization, events = evidence()
            (root/'organization.json').write_text(json.dumps(asdict(organization)))
            (root/'events.jsonl').write_text(json.dumps(events[0])+'\n')
            (root/'plan.json').write_text('[{"Plan":{"Node Type":"Custom Scan"}}]')
            (root/'pg-backend.json').write_text('{"backend_pid":17}')
            config = QueryConfig('unit', 'pg', 'map', 'rows', movie_id='absent', pg_total_budget=True,
                                 organization_config='organization.json', organization_sha256='a'*64)
            memory = WindowMemoryTests().value(retained_limit=config.pg_window_bytes,
                staging_limit=config.pg_staging_bytes, peak_retained_rows=0, peak_retained_bytes=0)
            (root/'q0-producer.log').write_text('LOG: SEMLOOM_WINDOW_MEMORY '+json.dumps(memory))
            @contextmanager
            def empty(): yield iter(())
            record_execution(root/'q0', empty, max_rows=1, max_result_bytes=4096)
            plan = SemanticMapPlan('Classify', 'fixture', 128)
            def run(selected):
                import shutil
                target = root/selected
                target.mkdir()
                for name in ('organization.json', 'events.jsonl', 'plan.json', 'pg-backend.json', 'q0-producer.log'):
                    shutil.copyfile(root/name, target/name)
                shutil.copytree(root/'q0', target/'q0')
                return evaluate(replace(config, movie_id=selected), QueryInputs('movie', 'rows', 1),
                                plan, root/'data/manifest.json', target, None)
            self.assertEqual(run('absent')['organization']['rows'], 0)
            # Identical empty execution evidence cannot validate a nonempty selection.
            with self.assertRaisesRegex(ValueError, 'independent expected task count'):
                run('other')

    def case(self, root, arm, outputs=('POSITIVE','NEGATIVE'), *, same_text=False):
        plan=SemanticMapPlan('Classify','fixture',128)
        texts=['same','same'] if same_text else ['alpha','beta']
        prepare(root/'data','movie',[(str(i),'movie',texts[i],label) for i,label in enumerate(('POSITIVE','NEGATIVE'))],
                {'source':'fixture'},max_rows=2)
        events=[]
        for i in range(2):
            key=dict(session_id=1,sequence=i)
            body=request_body(plan,texts[i])
            events.extend([dict(event='request',attempt=i+1,key=key,body=body,
                identity=dict(row_id=str(i),source_position=i)),
                dict(event='http_started',monotonic_ns=i*2),dict(event='http_finished',monotonic_ns=i*2+1)])
            if arm=='pg-source-direct':
                events.extend([dict(event='direct_input',key=key,row_id=str(i),request_values_sha256=content_digest(body)),
                               dict(event='direct_completion',key=key,raw_output=('POSITIVE','NEGATIVE')[i],
                                    response_model_id='fixture',prompt_tokens=10,output_tokens=1,finish_reason='stop')])
            else:
                events.append(dict(event='http_response',attempt=i+1,response=response(('POSITIVE','NEGATIVE')[i])))
        (root/'events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
        @contextmanager
        def string_ids():yield iter((str(i),v) for i,v in enumerate(outputs))
        record_execution(root/'q0',string_ids,max_rows=2,max_result_bytes=4096)
        return lambda:evaluate(QueryConfig('unit',arm,'map','rows'),QueryInputs('movie','rows',2),plan,
            root/'data/manifest.json',root,None)

    def test_changed_timestamp_incomplete_status_byte_count_and_missing_hash_fail_before_scoring(self):
        for field in ('timestamp','status','query_status','recorded_bytes','results_sha256'):
            with self.subTest(field=field),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);run=self.case(root,'pg-source-direct')
                path=root/'q0/execution.json';record=json.loads(path.read_text())
                if field=='timestamp':
                    path2=root/'q0/results.jsonl';values=[json.loads(l) for l in path2.read_text().splitlines()]
                    values[0]['received_ns']+=1;path2.write_text(''.join(json.dumps(v)+'\n' for v in values))
                elif field in ('status','query_status'):record[field]='failed'
                elif field=='recorded_bytes':record[field]+=1
                else:record.pop(field)
                path.write_text(json.dumps(record))
                with patch('src.experiments.postgresql.query_evaluation.classification_audit') as score:
                    with self.assertRaises(ValueError):run()
                    score.assert_not_called()

    def test_swapped_outputs_with_fresh_valid_hash_fail_in_both_native_map_arms(self):
        for arm in ('pg-source-direct','ray-data'):
            for duplicate in (False,True):
                with self.subTest(arm=arm,duplicate=duplicate),tempfile.TemporaryDirectory() as directory:
                    run=self.case(Path(directory),arm,('NEGATIVE','POSITIVE'),same_text=duplicate)
                    with self.assertRaisesRegex(ValueError,'per-row outputs'):run()

    def test_bound_wrong_model_answers_still_enter_quality_denominator(self):
        for arm in ('pg-source-direct','ray-data'):
            with self.subTest(arm=arm),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);run=self.case(root,arm,('NEGATIVE','POSITIVE'),same_text=True)
                events=[json.loads(l) for l in (root/'events.jsonl').read_text().splitlines()]
                for event in events:
                    if event['event']=='direct_completion':
                        event['raw_output']=('NEGATIVE','POSITIVE')[event['key']['sequence']]
                    if event['event']=='http_response':
                        event['response']=response(('NEGATIVE','POSITIVE')[event['attempt']-1])
                (root/'events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
                result=run()
                self.assertEqual(result['association']['rows'],2)
                self.assertEqual(result['quality']['false_positive'],1)
                self.assertEqual(result['quality']['false_negative'],1)

    def test_runtime_overage_negative_missing_usage_cannot_hide_behind_final_zero(self):
        limits=dict.fromkeys(RESOURCE_KEYS,4)
        for field in RESOURCE_KEYS:
            for value in (-1,5,None):
                with self.subTest(field=field,value=value):
                    usage=dict.fromkeys(RESOURCE_KEYS,1)
                    if value is None:usage.pop(field)
                    else:usage[field]=value
                    events=[dict(event='core_submitted',usage=usage),
                            dict(event='core_job_drained',usage=dict.fromkeys(RESOURCE_KEYS,0))]
                    with self.assertRaises(ValueError):verify_logical_resources(events,limits)
        with self.assertRaises(ValueError):verify_logical_resources([],limits)
        self.assertIsNone(verify_logical_resources([],limits,require_usage=False)['logical_peaks'])

    def test_ray_cpu_and_actor_configuration_do_not_follow_http_capacity(self):
        first=QueryConfig('unit','ray-data','map','rows')
        second=replace(first,concurrency=8)
        self.assertEqual(first.ray_num_cpus,second.ray_num_cpus)
        self.assertEqual(first.ray_actors,second.ray_actors)
        with self.assertRaises(ValueError):replace(first,concurrency=1)
