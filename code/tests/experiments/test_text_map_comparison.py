"""Finite selection preserves complete recordings, wrong labels and failures."""
from contextlib import nullcontext
from dataclasses import asdict, replace
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from src.baselines.common.private_artifacts import write_private_json
from src.baselines.text.sembench_movie import classification_audit
from src.experiments.postgresql.map_query_recording import record_execution, evaluate_recording
from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.query_execution import native_ray_runtime
from src.experiments.postgresql.text_map_comparison import SCHEMA, ROLES, read_run, summarize
from src.experiments.postgresql.m1_measurement import token_usage


def reference(path):
    return dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def recording(root, role, unit, seconds, *, capacity=2, manifest='a'*64):
    root.mkdir()
    config = QueryConfig(unit, 'pg' if role.startswith('pg-') and role != 'pg-source-direct' else role,
                         'map', 'inputs', concurrency=capacity)
    if role == 'pg-daft-ray':
        config = replace(config, map_transport_config='fixture.json', map_transport_sha256='d'*64)
    if role == 'ray-data':
        config = replace(config, ray_address='127.0.0.1:6379')
    outputs = [('first', 'POSITIVE'), ('second', 'POSITIVE')]
    execution = record_execution(root/'q0', lambda: nullcontext(iter(outputs)),
                                 max_rows=2, max_result_bytes=4096)
    evaluation = evaluate_recording(root/'q0', lambda rows: dict(actual_posts=2,
        quality=classification_audit({'first':'POSITIVE', 'second':'NEGATIVE'}, rows)))['result']
    summary = dict(config=asdict(config), status='passed', errors={}, executor_lifecycle='per-query',
        execution=execution, evaluation=evaluation, manifest_sha256=manifest,
        semantic_reference_sha256='b'*64, model_config_sha256='c'*64,
        query_preparation_started_ns=execution['t_query_terminal_ns']-int(seconds*1e9),
        resources=dict(ray_runtime=dict(owner='caller',startup='external',driver_disconnected=True)))
    write_private_json(root/'summary.json', summary)
    return reference(root/'summary.json')


def specification(root, *, manifest='a'*64):
    candidates=[]
    for role in ROLES:
        references = [recording(root/(role+str(i)),role,root.name+role+str(i),s,manifest=manifest)
                      for i,s in enumerate((8,2,3,2))]
        candidates.append(dict(id=role,role=role,warmup=references[:1],measured=references[1:]))
    return dict(schema=SCHEMA,stage='tuning',repeats=3,candidates=candidates)


class ComparisonTests(unittest.TestCase):
    def test_arrow_diagnostic_is_not_reported_as_daft_ray(self):
        with tempfile.TemporaryDirectory() as directory:
            ref = recording(Path(directory)/'unit', 'pg-daft-ray', 'unit', 1)
            path = Path(ref['path'])
            value = json.loads(path.read_text())
            value['config']['map_payload_backend'] = 'arrow'
            path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, 'Arrow/Ray'):
                read_run(reference(path), 'pg-daft-ray')

    def test_unique_best_selects_without_platform_or_quality_equivalence(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            spec=specification(root)
            runs=[recording(root/('fast'+str(i)),'pg-http','fast'+str(i),1,capacity=4) for i in range(4)]
            spec['candidates'].append(dict(id='fast',role='pg-http',warmup=runs[:1],measured=runs[1:]))
            result=summarize(spec)
            self.assertEqual(result['selected']['pg-http'],result['groups'][-1]['config_sha256'])
            self.assertEqual(result['groups'][0]['median_jct_seconds'],2)
            self.assertEqual(result['groups'][0]['measured'][0]['quality']['false_positive'],1)
            self.assertEqual(result['groups'][0]['warmup'][0]['jct_seconds'],8)
            self.assertFalse(result['saturation_proven'])
            self.assertFalse(result['quality_equivalence_proven'])

    def test_missing_reused_and_mixed_identity_records_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            spec=specification(Path(directory))
            broken=copy.deepcopy(spec);broken['candidates'][0]['measured'].pop()
            with self.assertRaises(ValueError):summarize(broken)
            broken=copy.deepcopy(spec);broken['candidates'][0]['measured'][1]=broken['candidates'][0]['measured'][0]
            with self.assertRaises(ValueError):summarize(broken)
            broken=copy.deepcopy(spec);broken['candidates'].pop()
            with self.assertRaises(ValueError):summarize(broken)
            ref=spec['candidates'][0]['measured'][0];path=Path(ref['path'])
            value=json.loads(path.read_text());value['model_config_sha256']='e'*64
            path.write_text(json.dumps(value));ref.update(reference(path))
            with self.assertRaises(ValueError):summarize(spec)

    def test_failed_record_or_modified_result_cannot_enter_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            ref=recording(Path(directory)/'unit','pg-http','unit',1)
            path=Path(ref['path']);original=path.read_bytes()
            value=json.loads(original);value['status']='failed'
            path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):read_run(reference(path),'pg-http')
            path.write_bytes(original)
            result_path=path.parent/'q0/results.jsonl'
            result_path.write_bytes(result_path.read_bytes()+b'\n')
            with self.assertRaises(ValueError):read_run(ref,'pg-http')
            self.assertEqual(path.read_bytes(),original)

    def test_shared_and_query_started_ray_results_cannot_mix(self):
        with tempfile.TemporaryDirectory() as directory:
            ref=recording(Path(directory)/'unit','ray-data','unit',1)
            path=Path(ref['path']);value=json.loads(path.read_text())
            value['resources']['ray_runtime']['owner']='query'
            path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):read_run(reference(path),'ray-data')

    def test_failed_saved_evaluation_is_not_accepted_even_with_matching_result(self):
        with tempfile.TemporaryDirectory() as directory:
            ref=recording(Path(directory)/'unit','pg-http','unit',1)
            path=Path(ref['path']).parent/'q0/evaluation.json'
            value=json.loads(path.read_text());value['status']='failed'
            path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):read_run(ref,'pg-http')

    def test_native_usage_counts_model_reported_tokens_without_inventing_missing_values(self):
        events=[dict(event='http_response',response=dict(usage=dict(prompt_tokens=3,completion_tokens=2))),
                dict(event='http_response',response=dict(usage=dict(prompt_tokens=5,completion_tokens=1)))]
        self.assertEqual(token_usage(events,2)['prompt_tokens'],8)
        self.assertEqual(token_usage(events,2)['output_tokens'],3)
        self.assertEqual(token_usage(events[:1],2)['status'],'unavailable')
        events[0]['response']['usage']['completion_tokens']=None
        self.assertEqual(token_usage(events,2)['status'],'unavailable')

    def test_evaluation_checks_tuning_identity_and_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);tune=root/'tune';tune.mkdir();evaluation=root/'evaluation';evaluation.mkdir()
            spec=specification(tune);result=summarize(spec)
            write_private_json(root/'selection.json',result)
            follow=specification(evaluation,manifest='f'*64)
            follow.update(stage='evaluation',tuning_result=reference(root/'selection.json'))
            self.assertIsNone(summarize(follow)['selected'])
            result['selected']['pg-http']='e'*64
            (root/'selection.json').write_text(json.dumps(result))
            with self.assertRaises(ValueError):summarize(follow)
            follow['tuning_result']=reference(root/'selection.json')
            with self.assertRaises(ValueError):summarize(follow)


class NativeRuntimeTests(unittest.TestCase):
    def ray(self):
        ray=Mock(__version__='2.56.1')
        ray.is_initialized.return_value=False
        ray.init.side_effect=lambda **kwargs:setattr(ray.is_initialized,'return_value',True)
        ray.shutdown.side_effect=lambda:setattr(ray.is_initialized,'return_value',False)
        ray.cluster_resources.return_value={'CPU':4,'object_store_memory':268435456}
        ray.nodes.return_value=[{'Alive':True, 'NodeManagerAddress':'127.0.0.1'}]
        return ray

    def test_existing_cluster_uses_connection_only_and_disconnects_on_error(self):
        ray=self.ray();config=QueryConfig('unit','ray-data','map','inputs',ray_address='127.0.0.1:6379')
        with self.assertRaisesRegex(ValueError,'fixture body'):
            with native_ray_runtime(ray,config,None) as report:
                raise ValueError('fixture body')
        ray.init.assert_called_once_with(address=config.ray_address, runtime_env=dict(env_vars={
            'RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY':'1'}))
        ray.shutdown.assert_called_once_with()
        self.assertTrue(report['driver_disconnected'])
        self.assertFalse(report['shared_cluster_shutdown_requested'])

    def test_mismatched_shared_resources_disconnect_before_work(self):
        for resources,nodes in (({'CPU':8,'object_store_memory':268435456},1),
                                ({'CPU':4,'GPU':1,'object_store_memory':268435456},1),
                                ({'CPU':4,'object_store_memory':134217728},1),
                                ({'CPU':4,'object_store_memory':268435456},2)):
            ray=self.ray();ray.cluster_resources.return_value=resources
            ray.nodes.return_value=[{'Alive':True, 'NodeManagerAddress':'127.0.0.1'}]*nodes
            config=QueryConfig('unit','ray-data','map','inputs',ray_address='127.0.0.1:6379')
            with self.subTest(resources=resources,nodes=nodes), self.assertRaises(ValueError):
                with native_ray_runtime(ray,config,None):self.fail('body should not run')
            ray.shutdown.assert_called_once_with()

    def test_owned_runtime_and_version_checks_remain(self):
        ray=self.ray();config=QueryConfig('unit','ray-data','map','inputs')
        with native_ray_runtime(ray,config,'/tmp/semloom-unused-fixture') as report:
            self.assertEqual(ray.init.call_args.kwargs['address'],'local')
            self.assertEqual(report['startup'],'included')
        self.assertTrue(report['driver_disconnected'])
        ray=self.ray();ray.__version__='other'
        with self.assertRaises(RuntimeError):
            with native_ray_runtime(ray,config,None):self.fail('body should not run')
        ray.init.assert_not_called();ray.shutdown.assert_not_called()

    def test_existing_cluster_selection_is_explicit_and_native_only(self):
        for address in ('auto','local','ray://localhost:10001','localhost:0','localhost:65536','999.0.0.1:6379',True):
            with self.subTest(address=address),self.assertRaises(ValueError):
                QueryConfig('unit','ray-data','map','inputs',ray_address=address)
        with self.assertRaises(ValueError):
            QueryConfig('unit','pg','map','inputs',ray_address='127.0.0.1:6379')

    def test_native_batch_overlap_is_explicit_bounded_and_sent_to_workers(self):
        config=QueryConfig('unit','ray-data','map','inputs',concurrency=8,ray_actors=2,
            ray_async_batches_per_actor=4,ray_batch_rows=1,ray_address='127.0.0.1:6379')
        ray=self.ray()
        with native_ray_runtime(ray,config,None):pass
        self.assertEqual(ray.init.call_args.kwargs['runtime_env']['env_vars'],
            {'RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY':'4'})
        with self.assertRaises(ValueError):replace(config,concurrency=7)
        for value in (0,True,1.5):
            with self.subTest(value=value),self.assertRaises(ValueError):
                replace(config,ray_async_batches_per_actor=value)
        with self.assertRaises(ValueError):
            QueryConfig('unit','pg','map','inputs',ray_async_batches_per_actor=4)

    def test_ray_reported_interface_address_is_validated_against_local_interfaces(self):
        from types import SimpleNamespace
        import socket
        from unittest.mock import patch
        config=QueryConfig('unit','ray-data','map','inputs',ray_address='192.0.2.1:6379')
        ray=self.ray();ray.nodes.return_value=[{'Alive':True,'NodeManagerAddress':'192.0.2.1'}]
        with patch('psutil.net_if_addrs',return_value={'fixture':[SimpleNamespace(family=socket.AF_INET,address='192.0.2.1')]}):
            with native_ray_runtime(ray,config,None) as report:
                self.assertEqual(report['owner'],'caller')
        ray=self.ray();ray.nodes.return_value=[{'Alive':True,'NodeManagerAddress':'192.0.2.1'}]
        with patch('psutil.net_if_addrs',return_value={}), self.assertRaises(ValueError):
            with native_ray_runtime(ray,config,None):self.fail('foreign node must not run')
        ray.shutdown.assert_called_once_with()
