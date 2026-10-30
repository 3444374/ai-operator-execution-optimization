"""Launch provenance and standard entries with scoped CPU-only native substitutes."""
import argparse
from contextlib import contextmanager, redirect_stdout
import csv
import hashlib
import io
import json
from pathlib import Path
import runpy
import sys
import tempfile
from types import ModuleType, SimpleNamespace as NS
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import adapter, cpu_native, native_server as server


ROOT = Path(__file__).resolve().parents[4]


def binding():
    return dict(native_launch='author_defaults', model='CPU-fixture-model', port=18103,
                source_commit=server.AUTHOR_SOURCE_COMMIT)


def worker_config():
    return NS(model_config=NS(max_model_len=8192,dtype='torch.bfloat16'),
        scheduler_config=NS(max_num_seqs=333,max_num_batched_tokens=8191,enable_chunked_prefill=True),
        cache_config=NS(gpu_memory_utilization=.91,enable_prefix_caching=True,block_size=16,num_gpu_blocks=None),
        parallel_config=NS(tensor_parallel_size=1,pipeline_parallel_size=1),
        compilation_config=NS(max_cudagraph_capture_size=512),attention_config=NS(backend='CPU-fixture'))


@contextmanager
def native_substitutes(model_length=8192):
    """No upstream parser/runtime qualification is claimed by this substitute."""
    modules = {}
    def module(name, **attrs):
        value=ModuleType(name);value.__path__=[];value.__dict__.update(attrs);modules[name]=value
        return value
    class Serve:
        def subparser_init(self, subparsers):
            p=subparsers.add_parser('serve');p.add_argument('model_tag')
            for flag in ('host','port','worker-extension-cls'):p.add_argument('--'+flag)
            p.set_defaults(max_num_seqs=None,max_num_batched_tokens=None,max_model_len=None,
                gpu_memory_utilization=.91,dtype='auto',tensor_parallel_size=1,seed=0,
                attention_backend=None,enable_prefix_caching=None,enable_chunked_prefill=None,
                kv_cache_metrics_sample=.01,model='other-native-default')
        def cmd(self, args):raise AssertionError('CPU check started a service')
    class Args:
        @classmethod
        def from_cli_args(cls, parsed):
            assert parsed.model==parsed.model_tag=='CPU-fixture-model'
            return cls()
        def create_model_config(self):return NS(max_model_len=model_length,dtype='torch.bfloat16')
    class Manager:
        def __init__(self):self.tokenizer=NS(chat_template='CPU-fixture-template')
        @classmethod
        def init(cls, *args):cls.instance=cls()
        @classmethod
        def get_instance(cls):return cls.instance
        def reset_stages(self):pass
        def apply_chat_template(self, prompt):return prompt
        def token_length(self, text):return len(text)
    class SemFilter:
        max_tokens=8;pin=False;unpin=False
        def _build_prompts(self, ctx):return None,ctx.data
    class Plan:
        def __init__(self, *args, **kw):pass
        def parse_ops(self, ops):return [SemFilter()]
        def build(self, contexts, ops):return [ops]
    class Executor:
        def __init__(self, model):self.model=model
        def _build_chat_request(self, *args):
            return NS(model=self.model,seed=42,frequency_penalty=.5,repetition_penalty=1.3)
    def data_source(raw, path, executor):
        with path.open() as stream:return [NS(data=row['data']) for row in csv.DictReader(stream)]
    module('torch',cuda=NS(is_initialized=lambda:False))
    module('vllm',__version__='CPU-native-substitute')
    for name in ('vllm.kalypso','vllm.kalypso.execution','vllm.kalypso.sem_ops',
                 'vllm.entrypoints','vllm.entrypoints.cli','vllm.utils','vllm.engine'):
        module(name)
    module('vllm.kalypso.budget',KVMemoryManager=Manager)
    module('vllm.kalypso.controller',SemanticPlan=Plan)
    module('vllm.kalypso.execution.vllm_executor',VLLMExecutor=Executor)
    module('vllm.kalypso.sem_ops.filters',SemFilter=SemFilter)
    module('vllm.kalypso.data',data=NS(_data_source=data_source))
    module('vllm.entrypoints.cli.serve',ServeSubcommand=Serve)
    module('vllm.utils.argparse_utils',FlexibleArgumentParser=argparse.ArgumentParser)
    module('vllm.engine.arg_utils',AsyncEngineArgs=Args)
    # Restore only our owned stub keys, never roll back the full import registry.
    with patch.dict(sys.modules,modules):yield


class NativeLaunchTests(unittest.TestCase):
    def test_launcher_omits_capacity_backend_and_generation_overrides(self):
        cfg=binding()
        self.assertEqual(server.server_argv(cfg),['serve',cfg['model'],'--host','127.0.0.1',
            '--port','18103','--worker-extension-cls','native_server.WorkerObservationExtension'])
        with self.assertRaisesRegex(ValueError,'fresh'):
            server.server_argv(dict(cfg,engine={'max_num_seqs':64}))
        with self.assertRaises(ValueError):server.server_argv(dict(cfg,engine=None))
        with self.assertRaises(ValueError):server.server_argv(dict(model='m',port=1))

    def test_parsed_unresolved_values_are_reported_without_inventing_hardware_capacity(self):
        cfg=binding();parsed=NS(model_tag=cfg['model'],worker_extension_cls='native_server.WorkerObservationExtension',
            max_num_seqs=None,max_num_batched_tokens=None,max_model_len=None)
        report=server.native_launch_report(cfg,parsed)
        self.assertIsNone(report['parsed_options']['max_num_seqs'])
        self.assertFalse(report['capacity_overrides'])
        self.assertIn('not hardware resolution',report['effective_capacity_source'])
        parsed.max_num_seqs=333
        self.assertEqual(server.native_launch_report(cfg,parsed)['parsed_options']['max_num_seqs'],333)

    def test_source_check_includes_native_default_definitions_and_rejects_changes(self):
        cfg=dict(binding(),install_root='/tmp/CPU-author-source')
        with patch.object(server.subprocess,'check_output',return_value=server.AUTHOR_SOURCE_COMMIT+'\n'), \
             patch.object(server.subprocess,'run',return_value=NS(returncode=0)) as diff:
            server.check_author_source(cfg)
            self.assertEqual(diff.call_args.args[0][-2:],['--','vllm'])
        with patch.object(server.subprocess,'check_output',return_value=server.AUTHOR_SOURCE_COMMIT), \
             patch.object(server.subprocess,'run',return_value=NS(returncode=1)):
            with self.assertRaisesRegex(ValueError,'defaults changed'):server.check_author_source(cfg)
        with self.assertRaisesRegex(ValueError,'fixed author commit'):
            server.check_author_source(dict(cfg,source_commit='other'))

    def test_worker_capacity_readback_preserves_actual_values_and_unallocated_state(self):
        c=worker_config();before=repr(c)
        value=server.native_engine_configuration(NS(vllm_config=c))
        self.assertEqual(value['scheduler']['max_num_seqs'],333)
        self.assertEqual(value['cache']['gpu_memory_utilization'],.91)
        self.assertIsNone(value['cache']['num_gpu_blocks'])
        self.assertEqual(repr(c),before)
        json.dumps(value)

    def test_worker_loader_observation_keeps_original_return_and_exception(self):
        with tempfile.TemporaryDirectory() as root:
            cfg=dict(binding(),observer_revision='CPU-observer',physical_gpu_index=0,gpu_uuid_sha256='CPU-UUID')
            class Runner:
                def load_model(self):return 'original-return'
            runner=Runner();runner.vllm_config=worker_config()
            runner.model_config=NS(model=cfg['model'],hf_text_config=NS(hidden_size=3584))
            parameter=NS(dtype='torch.bfloat16',shape=[1,3584],device='cuda:0',numel=lambda:3584)
            runner.model=NS(parameters=lambda:iter([parameter]))
            with patch.dict(sys.modules,{'adapter':NS(read_gpu_uuid_hash=lambda index:'CPU-UUID')}), \
                 patch.dict('os.environ',{'CUDA_VISIBLE_DEVICES':'0'}):
                server.install_worker_model_observation(cfg,root,Runner)
                self.assertEqual(runner.load_model(),'original-return')
            proof=json.loads((Path(root)/'loaded-model.json').read_text())
            self.assertEqual(proof['native_engine_configuration']['scheduler']['max_num_seqs'],333)
        with tempfile.TemporaryDirectory() as root:
            class Broken:
                def load_model(self):raise LookupError('original loader error')
            server.install_worker_model_observation(cfg,root,Broken)
            with self.assertRaisesRegex(LookupError,'original loader error'):Broken().load_model()
            self.assertFalse((Path(root)/'loaded-model.json').exists())

    def test_standard_server_cpu_entry_uses_native_defaults_without_starting_service(self):
        with tempfile.TemporaryDirectory() as root:
            p=Path(root)/'config.json'
            p.write_text(json.dumps(dict(binding(),units=[dict(unit_id='u')],baseline_source=str(ROOT))))
            argv=['run_kalypso_native_server.py','--config',str(p),'--unit','u','--evidence',root,'--cpu-check']
            original_path=list(sys.path);output=io.StringIO()
            try:
                with native_substitutes(),patch.object(server,'check_author_source'), \
                     patch.object(sys,'argv',argv),patch.dict('os.environ',{'CUDA_VISIBLE_DEVICES':''}),redirect_stdout(output):
                    runpy.run_path(str(ROOT/'code/scripts/experiments/run_kalypso_native_server.py'),run_name='__main__')
            finally:sys.path[:]=original_path
            report=json.loads(output.getvalue())
            self.assertIsNone(report['native_launch']['parsed_options']['max_num_seqs'])
            self.assertFalse(report['cuda_initialized'])
            self.assertFalse((Path(root)/'native-launch.json').exists())

    def test_cpu_planner_entry_uses_model_context_instead_of_fixed_4096(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);cfg=dict(binding(),reference_inputs={},native_instruction='unchanged instruction',
                native_sampling={'max_tokens':8},expected_template_sha256=hashlib.sha256(b'CPU-fixture-template').hexdigest())
            cfg['prior_model_receipt_path']=str(root/'prior.json');(root/'prior.json').write_text('{}')
            cfg['prior_model_receipt_sha256']=hashlib.sha256(b'{}').hexdigest()
            source=root/'input';source.mkdir();(source/'manifest.json').write_text('{}')
            with (source/'records.csv').open('w',newline='') as stream:
                writer=csv.DictWriter(stream,fieldnames=['id','reviewText','row_id','source_position']);writer.writeheader()
                writer.writerow(dict(id='taken_3',reviewText='x'*4500,row_id='r',source_position=0))
            for profile in ('qualification','warmup','full'):
                cfg['reference_inputs'][profile]={f'movie-q{q}':dict(manifest=str(source/'manifest.json'),
                    manifest_sha256=hashlib.sha256(b'{}').hexdigest(),calls=1) for q in (1,2,3)}
            config=root/'config.json';config.write_text(json.dumps(cfg))
            with native_substitutes(),patch.object(cpu_native,'check_author_source'), \
                 patch.object(sys,'argv',['check_kalypso_native_cpu.py','--config',str(config),'--output',str(root/'output')]), \
                 patch.dict('os.environ',{'CUDA_VISIBLE_DEVICES':''}),redirect_stdout(io.StringIO()):
                runpy.run_path(str(ROOT/'code/scripts/experiments/check_kalypso_native_cpu.py'),run_name='__main__')
            report=json.loads((root/'output/CPU-NATIVE-RESULT.json').read_text())
            self.assertEqual(report['native_model_config']['max_model_len'],8192)
            self.assertEqual(len(report['plans']),9)
            self.assertTrue(all(p['eligible_rows']==1 and p['max_prompt_tokens']==4500 for p in report['plans']))
            self.assertEqual(report['model_calls'],0)
            with native_substitutes(model_length=4500),patch.object(cpu_native,'check_author_source'), \
                 patch.object(sys,'argv',['check_kalypso_native_cpu.py','--config',str(config),'--output',str(root/'too-long')]), \
                 patch.dict('os.environ',{'CUDA_VISIBLE_DEVICES':''}),redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError,'native model context'):cpu_native.main()
            self.assertFalse((root/'too-long/CPU-NATIVE-RESULT.json').exists())

    def test_standard_query_entry_rejects_old_overrides_before_loading_dependencies(self):
        with tempfile.TemporaryDirectory() as root:
            config=Path(root)/'config.json';config.write_text(json.dumps(dict(binding(),engine={})))
            with patch.object(sys,'argv',['run_kalypso_sembench_query.py','--config',str(config),
                 '--unit','u','--output',str(Path(root)/'output')]),patch.object(adapter,'load_dependencies') as deps:
                with self.assertRaisesRegex(ValueError,'fresh'):
                    runpy.run_path(str(ROOT/'code/scripts/experiments/run_kalypso_sembench_query.py'),run_name='__main__')
                with self.assertRaisesRegex(ValueError,'fresh'):
                    adapter.run_attached(dict(binding(),engine={}),{},Path(root)/'direct-output')
                deps.assert_not_called()
            self.assertFalse((Path(root)/'output').exists())

    def test_query_keeps_entire_input_native_filter_calls_order_and_quality_audit(self):
        from tests.experiments.test_movie_records_session import PlanTests
        from tests.baselines.text.test_kalypso_native_observation import fake_events
        from src.baselines.text import sembench_movie
        helper=PlanTests();helper.setUp();self.addCleanup(helper.doCleanups)
        manifest=Path(helper.manifests['movie-q1'])
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);trace=root/'trace.jsonl'
            trace.write_text(''.join(json.dumps(e)+'\n' for e in fake_events(2,uid='CPU-model')))
            cfg=dict(binding(),native_instruction='unchanged native instruction',native_trace=str(trace),sembench='CPU-fixture')
            spec=dict(query_id='q',manifest=str(manifest),manifest_sha256=adapter.sha(manifest),
                      task='movie-q1',expected_calls=2)
            def transport(payload,qid,directory):
                self.assertFalse(payload['blocking'])
                self.assertEqual(payload['ops'],[dict(op='sem_filter',args=dict(prompt=cfg['native_instruction']))])
                with Path(payload['data_path']).open() as stream:
                    self.assertEqual([row['data'] for row in csv.DictReader(stream)],['good','bad'])
                return dict(model_name=cfg['model'],num_output_rows=1,results=[dict(idx=0)])
            save=lambda p,v:p.write_text(json.dumps(v))
            with patch.object(adapter,'load_dependencies',return_value=save), \
                 patch.object(sembench_movie,'evaluate_original',return_value={'original_metric':{'f1_score':1.0}}):
                driver=adapter.NativeQueryDriver(cfg,dict(queries=[spec]),root/'client',transport=transport)
                driver.model_uid='CPU-model'
                report=driver.query(spec,lambda event:None)
            self.assertEqual(report['calls'],dict(expected=2,native_completed=2))
            self.assertEqual(report['quality']['row_audit']['evaluated_rows'],2)
            self.assertEqual(report['quality']['row_audit']['true_positive'],1)
            self.assertEqual(report['quality']['row_audit']['true_negative'],1)
            self.assertEqual(len(json.loads((root/'client/q/decisions.json').read_text())),2)
            self.assertTrue((root/'client/q/quality.json').exists())

    def test_new_client_binding_requires_matching_launch_and_actual_capacity(self):
        with tempfile.TemporaryDirectory() as root,patch.object(adapter,'load_dependencies'):
            root=Path(root);cfg=dict(binding(),loaded_model_proof=str(root/'loaded-model.json'))
            proof=dict(model=cfg['model'],hidden_size=3584,first_parameter_shape=[1,3584],
                first_parameter_device='cuda:0',dtype='torch.bfloat16',model_uid='CPU-model',
                native_engine_configuration=server.native_engine_configuration(NS(vllm_config=worker_config())))
            (root/'loaded-model.json').write_text(json.dumps(proof))
            parsed=NS(model_tag=cfg['model'],worker_extension_cls='native_server.WorkerObservationExtension')
            launch=server.native_launch_report(cfg,parsed)
            (root/'native-launch.json').write_text(json.dumps(launch))
            driver=adapter.NativeQueryDriver(cfg,{},root/'client')
            self.assertEqual(driver.start()['native_launch']['policy'],'author_defaults')
            launch['argv'] += ['--max-num-seqs','64']
            (root/'native-launch.json').write_text(json.dumps(launch))
            with self.assertRaisesRegex(ValueError,'provenance'):driver.start()
            (root/'native-launch.json').write_text(json.dumps(server.native_launch_report(cfg,parsed)))
            proof['native_engine_configuration']['scheduler']['max_num_seqs']=None
            (root/'loaded-model.json').write_text(json.dumps(proof))
            with self.assertRaisesRegex(ValueError,'incomplete'):driver.start()


if __name__ == '__main__':
    unittest.main()
