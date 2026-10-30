"""Zero-model fixtures. No real torch/vLLM/Ray import, server or GPU access."""
import asyncio
from copy import deepcopy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
from types import ModuleType,SimpleNamespace
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation'
SECRET='DO_NOT_PERSIST_PRIVATE_PROMPT_OR_CREDENTIAL_1841'
IDENTITY=dict(query_id='query',model_role='main',algorithm_stage='map',row_id='row',source_position=0,call_index=0,call_id='call')


def load(name):
    spec=importlib.util.spec_from_file_location(name,ROOT/(name+'.py'))
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module);return module


def stubs():
    modules={}
    names=['src','src.serving','src.serving.backends','src.serving.backends.async_engine_process',
           'src.execution_provider','src.execution_provider.adapters','src.execution_provider.adapters.lotus_records_async_actor',
           'src.execution_provider.adapters.lotus_records_actor_owner','src.semantic_methods','src.semantic_methods.lotus',
           'src.semantic_methods.lotus.record_chain','src.semantic_methods.lotus.engine',
           'vllm','vllm.v1','vllm.v1.worker','vllm.v1.worker.gpu_worker']
    for name in names:
        value=ModuleType(name);value.__path__=[];modules[name]=value
        if '.' in name:setattr(modules[name.rsplit('.',1)[0]],name.rsplit('.',1)[1],value)
    class Base:
        def __init__(self,*args,**kwargs):self.received_args=(args,kwargs)
    native=modules['src.serving.backends.async_engine_process'];native.AsyncEngineProcess=Base
    native.socket=socket;native.json=json
    def frame(value,maximum):
        payload=json.dumps(value,ensure_ascii=False,separators=(',',':')).encode()
        if len(payload)>maximum:raise ValueError('fixture bound')
        return len(payload).to_bytes(4,'big')+payload
    native._frame=frame
    async def read_frame(sock,maximum):return {},1
    native._read_frame=read_frame
    async def serve(*args,**kwargs):return args
    native.serve=serve
    modules['src.execution_provider.adapters.lotus_records_async_actor'].AsyncCoreModelActor=Base
    modules['src.execution_provider.adapters.lotus_records_actor_owner']._PrepareActor=Base
    class Worker:
        def execute_model(self,scheduled):
            self.model_runner.execute_model_state=self.next_state
            return scheduled
        def sample_tokens(self,grammar):
            current=self.model_runner.execute_model_state
            self.model_runner.execute_model_state=None
            return current,grammar
    modules['vllm.v1.worker.gpu_worker'].Worker=Worker
    torch=ModuleType('torch');torch.cuda=SimpleNamespace(nvtx=SimpleNamespace(range_push=lambda marker:None,range_pop=lambda:None));modules['torch']=torch
    return modules


class CaptureTests(unittest.TestCase):
    def setUp(self):
        provider=sys.modules.get('src.scheduling.runtime.observations')
        self.feedback_provider=None if provider is None else provider.NonBlockingMetricsObservationProvider
        self.feedback_constructor=None if provider is None else self.feedback_provider.__init__
        self.tmp=tempfile.TemporaryDirectory();self.directory=Path(self.tmp.name)
        config={'observation':dict(mode='on',max_events_per_process=10000,max_event_bytes=65536,max_pending_frame_metadata=50,
                                   native_runner_contracts={})}
        (self.directory/'config.json').write_text(json.dumps(config))
        self.env=patch.dict(os.environ,dict(SEM_RECORD_OBSERVATION='on',SEM_RECORD_OBSERVER_ROOT=str(self.directory),
            SEM_RECORD_OBSERVER_CODE='fixture-code',SEM_RECORD_RUN_ID='run',SEM_RECORD_SESSION_ID='session'))
        self.env.start();self.modules=patch.dict(sys.modules,stubs());self.modules.start()
        self.observer=load('record_observer')

    def tearDown(self):
        self.observer.close_events()
        if self.feedback_provider is not None:self.feedback_provider.__init__=self.feedback_constructor
        self.modules.stop();self.env.stop();self.tmp.cleanup()

    def events(self):
        return [json.loads(line) for path in (self.directory/'events').glob('*.jsonl') for line in path.read_text().splitlines()]

    def gpu(self):
        value=load('record_gpu_observer');value.runner_contract=lambda runner:'v2';return value

    def worker(self,gpu):
        worker=gpu.ObservedWorker();worker.model_runner=SimpleNamespace(execute_model_state=None)
        worker.next_state=SimpleNamespace(input_batch=SimpleNamespace(req_ids=['native-A'],num_scheduled_tokens=[5]))
        return worker

    def test_sample_reads_actual_consumed_state_not_last_execute(self):
        gpu=self.gpu();worker=self.worker(gpu);schedule=SimpleNamespace(num_scheduled_tokens={'native-A':5})
        self.assertIs(worker.execute_model(schedule),schedule)
        actual=SimpleNamespace(input_batch=SimpleNamespace(req_ids=['native-B'],num_scheduled_tokens=[3]))
        worker.model_runner.execute_model_state=actual
        grammar=object();returned=worker.sample_tokens(grammar)
        self.assertIs(returned[0],actual);self.assertIs(returned[1],grammar)
        sample=next(e for e in self.events() if e['event']=='iteration_sample_start')
        self.assertEqual(sample['scheduled_tokens'],{'native-B':3});self.assertIsNone(sample['handoff_step'])
        self.assertLess(sample['iteration'],0);self.assertIsNone(worker._record_handoff)

    def test_execute_handoff_matches_actual_state_without_retaining_it(self):
        gpu=self.gpu();worker=self.worker(gpu);worker.execute_model(SimpleNamespace(num_scheduled_tokens={'native-A':5}))
        self.assertTrue(all(type(v) is int for v in worker._record_handoff))
        worker.sample_tokens(None)
        sample=next(e for e in self.events() if e['event']=='iteration_sample_start')
        self.assertEqual(sample['handoff_step'],1);self.assertEqual(sample['scheduled_tokens'],{'native-A':5})

    def test_NVTX_failures_preserve_original_native_exception_and_single_call(self):
        gpu=self.gpu();base=sys.modules['vllm.v1.worker.gpu_worker'].Worker
        for operation in ('execute_model','sample_tokens'):
            for boundary in ('range_push','range_pop'):
                with self.subTest(operation=operation,boundary=boundary):
                    worker=self.worker(gpu);argument=SimpleNamespace(num_scheduled_tokens={'native-A':5})
                    business_error=ValueError(SECRET);calls=[]
                    def fail_native(instance,value):
                        calls.append((instance,value));raise business_error
                    with patch.object(base,operation,fail_native),patch.object(sys.modules['torch'].cuda.nvtx,boundary,
                            side_effect=RuntimeError('fixture NVTX failed')):
                        with self.assertRaises(ValueError) as caught:getattr(worker,operation)(argument)
                    self.assertIs(caught.exception,business_error)
                    self.assertEqual(calls,[(worker,argument)])
                    self.assertIsNone(worker._record_handoff)
        failures=[e for e in self.events() if e['event']=='worker_clock_unknown']
        self.assertEqual(len(failures),4)
        self.assertTrue(all(e['status']=='unknown' and e['error_type']=='RuntimeError' for e in failures))
        self.assertNotIn(SECRET,json.dumps(self.events()))

    def test_gpu_counts_are_not_copied_or_estimated(self):
        gpu=self.gpu()
        class GPUCounts:
            device=SimpleNamespace(type='cuda')
            def tolist(self):raise AssertionError('GPU transfer forbidden')
        GPUCounts.__module__='torch.fixture'
        state=SimpleNamespace(input_batch=SimpleNamespace(req_ids=['native-A'],num_scheduled_tokens=GPUCounts()))
        members,reason=gpu.state_members(state,'v2')
        self.assertIsNone(members);self.assertIn('unavailable',reason)

    def test_v1_actual_scheduler_state_members(self):
        gpu=self.gpu();state=SimpleNamespace(scheduler_output=SimpleNamespace(num_scheduled_tokens={'old-native':2}))
        self.assertEqual(gpu.state_members(state,'v1')[0],{'old-native':2})
        self.assertIsNone(gpu.state_members(state,None)[0])

    def test_native_source_hash_mismatch_is_unknown(self):
        gpu=load('record_gpu_observer')
        cls=type('Runner',(),{});cls.__module__='vllm.v1.worker.gpu.model_runner'
        with patch.object(gpu.inspect,'getsourcefile',return_value=str(ROOT/'record_gpu_observer.py')),patch.object(gpu.importlib.metadata,'version',return_value='fixture-native'):
            self.assertIsNone(gpu.runner_contract(cls()))
            frozen=self.observer.settings();sha=hashlib.sha256((ROOT/'record_gpu_observer.py').read_bytes()).hexdigest()
            frozen['native_runner_contracts'][cls.__module__]=sha
            frozen.update(native_vllm_version='fixture-native',native_worker_sha256=sha)
            self.assertEqual(gpu.runner_contract(cls()),'v2')

    def test_shared_span_has_one_ID_complete_members_and_nested_parent(self):
        second=dict(IDENTITY,call_id='call-2',row_id='row-2',algorithm_stage='other')
        with self.observer.cpu_span('complete',[IDENTITY,second]) as parent:
            with self.observer.cpu_span('parser',[IDENTITY,second]) as child:pass
        self.observer.close_events()
        value=self.observer.summarize_cpu_spans((self.directory/'events').glob('*.jsonl'))
        self.assertEqual(len(value['spans']),2)
        byid={s['span_id']:s for s in value['spans']}
        self.assertEqual(byid[child]['parent_span_id'],parent);self.assertEqual(len(byid[parent]['members']),2)
        self.assertEqual(byid[parent]['exclusive_thread_CPU_ns']+byid[child]['exclusive_thread_CPU_ns'],byid[parent]['inclusive_thread_CPU_ns'])

    def test_method_wrapper_preserves_arguments_results_and_iterable(self):
        observed=[];result=[dict(IDENTITY,messages_json=SECRET)]
        class Formatter:
            def prepare(self,rows,*args,**kwargs):
                observed.append((rows,args,kwargs));list(rows);return result
        self.observer.observe_method(Formatter,'prepare','fixture_prepare',lambda *args:None)
        rows=iter([SECRET]);generation={'stop':[SECRET]};obj=Formatter()
        self.assertIs(obj.prepare(rows,generation=generation),result)
        self.assertIs(observed[0][0],rows);self.assertIs(observed[0][2]['generation'],generation)
        self.assertNotIn(SECRET,json.dumps(self.events()))
        self.assertEqual(next(e for e in self.events() if e['event']=='cpu_span_members')['members'],[IDENTITY])

    def test_actual_socket_partial_sends_and_reads_equal_wire_bytes(self):
        ipc=load('record_ipc_observer');ipc.install_transport(child=False)
        native=sys.modules['src.serving.backends.async_engine_process']
        packet=native._frame(dict(op='submit',request_id='request',messages_json=SECRET),4096)
        left,right=socket.socketpair()
        class Partial:
            def send(self,data,*args,**kwargs):return left.send(data[:3],*args,**kwargs)
            def __getattr__(self,name):return getattr(left,name)
        sender=ipc.ObservedSocket(Partial(),'data');receiver=ipc.ObservedSocket(right,'data')
        try:
            remaining=memoryview(packet)
            while remaining:remaining=remaining[sender.send(remaining):]
            actual=b''
            while len(actual)<len(packet):actual+=receiver.recv(7)
            self.assertEqual(actual,packet)
            reads=[e for e in self.events() if e['event']=='socket_io' and e['direction']=='recv']
            sends=[e for e in self.events() if e['event']=='socket_io' and e['direction']=='send']
            self.assertEqual(sum(e['actual_socket_bytes'] for e in sends),len(packet))
            self.assertEqual(sum(e['actual_socket_bytes'] for e in reads),len(packet))
            self.assertTrue(all(e['encoded_frame_id'] is not None for e in sends))
            self.assertNotIn(SECRET,json.dumps(self.events()))
        finally:sender.close();receiver.close()

    def test_socket_failure_is_unknown_not_zero(self):
        ipc=load('record_ipc_observer')
        class Broken:
            def send(self,*a):raise BrokenPipeError(SECRET)
        wrapped=ipc.ObservedSocket(Broken(),'control')
        with self.assertRaises(BrokenPipeError):wrapped.send(b'x')
        event=self.events()[-1]
        self.assertIsNone(event['actual_socket_bytes']);self.assertNotIn(SECRET,json.dumps(event))

    def test_frame_decoder_byte_spans_are_distinct_from_wire_counter(self):
        ipc=load('record_ipc_observer');left,right=socket.socketpair()
        try:
            wrapped=ipc.ObservedSocket(left,'results');wrapped.frame_decoded({'op':'terminal','text':SECRET},9)
            event=self.events()[-1]
            self.assertEqual((event['frame_start'],event['frame_end']),(0,13))
            self.assertNotIn('actual_socket_bytes',event);self.assertNotIn(SECRET,json.dumps(event))
        finally:left.close();right.close()

    def test_async_event_loop_uses_owned_proxy_transparently(self):
        ipc=load('record_ipc_observer');left,right=socket.socketpair();left.setblocking(False);right.setblocking(False)
        async def work():
            loop=asyncio.get_running_loop();proxy=ipc.ObservedSocket(left,'control')
            await loop.sock_sendall(proxy,b'abc')
            self.assertEqual(await loop.sock_recv(right,3),b'abc')
            await loop.sock_sendall(right,b'def')
            self.assertEqual(await loop.sock_recv(proxy,3),b'def')
        try:asyncio.run(work())
        finally:left.close();right.close()

    def test_encode_json_decode_preserves_payload_and_options(self):
        ipc=load('record_ipc_observer');native=sys.modules['src.serving.backends.async_engine_process']
        value=dict(op='submit',request_id='r',generation={'stop':[SECRET]},row={'messages_json':SECRET})
        before=deepcopy(value);ipc.install_transport(child=False)
        packet=native._frame(value,4096);decoded=native.json.loads(packet[4:])
        self.assertEqual(value,before);self.assertEqual(decoded,before)
        self.assertNotIn(SECRET,json.dumps(self.events()))
        self.assertFalse(any(isinstance(v,bytes) for row in ipc._frames.values() for v in row.values()))

    def test_sink_caps_and_flush_report_are_explicit(self):
        self.observer.settings()['max_events_per_process']=1
        self.observer.emit('first');self.observer.emit('second');self.observer.close_events()
        value=json.loads(next((self.directory/'events').glob('*.status.json')).read_text())
        self.assertEqual(value['events_written'],1);self.assertEqual(value['dropped'],1);self.assertTrue(value['flush_confirmed'])


    def test_memory_missing_metrics_remain_null(self):
        self.observer.emit('private_memory_snapshot',processes=[dict(pid=7,rss=12,uss=None,pss=None)])
        process=self.events()[0]['processes'][0]
        self.assertIsNone(process['uss']);self.assertIsNone(process['pss'])

    def test_incomplete_child_span_prevents_known_parent_exclusive_CPU(self):
        with self.observer.cpu_span('complete',[IDENTITY]) as parent:
            with self.observer.cpu_span('parser',[IDENTITY]) as child:pass
        self.observer.close_events()
        source=next((self.directory/'events').glob('*.jsonl'))
        rows=[json.loads(line) for line in source.read_text().splitlines()]
        rows=[e for e in rows if not(e['event']=='cpu_span_end' and e['span_id']==child)]
        source.write_text(''.join(json.dumps(e)+'\n' for e in rows))
        spans=self.observer.summarize_cpu_spans([source])['spans'];byid={s['span_id']:s for s in spans}
        self.assertEqual(byid[child]['status'],'unknown');self.assertIsNone(byid[parent]['exclusive_thread_CPU_ns'])

    def test_capture_status_never_promotes_missing_flush_or_CUPTI_proof(self):
        controller=load('control_resource_C2');self.observer.emit('iteration_sample_start',marker='sample')
        value=controller.capture_status(self.directory)
        self.assertEqual(value['observed_sinks'][0]['flush_status'],'unknown')
        self.assertIsNone(value['CUPTI_capture_complete']);self.assertEqual(value['unknown_sample_member_sets'],1)

    def test_actor_constructors_forward_generation_and_engine_unchanged(self):
        ipc=load('record_ipc_observer')
        engine=object();generation={'stop':[SECRET]};limits={'max_inflight':2}
        with patch.object(self.observer,'install_actor',lambda:None):
            actor=ipc.ObservedAsyncCoreModelActor(engine,generation,1,limits)
        self.assertIs(actor.received_args[0][0],engine);self.assertIs(actor.received_args[0][1],generation)
        self.assertIs(actor.received_args[0][3],limits)
        command=ipc.ObservedAsyncEngineProcess._command(SimpleNamespace(model_python='model-python'),[10,11,12])
        self.assertEqual(command[-3:],['10','11','12']);self.assertEqual(Path(command[3]),ROOT/'record_model_bootstrap.py')

    def test_sparse_memory_probe_reports_birth_cost_and_missing_values(self):
        class Process:
            pid=7
            def create_time(self):return 1.5
            def memory_full_info(self):return SimpleNamespace(rss=12)
            def children(self,recursive):return []
        with patch.dict(sys.modules,{'psutil':SimpleNamespace(Process=Process)}):self.observer.memory_snapshot('fixture')
        event=self.events()[-1];self.assertEqual(event['processes'][0]['process_birth'],1.5)
        self.assertIsNone(event['processes'][0]['pss']);self.assertGreaterEqual(event['collection_wall_ns'],0)

    def test_sink_excludes_prompt_and_credentials_from_arbitrary_metadata(self):
        self.observer.emit('fixture',messages_json=SECRET,text=SECRET,environment={'token':SECRET},
                           usage={'model_path':SECRET,'password':SECRET,'held_requests':2})
        event=self.events()[-1]
        self.assertNotIn(SECRET,json.dumps(event));self.assertEqual(event['usage']['held_requests'],2)

    def test_model_transport_import_does_not_import_driver_actor_environment(self):
        original=__import__
        def guard(name,*args,**kwargs):
            if name.startswith('src.execution_provider'):raise AssertionError('model imported driver actors')
            return original(name,*args,**kwargs)
        with patch('builtins.__import__',side_effect=guard):
            ipc=load('record_ipc_observer');ipc.install_transport(child=True)
        self.assertNotIn('ObservedAsyncCoreModelActor',ipc.__dict__)

    def test_model_hook_preserves_requests_generation_and_does_not_retry_profiler(self):
        extras={}
        names=['vllm.engine','vllm.engine.arg_utils','vllm.v1.engine','vllm.v1.engine.async_llm',
               'src.serving.backends.vllm_async_engine','src.serving.backends.vllm_engine',
               'src.observability','src.observability.native_model_api_trace']
        for name in names:
            m=ModuleType(name);m.__path__=[];extras[name]=m
        class Args:
            def __init__(self,*args,**kwargs):self.original=(args,kwargs)
        class Engine:
            def __init__(self,engine,generation):self.original=(engine,generation);self.engine_uid='native-engine';self.calls=[]
            async def generate_prepared(self,row,*args,**kwargs):
                self.calls.append((row,args,kwargs))
                if kwargs['request_id']=='r0':await self.second_started.wait()
                else:self.second_started.set();await self.first_wrapper_returned.wait()
                return row
            async def close(self,*args,**kwargs):return {'status':'closed'}
        extras['vllm.engine.arg_utils'].AsyncEngineArgs=Args
        extras['vllm.v1.engine.async_llm'].AsyncLLM=type('AsyncLLM',(),{})
        extras['src.serving.backends.vllm_async_engine'].VllmAsyncEngine=Engine
        common=extras['src.serving.backends.vllm_engine'];common._prepared=lambda rows:([rows[0]],[])
        common.result_from_output=lambda identity,output,*a,**k:dict(engine_uid='native-engine')
        extras['src.observability.native_model_api_trace'].install=lambda *a,**k:None
        attempts=[]
        def start():attempts.append(True);return 1
        sys.modules['torch'].cuda.cudart=lambda:SimpleNamespace(cudaProfilerStart=start)
        ranges=[];ended=[]
        def range_start(label):ranges.append(label);return len(ranges)
        def reject_stack(*args):raise AssertionError('thread stack cannot represent overlapping requests')
        sys.modules['torch'].cuda.nvtx=SimpleNamespace(range_start=range_start,range_end=ended.append,
                                                     range_push=reject_stack,range_pop=reject_stack)
        with patch.dict(sys.modules,extras):
            for name,m in extras.items():setattr(sys.modules[name.rsplit('.',1)[0]],name.rsplit('.',1)[1],m)
            self.observer.install_model()
            engine_options={'max_num_seqs':128};generation={'stop':[SECRET]};engine=Engine(engine_options,generation)
            rows=[dict(IDENTITY,call_id='call-'+str(i),messages_json=SECRET) for i in range(2)]
            async def run():
                engine.second_started=asyncio.Event();engine.first_wrapper_returned=asyncio.Event()
                async def request(i):
                    self.assertIs(await engine.generate_prepared(rows[i],request_id='r'+str(i),deadline_monotonic=123),rows[i])
                    if i==0:engine.first_wrapper_returned.set()
                await asyncio.gather(request(0),request(1))
                await engine.close()
            asyncio.run(run())
            self.assertIs(engine.original[0],engine_options);self.assertIs(engine.original[1],generation)
            self.assertEqual(len(engine.calls),2);self.assertEqual(len(attempts),1)
            self.assertEqual(ended,[1,2]);self.assertEqual(len(set(ranges)),2)
            windows=[e for e in self.events() if e['event'] in ('native_open','native_close')]
            self.assertEqual([(e['event'],e['range_handle']) for e in windows],
                             [('native_open',1),('native_open',2),('native_close',1),('native_close',2)])
            args=Args(model=SECRET,max_num_seqs=128,disable_log_stats=True)
            self.assertEqual(args.original[1]['max_num_seqs'],128);self.assertEqual(args.original[1]['model'],SECRET)
            self.assertEqual(set(args.original[1]),{'model','max_num_seqs','disable_log_stats','worker_cls'})
        self.assertNotIn(SECRET,json.dumps(self.events()))


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.runner=load('run_resource_C2');self.controller=load('control_resource_C2')
        self.base=dict(source_root='/fixture/source',artifact_root='/fixture/artifacts',run_id='run',cell_id='cell',
            observation={'mode':'off'},workload={'name':'MovieQ3','kind':'movie_session','expected_calls':120},
            movie_plan=dict(engine=dict(driver_python='fixture-python',max_model_calls=999,max_num_seqs=128),generation={'stop':[SECRET]},
                resources=dict(max_active_batches=9),profile={'mode':'async'},queries=[{'query_id':'q','task':'q3','manifest':'frozen','phase':'measured'}]),
            units={'q':120},runtime_binding={'cuda_visible_devices':'fixture-device'},external_owner={},sembench_checkout='/fixture/sembench',
            limits={'worker_timeout_s':10},worker_entry_ns=1,environment={})


    def test_off_has_no_observer_env_profile_or_import(self):
        config=deepcopy(self.base)
        env=self.controller.worker_environment(config,{'SEM_RECORD_OBSERVATION':'on','SEM_RECORD_OBSERVER_ROOT':'old'})
        self.assertFalse(any(k.startswith('SEM_RECORD_') for k in env));self.assertNotIn(str(ROOT),env['PYTHONPATH'])
        self.assertNotIn('profile',self.controller.worker_command(config))
        class Ledger:
            def claim_unit(self,unit):return ('actual-claim',unit)
        imported=[];native_import=__import__
        def guard(name,*args,**kwargs):
            imported.append(name)
            if name=='record_observer':raise AssertionError('off imported observer')
            return native_import(name,*args,**kwargs)
        args=[]
        def entry(plan,context,path,**kwargs):args.append((plan,context,kwargs));return {'status':'completed'}
        with patch('builtins.__import__',side_effect=guard):
            self.runner.dispatch(config,Ledger(),movie_entry=entry,movie_checker=lambda plan:{'expected_calls':120,'queries':[{'query_id':'q','expected_calls':120}]})
        self.assertIs(args[0][0],config['movie_plan']);self.assertIsNone(args[0][2]['observer'])
        self.assertNotIn('record_observer',imported)

    def test_public_entry_rejects_changed_declared_call_count(self):
        class Ledger:
            def claim_unit(self,unit):raise AssertionError('claim before validation')
        with self.assertRaises(ValueError):
            self.runner.dispatch(self.base,Ledger(),movie_entry=lambda *a:None,movie_checker=lambda plan:{'expected_calls':119,'queries':[]})

    def test_worker_command_reads_nsight_and_time_from_frozen_config(self):
        config=deepcopy(self.base);config['observation'].update(mode='on',nsys_executable='/fixture/nsys',trace_duration_s=77)
        command=self.controller.worker_command(config)
        self.assertEqual(command[0],'/fixture/nsys');self.assertIn('--duration=77',command)
        self.assertNotIn('a421',json.dumps(command))

    def test_safe_call_receipts_never_store_original_prompt_or_response(self):
        value=dict(IDENTITY,messages_json=SECRET,text=SECRET,credentials=SECRET,completion_tokens=5)
        self.assertNotIn(SECRET,json.dumps(self.runner.safe_calls([value])))



    def test_material_guard_rejects_partial_hash_inventory(self):
        config=deepcopy(self.base);config.update(cpu_check={},observer_sha256={})
        with patch.object(self.controller,'checked_json',return_value={'status':'passed_CPU_fixtures','observer_sha256':{}}):
            with self.assertRaises(ValueError):self.controller.check_material(config)



if __name__=='__main__':unittest.main(verbosity=2)
