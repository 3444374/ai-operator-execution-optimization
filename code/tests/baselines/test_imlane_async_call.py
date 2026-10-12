"""Caller-owned real loops and Arrow/session stand-ins; no author or HTTP server."""
import asyncio
import hashlib
import importlib.util
import inspect
import json
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from src.baselines.text.frameworks import native_calls as native
from src.execution_provider.adapters.full_response import FullModelResponse


class ArrowTable(dict):
    def __init__(self, values):
        super().__init__({name:[SimpleNamespace(as_py=lambda value=value:value) for value in column]
                          for name,column in values.items()})
        self.column_names=list(values)
        self.num_rows=len(next(iter(values.values())))

    def validate(self, *, full):
        if not full:
            raise AssertionError('full Arrow validation was omitted')


class Response:
    status=200
    raw_headers=((b'X-Repeat',b'one'),(b'X-Repeat',b'two'))
    version=SimpleNamespace(major=1,minor=1)
    def __init__(self,body):
        self.body=body;self.content=self;self.delivered=False
    async def __aenter__(self):return self
    async def __aexit__(self,*_error):return False
    def iter_chunked(self,_size):return self
    def __aiter__(self):return self
    async def __anext__(self):
        if self.delivered:raise StopAsyncIteration
        self.delivered=True;return self.body


class Session:
    def __init__(self,body=b'{}',*,close_error=False):
        self.body=body;self.close_error=close_error;self.closed=False;self.closes=0;self.requests=[]
    def post(self,endpoint,**options):
        self.requests.append((endpoint,options));return Response(self.body)
    async def close(self):
        self.closes+=1;self.closed=True
        if self.close_error:raise OSError('async close fixture')


class IMLaneAsyncCallTests(unittest.TestCase):
    def setUp(self):
        arrow=ModuleType('pyarrow')
        arrow.string=arrow.int64=lambda:None
        arrow.array=lambda values,**_options:values
        arrow.table=ArrowTable
        path=Path(__file__).resolve().parents[2]/'integrations/sema/imlane_async_complete_call.py'
        spec=importlib.util.spec_from_file_location('async_imlane_fixture',path)
        self.kernel=importlib.util.module_from_spec(spec)
        with patch.dict('sys.modules',{'pyarrow':arrow}):spec.loader.exec_module(self.kernel)
        self.loop=asyncio.new_event_loop();asyncio.set_event_loop(self.loop)
        self.addCleanup(self.finish_loop)
        self.env=patch.dict('os.environ',{'IMLANE_HTTP_TRACE_DIR':''})
        self.env.start();self.addCleanup(self.env.stop)

    def finish_loop(self):
        self.loop.close();asyncio.set_event_loop(None)

    def call(self,identity='async:1',row=5,**options):
        return native.NativeCall(identity,row,b'{"text":"duplicate"}','http://localhost/fixture',
            (('X-Repeat','a'),('X-Repeat','b')),1.,follow_redirects=True,**options)

    def table(self,call,**columns):
        values=dict(call_id=[call.call_id],row_id=[call.row_id],native_call=[native._call_wire(call)])
        values.update(columns)
        return ArrowTable(values)

    def decode(self,result):
        self.assertEqual(result.column_names,['call_id','row_id','native_outcome'])
        self.assertEqual(result.num_rows,1)
        return native._outcome_from_wire(result['native_outcome'][0].as_py())

    def run_call(self,call,**columns):
        return self.decode(self.loop.run_until_complete(self.kernel.complete_http(self.table(call,**columns))))

    def test_author_coroutine_dispatch_reuses_caller_loop_without_run_or_runner(self):
        self.assertTrue(inspect.iscoroutinefunction(self.kernel.complete_http))
        loops=[]
        async def request(call):
            loops.append(asyncio.get_running_loop())
            return native.NativeCallOutcome(call.call_id,call.row_id,FullModelResponse(200,(),call.payload))
        with patch.object(self.kernel,'_request',request), \
             patch.object(asyncio,'run',side_effect=AssertionError('nested asyncio.run')), \
             patch.object(asyncio,'Runner',side_effect=AssertionError('private Runner')):
            values=[self.run_call(self.call('one')),self.run_call(self.call('two'))]
        self.assertEqual(loops,[self.loop,self.loop])
        self.assertEqual([v.call_id for v in values],['one','two'])
        self.assertFalse(self.loop.is_closed())
        self.assertIs(asyncio.get_event_loop(),self.loop)

    def test_sessions_are_per_call_and_close_once_on_the_same_owner_loop(self):
        created=[]
        def factory():
            value=native._HttpClient(asyncio.get_running_loop(),Session())
            created.append(value);return value
        with patch.object(native,'_new_http_client',factory):
            outcomes=[self.run_call(self.call('one')),self.run_call(self.call('two'))]
        self.assertEqual(len(created),2)
        self.assertTrue(all(v.loop is self.loop and v.closed and v.session.closes==1 for v in created))
        self.assertEqual([v.call_id for v in outcomes],['one','two'])
        for value in created:
            endpoint,options=value.session.requests[0]
            self.assertEqual(endpoint,'http://localhost/fixture')
            self.assertEqual(options['data'],self.call().payload)
            self.assertEqual(options['headers'],self.call().headers)
            self.assertTrue(options['allow_redirects'])

    def test_complete_raw_response_status_duplicate_headers_and_bytes_are_unchanged(self):
        call=self.call();full=FullModelResponse(429,(('X-Repeat','one'),('X-Repeat','two')),b'\x00not-json\n','HTTP/1.1')
        expected=native.NativeCallOutcome(call.call_id,call.row_id,full)
        with patch.object(self.kernel,'_request',AsyncMock(return_value=expected)) as request:
            observed=self.run_call(call)
        self.assertEqual(observed,expected);request.assert_awaited_once_with(call)

    def test_transport_outcome_keeps_category_and_has_no_synthetic_status(self):
        call=self.call();expected=native.NativeCallOutcome(call.call_id,call.row_id,None,'TimeoutError: fixture')
        with patch.object(self.kernel,'_request',AsyncMock(return_value=expected)):
            observed=self.run_call(call)
        self.assertEqual(observed,expected)
        self.assertEqual(native.outcome_kind(observed),'transport_error')

    def test_capacity_error_retains_first_cause_close_note_and_session_cleanup(self):
        created=[]
        def factory():
            value=native._HttpClient(asyncio.get_running_loop(),Session(b'x'*512,close_error=True))
            created.append(value);return value
        with patch.object(native,'_new_http_client',factory):
            observed=self.run_call(self.call(max_response_bytes=128))
        self.assertEqual(native.outcome_kind(observed),'execution_error')
        self.assertTrue(observed.execution_error.startswith('ValueError: native complete response exceeds'))
        self.assertIn('pool close also failed: OSError',observed.execution_error)
        self.assertEqual(created[0].session.closes,1)
        self.assertTrue(created[0].closed)
        self.assertEqual(observed.transport_error,'')

    def test_identity_shape_and_validation_errors_do_not_submit_http(self):
        cases=[dict(call_id=['changed']),dict(row_id=[99]),dict(extra=['unexpected']),
               dict(call_id=['one','two'],row_id=[5,5],native_call=[native._call_wire(self.call())]*2)]
        with patch.object(self.kernel,'_request',AsyncMock()) as request:
            for columns in cases:
                with self.subTest(columns=columns):
                    observed=self.run_call(self.call(),**columns)
                    self.assertEqual(native.outcome_kind(observed),'execution_error')
                    self.assertTrue(observed.execution_error.startswith('ValueError:'))
                    self.assertEqual(observed.call_id,self.call().call_id)
            value=self.table(self.call());value.validate=lambda **_kwargs:(_ for _ in ()).throw(ValueError('Arrow fixture'))
            observed=self.decode(self.loop.run_until_complete(self.kernel.complete_http(value)))
            self.assertIn('Arrow fixture',observed.execution_error)
        request.assert_not_awaited()

    def test_unknown_execution_error_and_utf8_diagnostics_remain_bounded(self):
        error=RuntimeError('中文'*4096)
        with patch.object(self.kernel,'_request',AsyncMock(side_effect=error)):
            observed=self.run_call(self.call())
        self.assertEqual(native.outcome_kind(observed),'execution_error')
        self.assertTrue(observed.execution_error.startswith('RuntimeError:'))
        self.assertLessEqual(len(observed.execution_error.encode()),4096)

    def test_task_cancellation_propagates_and_closes_the_owned_session(self):
        started=asyncio.Event();created=[]
        class WaitingResponse(Response):
            async def __anext__(self):
                started.set();await asyncio.Event().wait()
                return b'unreachable'
        class WaitingSession(Session):
            def post(self,*_args,**_kwargs):return WaitingResponse(b'')
        def factory():
            value=native._HttpClient(asyncio.get_running_loop(),WaitingSession())
            created.append(value);return value
        async def cancel():
            task=asyncio.create_task(self.kernel.complete_http(self.table(self.call())))
            await started.wait();task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        with patch.object(native,'_new_http_client',factory):self.loop.run_until_complete(cancel())
        self.assertEqual(created[0].session.closes,1);self.assertTrue(created[0].closed)
        self.assertFalse(self.loop.is_closed())

    def test_same_loop_request_cookie_contexts_do_not_leak_across_calls(self):
        from yarl import URL
        created=[];before=[]
        original=native._new_http_client
        def factory():
            client=original();created.append(client)
            def post(*_args,**_options):
                jar=client.session.cookie_jar;url=URL('http://localhost/fixture')
                before.append(bool(jar.filter_cookies(url)))
                jar.update_cookies({'redirect':'within-this-call'},url)
                self.assertEqual(jar.filter_cookies(url)['redirect'].value,'within-this-call')
                return Response(b'{}')
            client.session.post=post
            return client
        with patch.object(native,'_new_http_client',factory):
            self.run_call(self.call('one'));self.run_call(self.call('two'))
        self.assertEqual(before,[False,False]);self.assertTrue(all(v.closed for v in created))
        self.assertFalse(any(v.session.cookie_jar.filter_cookies(URL('http://localhost/fixture')) for v in created))

    def test_optional_trace_uses_stable_loop_generation_and_same_kernel_digest(self):
        with tempfile.TemporaryDirectory(prefix='imlane-async-observe-') as directory:
            with patch.dict('os.environ',{'IMLANE_HTTP_TRACE_DIR':directory}), \
                 patch.object(self.kernel,'_request',AsyncMock(return_value=native.NativeCallOutcome('one',5,None,'TimeoutError: fixture'))):
                self.run_call(self.call('one'));self.run_call(self.call('one'))
            records=[json.loads(line) for p in Path(directory).glob('*.jsonl') for line in p.read_text().splitlines()]
        self.assertEqual(len(records),2)
        self.assertEqual(records[0]['loop_generation'],records[1]['loop_generation'])
        self.assertTrue(all(v['loop_running'] and not v['loop_closed'] for v in records))
        self.assertEqual(records[0]['kernel_sha256'],hashlib.sha256(Path(self.kernel.__file__).read_bytes()).hexdigest())
        self.assertEqual(records[0]['native_calls_sha256'],hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest())
        self.assertTrue(records[0]['loaded_http_code_matches_source'])

    def test_changed_disk_source_cannot_attest_an_older_loaded_function(self):
        with tempfile.TemporaryDirectory(prefix='imlane-loaded-code-') as directory:
            path=Path(directory)/'stale.py';old='def request():\n    return "old"\n'
            module=ModuleType('stale_fixture');module.__file__=str(path)
            exec(compile(old,str(path),'exec'),module.__dict__)
            path.write_text(old.replace('old','new'))
            with self.assertRaisesRegex(RuntimeError,'loaded native HTTP code differs'):
                self.kernel._check_loaded_source(module)

    def test_trace_failure_does_not_replace_the_result(self):
        call=self.call();expected=native.NativeCallOutcome(call.call_id,call.row_id,None,'TimeoutError: fixture')
        with patch.dict('os.environ',{'IMLANE_HTTP_TRACE_DIR':'unused'}), \
             patch.object(self.kernel,'_request',AsyncMock(return_value=expected)), \
             patch.object(self.kernel.Path,'mkdir',side_effect=OSError('trace fixture')):
            self.assertEqual(self.run_call(call),expected)


if __name__=='__main__':unittest.main()
