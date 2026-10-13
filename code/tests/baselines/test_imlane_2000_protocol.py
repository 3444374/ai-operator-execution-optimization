"""2000-row Python/child protocol fixture; no author runtime, Arrow or HTTP POST."""
import hashlib
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig, IMLaneCallExecutor
from src.baselines.text.frameworks.native_calls import NativeCall
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from tests.execution_provider.test_sema_imlane import SemaIMLaneBridgeTests


class IMLaneConfiguredProtocolTests(unittest.TestCase):
    def setUp(self):
        fixture=SemaIMLaneBridgeTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        self.fixture=fixture
        old=fixture.executor()
        script=Path(old.config['bridge_binary'])
        program='''import json,sys
sys.path.insert(0,CODE)
from src.baselines.text.frameworks.native_calls import _call_from_wire,_outcome_wire,NativeCallOutcome
from src.execution_provider.adapters.full_response import FullModelResponse
from src.baselines.text.frameworks.imlane_limits import IMLaneBatchLimits
values=[_call_from_wire(line) for line in sys.stdin]
profile=IMLaneBatchLimits(**dict(zip(IMLaneBatchLimits.__dataclass_fields__,map(int,sys.argv[4:]))))
print('SEMA_IMLANE_META '+json.dumps(dict(native_executors=2,native_task_threads=4)),flush=True)
print('SEMA_IMLANE_LIMITS '+json.dumps(profile.inspect_calls(values)['configured_limits']),flush=True)
for call in reversed(values):
 body=json.dumps(dict(occurrence=call.row_id,text=json.loads(call.payload)['text'])).encode()
 print('SEMA_IMLANE_RESULT '+_outcome_wire(NativeCallOutcome(call.call_id,call.row_id,FullModelResponse(200,(),body))),flush=True)
print('SEMA_IMLANE_STATS '+json.dumps(dict(submitted=len(values),delivered=len(values),pending_peak=min(128,len(values)))),flush=True)
'''.replace('CODE',repr(str(fixture.code_root)))
        script.write_text('#!'+sys.executable+'\n'+program)
        cfg=dict(old.config,bridge_sha256=hashlib.sha256(script.read_bytes()).hexdigest())
        path=fixture.root/'configured.json';path.write_text(json.dumps(cfg))
        public=IMLaneCallConfig('cpython',str(path),123,456,str(fixture.root/'author-build.json'))
        self.executor=IMLaneCallExecutor(public,NativeGraphOptions(concurrency=4),max_batch_rows=2000)
        self.addCleanup(self.executor.close)
        verification=patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator',
                           return_value=dict(runtime='cpython',evidence='protocol fixture only'))
        verification.start();self.addCleanup(verification.stop)

    def calls(self,count):
        return tuple(NativeCall('configured:'+str(i),i,json.dumps(dict(text='duplicate' if i%3==0 else 'text:'+str(i))).encode(),
                               'http://localhost/unused',(),1.) for i in range(count))

    def test_2000_distinct_occurrences_out_of_order_slow_consumer_and_next_query(self):
        values=self.calls(2000);iterator=self.executor.iter_batch(values)
        first=next(iterator);self.assertEqual(first.row_id,1999);time.sleep(.02)
        results=[first,*iterator]
        self.assertEqual(len(results),2000)
        self.assertEqual([v.row_id for v in results],list(reversed(range(2000))))
        self.assertEqual(len({v.call_id for v in results}),2000)
        for response in results:
            body=json.loads(response.response.body)
            self.assertEqual(body['occurrence'],response.row_id)
            self.assertEqual(body['text'],json.loads(values[response.row_id].payload)['text'])
        self.assertTrue(self.executor.last_close_report['clean'])
        self.assertEqual(self.executor.last_close_report['bridge_limits']['max_batch_rows'],2000)
        self.assertEqual(self.executor.last_close_report['bridge_stats']['submitted'],2000)
        self.assertFalse(self.executor._uncertain.exists())
        follow=list(self.executor.iter_batch(self.calls(2)))
        self.assertEqual([v.row_id for v in follow],[1,0])
        self.assertTrue(self.executor.last_close_report['clean'])
        self.assertEqual(self.fixture.server.calls,[])

    def test_2001_rejects_before_synthetic_service_verification(self):
        with patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator') as verification:
            with self.assertRaisesRegex(ValueError,'total row capacity'):
                list(self.executor.iter_batch(self.calls(2001)))
        verification.assert_not_called()
        self.assertEqual(self.fixture.server.calls,[])


if __name__=='__main__':
    unittest.main()
