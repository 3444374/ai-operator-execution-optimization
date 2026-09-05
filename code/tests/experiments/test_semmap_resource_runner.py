"""Real finish-file and child-process effects through the stress orchestrator."""
from contextlib import contextmanager
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.experiments.postgresql import semmap_resource_runner as runner
from src.experiments.postgresql.runtime_helpers import owned_child_process
from test_resource_lifecycle import FAST, ControlledWorkload

CLIENT = '''
import json, pathlib, sys, time
root=pathlib.Path(sys.argv[1]); release=root/'release'; finish=root/'finish'
active=root/'active'; events=root/'events.jsonl'
def event(value):
    value['monotonic_ns']=time.monotonic_ns()
    with events.open('a') as f: f.write(json.dumps(value)+'\\n')
event({'event':'session_start','session_id':1,'peer_pid':1,'gateway_pid':2,'accepted_fd':5,'accepted_socket_inode':5})
event({'event':'session_end','session_id':1,'connection_closed':True})
print(json.dumps({'event':'warmup_complete','backend_pid':1}),flush=True)
while not release.exists(): time.sleep(.001)
active.touch()
event({'event':'session_start','session_id':2,'peer_pid':1,'gateway_pid':2,'accepted_fd':5,'accepted_socket_inode':5})
for n in range(100):
    event({'event':'task','session_id':2,'task':n,'payload_digest':'fixture'})
    event({'event':'task_complete','session_id':2,'task':n})
time.sleep(.04)
active.unlink()
event({'event':'session_end','session_id':2,'connection_closed':True})
print(json.dumps({'event':'all_complete','rounds':1,'rows_per_round':100,'rows':100}),flush=True)
while not finish.exists(): time.sleep(.001)
(root/'backend_dead').touch()
'''


class StressOwnershipTests(unittest.TestCase):
    def test_cleanup_samples_same_live_backend_before_finish(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            args=SimpleNamespace(root=root,prefix=root,repo=root)
            client_root=root/'stress_large_payload/client'
            observations=[]
            workload=ControlledWorkload()
            class Sampler:
                def sample_all(self,ns):
                    self_alive=not (client_root/'finish').exists()
                    observations.append(self_alive)
                    workload.active=(client_root/'active').exists()
                    return workload.sample_all(ns)
            @contextmanager
            def fake_gateway(*args,**kwargs):
                def events():
                    path=client_root/'events.jsonl'
                    return [json.loads(x) for x in path.read_text().splitlines()] if path.exists() else []
                yield SimpleNamespace(pid=2),root/'gateway.sock',events
            @contextmanager
            def fake_client(command,path,name,env,user):
                self.assertEqual(command[1:3], ['/tmp/custom socket', '55499'])
                self.assertEqual(command[-2:], ['fixture-owner', 'fixture-db'])
                with owned_child_process([sys.executable,'-c',CLIENT,str(path)],path,name,env,None) as process:
                    yield process
            with patch.object(runner,'gateway',fake_gateway),patch.object(runner,'pg_file_context',return_value=None), \
                 patch.object(runner,'ProcfsTickSampler',return_value=Sampler()), \
                 patch('src.experiments.postgresql.runtime_helpers.owned_child_process',fake_client):
                connection = SimpleNamespace(info=SimpleNamespace(
                    host='/tmp/custom socket', port=55499, user='fixture-owner', dbname='fixture-db'))
                phases=runner.stress_case(args,FAST.__class__(**{**FAST.__dict__,'mode':'diagnostic'}),connection,None,root/'fixture','fixture')
            self.assertEqual(phases[0].policy_status,'passed',phases[0])
            self.assertTrue(all(observations))
            self.assertGreaterEqual(len(observations),FAST.baseline_samples+FAST.cleanup_samples+2)
            self.assertTrue((client_root/'backend_dead').exists())


class FaultBarrierTests(unittest.TestCase):
    def test_driver_releases_only_after_published_endpoint_observation(self):
        import threading,time
        with tempfile.TemporaryDirectory() as directory:
            release=Path(directory)/'release'
            workload=ControlledWorkload()
            probe=runner.ObservationProbe(workload)
            probe.sample_all(time.monotonic_ns())
            entered=threading.Event()
            observed=[]
            def query():
                entered.set()
                while not release.exists():time.sleep(.001)
                return 'completed'
            def publish():
                entered.wait(1)
                observed.append(release.exists())
                workload.active=True
                probe.sample_all(time.monotonic_ns())
            publisher=threading.Thread(target=publish)
            publisher.start()
            value=runner.query_after_observation(query,probe,release,lambda:None)
            publisher.join()
            self.assertEqual(observed,[False])
            self.assertEqual(value,'completed')
            self.assertTrue(release.exists())

    def test_unobserved_connection_cancels_and_joins_query(self):
        import threading,time
        with tempfile.TemporaryDirectory() as directory:
            release=Path(directory)/'release'
            probe=runner.ObservationProbe(ControlledWorkload())
            probe.sample_all(time.monotonic_ns())
            cancelled=[]
            def query():
                while not release.exists():time.sleep(.001)
            with self.assertRaises(TimeoutError):
                runner.query_after_observation(query,probe,release,lambda:cancelled.append(True),timeout=.02)
            self.assertEqual(cancelled,[True])
            self.assertFalse(any(t.name=='fixture-query' for t in threading.enumerate()))


if __name__=='__main__': unittest.main()
