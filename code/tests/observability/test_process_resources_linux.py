"""Linux integration: observe separate live client/gateway processes from a third process."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from src.observability.process_resources.linux_procfs import sample_tick, unix_socket_table
from src.observability.process_resources.model import FdKind, ResourceTrace, SnapshotStatus
from src.experiments.postgresql.provider_session_attribution import (
    attribute_provider_sessions, reclassify_clients, session_windows, residual_provider_fds)
from src.experiments.postgresql.resource_qualification import build_qualification_report

GATEWAY = '''
import json,os,socket,struct,sys,time
listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
listener.bind(sys.argv[1]); listener.listen(1)
unrelated=open(sys.argv[2],'w')
print('ready',flush=True)
connection,_=listener.accept()
peer=struct.unpack('3i',connection.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
inode=int(os.readlink('/proc/self/fd/'+str(connection.fileno()))[8:-1])
print(json.dumps({'event':'session_start','session_id':1,'monotonic_ns':time.monotonic_ns(),
'gateway_pid':os.getpid(),'peer_pid':peer[0],'accepted_fd':connection.fileno(),'accepted_socket_inode':inode}),flush=True)
sys.stdin.readline(); connection.close()
print(json.dumps({'event':'session_end','session_id':1,'monotonic_ns':time.monotonic_ns(),'connection_closed':True}),flush=True)
sys.stdin.readline(); unrelated.close(); listener.close()
'''
CLIENT = '''
import socket,sys
print('ready',flush=True); sys.stdin.readline()
client=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); client.connect(sys.argv[1])
print('connected',flush=True); sys.stdin.readline(); client.close()
print('closed',flush=True); sys.stdin.readline()
'''


def signal_child(child):
    child.stdin.write('go\n')
    child.stdin.flush()


@unittest.skipUnless(sys.platform.startswith('linux') and hasattr(socket,'SO_PEERCRED'), 'Linux procfs/SO_PEERCRED required')
class CrossProcessUdsIntegration(unittest.TestCase):
    def test_live_two_process_pair_and_same_identity_cleanup(self):
        with tempfile.TemporaryDirectory(prefix='slri-') as directory:
            path=str(Path(directory)/'provider.sock')
            children=[]
            try:
                gateway=subprocess.Popen([sys.executable,'-u','-c',GATEWAY,path,str(Path(directory)/'unrelated')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                children.append(gateway)
                self.assertEqual(gateway.stdout.readline().strip(),'ready')
                client=subprocess.Popen([sys.executable,'-u','-c',CLIENT,path],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                children.append(client)
                self.assertEqual(client.stdout.readline().strip(),'ready')
                pids={'backend':client.pid,'gateway':gateway.pid}
                baseline=sample_tick(pids,provider_socket_path=path)
                self.assertTrue(all(s.status is SnapshotStatus.VALID for s in baseline.processes.values()),baseline)
                self.assertEqual(baseline.processes['gateway'].count(FdKind.PROVIDER_UDS_LISTENER),1)
                self.assertGreaterEqual(baseline.processes['gateway'].count(FdKind.REGULAR_FILE_OTHER),1)
                signal_child(client)
                self.assertEqual(client.stdout.readline().strip(),'connected')
                start=json.loads(gateway.stdout.readline())
                ticks=tuple(sample_tick(pids,provider_socket_path=path) for _ in range(3))
                signal_child(client)
                self.assertEqual(client.stdout.readline().strip(),'closed')
                signal_child(gateway)
                end=json.loads(gateway.stdout.readline())
                trace=ResourceTrace(baseline.processes,ticks)
                result=attribute_provider_sessions(backend_pid=client.pid,baseline=baseline.processes,
                    trace=trace,windows=session_windows([start,end]))
                self.assertIsNotNone(result.attribution,result.problems)
                rewritten=reclassify_clients(trace,result.attribution)
                peak=build_qualification_report(baseline.processes,rewritten,phase='stress')
                self.assertTrue(peak.passed,peak)
                self.assertEqual(peak.diagnostics['peak']['provider_uds_session_fd_peak_delta_combined'],2)
                clean=ResourceTrace(baseline.processes,(sample_tick(pids,provider_socket_path=path),))
                report=build_qualification_report(baseline.processes,clean,phase='cleanup')
                self.assertTrue(report.passed,report)
                self.assertEqual(residual_provider_fds(clean,result.attribution),[])
                self.assertTrue(all(child.poll() is None for child in children))
            finally:
                for child in children:
                    if child.poll() is None:
                        child.terminate()
                    child.communicate(timeout=5)

    def test_valid_empty_table_distinct_from_unreadable(self):
        from unittest.mock import patch
        with patch('pathlib.Path.read_text',return_value='Num RefCount Protocol Flags Type St Inode Path\n'):
            self.assertEqual(unix_socket_table(),{})
        with patch('pathlib.Path.read_text',side_effect=PermissionError()):
            self.assertIsNone(unix_socket_table())


if __name__=='__main__': unittest.main()
