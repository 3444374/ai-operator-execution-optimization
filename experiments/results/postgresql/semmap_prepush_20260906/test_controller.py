"""No-model checks for endpoint binding and owned process-group cleanup."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest

spec=importlib.util.spec_from_file_location('prepush_launch',Path(__file__).with_name('launch.py'))
launch=importlib.util.module_from_spec(spec);spec.loader.exec_module(launch)


class ControllerTests(unittest.TestCase):
    def test_wrong_service_or_timeout_is_rejected_before_launch(self):
        settings={'model_port':18160}
        valid={'endpoint_url':'http://127.0.0.1:18160/v1/chat/completions','timeout_ms':120000}
        launch.validate_endpoint(settings,valid)
        for url in ('http://127.0.0.1:18161/v1/chat/completions','http://localhost:18160/v1/chat/completions',
                    'http://127.0.0.1:18160/other','http://127.0.0.1:18160/v1/chat/completions?x=1'):
            with self.subTest(url=url),self.assertRaises(AssertionError):
                launch.validate_endpoint(settings,{**valid,'endpoint_url':url})
        with self.assertRaises(AssertionError):
            launch.validate_endpoint(settings,{**valid,'timeout_ms':1000})

    def test_term_closes_and_reaps_owned_parent_and_child(self):
        code='''import signal,subprocess,sys,time
child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])
def stop(*_):
    child.wait(timeout=5)
    sys.exit(0)
signal.signal(signal.SIGTERM,stop)
print(child.pid,flush=True)
time.sleep(60)
'''
        with subprocess.Popen([sys.executable,'-c',code],stdout=subprocess.PIPE,text=True,start_new_session=True) as parent:
            child_pid=int(parent.stdout.readline())
            self.assertFalse(launch.stop_group(parent,5))
            self.assertEqual(parent.returncode,0)
            self.assertEqual(subprocess.run(['kill','-0',str(child_pid)],capture_output=True).returncode,1)

    def test_exited_leader_does_not_hide_a_stubborn_child(self):
        child="import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); time.sleep(60)"
        code="import subprocess,sys; p=subprocess.Popen([sys.executable,'-c',sys.argv[1]],stdout=subprocess.PIPE,text=True); p.stdout.readline(); print(p.pid,flush=True)"
        with subprocess.Popen([sys.executable,'-c',code,child],stdout=subprocess.PIPE,text=True,start_new_session=True) as parent:
            child_pid=int(parent.stdout.readline())
            parent.wait(timeout=5)
            self.assertTrue(launch.group_alive(parent.pid))
            self.assertTrue(launch.stop_group(parent,.05))
            self.assertFalse(launch.group_alive(parent.pid))

    def test_unresponsive_owned_process_is_killed_and_reaped(self):
        code='import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print("ready",flush=True); time.sleep(60)'
        with subprocess.Popen([sys.executable,'-c',code],stdout=subprocess.PIPE,text=True,start_new_session=True) as parent:
            self.assertEqual(parent.stdout.readline().strip(),'ready')
            self.assertTrue(launch.stop_group(parent,.05))
            self.assertLess(parent.returncode,0)


if __name__=='__main__':unittest.main()
