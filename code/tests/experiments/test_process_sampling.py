"""Observe newly spawned children and distinguish missing PID identity from zero RSS."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from src.experiments.process_sampling import ProcessSampler


class ProcessSamplingTests(unittest.TestCase):
    def test_new_child_is_sampled_and_reused_pid_is_not_accepted(self):
        import psutil
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            child=None
            try:
                with ProcessSampler(root/'rss.jsonl',{'driver':os.getpid()},interval=.05,
                    include_children=True,pid_provider=lambda:{'stale':(os.getpid(),0)}) as sampler:
                    child=subprocess.Popen([sys.executable,'-c','import sys; memory=bytearray(8000000); print("ready",flush=True); sys.stdin.read()'],
                        stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
                    self.assertEqual(child.stdout.readline().strip(),'ready')
                    until=time.monotonic()+2
                    while not any(v['pid']==child.pid for v in sampler.identities.values()) and time.monotonic()<until:
                        time.sleep(.02)
                report=sampler.summary()
                self.assertIn(child.pid,{v['pid'] for v in report['processes'].values()})
                sampler.missing.add('driver')
                self.assertEqual(sampler.summary()['discovered_but_unobserved'],['stale'])
                self.assertEqual(report['discovered_but_unobserved'],['stale'])
                samples=[json.loads(l) for l in (root/'rss.jsonl').read_text().splitlines()]
                self.assertTrue(any(v['rss_bytes'].get('child:'+str(child.pid),0)>0 for v in samples))
                self.assertFalse(any('stale' in v['rss_bytes'] for v in samples))
            finally:
                if child is not None:
                    child.communicate(timeout=3)
