"""Stop only the endpoint whose PID/start-time identity this slice recorded."""
import argparse
import json
import os
from pathlib import Path
import signal
import time

parser = argparse.ArgumentParser()
parser.add_argument('--root', type=Path, required=True)
args = parser.parse_args()
identity = json.loads((args.root/'service-identity-r4.json').read_text())
pid = identity['pid']
proc = Path('/proc')/str(pid)
assert proc.joinpath('stat').read_text().rsplit(')', 1)[1].split()[19] == identity['process_start_time_ticks']
assert os.getpgid(pid) == pid
assert b'vllm.entrypoints.openai.api_server' in proc.joinpath('cmdline').read_bytes()
os.killpg(pid, signal.SIGTERM)
for _ in range(100):
    if not proc.exists():
        break
    time.sleep(0.1)
with (args.root/'stop-r4.json').open('x') as handle:
    json.dump(dict(pid=pid, signal='SIGTERM', group_verified=True,
                   start_time_verified=True, api_process_absent=not proc.exists()), handle, indent=2)
assert not proc.exists()
print('owned model API process stopped')
