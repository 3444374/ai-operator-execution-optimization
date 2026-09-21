"""Restart the owned endpoint after correcting only its IPC temporary path."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser()
for name in ('repo', 'root', 'ipc', 'ledger'):
    parser.add_argument('--'+name,type=Path,required=True)
args = parser.parse_args()
sys.path.insert(0,str(args.repo/'code'))
from src.experiments.choice_attempt_ledger import AttemptLedger
from src.experiments.choice_service_checks import save

assert AttemptLedger(args.ledger).attempts == 0
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
assert args.ipc.is_dir()
assert len(str(args.ipc).encode()) + 1 + 36 <= 107
command = json.loads((args.root/'launch.json').read_text())['argv']
command[command.index('--identity-output')+1] = str(args.root/'service-identity-r3.json')
cache = args.root/'model-service'
environment = dict(os.environ, PATH=str(Path(command[0]).parent)+os.pathsep+os.environ['PATH'],
    CUDA_VISIBLE_DEVICES='0', HF_HUB_OFFLINE='1',
    TRANSFORMERS_OFFLINE='1', HF_HOME=str(cache/'hf'), XDG_CACHE_HOME=str(cache/'xdg'),
    VLLM_CACHE_ROOT=str(cache/'vllm'), TRITON_CACHE_DIR=str(cache/'triton'),
    TORCHINDUCTOR_CACHE_DIR=str(cache/'inductor'), TMPDIR=str(args.ipc),
    VLLM_NO_USAGE_STATS='1', DO_NOT_TRACK='1', PYTHONDONTWRITEBYTECODE='1')
assert (Path(command[0]).parent/'ninja').is_file()
with (args.root/'model-r3.log').open('x') as log:
    process = subprocess.Popen(command,env=environment,stdin=subprocess.DEVNULL,
        stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
save(args.root/'launch-r3.json',dict(pid=process.pid,argv=command,gpu=0,ipc_directory=str(args.ipc),
                                  path_prefix=str(Path(command[0]).parent)))
print('owned service PID',process.pid)
