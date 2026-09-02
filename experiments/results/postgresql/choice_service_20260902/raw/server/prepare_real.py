"""Verify existing assets, create the first real budget and start one owned endpoint."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
import socket
import subprocess
import sys

parser = argparse.ArgumentParser()
for name in ('repo', 'root', 'model', 'python', 'ledger'):
    parser.add_argument('--'+name, type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo/'code'))
from src.experiments.choice_attempt_ledger import AttemptLedger
from src.experiments.choice_service_checks import MODEL, file_sha, save

manifest = args.repo/'experiments/results/postgresql/semfilter_qualification_20260901/raw/model-files.json'
expected = json.loads(manifest.read_text())
assert all(file_sha(args.model/name) == value for name,value in expected.items())
assert not subprocess.check_output(['nvidia-smi','--query-compute-apps=pid','--format=csv,noheader'],text=True).strip()
with socket.socket() as probe:
    probe.bind(('127.0.0.1',8013))
model_root = args.root/'model-service'
model_root.mkdir()
for name in ('tmp', 'hf', 'xdg', 'vllm', 'triton', 'inductor'):
    (model_root/name).mkdir()
ledger = AttemptLedger.create(args.ledger)
user = pwd.getpwnam('postgres')
os.chown(args.ledger,user.pw_uid,user.pw_gid)
save(args.root/'budget-audit.json', dict(prior_4c_real_attempts=ledger.attempts,
    basis='previous choice slices recorded zero real requests; no existing real ledger found',
    cap=100, planned_requests=14, fixture_ledger_is_separate=True))
save(args.root/'model-files.json', expected)
save(args.root/'fixed.json', dict(endpoint_url='http://127.0.0.1:8013/v1/chat/completions',
     model_id=MODEL, timeout_ms=60000, choice_format='vllm_structured_outputs'))
environment = dict(os.environ, CUDA_VISIBLE_DEVICES='0', HF_HUB_OFFLINE='1',
    TRANSFORMERS_OFFLINE='1', HF_HOME=str(model_root/'hf'), XDG_CACHE_HOME=str(model_root/'xdg'),
    VLLM_CACHE_ROOT=str(model_root/'vllm'), TRITON_CACHE_DIR=str(model_root/'triton'),
    TORCHINDUCTOR_CACHE_DIR=str(model_root/'inductor'), TMPDIR=str(model_root/'tmp'),
    VLLM_NO_USAGE_STATS='1', DO_NOT_TRACK='1', PYTHONDONTWRITEBYTECODE='1')
command = [str(args.python),str(args.repo/'code/scripts/services/launch_vllm_with_identity.py'),
    '--identity-output',str(args.root/'service-identity.json'),'--port','8013','--',
    '--model',str(args.model),'--served-model-name',MODEL,'--dtype','bfloat16',
    '--max-model-len','4096','--gpu-memory-utilization','0.25','--scheduling-policy','fcfs',
    '--port','8013','--host','127.0.0.1','--enforce-eager','--no-enable-prefix-caching',
    '--max-num-seqs','1','--max-num-batched-tokens','4096','--tensor-parallel-size','1',
    '--generation-config','auto']
with (args.root/'model.log').open('x') as log:
    process = subprocess.Popen(command,env=environment,stdin=subprocess.DEVNULL,
        stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
save(args.root/'launch.json',dict(pid=process.pid,argv=command,gpu=0))
print('owned service PID',process.pid)
