"""Record one healthy service and one invocation of the planned SQL smoke; never retry."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import urllib.request

parser = argparse.ArgumentParser()
for name in ('repo', 'root', 'prefix', 'ledger', 'model'):
    parser.add_argument('--'+name, type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo/'code'))
from src.experiments.choice_service_checks import save

with urllib.request.urlopen('http://127.0.0.1:8013/health', timeout=2) as response:
    assert response.status == 200
    save(args.root/'health-r4.json', dict(status=response.status, method='GET', model_request=False))
command = [sys.executable, str(args.repo/'code/scripts/experiments/run_choice_service_checks.py'),
    '--repo', str(args.repo), '--root', str(args.root/'pg-real'), '--prefix', str(args.prefix),
    '--config', str(args.root/'fixed.json'), '--ledger', str(args.ledger),
    '--identity', str(args.root/'service-identity-r4.json'), '--model-root', str(args.model),
    '--model-manifest', str(args.root/'model-files.json')]
with (args.root/'real-run.log').open('x') as log:
    completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
        env=dict(os.environ, PYTHONPATH=str(args.repo/'code'), PYTHONDONTWRITEBYTECODE='1'))
save(args.root/'real-invocation.json', dict(argv=command, exit_code=completed.returncode, retries=0))
print('real collector exit', completed.returncode)
sys.exit(completed.returncode)
