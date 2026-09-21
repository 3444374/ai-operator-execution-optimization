"""Exercise the exact smoke runner through a local deterministic HTTP fixture."""
import argparse
import json
import os
from pathlib import Path
import pwd
import sys
from types import SimpleNamespace

parser = argparse.ArgumentParser()
for name in ('repo', 'root', 'prefix'):
    parser.add_argument('--'+name, type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo/'code'))
from src.experiments.choice_attempt_ledger import AttemptLedger
from src.experiments.choice_resource_checks import child, wait_file
from src.experiments.choice_service_checks import MODEL, run

args.root.mkdir()
user = pwd.getpwnam('postgres')
os.chown(args.root, user.pw_uid, user.pw_gid)
ledger_path = args.root/'fixture-attempts.jsonl'
AttemptLedger.create(ledger_path)
os.chown(ledger_path, user.pw_uid, user.pw_gid)
port = args.root/'http.port'
env = dict(os.environ, PYTHONPATH=str(args.repo/'code'), PYTHONDONTWRITEBYTECODE='1')
command = [sys.executable, str(args.repo/'code/postgres/semloom_pg/t/fixtures/openai_compatible_server.py'),
           '--port-file', str(port), '--model-id', MODEL, '--max-requests', '14',
           '--allow-choice', '--raw-outputs', 'TRUE,FALSE,UNKNOWN', '--request-log', str(args.root/'requests.jsonl')]
with child(command, args.root, 'http', env, user) as fixture:
    wait_file(port, fixture)
    config = args.root/'fixed.json'
    with config.open('x') as handle:
        json.dump(dict(endpoint_url=f'http://127.0.0.1:{int(port.read_text())}/v1/chat/completions',
                       model_id=MODEL, timeout_ms=60000, choice_format='vllm_structured_outputs'), handle)
    run(SimpleNamespace(repo=args.repo, root=args.root/'pg', prefix=args.prefix,
        config=config, ledger=ledger_path, fixture_only=True))
    assert fixture.wait(timeout=5) == 0 and not port.exists()
assert len((args.root/'requests.jsonl').read_text().splitlines()) == 14
print('fixture SQL/HTTP smoke passed: 14 requests and 2 NULL controls')
