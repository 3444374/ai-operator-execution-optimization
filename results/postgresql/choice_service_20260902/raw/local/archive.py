"""Seal finished choice validation evidence without launching services or requests."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from datetime import datetime, timezone

parser = argparse.ArgumentParser()
for name in ('root', 'repo', 'prefix', 'ledger'):
    parser.add_argument('--'+name, type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0,str(args.repo/'code'))
from src.baselines.common.redact import redact_text
from src.experiments.choice_attempt_ledger import AttemptLedger

real_attempts = AttemptLedger(args.ledger).attempts
assert 0 <= real_attempts <= 100
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=args.repo, text=True)

processes = []
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit() or int(proc.name) == os.getpid():
        continue
    try:
        command = proc.joinpath('cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')
    except (OSError, PermissionError):
        continue
    if str(args.root) in command:
        processes.append(dict(pid=int(proc.name), command=command))
assert not processes, processes
tcp = subprocess.check_output(['ss', '-ltn'], text=True)
unix = subprocess.check_output(['ss', '-lx'], text=True)
assert not re.search(r':(?:8013|55446)\s', tcp)
assert str(args.root) not in unix
assert not (args.root/'fixture/pg/data/postmaster.pid').exists()
assert not (args.root/'pg-real/data/postmaster.pid').exists()
assert not (args.root/'pg-real-r2/data/postmaster.pid').exists()
assert not (args.root/'fixture-r2/pg/data/postmaster.pid').exists()
cache = Path('<runtime-path>')
snapshot = dict(observed_at_utc=datetime.now(timezone.utc).isoformat(),
    scoped_processes=processes, scoped_tcp_listeners=[], scoped_unix_listeners=[],
    pg_postmaster_pid_present=False, real_attempts=real_attempts,
    real_collector_started=(args.root/'pg-real').exists(),
    fourth_start_executed=(args.root/'launch-r4.json').exists(), source_worktree_retained=True,
    gpu_compute_processes=subprocess.check_output(['nvidia-smi',
        '--query-compute-apps=pid,process_name,used_gpu_memory', '--format=csv,noheader'], text=True).splitlines(),
    flashinfer_system_cache_bytes=sum(p.stat().st_size for p in cache.rglob('*') if p.is_file()),
    flashinfer_system_cache_disposition='retained; not all cache ownership established; no broad deletion')
with (args.root/'recording-state.json').open('x') as handle:
    handle.write(redact_text(json.dumps(snapshot, indent=2))+'\n')

destination = args.root/'public'
destination.mkdir()
def sha(data):
    return hashlib.sha256(data).hexdigest()
def save(path, value):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x') as handle:
        json.dump(value,handle,indent=2)
        handle.write('\n')
def redact(text):
    for path, label in ((args.repo,'<test-worktree>'), (args.prefix,'<pg18.3-prefix>'),
                        (args.root,'<artifact-root>')):
        text = text.replace(str(path), label)
    text = re.sub(r'<runtime-path>"\',)]+', '<runtime-path>', text)
    text = re.sub(r'autodl-container-[A-Za-z0-9-]+', '<test-host>', text)
    return redact_text(text)

paths = [p for p in args.root.iterdir() if p.is_file() and p.suffix in ('.json','.py','.log')
         and not p.name.startswith('archive')]
for name in ('fixture', 'fixture-r2', 'pg-real', 'pg-real-r2'):
    paths += [p for p in (args.root/name).rglob('*') if p.is_file() and p.suffix in ('.json','.jsonl','.log')
              and 'data' not in p.relative_to(args.root/name).parts]
original = {}
for path in sorted(paths):
    name = path.relative_to(args.root)
    original[str(name)] = sha(path.read_bytes())
    target = destination/name
    target.parent.mkdir(parents=True,exist_ok=True)
    with target.open('x') as handle:
        handle.write(redact(path.read_text()))
with (destination/'real-attempts.jsonl').open('x') as handle:
    handle.write(args.ledger.read_text())
original['real-attempt-ledger'] = sha(args.ledger.read_bytes())
sources = subprocess.check_output(['git','ls-files','code/postgres/semloom_pg/src',
    'code/src/execution_provider','code/src/experiments/choice*',
    'code/tests/experiments/test_choice*','code/scripts/experiments/run_choice*',
    'code/postgres/semloom_pg/t/fixtures/openai_compatible_server.py'],cwd=args.repo,text=True).splitlines()
save(destination/'qualification.json',dict(
    source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=args.repo,text=True).strip(),
    source_sha256={name:sha((args.repo/name).read_bytes()) for name in sources},
    pg_version=subprocess.check_output([str(args.prefix/'bin/pg_config'),'--version'],text=True).strip(),
    extension_sha256=sha((args.prefix/'lib/semloom_pg.so').read_bytes()),
    binary_qualification_commit='39007150d5d0f84904fcd0c36b7bab87de7c07c1',
    fresh_pg_build=False,fresh_tap=False,original_sha256=original))

summary = json.loads((args.root/'fixture/pg/summary.json').read_text())
assert summary['kind'] == 'fixture' and summary['status'] == 'passed'
assert summary['attempted_requests'] == 14
queries = [json.loads(path.read_text()) for path in sorted((args.root/'fixture/pg').glob('query-*.json'))]
ledger = [json.loads(line) for line in (args.root/'fixture/fixture-attempts.jsonl').read_text().splitlines()][1:]
assert len(queries) == len(ledger) == 14
rows = []
for query in queries:
    request, completion = query['events']
    body = json.dumps(request['body'],ensure_ascii=False,separators=(',',':')).encode()
    assert ledger[request['attempt']-1]['request_sha256'] == sha(body)
    rows.append(dict(attempt=request['attempt'],choice=query['choice'],input_index=query['input_index'],
        repeat=query['repeat'],raw_output=completion['raw_output'],prompt_tokens=completion['prompt_tokens'],
        output_tokens=completion['output_tokens'],finish_reason=completion['finish_reason'],
        sqlstate=query['sqlstate'],sql_rows=query['plan']['Actual Rows'] if query['plan'] else None))
save(destination/'fixture-request-summary.json',dict(**summary,requests=rows))
real_summary = json.loads((args.root/'pg-real-r2/summary.json').read_text())
assert real_summary['kind'] == 'real'
real_queries = [json.loads(p.read_text()) for p in sorted((args.root/'pg-real-r2').glob('query-*.json'))]
real_ledger = [json.loads(line) for line in args.ledger.read_text().splitlines()][1:]
real_rows = []
for query in real_queries:
    request = next((event for event in query['events'] if event['event'] == 'request'), None)
    completion = next((event for event in query['events'] if event['event'] == 'completion'), None)
    if request:
        body = json.dumps(request['body'],ensure_ascii=False,separators=(',',':')).encode()
        assert real_ledger[request['attempt']-1]['request_sha256'] == sha(body)
    real_rows.append(dict(attempt=request['attempt'] if request else None, choice=query['choice'],
        input_index=query['input_index'], repeat=query['repeat'], completion=completion,
        sqlstate=query['sqlstate'], sql_rows=query['plan']['Actual Rows'] if query['plan'] else None))
save(destination/'real-request-summary.json', dict(**real_summary, requests=real_rows))
record = dict(status='fixture-passed-real-'+real_summary['status'], fixture_requests=14, fixture_null_controls=2,
    real_model_requests=real_summary['attempted_requests'], cumulative_real_budget_used=real_attempts,
    cumulative_real_budget_limit=100, quality_evaluated=False, calibration_resumed=False,
    previous_run=dict(source_commit='87b7963b', requests=1, status='failed',
                      reason='audit counted BatchEncoding fields; PG received TRUE with 65 prompt tokens'),
    starts=[dict(attempt=1,status='failed',reason='IPC path too long',log='model.log'),
            dict(attempt=2,status='failed',reason='ninja not on service PATH',log='model-r2.log'),
            dict(attempt=3,status='started-then-stopped',startup_complete_logged=True,
                 health_endpoint_verified=False,reason='FlashInfer compilation cache on system disk',
                 shutdown_warning='resource tracker reported one semaphore; not proof of a persistent leak',
                 log='model-r3.log'),
            dict(attempt=4,status='served-real-queries-then-stopped',log='model-r4.log')],
    fourth_start_executed=True)
save(destination/'record-summary.json',record)
save(args.root/'original-manifest.json',original)
save(destination/'manifest.json',{str(p.relative_to(destination)):sha(p.read_bytes())
    for p in sorted(destination.rglob('*')) if p.is_file()})
print(json.dumps(record,indent=2))
