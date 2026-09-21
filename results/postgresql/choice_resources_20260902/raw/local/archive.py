"""Archive only this slice's measured logs, traces and source identity, with path redaction."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--root', type=Path, required=True)
parser.add_argument('--repo', type=Path, required=True)
parser.add_argument('--prefix', type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo/'code'))
from src.baselines.common.redact import redact_text

destination = args.root/'public-r2'
destination.mkdir()

def sha(value):
    return hashlib.sha256(value).hexdigest()

def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as handle:
        json.dump(value, handle, indent=2)
        handle.write('\n')

replacements = [(str(args.repo), '<test-worktree>'), (str(args.prefix), '<pg18.3-prefix>'),
                (str(args.root), '<artifact-root>')]
replacements += [(str(Path(sys.executable).parent.parent), '<driver-env>')]

def redact(value):
    for original, replacement in replacements:
        value = value.replace(original, replacement)
    value = re.sub(r'/root/(?:[^\s"\']+)', '<runtime-path>', value)
    value = re.sub(r'autodl-container-[A-Za-z0-9-]+', '<test-host>', value)
    return redact_text(value)

selected = [p for p in args.root.glob('*.log') if not p.name.startswith('archive')]
selected += list(args.root.glob('*preflight.json'))
for name in ('run', 'run-diagnostics', 'run-diagnostics-r2', 'run-stable'):
    selected += [p for p in (args.root/name).rglob('*') if p.is_file()
                 and p.suffix in ('.json', '.jsonl', '.log') and 'data' not in p.relative_to(args.root/name).parts]
original_hashes = {}
for path in sorted(set(selected)):
    relative = path.relative_to(args.root)
    original_hashes[str(relative)] = sha(path.read_bytes())
    target = destination/relative
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open('x') as handle:
        handle.write(redact(path.read_text()))

source_paths = subprocess.check_output(['git','ls-files','code/postgres/semloom_pg/src',
    'code/src/execution_provider','code/src/experiments/choice_attempt_ledger.py',
    'code/src/experiments/choice_gateway_observer.py','code/src/experiments/choice_resource_checks.py',
    'code/tests/experiments/test_choice_attempt_ledger.py','code/tests/experiments/test_choice_http_observer.py',
    'code/postgres/semloom_pg/t/fixtures/openai_compatible_server.py'], cwd=args.repo, text=True).splitlines()
save(destination/'qualification.json', dict(
    source_commit=subprocess.check_output(['git','rev-parse','HEAD'], cwd=args.repo,text=True).strip(),
    source_worktree_clean=not subprocess.check_output(['git','status','--porcelain'],cwd=args.repo,text=True),
    source_sha256={name:sha((args.repo/name).read_bytes()) for name in source_paths},
    pg_version=subprocess.check_output([str(args.prefix/'bin/pg_config'),'--version'],text=True).strip(),
    extension_sha256=sha((args.prefix/'lib/semloom_pg.so').read_bytes()),
    binary_qualification_commit='39007150d5d0f84904fcd0c36b7bab87de7c07c1',
    fresh_pg_build=False, fresh_tap=False, real_model_requests=0,
    original_sha256=original_hashes))

summary = {'runs': []}
for profile in ('v3', 'v4'):
    root = args.root/'run-stable'/profile
    baseline = json.loads((root/'baseline.json').read_text())
    for count in (100,1000,4000):
        cell = json.loads((root/f'rows-{count}.json').read_text())
        summary['runs'].append(dict(profile=profile, rows=count, result=cell['result'],
            resources={key:dict(
                baseline={metric:baseline[key][metric] for metric in ('rss_bytes','fd','threads')},
                start={metric:cell['start'][key][metric] for metric in ('rss_bytes','fd','threads')},
                peak={metric:max(p['processes'][key][metric] for p in cell['samples'])
                      for metric in ('rss_bytes','fd','threads')},
                end={metric:cell['ending'][key][metric] for metric in ('rss_bytes','fd','threads')})
                for key in baseline}))
save(destination/'resource-summary.json', summary)
save(args.root/'original-manifest-r2.json', original_hashes)
save(destination/'manifest.json', {str(p.relative_to(destination)):sha(p.read_bytes())
    for p in sorted(destination.rglob('*')) if p.is_file()})
print(json.dumps(summary, indent=2))
