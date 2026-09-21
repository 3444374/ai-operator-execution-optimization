"""Run a source-bound local check, sanitizing captured output before persistence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--repo', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('command', nargs=argparse.REMAINDER)
args = parser.parse_args()
sys.path.insert(0, str(args.repo / 'code'))
from src.baselines.common.redact import redact_text, redact_argument_list

command = args.command[1:] if args.command[0] == '--' else args.command
env = dict(os.environ, PYTHONPATH=str(args.repo / 'code'))
result = subprocess.run(command, cwd=args.repo, env=env, text=True,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
output = redact_text(result.stdout).replace(str(args.repo), '<test-worktree>')
output = '\n'.join(line.rstrip() for line in output.splitlines()) + '\n'
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(output)
names = subprocess.check_output(['git', 'ls-files', 'code/postgres/semloom_pg',
    'code/src/execution_provider', 'code/tests/postgres'], cwd=args.repo, text=True).splitlines()
record = {
    'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=args.repo, text=True).strip(),
    'dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=args.repo, text=True).strip()),
    'command': [redact_text(value).replace(str(args.repo), '<test-worktree>')
                for value in redact_argument_list(command)], 'exit_code': result.returncode,
    'source_sha256': {name: hashlib.sha256((args.repo/name).read_bytes()).hexdigest() for name in names},
}
args.output.with_suffix('.json').write_text(redact_text(json.dumps(record, indent=2)) + '\n')
print(output, end='')
raise SystemExit(result.returncode)
