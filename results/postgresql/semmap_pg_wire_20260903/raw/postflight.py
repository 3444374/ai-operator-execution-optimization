"""Read-only ownership checks for the six Map wire test runs."""
import json
import os
from pathlib import Path
import subprocess
import sys

parent = Path(sys.argv[1])
main = Path(sys.argv[2])
revisions = ('76156526', '6941b91e', 'f7765d6c', '69139c03', '114a411a', '5031bb50')
rows = []
for revision in revisions:
    root = parent / f'semmap_wire_{revision}_20260903'
    source = root / 'source'
    command = ['git', '-c', f'safe.directory={source}', '-C', str(source)]
    commit = subprocess.check_output(command + ['rev-parse', 'HEAD'], text=True).strip()
    dirty = bool(subprocess.check_output(command + ['status', '--porcelain'], text=True).strip())
    processes = []
    for proc in Path('/proc').glob('[0-9]*'):
        if int(proc.name) in (os.getpid(), os.getppid()):
            continue
        try:
            executable = (proc / 'comm').read_text().strip()
            argv = (proc / 'cmdline').read_bytes()
        except (OSError, ProcessLookupError):
            continue
        if (executable.startswith('postgres') or executable.startswith('python')) and str(root).encode() in argv:
            processes.append(int(proc.name))
    rows.append(dict(commit=commit, dirty=dirty, pg_pid_files=len(list(root.rglob('postmaster.pid'))),
                     owned_pg_or_python_processes=processes))
assert all(not row['dirty'] and not row['pg_pid_files'] and not row['owned_pg_or_python_processes'] for row in rows)
record = {'runs': rows, 'scope': 'only the six Map wire slice roots',
          'main_head': subprocess.check_output(['git', '-C', str(main), 'rev-parse', 'HEAD'], text=True).strip(),
          'main_dirty': bool(subprocess.check_output(['git', '-C', str(main), 'status', '--porcelain'], text=True).strip()),
          'temporary_worktrees_retained': True, 'real_model_requests': 0}
print(json.dumps(record, indent=2))
