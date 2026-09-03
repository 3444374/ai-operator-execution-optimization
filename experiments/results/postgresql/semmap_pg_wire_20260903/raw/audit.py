"""Audit new Map evidence, preserved prior evidence and changed document links."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.parse import unquote

repo = Path(sys.argv[1]).resolve()
raw = repo / 'experiments/results/postgresql/semmap_pg_wire_20260903/raw'
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def git(*args):
    return subprocess.check_output(['git', *args], cwd=repo, text=True).strip()

verified = 0
for parent in (raw, repo / 'experiments/results/postgresql/semmap_pg_plan_20260903/raw'):
    for manifest in sorted(parent.glob('**/SHA256SUMS')):
        if manifest.parent == raw:
            continue
        for line in manifest.read_text().splitlines():
            digest, name = line.split('  ', 1)
            target = (manifest.parent / name).resolve()
            assert target.is_relative_to(manifest.parent)
            assert sha(target) == digest, str(target)
            verified += 1
qualification = json.loads((raw / '5031bb50/qualification.json').read_text())
differences = []
for name, digest in qualification['source_sha256'].items():
    if sha(repo / name) != digest:
        assert name.endswith('.md'), name
        differences.append(name)
changed = set(git('diff', '--name-only', '5031bb50').splitlines())
changed.update(git('ls-files', '--others', '--exclude-standard').splitlines())
links = 0
for name in sorted(changed):
    if not name.endswith('.md'):
        continue
    source = repo / name
    for match in re.finditer(r'(?<!!)\[[^\]\n]*\]\(([^)\n]+)\)', source.read_text()):
        target = match[1].strip().split(' "', 1)[0].strip('<>')
        if re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*:', target) or target.startswith('#'):
            continue
        target = unquote(target.split('#', 1)[0])
        if not target:
            continue
        assert (source.parent / target).resolve().exists(), (name, target)
        links += 1
assert qualification['base_extension_unchanged']
assert qualification['pg_build_warning_free']
postflight = json.loads((raw / 'postflight.json').read_text())
assert all(not run['pg_pid_files'] and not run['owned_pg_or_python_processes'] and not run['dirty']
           for run in postflight['runs'])
assert not postflight['main_dirty']
record = dict(source_commit=qualification['source_commit'], verified_manifest_entries_including_history=verified,
    changed_local_links_checked=links, documentation_differences_since_qualification=differences,
    unchanged_production_and_test_source=True, owned_test_processes_stopped=True,
    reader_language_checks={'files': ['code/README.md', 'code/postgres/semloom_pg/README.md'],
        'hits': 'only SQLSTATE 08P01, explicitly described as a technical error code'},
    internal_documents='plans, project outline, status, evidence registry and log describe implementation/verification state')
(raw / 'handoff-check.json').write_text(json.dumps(record, indent=2) + '\n')
shutil.copyfile(__file__, raw / 'audit.py')
files = sorted(path for path in raw.rglob('*') if path.is_file() and path != raw / 'SHA256SUMS')
(raw / 'SHA256SUMS').write_text(''.join(sha(path) + '  ' + str(path.relative_to(raw)) + '\n' for path in files))
print(json.dumps(dict(record, archived_files=len(files)), indent=2))
