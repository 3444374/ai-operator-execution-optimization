"""Verify copied Map PG artifacts, source identities, and changed Markdown links."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.parse import unquote

repo = Path(sys.argv[1]).resolve()
raw = repo / 'experiments/results/postgresql/semmap_pg_plan_20260903/raw'
def git(*args):
    return subprocess.check_output(['git', *args], cwd=repo)
def sha(data):
    return hashlib.sha256(data).hexdigest()

verified_files = 0
for manifest in sorted(raw.glob('**/SHA256SUMS')):
    if manifest.parent == raw:
        continue
    for line in manifest.read_text().splitlines():
        digest, name = line.split('  ', 1)
        target = (manifest.parent / name).resolve()
        assert target.is_relative_to(manifest.parent), name
        assert sha(target.read_bytes()) == digest, str(target)
        verified_files += 1

identities = []
stages = []
for path in sorted(raw.glob('**/qualification.json')):
    record = json.loads(path.read_text())
    commit = record['source_commit']
    for name, digest in record['source_sha256'].items():
        identities.append((commit + ':' + name, digest))
    log = (path.parent / 'tap.log').read_text()
    match = re.search(r'Files=(\d+), Tests=(\d+)', log)
    stages.append({'commit': commit, 'directory': str(path.parent.relative_to(raw)),
                   'tap_tests': int(match[2]) if match else None,
                   'tap_passed': 'Result: PASS' in log,
                   'expected_failure': record.get('expected_failure', False)})

assert identities
objects = subprocess.check_output(
    ['git', 'cat-file', '--batch'], cwd=repo,
    input=('\n'.join(name for name, _ in identities) + '\n').encode())
offset = 0
for name, expected in identities:
    end = objects.index(b'\n', offset)
    header = objects[offset:end].split()
    assert len(header) == 3 and header[1] == b'blob', name
    size = int(header[2])
    contents = objects[end + 1:end + 1 + size]
    assert sha(contents) == expected, name
    offset = end + 2 + size
assert offset == len(objects)

server = json.loads((raw / 'server/qualification.json').read_text())
local = json.loads((raw / 'local/verification.json').read_text())
assert local['passed'] and local['commit'] == server['source_commit']
assert server['tap_tests'] == 1260 and server['regression_tests'] == 1
assert server['pg_build_version'] == 'PostgreSQL 18.3' and server['pg_runtime_version'] == '18.3'
assert server['real_model_requests_attempted'] == 0
assert server['regression_actual_sha256'] == server['regression_expected_sha256']
assert not server['generated_map_execution_connected']
current_differences = []
for name, expected in server['source_sha256'].items():
    if sha((repo / name).read_bytes()) != expected:
        assert Path(name).suffix == '.md', name
        current_differences.append(name)

changed = set(git('diff', '--name-only', '340356e8').decode().splitlines())
changed.update(git('ls-files', '--others', '--exclude-standard').decode().splitlines())
links = 0
markdown = sorted(name for name in changed if name.endswith('.md'))
for name in markdown:
    source = repo / name
    for match in re.finditer(r'(?<!!)\[[^\]\n]*\]\(([^)\n]+)\)', source.read_text()):
        target = match[1].strip().split(' "', 1)[0].strip('<>')
        if re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*:', target) or target.startswith('#'):
            continue
        target = unquote(target.split('#', 1)[0])
        if not target:
            continue
        resolved = (source.parent / target).resolve()
        if resolved not in (raw / 'verification.json', raw / 'SHA256SUMS'):
            assert resolved.exists(), (name, target)
        links += 1

report = {'source_commit': server['source_commit'], 'verified_manifest_entries': verified_files,
          'verified_source_blobs': len(identities), 'stages': stages,
          'current_documentation_only_source_differences': current_differences,
          'local_tests': local['test_total'], 'server_tests': sum(server['tests'].values()),
          'tap_tests': server['tap_tests'], 'regression_tests': server['regression_tests'],
          'markdown_files_checked': markdown, 'local_link_targets_checked': links,
          'model_requests': 0, 'passed': True}
(raw / 'verification.json').write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
if Path(__file__).resolve() != raw / 'audit.py':
    shutil.copyfile(__file__, raw / 'audit.py')
paths = sorted(path for path in raw.rglob('*') if path.is_file() and path != raw / 'SHA256SUMS')
(raw / 'SHA256SUMS').write_text(''.join(sha(path.read_bytes()) + '  ' + str(path.relative_to(raw)) + '\n' for path in paths))
print(json.dumps({'artifacts': len(paths), 'source_blobs': len(identities), 'links': links, 'stages': stages}, indent=2))
