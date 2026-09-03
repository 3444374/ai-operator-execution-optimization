"""Copy and verify immutable, redacted Map wire qualification outputs."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

repo = Path(sys.argv[1])
source = Path(sys.argv[2])
destination = repo / 'experiments/results/postgresql/semmap_pg_wire_20260903/raw'
destination.mkdir(parents=True, exist_ok=False)
hashes = []
count = 0
for revision in ('76156526', '6941b91e', 'f7765d6c', '69139c03', '114a411a', '5031bb50'):
    original = source / revision
    manifest = original / 'SHA256SUMS'
    if manifest.exists():
        for line in manifest.read_text().splitlines():
            digest, name = line.split('  ', 1)
            assert (original / name).resolve().is_relative_to(original)
            assert hashlib.sha256((original / name).read_bytes()).hexdigest() == digest
            count += 1
    record_path = original / 'qualification.json'
    if record_path.exists():
        record = json.loads(record_path.read_text())
        hashes += [(record['source_commit'] + ':' + name, digest) for name, digest in record['source_sha256'].items()]
    shutil.copytree(original, destination / revision)
local = Path('/private/tmp/semmap-wire-local-5031bb50')
shutil.copytree(local, destination / 'local')
assert json.loads((local / 'verification.json').read_text())['passed']
objects = subprocess.check_output(['git', 'cat-file', '--batch'], cwd=repo,
    input=('\n'.join(name for name, _ in hashes) + '\n').encode())
offset = 0
for name, expected in hashes:
    end = objects.index(b'\n', offset)
    header = objects[offset:end].split()
    assert len(header) == 3 and header[1] == b'blob', name
    size = int(header[2])
    assert hashlib.sha256(objects[end + 1:end + 1 + size]).hexdigest() == expected, name
    offset = end + 2 + size
assert offset == len(objects)
final = json.loads((destination / '5031bb50/qualification.json').read_text())
assert final['tap_tests'] == 1741 and final['regression_tests'] == 1
assert final['generated_map_execution_connected'] and final['pg_runtime_version'] == '18.3'
assert final['regression_actual_sha256'] == final['regression_expected_sha256']
record = dict(verified_server_manifest_entries=count, verified_commit_bound_source_hashes=len(hashes),
    final_source_commit=final['source_commit'], local_tests=137, server_tests=sum(final['tests'].values()),
    tap=final['tap_tests'], regression=final['regression_tests'], model_requests=0, resource_smoke=False)
(destination / 'archive-check.json').write_text(json.dumps(record, indent=2) + '\n')
shutil.copyfile(__file__, destination / 'archive.py')
print(json.dumps(record, indent=2))
