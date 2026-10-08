"""Verify the retained main-source fixture without starting Ray or a model."""
from pathlib import Path, PurePosixPath
import hashlib, json, runpy, sys, tarfile, tempfile

root=Path(__file__).resolve().parent
identity=json.loads((root/'identity.json').read_text())
archive=root/'evidence.tar.gz'
assert archive.stat().st_size==identity['archive_bytes']
assert hashlib.sha256(archive.read_bytes()).hexdigest()==identity['archive_sha256']
with tempfile.TemporaryDirectory(prefix='semantic-return-replay-') as temporary:
    target=Path(temporary);seen=set()
    with tarfile.open(archive,'r:gz') as bundle:
        for member in bundle:
            name=PurePosixPath(member.name)
            assert member.isfile() and not name.is_absolute() and '..' not in name.parts
            assert member.name in identity['members'] and member.name not in seen
            data=bundle.extractfile(member).read();expected=identity['members'][member.name]
            assert len(data)==expected['bytes'] and hashlib.sha256(data).hexdigest()==expected['sha256']
            path=target/member.name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
            seen.add(member.name)
    assert seen==set(identity['members'])
    previous=sys.argv
    try:
        sys.argv=[str(target/'analyze.py'),str(target/'cases'),str(target/'analysis.json')]
        runpy.run_path(str(target/'analyze.py'),run_name='__main__')
    finally:sys.argv=previous
    assert json.loads((target/'analysis.json').read_text())==json.loads((root/'analysis.json').read_text())
assert identity['owner_summary']['remaining_owned_pids']==[]
assert json.loads((root/'backup-verification.json').read_text())['status']=='passed'
print(json.dumps(dict(status='passed',public_members=len(seen),model_requests=0)))
