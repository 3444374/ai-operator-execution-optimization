"""Restore retained evidence and run the six offline checks; no model calls."""
from pathlib import Path
import json, subprocess, sys, tempfile

raw=Path(__file__).resolve().parent
repo=next(p for p in raw.parents if (p/'code/src').is_dir())
sys.path.insert(0,str(repo/'code'))
from src.experiments.evidence_storage import restore_result

checks=('raw/replay.py','raw/repair/replay.py','raw/completion/replay.py',
        'raw/tail/replay.py','raw/tail-failure/replay.py','raw/main-return/replay.py')
with tempfile.TemporaryDirectory(prefix='semantic-evidence-check-') as directory:
    restored=Path(directory)/'result'
    storage=restore_result(raw/'storage-manifest.jsonl',restored,repository=repo)
    for name in checks:
        check=subprocess.run([sys.executable,str(restored/name)],cwd=restored,
                             capture_output=True,text=True)
        if check.returncode:
            sys.stderr.write(check.stdout+check.stderr)
            raise SystemExit(check.returncode)
        print(json.dumps(dict(check=name,status='passed')),flush=True)
    print(json.dumps(dict(restoration=storage,offline_checks=len(checks),model_requests=0)))
