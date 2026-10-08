"""Restore and replay ready-query evidence without model or database calls."""
from pathlib import Path
import hashlib, json, subprocess, sys, tempfile

raw = Path(__file__).resolve().parent
repo = next(p for p in raw.parents if (p / 'code/src').is_dir())
sys.path.insert(0, str(repo / 'code'))
from src.experiments.evidence_storage import restore_result

with tempfile.TemporaryDirectory(prefix='ready-evidence-check-') as directory:
    restored = Path(directory) / 'result'
    storage = restore_result(raw / 'storage-manifest.jsonl', restored, repository=repo)
    evidence = restored / 'raw'
    for name, identity in json.loads((evidence / 'export-identity.json').read_text()).items():
        data = (evidence / name).read_bytes()
        assert len(data) == identity['bytes']
        assert hashlib.sha256(data).hexdigest() == identity['sha256']
    original_analysis = (evidence / 'analysis.json').read_bytes()
    check = subprocess.run([sys.executable, str(evidence / 'analyze.py')],
                           capture_output=True, text=True)
    if check.returncode:
        sys.stderr.write(check.stdout + check.stderr)
        raise SystemExit(check.returncode)
    assert (evidence / 'analysis.json').read_bytes() == original_analysis
    print(check.stdout.strip())
    print(json.dumps(dict(restoration=storage, export_files_verified=21,
                          exact_analysis_replay=True, model_requests=0)))
