"""Verify and replay the full retained export without any model or network call."""
from pathlib import Path
import json, runpy, tempfile
from restore import restore

root=Path(__file__).resolve().parent
with tempfile.TemporaryDirectory(prefix='semantic-tail-replay-') as temp:
    verified=restore(root, temp)
    print(json.dumps(dict(restoration=verified)))
    runpy.run_path(str(Path(temp)/'replay.py'),run_name='__main__')
