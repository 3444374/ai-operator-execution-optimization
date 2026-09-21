"""Redact and bind completed local test output to the qualified source."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--repo", type=Path, required=True)
parser.add_argument("--logs", type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo / "code"))
from src.baselines.common.redact import redact_text

result = Path(__file__).parent
qualified = json.loads((result / "raw/qualification.json").read_text())
commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.repo, text=True).strip()
assert commit == qualified["source_commit"]
for name, digest in qualified["source_sha256"].items():
    snapshot = subprocess.check_output(["git", "show", f"{commit}:{name}"], cwd=args.repo)
    assert hashlib.sha256(snapshot).hexdigest() == digest, name
    if Path(name).suffix != ".md":
        assert hashlib.sha256((args.repo / name).read_bytes()).hexdigest() == digest, name
output = result / "local"
output.mkdir()
counts = {}
for name in ("postgres", "gateway", "calibration"):
    raw = (args.logs / (name + ".log")).read_text()
    assert raw.rstrip().endswith("OK")
    counts[name] = int(re.search(r"Ran (\d+) tests", raw).group(1))
    assert counts[name] == qualified["tests"][name]
    text = redact_text(raw).replace(str(args.repo), "<test-worktree>")
    (output / (name + ".log")).write_text(text.rstrip() + "\n")
(output / "qualification.json").write_text(json.dumps(dict(
    source_commit=commit, tests=counts, matching_server_source_hashes=True,
    collector_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()), indent=2) + "\n")
with (output / "SHA256SUMS").open("x") as handle:
    for path in sorted(output.iterdir()):
        if path.name != "SHA256SUMS":
            handle.write(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n")
print(json.dumps(counts))
