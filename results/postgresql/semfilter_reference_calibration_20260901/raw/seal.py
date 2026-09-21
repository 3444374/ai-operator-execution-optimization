"""Archive scoped cleanup and sanitized logs; preserve full private evidence."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--root", type=Path, required=True)
parser.add_argument("--repo", type=Path, required=True)
parser.add_argument("--pg-prefix", type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo / "code"))
from src.baselines.common.redact import redact_text

root, public = args.root, args.root / "public"
def save(path, value):
    with path.open("x") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
def sanitize(text):
    for original, label in ((str(root), "<artifact-root>"), (str(args.repo), "<source-tree>"),
                            (str(args.pg_prefix), "<pg18.3-prefix>"), (sys.prefix, "<driver-env>")):
        text = text.replace(original, label)
    return redact_text(text)
identity = json.loads((root / "vllm/ep_8013.runtime_identity.json").read_text())
assert not Path(f"/proc/{identity['pid']}").exists()
assert not (root / "pgdata/postmaster.pid").exists()
assert not Path("/tmp/semcal-gw-20260901/provider.sock").exists()
assert not Path("/tmp/semcal-pg-20260901/.s.PGSQL.55439").exists()
compute = subprocess.check_output(["nvidia-smi", "--query-compute-apps=pid,process_name", "--format=csv,noheader"], text=True)
assert not compute.strip(), "GPU compute process still active"
cleanup = dict(experiment_endpoint_stopped=True, experiment_pg18_3_stopped=True,
               experiment_gateway_socket_absent=True, experiment_pg_socket_absent=True,
               gpu_compute_processes_observed=0, scope="this experiment only",
               stopped_pgdata_preserved_for_audit=True)
save(root / "cleanup.json", cleanup)
save(public / "cleanup.json", cleanup)
logs = public / "logs"
logs.mkdir()
for name in ("build.log", "install.log", "initdb.log", "pg-stop.log", "python-postgres.log",
             "python-gateway.log", "python-calibration.log", "collector-tests.log"):
    (logs / name).write_text(sanitize((root / name).read_text()))
(logs / "postgres-server.log").write_text(sanitize((root / "pgdata/server.log").read_text()))
tests = {}
for key, name in (("postgres_python", "python-postgres.log"), ("gateway_migration", "python-gateway.log"),
                  ("calibration_contract", "python-calibration.log"), ("collector", "collector-tests.log")):
    content = (root / name).read_text()
    assert content.rstrip().endswith("OK")
    tests[key] = int(re.search(r"Ran (\d+) tests", content).group(1))
preflight = json.loads((root / "preflight.json").read_text())
assert "-Werror" in (root / "build.log").read_text()
save(public / "qualification.json", dict(pg_version="18.3", source_commit="7042132e",
     production_implementation_commit="dcde2be5", tests=tests, preflight_status=preflight["status"],
     build_werror=True, full_tap_rerun=False, prior_tap_commit="dcde2be5", prior_tap_count=437))
shutil.copy2(Path(__file__), public / "seal.py")
def manifest(directory):
    rows = []
    for path in sorted(directory.rglob("*")):
        if path.is_file() and path != directory / "SHA256SUMS":
            rows.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(directory)}")
    directory.joinpath("SHA256SUMS").write_text("\n".join(rows) + "\n")
    return len(rows)
print(json.dumps(dict(public_files=manifest(public), private_files=manifest(root), cleanup=cleanup, tests=tests)))
