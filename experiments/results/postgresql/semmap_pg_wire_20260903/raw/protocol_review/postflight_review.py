"""Read-only ownership audit after isolated Map protocol qualification."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import psutil

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--repo", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--run", action="append", required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo / "code"))
from src.baselines.common.redact import redact_text

def git(repo, *arguments):
    return subprocess.check_output(
        ["git", "-c", "safe.directory=" + str(repo), "-C", str(repo), *arguments],
        text=True,
    ).strip()

excluded = {psutil.Process().pid, *(p.pid for p in psutil.Process().parents())}
processes = []
for process in psutil.process_iter(["pid", "name", "cmdline", "cwd"]):
    if process.pid not in excluded:
        processes.append(process.info)
runs = []
for item in args.run:
    label, raw_path = item.split("=", 1)
    root = Path(raw_path).resolve(strict=True)
    source = root / "source"
    owned = []
    for process in processes:
        values = [process.get("cwd") or "", *(process.get("cmdline") or [])]
        if any(str(root) in value for value in values):
            owned.append({"pid": process["pid"], "name": process["name"]})
    pidfiles = [str(path.relative_to(root)) for path in root.rglob("postmaster.pid")]
    record = {
        "run": label, "source_commit": git(source, "rev-parse", "HEAD"),
        "dirty": bool(git(source, "status", "--porcelain")),
        "pg_pid_files": pidfiles, "owned_processes": owned,
    }
    server_log = root / "regression/server.log"
    if label == "long-path" and server_log.exists():
        text = redact_text(server_log.read_text()).replace(str(root), "<artifact-root>")
        (root / "public/regression-start-failure.log").write_text(text)
    runs.append(record)
record = {
    "schema_version": 1, "scope": "four current-task fixture runs only",
    "runs": runs, "server_main_commit": git(args.repo, "rev-parse", "HEAD"),
    "server_main_dirty": bool(git(args.repo, "status", "--porcelain")),
    "passed": all(not run["dirty"] and not run["pg_pid_files"] and not run["owned_processes"] for run in runs),
}
args.output.write_text(redact_text(json.dumps(record, indent=2)) + "\n")
print(json.dumps(record))
raise SystemExit(0 if record["passed"] else 1)
