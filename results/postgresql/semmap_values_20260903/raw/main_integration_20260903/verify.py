"""Recheck the fixed Map integration tree locally; no PostgreSQL or model runs."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--repo", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
repo = args.repo.resolve()
sys.path.insert(0, str(repo / "code"))
from src.baselines.common.redact import redact_argument_list, redact_text

def git(*command):
    return subprocess.check_output(["git", *command], cwd=repo, text=True).strip()

assert git("branch", "--show-current") == "main"
assert git("rev-parse", "HEAD") == "b04009449a65f3b961959c07466e0de443d31dd6"
assert not git("status", "--porcelain")
changed_code_files = git("diff", "--name-only", "a1bbdd30", "HEAD", "--", "code").splitlines()
assert all(Path(name).suffix == ".md" for name in changed_code_files), changed_code_files
args.output.mkdir(parents=True, exist_ok=False)
env = dict(os.environ, PYTHONPATH=str(repo / "code"), PYTHONDONTWRITEBYTECODE="1")
cases = [
    ("postgres-contracts", ["python3", "-m", "unittest", "discover", "-s", "code/tests/postgres", "-v"], 109),
    ("gateway", ["python3", "-W", "error::ResourceWarning", "-m", "unittest", "discover", "-s", "code/tests/execution_provider", "-v"], 6),
    ("calibration", ["python3", "-m", "unittest", "discover", "-s", "code/tests/planning", "-p", "test_semfilter_reference_calibration.py", "-v"], 10),
    ("choice-tools", ["python3", "-m", "unittest", "discover", "-s", "code/tests/experiments", "-p", "test_choice*.py", "-v"], 11),
]
for module in ("generation_profile", "sem_operator_machine", "sem_filter_machine", "sem_map_machine",
               "sem_message_writer", "sem_text", "semantic_map_contract"):
    cases.append(("c11-" + module, ["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-pedantic",
                  "-fsyntax-only", f"code/postgres/semloom_pg/src/{module}.c"], None))
cases.append(("c11-neutral-header", ["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-pedantic",
             "-x", "c", "-fsyntax-only", "code/postgres/semloom_pg/src/ai_provider_port.h"], None))

def run(case):
    name, command, expected = case
    started = time.monotonic()
    result = subprocess.run(command, cwd=repo, env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=180)
    output = redact_text(result.stdout).replace(str(repo), "<test-worktree>")
    output = "\n".join(line.rstrip() for line in output.splitlines()) + "\n"
    matches = re.findall(r"Ran (\d+) tests? in", output)
    count = int(matches[-1]) if matches else None
    record = {"name": name, "command": [redact_text(value).replace(str(repo), "<test-worktree>")
              for value in redact_argument_list(command)], "exit_code": result.returncode,
              "tests": count, "expected_tests": expected, "seconds": round(time.monotonic() - started, 3),
              "passed": result.returncode == 0 and (expected is None or count == expected)}
    return record, output

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(run, cases))
summary = {"commit": git("rev-parse", "HEAD"), "branch": "main", "dirty_before": False,
           "code_tree": git("rev-parse", "HEAD:code"), "implementation_matches": "a1bbdd3057b194de528e4a1e3b89786dd51d76a8",
           "documentation_only_differences": changed_code_files,
           "preflight_correction": "The first runner stopped before tests: its Markdown exclusion pathspec did not exclude top-level code Markdown files. Replaced with explicit changed-file suffix validation; no implementation mismatch.",
           "checks": [record for record, _ in results], "test_total": sum(record["tests"] or 0 for record, _ in results),
           "passed": all(record["passed"] for record, _ in results), "postgres_server_rerun": False,
           "model_requests": 0, "resource_smoke": False}
(args.output / "checks.log").write_text("\n".join("## " + record["name"] + "\n" + output for record, output in results))
(args.output / "verification.json").write_text(redact_text(json.dumps(summary, indent=2)) + "\n")
shutil.copyfile(__file__, args.output / "verify.py")
files = sorted(args.output.iterdir())
(args.output / "SHA256SUMS").write_text("".join(hashlib.sha256(path.read_bytes()).hexdigest() + "  " + path.name + "\n" for path in files))
print(json.dumps({"tests": summary["test_total"], "passed": summary["passed"], "checks": summary["checks"]}, indent=2))
raise SystemExit(0 if summary["passed"] else 1)
