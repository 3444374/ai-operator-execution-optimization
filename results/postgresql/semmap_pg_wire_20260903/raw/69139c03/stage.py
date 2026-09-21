"""Build a fixed Map PG slice in an isolated prefix and record targeted TAP."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
for name in ("repo", "root", "base-prefix", "env-file", "pg-source"):
    parser.add_argument("--" + name, type=Path, required=True)
parser.add_argument("--commit", required=True)
parser.add_argument("--expect-tap-failure", action="store_true")
parser.add_argument("--tap-test", default="t/006_map_plan.pl")
args = parser.parse_args()
sys.path.insert(0, str(args.repo / "code"))
from src.baselines.common.redact import redact_text, redact_argument_list

prefix = args.root / "pg18.3"
public = args.root / "public"
public.mkdir()
extension = args.repo / "code/postgres/semloom_pg"
env = dict(os.environ, PYTHONPATH=str(args.repo / "code"), AI_OPERATOR_ENV_FILE=str(args.env_file),
           PATH=str(prefix / "bin") + ":/usr/sbin:/usr/bin:/sbin:/bin")
as_pg = ["runuser", "-u", "postgres", "--"]
steps = []

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def scrub(value):
    value = redact_text(value)
    for path, label in ((args.env_file, "<runtime-env>"), (args.repo, "<test-worktree>"),
                        (prefix, "<pg18.3-prefix>"), (args.base_prefix, "<base-pg18.3-prefix>"),
                        (args.pg_source, "<pg18.3-source>"), (args.root, "<artifact-root>"),
                        (Path(sys.prefix), "<driver-env>")):
        value = value.replace(str(path), label)
    value = re.sub(r"/root/[^\s'\"]+", "<private-runtime-path>", value)
    return "\n".join(line.rstrip() for line in value.splitlines()).rstrip() + "\n"

def save(name, value):
    (public / name).write_text(scrub(json.dumps(value, indent=2)))

def run(name, command, cwd=None, allow_failure=False):
    result = subprocess.run(command, cwd=cwd or args.repo, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300)
    output = scrub(result.stdout)
    (public / (name + ".log")).write_text(output)
    steps.append({"name": name, "argv": [scrub(s).strip() for s in redact_argument_list(command)],
                  "exit_code": result.returncode})
    save("steps.json", steps)
    print(name + ": " + str(result.returncode), flush=True)
    if result.returncode and not allow_failure:
        raise RuntimeError(name + " failed")
    return result.returncode, output

git = ["git", "-c", "safe.directory=" + str(args.repo)]
assert subprocess.check_output(git + ["rev-parse", "HEAD"], cwd=args.repo, text=True).strip() == args.commit
assert not subprocess.check_output(git + ["status", "--porcelain"], cwd=args.repo, text=True).strip()
assert args.env_file.is_file() and not prefix.exists()
assert subprocess.check_output(["git", "-C", str(args.pg_source), "rev-parse", "HEAD"], text=True).strip() == "62d6c7d3df6287f1bd83199c1a746e50d31571a0"
assert subprocess.check_output([str(args.base_prefix / "bin/pg_config"), "--version"], text=True).strip() == "PostgreSQL 18.3"
base_extension = args.base_prefix / "lib/semloom_pg.so"
base_sha = sha(base_extension) if base_extension.exists() else None
run("preflight", [sys.executable, "code/scripts/environment/manage_environment.py", "check", "--groups", "core",
                  "--json-out", str(args.root / "preflight.json")])
assert json.loads((args.root / "preflight.json").read_text())["status"] == "ok"
shutil.copytree(args.base_prefix, prefix)
run("version", [str(prefix / "bin/pg_config"), "--version"])
assert subprocess.check_output([str(prefix / "bin/pg_config"), "--pkglibdir"], text=True).strip() == str(prefix / "lib")
_, build = run("build", ["make", "PG_CONFIG=" + str(prefix / "bin/pg_config"), "COPT=-O2 -Werror", "-j8"], extension)
assert "-Werror" in build and "warning:" not in build
run("install", ["make", "PG_CONFIG=" + str(prefix / "bin/pg_config"), "install"], extension)
assert sha(extension / "semloom_pg.so") == sha(prefix / "lib/semloom_pg.so")
run("test-owner", ["chown", "-R", "postgres:postgres", str(args.repo)])
try:
    tap_code, tap = run("tap", as_pg + ["env", "PATH=" + env["PATH"], "PG_TEST_NOCLEAN=1", "make",
        "PG_CONFIG=" + str(prefix / "bin/pg_config"), "installcheck", "REGRESS=", "PROVE_TESTS=" + args.tap_test],
        extension, allow_failure=True)
finally:
    for pidfile in (extension / "tmp_check").rglob("postmaster.pid"):
        run("cleanup-" + pidfile.parent.name, as_pg + [str(prefix / "bin/pg_ctl"), "-D", str(pidfile.parent), "-m", "fast", "-w", "stop"])
    for path in sorted((extension / "tmp_check/log").glob("*")):
        if path.is_file():
            (public / ("tap-" + path.name)).write_text(scrub(path.read_text()))
assert (sha(base_extension) if base_extension.exists() else None) == base_sha
expected = tap_code != 0 if args.expect_tap_failure else tap_code == 0 and "Result: PASS" in tap
sources = subprocess.check_output(git + ["ls-files", "code/postgres/semloom_pg"], cwd=args.repo, text=True).splitlines()
save("qualification.json", {"source_commit": args.commit, "pg_version": "18.3", "warning_free_build": True,
     "source_sha256": {name: sha(args.repo / name) for name in sources}, "extension_sha256": sha(extension / "semloom_pg.so"),
     "tap_exit_code": tap_code, "expected_failure": args.expect_tap_failure, "expected_outcome": expected,
     "model_requests": 0, "resource_smoke": False, "full_regression_run": False,
     "base_extension_unchanged": True, "preflight_sha256": sha(args.root / "preflight.json")})
shutil.copyfile(__file__, public / "stage.py")
(public / "SHA256SUMS").write_text("".join(sha(path) + "  " + path.name + "\n" for path in sorted(public.iterdir())))
print("Expected outcome: " + str(expected), flush=True)
raise SystemExit(0 if expected else 1)
