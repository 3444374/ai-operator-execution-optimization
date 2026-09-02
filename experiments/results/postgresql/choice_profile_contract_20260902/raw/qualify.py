"""Isolated choice-value qualification: no install, PG cluster, or model calls."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

p = argparse.ArgumentParser(description=__doc__)
for name in ("repo", "root", "prefix"):
    p.add_argument("--" + name, type=Path, required=True)
a = p.parse_args()
sys.path.insert(0, str(a.repo / "code"))
from src.baselines.common.redact import redact_text

public = a.root / "public"
public.mkdir()
steps = []

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def save(path, value):
    with path.open("x") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")

def scrub(value):
    value = redact_text(value)
    for path, label in ((a.repo, "<test-worktree>"), (a.prefix, "<pg18.3-prefix>"),
                        (a.root, "<artifact-root>"), (Path(sys.prefix), "<driver-env>")):
        value = value.replace(str(path), label)
    return "\n".join(line.rstrip() for line in value.splitlines()) + "\n"

def run(name, command, cwd=None):
    result = subprocess.run(command, cwd=cwd or a.repo, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    text = redact_text(result.stdout)
    (a.root / (name + ".log")).write_text(text)
    (public / (name + ".log")).write_text(scrub(text))
    steps.append(dict(name=name, argv=[scrub(s).strip() for s in command], exit_code=result.returncode,
                      raw_log_sha256=sha(a.root / (name + ".log"))))
    if result.returncode:
        save(public / "failed-steps.json", steps)
        raise SystemExit(f"{name} failed: {result.returncode}")
    print(name + ": passed", flush=True)
    return text

commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=a.repo, text=True).strip()
assert commit.startswith("d26e210d")
version = run("pg-version", [str(a.prefix / "bin/pg_config"), "--version"]).strip()
assert version == "PostgreSQL 18.3"
tests = {}
for name, directory, pattern in (("postgres", "postgres", "test*.py"), ("gateway", "execution_provider", "test*.py"),
                                 ("calibration", "planning", "test_semfilter_reference_calibration.py")):
    text = run("tests-" + name, [sys.executable, "-m", "unittest", "discover", "-s", "code/tests/" + directory,
                                "-p", pattern, "-v"])
    assert text.rstrip().endswith("OK")
    tests[name] = int(re.search(r"Ran (\d+) tests", text).group(1))
assert tests == dict(postgres=53, gateway=5, calibration=10)
run("c11-werror", ["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-pedantic", "-c",
                   "code/postgres/semloom_pg/src/generation_profile.c", "-o", str(a.root / "generation_profile.o")])
build = run("pg18.3-build", ["make", "-j4", "PG_CONFIG=" + str(a.prefix / "bin/pg_config"), "COPT=-O2 -Werror"],
            cwd=a.repo / "code/postgres/semloom_pg")
assert "-Werror" in build and "warning:" not in build
shutil.copy2(a.repo / "code/postgres/semloom_pg/semloom_pg.so", a.root / "semloom_pg.so")
sources = ["code/postgres/semloom_pg/src/" + name for name in ("ai_provider_port.h", "generation_profile.h", "generation_profile.c")]
sources += ["code/src/execution_provider/generation_profile.py", "code/tests/postgres/test_semloom_generation_profile.py"]
save(public / "qualification.json", dict(source_commit=commit, tests=tests, steps=steps,
    source_sha256={name:sha(a.repo/name) for name in sources}, standalone_c11_werror=True,
    pg_build_version=version, pg_build_warning_free=True, pg_build_only=True, postgres_regression_tap_run=False,
    installed_extension_modified=False, profile_linked_into_extension=False, model_requests_attempted=0,
    calibration_held_out_used=False, preflight_status=json.loads((a.root/"preflight.json").read_text())["status"],
    extension_build_sha256=sha(a.root/"semloom_pg.so")))
shutil.copy2(Path(__file__), public / "qualify.py")
with (public/"SHA256SUMS").open("x") as handle:
    for path in sorted(public.iterdir()):
        if path.name != "SHA256SUMS":
            handle.write(f"{sha(path)}  {path.name}\n")
print(json.dumps(dict(source_commit=commit, tests=tests, model_requests=0)))
