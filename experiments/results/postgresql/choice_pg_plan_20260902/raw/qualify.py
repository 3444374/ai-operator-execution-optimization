"""Archive choice PG-plan qualification; isolated install, no real model calls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
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
env = dict(os.environ, PYTHONPATH=str(a.repo / "code"),
           PATH=str(a.prefix / "bin") + ":/usr/sbin:/usr/bin:/sbin:/bin")
extension = a.repo / "code/postgres/semloom_pg"

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def scrub(text):
    text = redact_text(text)
    for path, label in ((a.repo, "<test-worktree>"), (a.prefix, "<pg18.3-prefix>"),
                        (a.root, "<artifact-root>"), (Path(sys.prefix), "<driver-env>")):
        text = text.replace(str(path), label)
    text = re.sub(r"/root/[^\s'\"]+", "<private-runtime-path>", text)
    return "\n".join(line.rstrip() for line in text.splitlines()) + "\n"

def save(name, value):
    with (public / name).open("x") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")

def run(name, command, cwd=None, input=None):
    result = subprocess.run(command, cwd=cwd or a.repo, env=env, input=input,
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = scrub(result.stdout)
    (public / (name + ".log")).write_text(output)
    steps.append(dict(name=name, argv=[scrub(str(s)).strip() for s in command],
                      exit_code=result.returncode))
    if result.returncode:
        save("failed-steps.json", steps)
        raise RuntimeError(f"{name} failed: {result.returncode}")
    print(name + ": passed", flush=True)
    return output

git = ["git", "-c", "safe.directory=" + str(a.repo)]
commit = subprocess.check_output(git + ["rev-parse", "HEAD"], cwd=a.repo, text=True).strip()
assert commit.startswith("134447dd")
assert not subprocess.check_output(git + ["status", "--porcelain"], cwd=a.repo, text=True).strip()
version = run("pg-version", [str(a.prefix / "bin/pg_config"), "--version"]).strip()
assert version == "PostgreSQL 18.3"
assert json.loads((a.root / "preflight.json").read_text())["status"] == "ok"

# These commands already completed for this exact clean commit; preserve raw output.
for name in ("red-build", "red-tap", "green1-build", "slice2-red-tap", "final-build", "final-install", "final-tap", "tap-before-sentinel-fix", "qualification-driver-attempt1"):
    (public / (name + ".log")).write_text(scrub((a.root / (name + ".log")).read_text()))
build = (public / "final-build.log").read_text()
tap = (public / "final-tap.log").read_text()
assert "-O2" in build and "-Werror" in build and "warning:" not in build and "error:" not in build
assert "All tests successful." in tap and "Result: PASS" in tap
tap_count = int(re.search(r"Files=2, Tests=(\d+)", tap).group(1))
shutil.copy2(extension / "semloom_pg.so", a.root / "qualified-semloom_pg.so")
assert sha(extension / "semloom_pg.so") == sha(a.prefix / "lib/semloom_pg.so")
for path in sorted((extension / "tmp_check/log").glob("*.log")):
    (public / ("tap-" + path.name)).write_text(scrub(path.read_text()))
for path in sorted((a.root / "slice2-red-pg-logs").glob("*.log")):
    (public / ("red-" + path.name)).write_text(scrub(path.read_text()))

tests = {}
for name, directory, pattern in (("postgres", "postgres", "test*.py"),
                                 ("gateway", "execution_provider", "test*.py"),
                                 ("calibration", "planning", "test_semfilter_reference_calibration.py")):
    output = run("tests-" + name, [sys.executable, "-m", "unittest", "discover", "-s",
                 "code/tests/" + directory, "-p", pattern, "-v"])
    assert output.rstrip().endswith("OK")
    tests[name] = int(re.search(r"Ran (\d+) tests", output).group(1))
for source in ("generation_profile", "sem_operator_machine", "sem_filter_machine", "sem_map_machine"):
    run("c11-" + source, ["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-pedantic",
        "-c", "code/postgres/semloom_pg/src/" + source + ".c", "-o", str(a.root / (source + ".o"))])
run("c11-neutral-header", ["cc", "-std=c11", "-Wall", "-Wextra", "-Werror", "-pedantic",
    "-Icode/postgres/semloom_pg/src", "-x", "c", "-fsyntax-only", "-"],
    input='#include "ai_provider_port.h"\nint main(void) { return 0; }\n')

pg_user = pwd.getpwnam("postgres")
reg = a.root / "regression"
reg.mkdir(mode=0o700)
os.chown(reg, pg_user.pw_uid, pg_user.pw_gid)
socket = reg / "socket"
socket.mkdir()
os.chown(socket, pg_user.pw_uid, pg_user.pw_gid)
pgdata = reg / "data"
as_pg = ["runuser", "-u", "postgres", "--"]
pg_ctl = as_pg + [str(a.prefix / "bin/pg_ctl"), "-D", str(pgdata)]
run("reg-initdb", as_pg + [str(a.prefix / "bin/initdb"), "-D", str(pgdata), "--no-locale", "-E", "UTF8"])
try:
    run("reg-start", pg_ctl + ["-l", str(reg / "server.log"), "-o",
        f"-c listen_addresses='' -c shared_preload_libraries=semloom_pg -k {socket} -p 55439", "-w", "start"])
    env.update(PGHOST=str(socket), PGPORT="55439", PGUSER="postgres")
    server_version = run("reg-server-version", as_pg + [str(a.prefix / "bin/psql"),
        "-h", str(socket), "-p", "55439", "-d", "postgres", "-Atc", "SHOW server_version;"]).strip()
    assert server_version == "18.3"
    run("regression", as_pg + ["make", "PG_CONFIG=" + str(a.prefix / "bin/pg_config"),
        "TAP_TESTS=", "installcheck"], cwd=extension)
finally:
    run("reg-stop", pg_ctl + ["-m", "fast", "-w", "stop"])
actual = extension / "results/semloom_pg.out"
expected = extension / "expected/semloom_pg.out"
assert actual.read_bytes() == expected.read_bytes()
(public / "regression-actual.out").write_text(scrub(actual.read_text()))
(public / "regression-server.log").write_text(scrub((reg / "server.log").read_text()))
sources = subprocess.check_output(git + ["diff", "--name-only", "655331d3..HEAD"], cwd=a.repo, text=True).splitlines()
save("qualification.json", dict(source_commit=commit, tests=tests, steps=steps,
    source_sha256={name:sha(a.repo/name) for name in sources},
    preflight_status="ok", pg_build_version=version, pg_build_warning_free=True,
    pg_runtime_version=server_version, tap_tests=tap_count, regression_tests=1,
    regression_actual_sha256=sha(actual), regression_expected_sha256=sha(expected),
    extension_sha256=sha(extension / "semloom_pg.so"),
    model_requests_attempted=0, calibration_held_out_used=False,
    wire_v4_implemented=False, isolated_prefix_only=True,
    archived_build_and_tap="commands completed before this collector; full output retained"))
shutil.copy2(Path(__file__), public / "qualify.py")
with (public / "SHA256SUMS").open("x") as handle:
    for path in sorted(public.iterdir()):
        if path.name != "SHA256SUMS":
            handle.write(f"{sha(path)}  {path.name}\n")
print(json.dumps(dict(source_commit=commit, tests=tests, tap=tap_count, regression=1)))
