"""Qualify Filter INSERT with isolated PG18.3 and synthetic provider fixtures."""
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

parser = argparse.ArgumentParser(description=__doc__)
for name in ("repo", "root", "prefix", "env-file"):
    parser.add_argument("--" + name, type=Path, required=True)
parser.add_argument("--commit", required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo / "code"))
from src.baselines.common.redact import redact_text

public = args.root / "public"
public.mkdir()
steps = []
env = dict(os.environ, PYTHONPATH=str(args.repo / "code"),
           AI_OPERATOR_ENV_FILE=str(args.env_file),
           PATH=str(args.prefix / "bin") + ":/usr/sbin:/usr/bin:/sbin:/bin")
extension = args.repo / "code/postgres/semloom_pg"
as_pg = ["runuser", "-u", "postgres", "--"]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def scrub(text):
    text = redact_text(text)
    for path, label in ((args.env_file, "<runtime-env>"), (args.repo, "<test-worktree>"),
                        (args.prefix, "<pg18.3-prefix>"), (args.root, "<artifact-root>"),
                        (Path(sys.prefix), "<driver-env>")):
        text = text.replace(str(path), label)
    text = re.sub(r"/root/[^\s'\"]+", "<private-runtime-path>", text)
    return "\n".join(line.rstrip() for line in text.splitlines()).rstrip() + "\n"


def save(name, value):
    with (public / name).open("x") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")


def run(name, command, cwd=None, input=None):
    result = subprocess.run(command, cwd=cwd or args.repo, env=env, input=input,
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (args.root / (name + ".log")).write_text(result.stdout)
    output = scrub(result.stdout)
    (public / (name + ".log")).write_text(output)
    steps.append(dict(name=name, argv=[scrub(str(s)).strip() for s in command],
                      exit_code=result.returncode))
    if result.returncode:
        save("failed-steps.json", steps)
        raise RuntimeError(f"{name} failed: {result.returncode}")
    print(name + ": passed", flush=True)
    return output


git = ["git", "-c", "safe.directory=" + str(args.repo)]
commit = subprocess.check_output(git + ["rev-parse", "HEAD"], cwd=args.repo, text=True).strip()
assert commit == args.commit
assert not subprocess.check_output(git + ["status", "--porcelain"], cwd=args.repo, text=True).strip()
assert args.env_file.is_file()
version = run("pg-version", [str(args.prefix / "bin/pg_config"), "--version"]).strip()
assert version == "PostgreSQL 18.3"
run("source-preflight", [sys.executable, "code/scripts/environment/manage_environment.py", "check",
                        "--groups", "core", "--json-out", str(args.root / "source-preflight.json")])
assert json.loads((args.root / "source-preflight.json").read_text())["status"] == "ok"
run("clean", ["make", "PG_CONFIG=" + str(args.prefix / "bin/pg_config"), "clean"], cwd=extension)
run("plan-caller-clean", ["make", "PG_CONFIG=" + str(args.prefix / "bin/pg_config"),
                         "clean"], cwd=extension / "t/plan_contract")
build = run("build", ["make", "PG_CONFIG=" + str(args.prefix / "bin/pg_config"),
                      "COPT=-O2 -Werror", "-j8"], cwd=extension)
assert "-Werror" in build and "warning:" not in build and "error:" not in build
run("install", ["make", "PG_CONFIG=" + str(args.prefix / "bin/pg_config"), "install"], cwd=extension)
shutil.copy2(extension / "semloom_pg.so", args.root / "qualified-semloom_pg.so")
assert sha(extension / "semloom_pg.so") == sha(args.prefix / "lib/semloom_pg.so")
shutil.chown(args.root, user="postgres", group="postgres")
run("test-owner", ["chown", "-R", "postgres:postgres", str(args.repo)])
tap = run("tap", as_pg + ["env", "PATH=" + env["PATH"], "PG_TEST_NOCLEAN=1", "make",
          "PG_CONFIG=" + str(args.prefix / "bin/pg_config"), "installcheck", "REGRESS="], cwd=extension)
assert "All tests successful." in tap and "Result: PASS" in tap
tap_count = int(re.search(r"Files=4, Tests=(\d+)", tap).group(1))
for path in sorted((extension / "tmp_check/log").glob("*.log")):
    (public / ("tap-" + path.name)).write_text(scrub(path.read_text()))

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
        "-c", "code/postgres/semloom_pg/src/" + source + ".c", "-o", str(args.root / (source + ".o"))])
run("c11-neutral-header", ["cc", "-std=c11", "-Wall", "-Wextra", "-Werror", "-pedantic",
    "-Icode/postgres/semloom_pg/src", "-x", "c", "-fsyntax-only", "-"],
    input='#include "ai_provider_port.h"\nint main(void) { return 0; }\n')

pg_user = pwd.getpwnam("postgres")
reg = args.root / "regression"
reg.mkdir(mode=0o700)
os.chown(reg, pg_user.pw_uid, pg_user.pw_gid)
socket = reg / "socket"
socket.mkdir()
os.chown(socket, pg_user.pw_uid, pg_user.pw_gid)
pgdata = reg / "data"
pg_ctl = as_pg + [str(args.prefix / "bin/pg_ctl"), "-D", str(pgdata)]
run("reg-initdb", as_pg + [str(args.prefix / "bin/initdb"), "-D", str(pgdata), "--no-locale", "-E", "UTF8"])
try:
    run("reg-start", pg_ctl + ["-l", str(reg / "server.log"), "-o",
        f"-c listen_addresses='' -c shared_preload_libraries=semloom_pg -k {socket} -p 55445", "-w", "start"])
    env.update(PGHOST=str(socket), PGPORT="55445", PGUSER="postgres")
    server_version = run("reg-server-version", as_pg + [str(args.prefix / "bin/psql"),
        "-h", str(socket), "-p", "55445", "-d", "postgres", "-Atc", "SHOW server_version;"]).strip()
    assert server_version == "18.3"
    run("regression", as_pg + ["make", "PG_CONFIG=" + str(args.prefix / "bin/pg_config"),
        "TAP_TESTS=", "installcheck"], cwd=extension)
finally:
    run("reg-stop", pg_ctl + ["-m", "fast", "-w", "stop"])
actual = extension / "results/semloom_pg.out"
expected = extension / "expected/semloom_pg.out"
assert actual.read_bytes() == expected.read_bytes()
(public / "regression-actual.out").write_text(scrub(actual.read_text()))
(public / "regression-server.log").write_text(scrub((reg / "server.log").read_text()))
sources = subprocess.check_output(git + ["ls-files", "code/src/execution_provider", "code/tests/postgres",
    "code/postgres/semloom_pg"], cwd=args.repo, text=True).splitlines()
save("qualification.json", dict(source_commit=commit, tests=tests, steps=steps,
    source_sha256={name:sha(args.repo/name) for name in sources}, preflight_status="ok",
    preflight_sha256=sha(args.root / "source-preflight.json"),
    pg_build_version=version, pg_build_warning_free=True, pg_runtime_version=server_version,
    tap_tests=tap_count, regression_tests=1,
    regression_actual_sha256=sha(actual), regression_expected_sha256=sha(expected),
    extension_sha256=sha(extension / "semloom_pg.so"),
    real_model_requests_attempted=0, calibration_held_out_used=False,
    gateway_wire_v4_implemented=True, pg_wire_v4_implemented=True, isolated_prefix_only=True,
    filter_insert_supported=True,
    filter_insert_profiles=["recording", "exact-v3", "choice-v4"],
    resource_smoke_run=False,
    excluded_attempts=["fix-tap used old efc24bb1 source after Git ownership rejection; rerun after verified fast-forward"]))
for name in ("red-build", "red-install", "red-tap", "fix-build", "fix-tap",
             "green-build", "green-tap", "behavior-tap", "coverage-tap"):
    (public / ("history-" + name + ".log")).write_text(scrub((args.root / (name + ".log")).read_text()))
shutil.copy2(Path(__file__), public / "qualify.py")
with (public / "SHA256SUMS").open("x") as handle:
    for path in sorted(public.iterdir()):
        if path.name != "SHA256SUMS":
            handle.write(f"{sha(path)}  {path.name}\n")
print(json.dumps(dict(source_commit=commit, tests=tests, tap=tap_count, regression=1)), flush=True)
