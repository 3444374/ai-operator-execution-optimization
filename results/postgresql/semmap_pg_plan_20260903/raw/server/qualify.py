"""Qualify the Map PG plan slice and old paths using an isolated PG18.3 prefix."""
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
for name in ("repo", "root", "base-prefix", "env-file", "pg-source"):
    parser.add_argument("--" + name, type=Path, required=True)
parser.add_argument("--commit", required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo / "code"))
from src.baselines.common.redact import redact_text, redact_argument_list

prefix = args.root / "pg18.3"
public = args.root / "public"
public.mkdir()
steps = []
env = dict(os.environ, PYTHONPATH=str(args.repo / "code"),
           AI_OPERATOR_ENV_FILE=str(args.env_file),
           PATH=str(prefix / "bin") + ":/usr/sbin:/usr/bin:/sbin:/bin")
extension = args.repo / "code/postgres/semloom_pg"
as_pg = ["runuser", "-u", "postgres", "--"]
git = ["git", "-c", "safe.directory=" + str(args.repo)]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def scrub(text):
    text = redact_text(text)
    for path, label in ((args.env_file, "<runtime-env>"), (args.repo, "<test-worktree>"),
                        (prefix, "<pg18.3-prefix>"), (args.base_prefix, "<base-pg18.3-prefix>"),
                        (args.pg_source, "<pg18.3-source>"), (args.root, "<artifact-root>"),
                        (Path(sys.prefix), "<driver-env>")):
        text = text.replace(str(path), label)
    text = re.sub(r"/root/[^\s'\"]+", "<private-runtime-path>", text)
    return "\n".join(line.rstrip() for line in text.splitlines()).rstrip() + "\n"


def save(name, value):
    (public / name).write_text(scrub(json.dumps(value, indent=2)))


def run(name, command, cwd=None, input=None, expected=0):
    result = subprocess.run(command, cwd=cwd or args.repo, env=env, input=input,
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            timeout=1800)
    output = scrub(result.stdout)
    (public / (name + ".log")).write_text(output)
    steps.append(dict(name=name, argv=[scrub(str(s)).strip() for s in redact_argument_list(command)],
                      exit_code=result.returncode, expected_exit_code=expected))
    save("steps.json", steps)
    if result.returncode != expected:
        raise RuntimeError(f"{name} failed: {result.returncode}")
    print(name + ": passed", flush=True)
    return output


commit = subprocess.check_output(git + ["rev-parse", "HEAD"], cwd=args.repo, text=True).strip()
assert commit == args.commit
assert not subprocess.check_output(git + ["status", "--porcelain"], cwd=args.repo, text=True).strip()
assert args.env_file.is_file() and not prefix.exists()
pg_upstream = subprocess.check_output(["git", "-C", str(args.pg_source), "rev-parse", "HEAD"], text=True).strip()
assert pg_upstream == "62d6c7d3df6287f1bd83199c1a746e50d31571a0"
assert subprocess.check_output([str(args.base_prefix / "bin/pg_config"), "--version"], text=True).strip() == "PostgreSQL 18.3"
base_extension = args.base_prefix / "lib/semloom_pg.so"
base_sha = sha(base_extension) if base_extension.exists() else None
run("source-preflight", [sys.executable, "code/scripts/environment/manage_environment.py", "check",
                        "--groups", "core", "--json-out", str(args.root / "source-preflight.json")])
assert json.loads((args.root / "source-preflight.json").read_text())["status"] == "ok"
shutil.copytree(args.base_prefix, prefix)
version = run("pg-version", [str(prefix / "bin/pg_config"), "--version"]).strip()
assert version == "PostgreSQL 18.3"
for option, expected in (("--bindir", prefix / "bin"), ("--pkglibdir", prefix / "lib")):
    assert subprocess.check_output([str(prefix / "bin/pg_config"), option], text=True).strip() == str(expected)
for cwd, label in ((extension, "extension"), (extension / "t/plan_contract", "plan-caller")):
    run(label + "-clean", ["make", "PG_CONFIG=" + str(prefix / "bin/pg_config"), "clean"], cwd=cwd)
build = run("build", ["make", "PG_CONFIG=" + str(prefix / "bin/pg_config"),
                      "COPT=-O2 -Werror", "-j8"], cwd=extension)
assert "-Werror" in build and "warning:" not in build and "error:" not in build
run("install", ["make", "PG_CONFIG=" + str(prefix / "bin/pg_config"), "install"], cwd=extension)
shutil.copy2(extension / "semloom_pg.so", args.root / "qualified-semloom_pg.so")
assert sha(extension / "semloom_pg.so") == sha(prefix / "lib/semloom_pg.so")
run("test-owner", ["chown", "-R", "postgres:postgres", str(args.repo)])
try:
    tap = run("tap", as_pg + ["env", "PATH=" + env["PATH"], "PG_TEST_NOCLEAN=1", "make",
              "PG_CONFIG=" + str(prefix / "bin/pg_config"), "installcheck", "REGRESS="], cwd=extension)
finally:
    for pidfile in (extension / "tmp_check").rglob("postmaster.pid"):
        run("tap-cleanup-" + pidfile.parent.name, as_pg + [str(prefix / "bin/pg_ctl"), "-D",
            str(pidfile.parent), "-m", "fast", "-w", "stop"])
    for path in sorted((extension / "tmp_check/log").glob("*")):
        if path.is_file():
            (public / ("tap-" + path.name)).write_text(scrub(path.read_text()))
assert "All tests successful." in tap and "Result: PASS" in tap
tap_files, tap_count = map(int, re.search(r"Files=(\d+), Tests=(\d+)", tap).groups())

tests = {}
for name, directory, pattern in (("postgres", "postgres", "test*.py"),
                                 ("gateway", "execution_provider", "test*.py"),
                                 ("calibration", "planning", "test_semfilter_reference_calibration.py"),
                                 ("choice-tools", "experiments", "test_choice*.py")):
    output = run("tests-" + name, [sys.executable, "-m", "unittest", "discover", "-s",
                 "code/tests/" + directory, "-p", pattern, "-v"])
    assert output.rstrip().endswith("OK")
    tests[name] = int(re.search(r"Ran (\d+) tests", output).group(1))
for source in ("generation_profile", "sem_operator_machine", "sem_filter_machine", "sem_map_machine", "sem_message_writer", "sem_text", "semantic_map_contract"):
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
pg_ctl = as_pg + [str(prefix / "bin/pg_ctl"), "-D", str(pgdata)]
run("reg-initdb", as_pg + [str(prefix / "bin/initdb"), "-D", str(pgdata), "--no-locale", "-E", "UTF8"])
try:
    run("reg-start", pg_ctl + ["-l", str(reg / "server.log"), "-o",
        f"-c listen_addresses='' -c shared_preload_libraries=semloom_pg -k {socket} -p 55446", "-w", "start"])
    env.update(PGHOST=str(socket), PGPORT="55446", PGUSER="postgres")
    server_version = run("reg-server-version", as_pg + [str(prefix / "bin/psql"),
        "-h", str(socket), "-p", "55446", "-d", "postgres", "-Atc", "SHOW server_version;"]).strip()
    assert server_version == "18.3"
    run("regression", as_pg + ["make", "PG_CONFIG=" + str(prefix / "bin/pg_config"),
        "TAP_TESTS=", "installcheck"], cwd=extension)
finally:
    if (pgdata / "postmaster.pid").exists():
        run("reg-stop", pg_ctl + ["-m", "fast", "-w", "stop"])
actual = extension / "results/semloom_pg.out"
expected = extension / "expected/semloom_pg.out"
assert actual.read_bytes() == expected.read_bytes()
(public / "regression-actual.out").write_text(scrub(actual.read_text()))
(public / "regression-server.log").write_text(scrub((reg / "server.log").read_text()))
assert (sha(base_extension) if base_extension.exists() else None) == base_sha
assert not subprocess.check_output(git + ["status", "--porcelain"], cwd=args.repo, text=True).strip()
sources = subprocess.check_output(git + ["ls-files", "code/src/execution_provider", "code/tests/postgres",
    "code/tests/execution_provider", "code/postgres/semloom_pg"], cwd=args.repo, text=True).splitlines()
save("qualification.json", dict(source_commit=commit, tests=tests, steps=steps,
    source_sha256={name: sha(args.repo/name) for name in sources}, preflight_status="ok",
    preflight_sha256=sha(args.root / "source-preflight.json"), pg_upstream_commit=pg_upstream,
    pg_build_version=version, pg_build_warning_free=True, pg_runtime_version=server_version,
    tap_files=tap_files, tap_tests=tap_count, regression_tests=1,
    regression_actual_sha256=sha(actual), regression_expected_sha256=sha(expected),
    extension_sha256=sha(extension / "semloom_pg.so"), base_extension_unchanged=True,
    real_model_requests_attempted=0, calibration_held_out_used=False,
    isolated_prefix_only=True, resource_smoke_run=False, generated_map_execution_connected=False))
shutil.copy2(Path(__file__), public / "qualify.py")
with (public / "SHA256SUMS").open("x") as handle:
    for path in sorted(public.iterdir()):
        if path.name != "SHA256SUMS":
            handle.write(f"{sha(path)}  {path.name}\n")
print(json.dumps(dict(source_commit=commit, tests=tests, tap=tap_count, regression=1)), flush=True)
