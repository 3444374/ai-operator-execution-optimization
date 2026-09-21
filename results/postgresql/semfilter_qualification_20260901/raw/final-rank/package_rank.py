"""Preserve final numeric hardening evidence separately from model observations."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys

p=argparse.ArgumentParser(description=__doc__)
for name in ("root","repo","worktree","pg_prefix"):
    p.add_argument("--"+name.replace("_","-"),type=Path,required=True)
a=p.parse_args()
sys.path.insert(0,str(a.repo / "code"))
from src.baselines.common.redact import redact_text
r=a.root
extension=a.worktree / "code/postgres/semloom_pg"
assert not (r / "pgdata/postmaster.pid").exists()
assert not Path("/tmp/semrank-pg-20260901/.s.PGSQL.55439").exists()
shutil.copytree(extension / "tmp_check",r / "tap_tmp_check")
for source,target in ((extension / "results/semloom_pg.out","actual.out"),
                      (extension / "expected/semloom_pg.out","expected.out"),
                      (extension / "semloom_pg.so","semloom_pg.so")):
    shutil.copy2(source,r / target)
assert (r / "actual.out").read_bytes()==(r / "expected.out").read_bytes()
assert (r / "semloom_pg.so").read_bytes()==(a.pg_prefix / "lib/semloom_pg.so").read_bytes()
counts={}
for key in ("calibration","postgres-python","gateway"):
    content=(r / (key+".log")).read_text()
    assert content.rstrip().endswith("OK")
    counts[key]=int(re.search(r"Ran (\d+) tests",content).group(1))
assert counts=={"calibration":10,"postgres-python":45,"gateway":5}
assert "Tests=437" in (r / "installcheck.log").read_text() and "Result: PASS" in (r / "installcheck.log").read_text()
assert "-Werror" in (r / "build.log").read_text() and "warning:" not in (r / "build.log").read_text()
public=r / "public"
public.mkdir()
logs=public / "logs"
logs.mkdir()
def scrub(text):
    for path,label in ((r,"<artifact-root>"),(a.worktree,"<test-worktree>"),
                       (a.repo,"<repository>"),(a.pg_prefix,"<pg18.3-prefix>"),(Path(sys.prefix),"<driver-env>")):
        text=text.replace(str(path),label)
    return "\n".join(line.rstrip() for line in redact_text(text).splitlines()).rstrip()+"\n"
for name in ("calibration.log","postgres-python.log","gateway.log","build.log","installcheck.log","condition-repro.log","pg-stop.log"):
    (logs / name).write_text(scrub((r / name).read_text()))
for path in (r / "tap_tmp_check/log").iterdir():
    (logs / (path.name+".txt")).write_text(scrub(path.read_text()))
for name in ("condition_repro.py","package_rank.py"):
    shutil.copy2(Path(__file__).with_name(name),public / name)
shutil.copy2(r / "condition-repro.json",public / "condition-repro.json")
with (public / "qualification.json").open("x") as f:
    json.dump(dict(source_commit="44f6632c",postgresql_version="18.3",python_tests=counts,
        regression=1,tap=437,warning_free_werror=True,installed_binary_matches=True,
        regression_original_sha256=hashlib.sha256((r / "actual.out").read_bytes()).hexdigest(),
        model_calls=0,held_out_used=False,pg_stopped=True),f,indent=2,sort_keys=True)
    f.write("\n")
def manifest(directory):
    rows=[f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(directory)}"
          for path in sorted(directory.rglob("*")) if path.is_file() and path!=directory / "SHA256SUMS"]
    (directory / "SHA256SUMS").write_text("\n".join(rows)+"\n")
    return len(rows)
print(json.dumps(dict(public_files=manifest(public),private_files=manifest(r))))
