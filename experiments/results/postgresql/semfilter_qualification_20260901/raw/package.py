"""Seal a stopped qualification attempt and publish only a redacted evidence subset."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

p=argparse.ArgumentParser(description=__doc__)
for name in ("root","repo","worktree","pg_prefix","model"):
    p.add_argument("--"+name.replace("_","-"),type=Path,required=True)
a=p.parse_args()
sys.path.insert(0,str(a.repo / "code"))
from src.baselines.common.redact import redact_text

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def save(path,value):
    with path.open("x") as f:
        json.dump(value,f,sort_keys=True,indent=2)
        f.write("\n")

def scrub(text):
    for path,label in ((a.root,"<artifact-root>"),(a.worktree,"<test-worktree>"),
                       (a.repo,"<repository>"),(a.pg_prefix,"<pg18.3-prefix>"),
                       (a.model,"<model-dir>"),(Path(sys.prefix),"<driver-env>")):
        text=text.replace(str(path),label)
    return "\n".join(line.rstrip() for line in redact_text(text).splitlines()).rstrip()+"\n"

root=a.root
service=json.loads((root / "service-identity.json").read_text())
assert not Path(f"/proc/{service['sidecar']['pid']}").exists()
assert not (root / "pgdata/postmaster.pid").exists()
assert not Path("/tmp/semqual-pg-20260901/.s.PGSQL.55439").exists()
assert not subprocess.check_output(["nvidia-smi","--query-compute-apps=pid","--format=csv,noheader"],text=True).strip()
assert (root / "regression-actual.out").read_bytes() == (root / "regression-expected.out").read_bytes()
assert sha(root / "semloom_pg.so") == sha(a.pg_prefix / "lib/semloom_pg.so")
tests={}
for key in ("postgres","gateway","calibration"):
    text=(root / f"python-{key}.log").read_text()
    assert text.rstrip().endswith("OK")
    tests[key]=int(re.search(r"Ran (\d+) tests",text).group(1))
assert tests==dict(postgres=45,gateway=5,calibration=9)
assert "Tests=437" in (root / "installcheck.log").read_text()
assert "Result: PASS" in (root / "installcheck.log").read_text()
build=(root / "build.log").read_text()
assert "-Werror" in build and "warning:" not in build
save(root / "cleanup.json",dict(model_endpoint_stopped=True,statistics_pg18_3_stopped=True,
    tap_nodes_stopped=True,observed_gpu_compute_processes=0,scope="this qualification attempt only",
    old_calibration_bundle_unchanged=True))
public=root / "public"
public.mkdir()
for name in ("reference-summary.json","responses.jsonl","parser-controls.json","plans.json","cases.json",
             "schedule.json","stats.json","rank-repro.json","model-files.json","cleanup.json"):
    shutil.copy2(root / name,public / name)
save(public / "qualification.json",dict(source_commit=subprocess.check_output(["git","rev-parse","HEAD"],cwd=a.repo,text=True).strip(),
    postgresql_version="18.3",regression_passed=1,tap_passed=437,python_tests=tests,
    warning_free_werror_build=True,regression_original_sha256=sha(root / "regression-actual.out"),
    tested_binary_matches_clean_build=True,extension_sha256=sha(root / "semloom_pg.so"),
    preflight_status=json.loads((root / "preflight.json").read_text())["status"],
    full_calibration_resumed=False,production_generation_unchanged=True))
argv=["<model-dir>" if arg==str(a.model) else "<vllm-python>" if i==0 else arg for i,arg in enumerate(service["argv"])]
save(public / "service.json",dict(vllm_version=service["sidecar"]["package_version"],argv=argv,
    cuda_visible_devices_verified="0",raw_identity_sha256=sha(root / "service-identity.json"),
    identity_checked_before_after=True,model_files_checked_before_after=True))
logs=public / "logs"
logs.mkdir()
for name in ("build.log","installcheck.log","python-postgres.log","python-gateway.log","python-calibration.log",
             "stats.log","reference.log","rank-repro.log","pg-stop.log"):
    (logs / name).write_text(scrub((root / name).read_text()))
for path in (root / "tap_tmp_check/log").iterdir():
    (logs / (path.name+".txt")).write_text(scrub(path.read_text()))
for name in ("reference.py","stats.py","rank_repro.py","package.py"):
    shutil.copy2(Path(__file__).with_name(name),public / name)
def manifest(directory):
    rows=[]
    for path in sorted(directory.rglob("*")):
        if path.is_file() and path!=directory / "SHA256SUMS":
            rows.append(f"{sha(path)}  {path.relative_to(directory)}")
    (directory / "SHA256SUMS").write_text("\n".join(rows)+"\n")
    return len(rows)
print(json.dumps(dict(public_files=manifest(public),private_files=manifest(root))))
