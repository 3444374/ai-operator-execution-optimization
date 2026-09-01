"""Public-builder differential witness, without writing a calibration artifact."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import types

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--repo",type=Path,required=True)
parser.add_argument("--output",type=Path,required=True)
args=parser.parse_args()
sys.path.insert(0,str(args.repo / "code"))
from src.planning.semfilter_reference_calibration import build_reference_calibration
from tests.planning.test_semfilter_reference_calibration import _source
source=_source()
source["training_observations"]=[dict(semantic_input_rows=c,output_rows=n,model_calls=c,
    prompt_tokens=p,output_tokens=o,service_milliseconds=t)
    for c,n,p,o,t in ((90,36,360,180,316),(60,24,1260,120,316),(100,40,1100,200,420),(15,6,270,30,82))]
source["held_out_observations"]=[dict(semantic_input_rows=265,output_rows=106,model_calls=265,
    prompt_tokens=2990,output_tokens=530,service_milliseconds=1104)]
old_source=subprocess.check_output(["git","show","c77c1441:code/src/planning/semfilter_reference_calibration.py"],cwd=args.repo)
old=types.ModuleType("baseline_calibration")
sys.modules[old.__name__]=old
exec(compile(old_source,"<baseline-calibration>","exec"),old.__dict__)
artifact=old.build_reference_calibration(source)
try:
    build_reference_calibration(source)
except ValueError as error:
    rejection=str(error)
else:
    raise AssertionError("fixed builder still accepts the rank-deficient witness")
assert artifact["held_out_max_relative_error"] == "0"
assert rejection == "service observations do not identify all cost coefficients"
report=dict(baseline_commit="c77c1441",fixed_commit=subprocess.check_output(["git","rev-parse","HEAD"],cwd=args.repo,text=True).strip(),
    baseline_source_sha256=hashlib.sha256(old_source).hexdigest(),source=source,
    baseline_accepted_coefficients={k:v for k,v in artifact.items() if k.startswith("service_ms") or k=="service_fixed_milliseconds"},
    baseline_held_out_max_relative_error=artifact["held_out_max_relative_error"],fixed_rejection=rejection,
    artifact_file_written=False)
with args.output.open("x") as handle:
    json.dump(report,handle,indent=2,sort_keys=True)
    handle.write("\n")
print("old public builder accepted exactly collinear design with zero held-out error; fixed builder rejected it")
