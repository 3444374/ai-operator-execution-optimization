"""Differential public-builder witness for jointly ill-conditioned features."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import types

p=argparse.ArgumentParser(description=__doc__)
p.add_argument("--repo",type=Path,required=True)
p.add_argument("--output",type=Path,required=True)
a=p.parse_args()
sys.path.insert(0,str(a.repo / "code"))
from tests.planning.test_semfilter_reference_calibration import _source
from src.planning.semfilter_reference_calibration import build_reference_calibration
s=_source()
s["training_observations"]=[dict(semantic_input_rows=2_000_000_000,output_rows=800_000_000,
    model_calls=c,prompt_tokens=p,output_tokens=o,service_milliseconds=1000+c+p/100+o/2)
    for c,p,o in ((10**9,10**9,10**9),(10**9+100,2*10**9,10**9),
                  (10**9,10**9+100,2*10**9),(10**9,10**9,10**9+100))]
h={k:sum(r[k] for r in s["training_observations"]) for k in s["training_observations"][0]}
h["service_milliseconds"]-=3000
s["held_out_observations"]=[h]
old=types.ModuleType("pivot_only_baseline")
sys.modules[old.__name__]=old
code=subprocess.check_output(["git","show","6c111b24:code/src/planning/semfilter_reference_calibration.py"],cwd=a.repo)
exec(compile(code,"<pivot-only-baseline>","exec"),old.__dict__)
artifact=old.build_reference_calibration(s)
try:
    build_reference_calibration(s)
except ValueError as error:
    assert str(error)=="service observations are nearly collinear"
else:
    raise AssertionError("joint near-dependence still accepted")
with a.output.open("x") as f:
    json.dump(dict(source=s,baseline_commit="6c111b24",fixed_commit="44f6632c",
        baseline_accepted=True,baseline_held_out_error=artifact["held_out_max_relative_error"],
        fixed_rejection="service observations are nearly collinear",artifact_file_written=False),f,indent=2,sort_keys=True)
    f.write("\n")
print("pivot-only baseline accepted joint near-dependence; final full-condition check rejected it")
