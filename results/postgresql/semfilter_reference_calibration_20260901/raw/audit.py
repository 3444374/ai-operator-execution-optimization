"""Seal the failed attempt without replaying any model request or fitting data."""
import argparse
from collections import Counter
import importlib.util
import json
from pathlib import Path
import shutil
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--root", type=Path, required=True)
parser.add_argument("--repo", type=Path, required=True)
parser.add_argument("--model", type=Path, required=True)
parser.add_argument("--raw", type=Path, required=True)
parser.add_argument("--socket", type=Path, required=True)
parser.add_argument("--identity", type=Path, required=True)
parser.add_argument("--extension", type=Path, required=True)
parser.add_argument("--endpoint", default="http://127.0.0.1:8013")
args = parser.parse_args()
spec = importlib.util.spec_from_file_location("collector", Path(__file__).with_name("collect.py"))
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
sys.path.insert(0, str(args.repo / "code"))
import psycopg
from src.baselines.common.redact import redact_text
import os

root = args.root
before = json.loads((root / "service_identity.json").read_text())
after = c.service_identity(args)
assert before == after, "service drift"
assert c.model_files(args.model) == before["model_files"], "model drift"
workload = json.loads((root / "workload_manifest.json").read_text())
assert c.sha(args.raw) == workload["raw_sha256"], "source drift"
assert not args.socket.exists(), "gateway socket remains after SQL failure"
rows = json.loads((root / "private_rows.json").read_text())
with psycopg.connect(os.environ["SEMLOOM_CAL_DSN"], autocommit=True) as connection:
    version = connection.execute("SELECT version()").fetchone()[0]
    actual = connection.execute("SELECT doc_id,split,cell,payload FROM calibration_inputs ORDER BY doc_id").fetchall()
    assert actual == [(r["doc_id"], r["split"], r["cell"], r["payload"]) for r in rows]
    assert connection.execute("SELECT 1").fetchone()[0] == 1
    recovery = dict(version=version, imported_rows=len(actual), exact_readback=True,
                    fresh_connection_select_1=True, gateway_socket_absent=True)
c.save(root / "post_failure_audit.json", dict(service_identity_unchanged=True,
       model_files_unchanged=True, source_unchanged=True, recovery=recovery))

public = root / "public"
public.mkdir()
summaries = []
for d in sorted((root / "runs").iterdir()):
    tasks = [json.loads(line) for line in (d / "tasks.jsonl").read_text().splitlines()]
    split = d.name.split("-c")[0]
    cell = int(d.name.split("-c")[1].split("-r")[0])
    expected = {r["payload_sha256"] for r in rows if r["split"] == split and r["cell"] == cell}
    assert len({t["input_sha256"] for t in tasks}) == len(tasks)
    assert all(t["input_sha256"] in expected for t in tasks)
    counts = Counter(t["raw_output"] or "INVALID" for t in tasks)
    observation = json.loads((d / "observation.json").read_text()) if (d / "observation.json").exists() else None
    summary = dict(run_id=d.name, planned_input_rows=len(expected), observed_model_responses=len(tasks),
                   raw_output_counts=dict(counts), complete_query=observation is not None,
                   observation=observation, partial_prompt_tokens=sum(t["prompt_tokens"] for t in tasks),
                   partial_output_tokens=sum(t["output_tokens"] for t in tasks),
                   partial_request_wall_ms=sum(t["duration_ns"] for t in tasks)/1e6)
    summaries.append(summary)
    dest = public / "runs" / d.name
    dest.mkdir(parents=True)
    for name in ("tasks.jsonl", "observation.json", "explain.json"):
        if (d / name).exists():
            shutil.copy2(d / name, dest / name)
failure = json.loads((root / "collect-failure.json").read_text())
assert failure["sqlstate"] == "22000"
assert not (root / "source.json").exists() and not (root / "reference-calibration.json").exists()
report = dict(status="rejected_before_fit", reason="strict_model_output_contract_failed",
    sqlstate=failure["sqlstate"], sql_message="SemFilter model completion must be TRUE, FALSE, or UNKNOWN",
    complete_training_observations=0, complete_held_out_observations=0,
    training_rank=None, service_coefficients=None, held_out_errors=None,
    held_out_status="not_run_due_to_training_query_failure", artifact_published=False,
    fitted=False, tuned_after_observation=False, accepted_max_relative_error=0.20,
    planner_artifact_load_status="not_run_no_qualified_artifact", runs=summaries)
c.save(root / "held_out_report.json", report)
c.save(public / "held_out_report.json", report)
for name in ("model_files.json", "schedule.json", "post_failure_audit.json", "provenance.json", "collection_provenance.json"):
    shutil.copy2(root / name, public / name)
public_workload = {**workload, "rows": [{**{k:v for k,v in r.items() if k != "conversation_id"},
    "conversation_id_sha256": c.identity(r["conversation_id"])} for r in workload["rows"]]}
c.save(public / "workload_manifest.json", public_workload)
argv = ["<model-dir>" if arg == str(args.model) else "<vllm-python>" if arg == before["argv"][0] else arg
        for arg in before["argv"]]
c.save(public / "execution_identity.json", dict(semantic_instruction=c.INSTRUCTION, model=c.MODEL,
    service_argv=argv, vllm_version=before["package_version"], gpu_driver=before["gpu"],
    extension_sha256=before["extension_sha256"], adapter_sha256=before["adapter_sha256"],
    original_service_signature=c.identity(before), original_workload_signature=c.identity(workload),
    identity_verification="collector checked actual PID/start/config, model/source bytes before and after",
    request_timing="monotonic around unchanged fixed adapter complete(); excludes PG/UDS/observer writes",
    calibrated_cost_available=False))
failure_text = redact_text(failure["traceback"]).replace(str(root), "<artifact-root>").replace(str(args.repo), "<source-tree>")
failure_text = failure_text.replace(sys.prefix, "<driver-env>")
c.save(public / "collect-failure.json", {**failure, "traceback": failure_text})
for name in ("collect.py", "collect-preparation.py", "test_collect.py", "audit.py"):
    shutil.copy2(Path(__file__).with_name(name), public / name)
print(json.dumps(report, indent=2))
