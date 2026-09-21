"""One-run observer for work-package-five's preregistered 2026-09-01 attempt.

Preserved with raw evidence, not a new production runtime. No request/result
rewrites: the observed gateway reuses server.main and the fixed adapter.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import sys
import time
import traceback
from urllib.request import urlopen

INSTRUCTION = "The input asks for writing, explaining, or debugging computer code."
MODEL = "Qwen2.5-1.5B-Instruct"
TRAIN_SIZES = (32, 48, 64, 80, 96, 112, 144, 192)
HOLDOUT_SIZES = (64, 80, 112, 128)
SEED = 20260901


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def save(path, value):
    with Path(path).open("x", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")


def model_files(model):
    return {str(p.relative_to(model)): sha(p) for p in sorted(model.iterdir())
            if p.is_file() and p.suffix in (".json", ".safetensors", ".model", ".txt")}


def select_rows(raw):
    candidates = []
    payloads, conversations = set(), set()
    for item in raw:
        cid = str(item["id"])
        turns = [t["value"] for t in item["conversations"] if t["from"] == "human"]
        if not turns or cid in conversations:
            continue
        payload = turns[0]
        if not payload.strip() or "\x00" in payload or len(payload.encode()) > 4096:
            continue
        digest = hashlib.sha256(payload.encode()).hexdigest()
        if digest in payloads:
            continue
        payloads.add(digest)
        conversations.add(cid)
        key = hashlib.sha256(f"semfilter-calibration-20260901:{cid}:{digest}".encode()).hexdigest()
        candidates.append((key, cid, digest, payload))
    candidates.sort()
    assert len(candidates) >= 1216, "insufficient distinct eligible source rows"
    rows, offset = [], 0
    for split, sizes in (("warmup", (64,)), ("training", TRAIN_SIZES), ("held_out", HOLDOUT_SIZES)):
        for cell, size in enumerate(sizes):
            for _, cid, digest, payload in candidates[offset:offset + size]:
                rows.append(dict(doc_id=len(rows), conversation_id=cid, payload_sha256=digest,
                                 payload=payload, split=split, cell=cell))
            offset += size
    assert len({r["conversation_id"] for r in rows}) == len(rows)
    assert len({r["payload_sha256"] for r in rows}) == len(rows)
    return rows


def prepare(args):
    rows = select_rows(json.loads(args.raw.read_text()))
    public = [{k: v for k, v in row.items() if k != "payload"} for row in rows]
    workload = dict(raw_sha256=sha(args.raw), source="ShareGPT Vicuna unfiltered",
                    selection="first human turn; nonempty; no NUL; <=4096 UTF8 bytes; unique conversation/payload",
                    seed=SEED, rows=public)
    save(args.root / "private_rows.json", rows)
    save(args.root / "workload_manifest.json", workload)
    save(args.root / "model_files.json", model_files(args.model))
    save(args.root / "provenance.json", dict(
        source_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.repo, text=True).strip(),
        observer_sha256=sha(Path(__file__)), preregistration_commit="7042132e",
        model_requests_before_preregistration=0))
    save(args.root / "fixed-model.json", dict(endpoint_url=args.endpoint + "/v1/chat/completions",
                                             model_id=MODEL, timeout_ms=60000))
    print(json.dumps(dict(rows=len(rows), workload_signature=identity(workload))))


def gateway(args):
    from src.execution_provider import server
    from src.execution_provider.adapters.openai_compatible_fixed import OpenAICompatibleFixedAdapter

    class ObservedAdapter(OpenAICompatibleFixedAdapter):
        def complete(self, request):
            before = time.monotonic_ns()
            try:
                result = super().complete(request)
            except Exception as error:
                elapsed = time.monotonic_ns() - before
                with args.trace.open("a") as handle:
                    handle.write(json.dumps(dict(error_code=getattr(error, "code", type(error).__name__),
                                                 duration_ns=elapsed)) + "\n")
                raise
            elapsed = time.monotonic_ns() - before
            raw = result.raw_output
            record = dict(payload_digest=request.semantic_payload_digest,
                          input_sha256=hashlib.sha256(request.canonical_messages[-1]["content"].encode()).hexdigest(),
                          model_id=result.response_model_id, prompt_tokens=result.prompt_tokens,
                          output_tokens=result.output_tokens, finish_reason=result.finish_reason,
                          raw_output=raw if raw in ("TRUE", "FALSE", "UNKNOWN") else None,
                          raw_output_sha256=hashlib.sha256(raw.encode()).hexdigest(),
                          raw_output_bytes=len(raw.encode()), duration_ns=elapsed)
            with args.trace.open("a") as handle:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
            return result

    server.OpenAICompatibleFixedAdapter = ObservedAdapter
    sys.argv = ["observed-gateway", "--socket", str(args.socket), "--once",
                "--fixed-model-config", str(args.root / "fixed-model.json")]
    return server.main()


def service_identity(args):
    sidecar = json.loads(args.identity.read_text())
    pid = sidecar["pid"]
    proc = Path(f"/proc/{pid}")
    start = proc.joinpath("stat").read_text().rsplit(")", 1)[1].split()[19]
    assert start == sidecar["process_start_time_ticks"], "service PID/start identity changed"
    argv = proc.joinpath("cmdline").read_bytes().decode().split("\0")[:-1]
    expected = {"--model": str(args.model), "--served-model-name": MODEL, "--dtype": "bfloat16",
                "--max-model-len": "4096", "--gpu-memory-utilization": "0.25",
                "--scheduling-policy": "fcfs", "--max-num-seqs": "1",
                "--max-num-batched-tokens": "4096", "--tensor-parallel-size": "1"}
    for flag, value in expected.items():
        assert argv.count(flag) == 1 and argv[argv.index(flag) + 1] == value, f"service flag drift: {flag}"
    assert "--enforce-eager" in argv and "--no-enable-prefix-caching" in argv
    env = dict(p.split(b"=", 1) for p in proc.joinpath("environ").read_bytes().split(b"\0") if b"=" in p)
    assert env.get(b"CUDA_VISIBLE_DEVICES") == b"0"
    with urlopen(args.endpoint + "/v1/models", timeout=5) as response:
        models = json.load(response)
    assert [m["id"] for m in models["data"]] == [MODEL]
    return dict(pid=pid, start_ticks=start, argv=argv, package_version=sidecar["package_version"],
                gpu=subprocess.check_output(["nvidia-smi", "--query-gpu=index,name,driver_version", "--format=csv,noheader"], text=True),
                model_files=json.loads((args.root / "model_files.json").read_text()),
                adapter_sha256=sha(args.repo / "code/src/execution_provider/adapters/openai_compatible_fixed.py"),
                extension_sha256=sha(args.extension))


def filter_node(plan):
    if plan.get("Custom Plan Provider") == "SemLoom SemFilter":
        return plan
    for child in plan.get("Plans", []):
        found = filter_node(child)
        if found is not None:
            return found
    return None


def execute_cell(args, connection, rows, split, cell, repeat, service):
    from psycopg import sql
    from src.execution_provider.wire.v3 import (SemanticFilterPlan, canonical_messages,
                                               semantic_spec_digest, semantic_payload_digest)
    assert service_identity(args) == service, "service identity drift before query"
    run_id = f"{split}-c{cell}-r{repeat}"
    run = args.root / "runs" / run_id
    run.mkdir()
    shutil.chown(run, user="postgres", group="postgres")
    os.chmod(run, 0o700)
    trace, sock = run / "tasks.jsonl", args.socket
    log = (run / "gateway.log").open("x")
    command = [sys.executable, str(Path(__file__).resolve()), "gateway", "--repo", str(args.repo),
               "--root", str(args.root), "--socket", str(sock), "--trace", str(trace)]
    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, user="postgres")
    subset = [r for r in rows if r["split"] == split and r["cell"] == cell]
    try:
        for _ in range(500):
            if sock.exists():
                break
            assert process.poll() is None, "observer gateway exited before ready"
            time.sleep(0.01)
        assert sock.exists(), "observer socket timeout"
        query = sql.SQL("EXPLAIN (ANALYZE, FORMAT JSON, TIMING ON) SELECT doc_id FROM calibration_inputs "
                        "WHERE split={} AND cell={} AND ai_semantic.filter(payload,{},{}::jsonb)").format(
                            sql.Literal(split), sql.Literal(cell), sql.Literal(INSTRUCTION),
                            sql.Literal(json.dumps(dict(model=MODEL, temperature=0, max_tokens=8))))
        result = connection.execute(query).fetchone()[0]
        save(run / "explain.json", result)
    finally:
        try:
            process.wait(timeout=65)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=10)
        log.close()
    assert process.returncode == 0 and not sock.exists(), "observer session cleanup failure"
    tasks = [json.loads(line) for line in trace.read_text().splitlines()]
    expected_inputs = Counter(r["payload_sha256"] for r in subset)
    assert Counter(t["input_sha256"] for t in tasks) == expected_inputs, "task exactly-once mismatch"
    spec = semantic_spec_digest(SemanticFilterPlan(INSTRUCTION, MODEL))
    expected_payloads = Counter(semantic_payload_digest(semantic_spec_sha256=spec,
                                input_value=r["payload"], canonical_messages_utf8=canonical_messages(INSTRUCTION, r["payload"]))
                                for r in subset)
    assert Counter(t["payload_digest"] for t in tasks) == expected_payloads
    assert all(t["raw_output"] in ("TRUE", "FALSE", "UNKNOWN") and t["model_id"] == MODEL for t in tasks)
    node = filter_node(result[0]["Plan"])
    assert node is not None, "no SemFilter CustomScan"
    output_rows = sum(t["raw_output"] == "TRUE" for t in tasks)
    prompt_tokens = sum(t["prompt_tokens"] for t in tasks)
    output_tokens = sum(t["output_tokens"] for t in tasks)
    assert node["Model Calls"] == len(tasks) == len(subset)
    assert node["Emitted Rows"] == node["Actual Rows"] == output_rows
    assert node["Prompt Tokens"] == prompt_tokens and node["Output Tokens"] == output_tokens
    assert node["Plans"][0]["Actual Rows"] == len(subset)
    observation = dict(semantic_input_rows=len(subset), output_rows=output_rows, model_calls=len(tasks),
                       prompt_tokens=prompt_tokens, output_tokens=output_tokens,
                       service_milliseconds=sum(t["duration_ns"] for t in tasks) / 1e6)
    save(run / "observation.json", observation)
    print(json.dumps(dict(run_id=run_id, status="ok", calls=len(tasks))), flush=True)
    return observation


def collect(args):
    import psycopg
    from psycopg import sql
    from src.execution_provider.wire.v3 import SemanticFilterPlan, semantic_spec_digest, physical_algorithm_digest
    rows = json.loads((args.root / "private_rows.json").read_text())
    save(args.root / "collection_provenance.json", dict(observer_sha256=sha(Path(__file__)),
        started_at=datetime.now(timezone.utc).isoformat(),
        source_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.repo, text=True).strip()))
    service = service_identity(args)
    save(args.root / "service_identity.json", service)
    assert model_files(args.model) == service["model_files"]
    (args.root / "runs").mkdir()
    with psycopg.connect(os.environ["SEMLOOM_CAL_DSN"], autocommit=True) as connection:
        assert connection.execute("show server_version").fetchone()[0] == "18.3"
        connection.execute("CREATE EXTENSION semloom_pg")
        connection.execute("CREATE TABLE calibration_inputs(doc_id bigint PRIMARY KEY, split text, cell integer, payload text)")
        with connection.cursor().copy("COPY calibration_inputs FROM STDIN") as copy:
            for r in rows:
                copy.write_row((r["doc_id"], r["split"], r["cell"], r["payload"]))
        connection.execute("ANALYZE calibration_inputs")
        actual = connection.execute("SELECT doc_id,split,cell,payload FROM calibration_inputs ORDER BY doc_id").fetchall()
        assert actual == [(r["doc_id"], r["split"], r["cell"], r["payload"]) for r in rows]
        connection.execute(sql.SQL("SET semloom_pg.gateway_socket={}").format(sql.Literal(str(args.socket))))
        connection.execute("SET semloom_pg.provider_execution_profile='openai-compatible-fixed'")
        connection.execute("SET statement_timeout='5min'")
        execute_cell(args, connection, rows, "warmup", 0, 0, service)
        schedule = [(split, cell, repeat) for repeat in range(3)
                    for split, sizes in (("training", TRAIN_SIZES), ("held_out", HOLDOUT_SIZES))
                    for cell in range(len(sizes))]
        random.Random(SEED).shuffle(schedule)
        save(args.root / "schedule.json", schedule)
        observations = {"training": [], "held_out": []}
        for split, cell, repeat in schedule:
            observations[split].append(execute_cell(args, connection, rows, split, cell, repeat, service))
        assert service_identity(args) == service and model_files(args.model) == service["model_files"]
        workload = json.loads((args.root / "workload_manifest.json").read_text())
        assert sha(args.raw) == workload["raw_sha256"]
        source = dict(schema_version=1, generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                      semantic_spec_digest=semantic_spec_digest(SemanticFilterPlan(INSTRUCTION, MODEL)),
                      physical_algorithm_digest=physical_algorithm_digest(),
                      provider_execution_profile="openai-compatible-fixed", model_id=MODEL, model_role="reference",
                      workload_signature=identity(workload), service_signature=identity(service),
                      accepted_max_relative_error=0.20, training_observations=observations["training"],
                      held_out_observations=observations["held_out"])
        save(args.root / "source.json", source)


def analyze(args):
    import numpy as np
    from src.planning.semfilter_reference_calibration import (build_reference_calibration,
        _read_observations, _fit_service_coefficients)
    source = json.loads((args.root / "source.json").read_text())
    x = np.array([[1, r["model_calls"], r["prompt_tokens"], r["output_tokens"]]
                  for r in source["training_observations"]], dtype=float)
    scaled = x / np.linalg.norm(x, axis=0)
    report = dict(training_samples=len(x), held_out_samples=len(source["held_out_observations"]),
                  training_rank=int(np.linalg.matrix_rank(scaled)),
                  training_singular_values=np.linalg.svd(scaled, compute_uv=False).tolist(),
                  accepted_max_relative_error=source["accepted_max_relative_error"],
                  held_out_used_for_fit=False, tuned_after_held_out=False)
    training = source["training_observations"]
    totals = {name: sum(r[name] for r in training) for name in training[0]}
    rates = dict(output_selectivity=totals["output_rows"] / totals["semantic_input_rows"],
                 calls_per_input=totals["model_calls"] / totals["semantic_input_rows"],
                 prompt_per_call=totals["prompt_tokens"] / totals["model_calls"],
                 output_per_call=totals["output_tokens"] / totals["model_calls"])
    report["training_rates"] = rates
    coefficients = None
    try:
        assert report["training_rank"] == 4, "training design is rank deficient"
        coefficients = _fit_service_coefficients(_read_observations(training, "training"))
        report["training_service_coefficients"] = coefficients
    except (ValueError, AssertionError) as error:
        report["service_fit_rejection"] = str(error)
    predictions = []
    for index, actual in enumerate(source["held_out_observations"]):
        calls = actual["semantic_input_rows"] * rates["calls_per_input"]
        predicted = dict(output_rows=actual["semantic_input_rows"] * rates["output_selectivity"],
                         model_calls=calls, prompt_tokens=calls * rates["prompt_per_call"],
                         output_tokens=calls * rates["output_per_call"])
        if coefficients is not None:
            predicted["service_milliseconds"] = (coefficients[0] + coefficients[1] * calls
                + coefficients[2] * predicted["prompt_tokens"] + coefficients[3] * predicted["output_tokens"])
        errors = {name: abs(value - actual[name]) / max(abs(actual[name]), 1)
                  for name, value in predicted.items()}
        predictions.append(dict(index=index, actual=actual, predicted=predicted, relative_errors=errors))
    report["held_out_predictions"] = predictions
    report["held_out_max_errors_by_metric"] = {
        name: max(row["relative_errors"][name] for row in predictions)
        for name in predictions[0]["relative_errors"]}
    try:
        assert report["training_rank"] == 4, "training design is rank deficient"
        artifact = build_reference_calibration(source)
    except (ValueError, AssertionError) as error:
        report.update(status="rejected", reason=str(error), artifact_published=False,
                      held_out_prediction_status="not_qualified")
    else:
        save(args.root / "reference-calibration.json", artifact)
        report.update(status="passed", artifact_id=artifact["artifact_id"], artifact_published=True,
                      held_out_max_relative_error=artifact["held_out_max_relative_error"])
    save(args.root / "held_out_report.json", report)
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "gateway", "collect", "analyze"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--model", type=Path)
    parser.add_argument("--raw", type=Path)
    parser.add_argument("--socket", type=Path)
    parser.add_argument("--trace", type=Path)
    parser.add_argument("--identity", type=Path)
    parser.add_argument("--extension", type=Path)
    parser.add_argument("--endpoint", default="http://127.0.0.1:8013")
    args = parser.parse_args()
    sys.path.insert(0, str(args.repo / "code"))
    try:
        return globals()[args.mode](args) or 0
    except Exception as error:
        from src.baselines.common.redact import redact_text
        failure = redact_text(traceback.format_exc())
        if args.mode != "gateway":
            save(args.root / f"{args.mode}-failure.json", dict(status="failed", traceback=failure,
                sqlstate=getattr(error, "sqlstate", None), exception_type=type(error).__name__))
        print(failure, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
