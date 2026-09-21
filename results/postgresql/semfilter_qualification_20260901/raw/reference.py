"""Bounded, separately identified reference qualification; never reads held-out rows."""
import argparse
import ctypes as ct
import hashlib
import json
from pathlib import Path
import random
import subprocess
import sys
import time

INSTRUCTION = "The input asks for writing, explaining, or debugging computer code."
MODEL = "Qwen2.5-1.5B-Instruct"
FAILED_SHA = "84153b846c2b6e6d78b47efbe40670d005b3a8d0f7382f306d509603867011d2"
CASES = [
    ("positive-python", "TRUE", "Write a Python function that adds two integers."),
    ("positive-sql", "TRUE", "Explain what the SQL statement SELECT COUNT(*) FROM orders does."),
    ("positive-debug", "TRUE", "Debug this JavaScript function: function add(a,b) { return a - b; } It should add the numbers."),
    ("negative-recipe", "FALSE", "Give me a recipe for tomato soup."),
    ("negative-poem", "FALSE", "Write a short poem about a mountain."),
    ("negative-geography", "FALSE", "What is the capital of France?"),
    ("unknown-fix", "UNKNOWN", "Please help me fix it."),
    ("unknown-explain", "UNKNOWN", "Can you explain this?"),
    ("unknown-write", "UNKNOWN", "I need help writing something, but I have not said what."),
]

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()

def file_sha(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8*1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def save(path, value):
    with path.open("x") as handle:
        json.dump(value, handle, sort_keys=True, ensure_ascii=False, indent=2)
        handle.write("\n")

def failed_input(args):
    manifest = json.loads((args.previous / "public/workload_manifest.json").read_text())
    target = next(row for row in manifest["rows"] if row["payload_sha256"] == FAILED_SHA)
    assert target["split"] == "training" and target["doc_id"] < 832
    # The previous snapshot is pretty-printed flat row objects. Stop immediately
    # after the known training row, before reaching the held-out part of the file.
    block = None
    with (args.previous / "private_rows.json").open() as handle:
        for line in handle:
            if line.rstrip() == "  {":
                block = [line]
            elif block is not None:
                block.append(line)
                if line.rstrip() in ("  },", "  }"):
                    row = json.loads("".join(block).rstrip().rstrip(","))
                    assert row["split"] != "held_out"
                    if row["doc_id"] == target["doc_id"]:
                        assert hashlib.sha256(row["payload"].encode()).hexdigest() == FAILED_SHA
                        return row["payload"]
                    block = None
    raise AssertionError("known failed training input not found")

def parser_library(args):
    shared = args.root / "filter-parser.so"
    subprocess.run(["cc", "-std=c11", "-Wall", "-Wextra", "-Werror", "-shared", "-fPIC",
                    "-I", str(args.repo / "code/postgres/semloom_pg/src"),
                    str(args.repo / "code/postgres/semloom_pg/src/sem_filter_machine.c"),
                    "-o", str(shared)], check=True)
    class Completion(ct.Structure):
        _fields_ = [("data", ct.c_char_p), ("length", ct.c_uint32), ("is_null", ct.c_bool)]
    Apply = ct.CFUNCTYPE(ct.c_int, ct.POINTER(Completion))
    class Methods(ct.Structure):
        _fields_ = [("property", ct.c_char_p), ("null", ct.c_void_p), ("apply", Apply)]
    library = ct.CDLL(str(shared))
    methods = Methods.in_dll(library, "semloom_filter_exact_machine_methods")
    def parse(raw):
        encoded = raw.encode()
        return methods.apply(ct.byref(Completion(encoded, len(encoded), False)))
    controls = {raw: parse(raw) for raw in ("TRUE", "FALSE", "UNKNOWN", "yes", "TRUE\n", "", "FALSE ")}
    assert list(controls.values()) == [1, 2, 2, 4, 4, 4, 4]
    save(args.root / "parser-controls.json", dict(results=controls, shared_sha256=file_sha(shared),
        source_sha256=file_sha(args.repo / "code/postgres/semloom_pg/src/sem_filter_machine.c")))
    return parse

def snapshot(args):
    sidecar = json.loads(args.identity.read_text())
    proc = Path(f"/proc/{sidecar['pid']}")
    assert proc.joinpath("stat").read_text().rsplit(")",1)[1].split()[19] == sidecar["process_start_time_ticks"]
    argv = proc.joinpath("cmdline").read_bytes().decode().split("\0")[:-1]
    expected = {"--model":str(args.model), "--served-model-name":MODEL, "--dtype":"bfloat16",
        "--max-model-len":"4096", "--gpu-memory-utilization":"0.25", "--scheduling-policy":"fcfs",
        "--max-num-seqs":"1", "--max-num-batched-tokens":"4096", "--tensor-parallel-size":"1"}
    for key,value in expected.items():
        assert argv.count(key) == 1 and argv[argv.index(key)+1] == value
    assert "--enforce-eager" in argv and "--no-enable-prefix-caching" in argv
    assert sidecar["package_version"] == "0.25.1"
    env = proc.joinpath("environ").read_bytes().split(b"\0")
    assert b"CUDA_VISIBLE_DEVICES=0" in env
    return dict(sidecar=sidecar, argv=argv)

def run(args):
    from src.execution_provider.adapters.openai_compatible_fixed import FixedModelConfig, OpenAICompatibleFixedAdapter
    from src.execution_provider.adapters.v3_session import V3CompletionRequest
    from src.execution_provider.wire.v3 import GENERATION_CONSTRAINTS, SemanticFilterPlan, canonical_messages, semantic_spec_digest
    before = snapshot(args)
    save(args.root / "service-identity.json", before)
    model_files = json.loads((args.previous / "model_files.json").read_text())
    assert all(file_sha(args.model / name) == expected for name, expected in model_files.items())
    save(args.root / "model-files.json", model_files)
    cases = CASES + [("failed-training-input", None, failed_input(args))]
    save(args.root / "cases.json", [dict(case_id=key, expected=label, input_sha256=hashlib.sha256(value.encode()).hexdigest())
                                    for key,label,value in cases])
    parse = parser_library(args)
    profiles = {"baseline": dict(GENERATION_CONSTRAINTS),
                "choice": {**GENERATION_CONSTRAINTS, "structured_outputs": {"choice":["TRUE","FALSE","UNKNOWN"]}}}
    plans = {}
    for profile, generation in profiles.items():
        plan = dict(schema="semloom.reference_qualification.plan.v1", production_pg_plan=False,
            base_semantic_spec_digest=semantic_spec_digest(SemanticFilterPlan(INSTRUCTION,MODEL)),
            instruction=INSTRUCTION, model=MODEL, generation_profile=f"semloom.qualification.{profile}.v1",
            generation_constraints=generation, result_parser="semloom.sem_filter.tristate_ascii.v1")
        plan["plan_digest"] = digest(plan)
        plans[profile] = plan
    assert plans["baseline"]["plan_digest"] != plans["choice"]["plan_digest"]
    save(args.root / "plans.json", plans)
    schedule = [(profile,index,repeat) for repeat in range(3) for index in range(len(cases)) for profile in profiles]
    random.Random(20260901).shuffle(schedule)
    save(args.root / "schedule.json", schedule)
    adapter = OpenAICompatibleFixedAdapter(FixedModelConfig(args.endpoint+"/v1/chat/completions",MODEL,60000))
    records = []
    with (args.root / "private-responses.jsonl").open("x") as private, (args.root / "responses.jsonl").open("x") as public:
        for profile,index,repeat in [("baseline",0,-1),("choice",0,-1)] + schedule:
            key,expected,value = cases[index]
            plan = plans[profile]
            messages = tuple(json.loads(canonical_messages(INSTRUCTION,value)))
            payload_digest = digest(dict(plan_digest=plan["plan_digest"],messages=messages))
            request = V3CompletionRequest(payload_digest,MODEL,messages,profiles[profile])
            start = time.monotonic_ns()
            result = adapter.complete(request)
            elapsed = time.monotonic_ns()-start
            assert result.response_model_id == MODEL
            disposition = parse(result.raw_output)
            record = dict(profile=profile,case_id=key,repeat=repeat,plan_digest=plan["plan_digest"],
                payload_digest=payload_digest,expected=expected,
                raw_output=result.raw_output if result.raw_output in ("TRUE","FALSE","UNKNOWN") else None,
                raw_output_sha256=hashlib.sha256(result.raw_output.encode()).hexdigest(),
                raw_output_bytes=len(result.raw_output.encode()), c_disposition=disposition,
                prompt_tokens=result.prompt_tokens,output_tokens=result.output_tokens,finish_reason=result.finish_reason,
                request_wall_ns=elapsed, format_valid=disposition in (1,2),
                matches_expected=result.raw_output == expected if expected is not None else None)
            from src.baselines.common.redact import redact_text
            private.write(json.dumps({**record,"raw_output":redact_text(result.raw_output)})+"\n"); private.flush()
            public.write(json.dumps(record)+"\n"); public.flush()
            records.append(record)
    assert snapshot(args) == before
    assert all(file_sha(args.model / name) == expected for name, expected in model_files.items())
    summary = {}
    for profile in profiles:
        measured = [r for r in records if r["profile"] == profile and r["repeat"] >= 0]
        valid = sum(r["format_valid"] for r in measured)
        correct = sum(r["matches_expected"] is True for r in measured)
        summary[profile] = dict(responses=len(measured), format_valid=valid, expected_matches=correct,
            labelled_responses=27, qualified=valid == 30 and correct == 27,
            output_tokens_histogram={str(n):sum(r["output_tokens"] == n for r in measured) for n in sorted({r["output_tokens"] for r in measured})})
    save(args.root / "reference-summary.json", dict(profiles=summary, held_out_payload_read=False,
        held_out_model_calls=0, plan_schema_is_experimental=True, production_wire_unchanged=True,
        service_and_model_unchanged=True, source_commit=subprocess.check_output(["git","rev-parse","HEAD"],cwd=args.repo,text=True).strip()))
    print(json.dumps(summary,indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("root","repo","model","previous","identity"):
        parser.add_argument("--"+name,type=Path,required=True)
    parser.add_argument("--endpoint",default="http://127.0.0.1:8013")
    args = parser.parse_args()
    sys.path.insert(0,str(args.repo / "code"))
    run(args)
