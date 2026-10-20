#!/usr/bin/env python3
"""Prepare one explicitly selected Movie task, with no model/framework launch."""

import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO / "code"))

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.baselines.text.sembench_movie import verify_checkout
from src.baselines.text.movie_tasks import MovieTask
from src.semantic_methods.lotus.records import FilterMethod


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load(path):
    return json.loads(Path(path).read_text())


def prepare(args):
    plan, backends = load(HERE / "comparison.json"), load(HERE / "backends.json")["backends"]
    catalog = load(HERE / "tasks.json")
    selection = load(args.task_config or HERE / "task.example.json")
    if (type(selection) is not dict or set(selection) != {"schema", "task", "input_profile"}
            or selection["schema"] != "semloom.movie_task_selection.v1"):
        raise ValueError("an explicit Movie task and input profile selection is required")
    task = MovieTask.from_config(selection["task"])
    task_plan = catalog["tasks"][task.name]
    if selection["input_profile"] != task_plan["input_profile"]:
        raise ValueError("task input profile differs from the reviewed catalog; never substitute historical/official input")
    input_profile = catalog["input_profiles"][selection["input_profile"]]
    plan["method"] = dict(plan["method"], instruction=task.instruction)
    runtime = load(args.runtime_json)
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", args.run_id):
        raise ValueError("run-id must be at most64 lowercase letters/digits/hyphens")
    required = set(load(HERE / "runtime.example.json"))
    if set(runtime) != required or any(type(v) is not str or not v for v in runtime.values()):
        raise ValueError("runtime binding must have exactly the example's nonempty string fields")
    if not all(Path(runtime[k]).is_absolute() for k in required - {"binding_status", "cuda_visible_devices"}):
        raise ValueError("runtime paths must be absolute, even when preparation placeholders")
    if plan["status"] != "preparation_only" or plan["execution_authorized"] is not False:
        raise ValueError("this preparer accepts preparation-only plans")
    locked = load(HERE / "source.lock.json")
    if any(digest(REPO / name) != value for name, value in locked["runtime_files_sha256"].items()):
        raise ValueError("runtime code differs from the reviewed production source")
    if args.check_source:
        subprocess.run(["git", "-C", str(REPO), "merge-base", "--is-ancestor", plan["source_commit"], "HEAD"], check=True)
        if subprocess.check_output(["git", "-C", str(REPO), "status", "--porcelain"], text=True).strip():
            raise ValueError("source-bound preparation requires a clean reviewed checkout")
    checkout = verify_checkout(args.sembench_checkout)
    for name, value in locked["sembench_files_sha256"].items():
        if digest(checkout / name) != value:
            raise ValueError("SemBench source bytes differ from the locked query/evaluator")
    for path, expected_bytes, expected_sha in (
        (Path(args.input_csv), input_profile["csv_bytes"], input_profile["csv_sha256"]),
        (Path(args.upstream_csv), 654791, plan["input"]["upstream_csv_sha256"]),
    ):
        if path.is_symlink() or not path.is_file() or path.stat().st_size != expected_bytes or digest(path) != expected_sha:
            raise ValueError("input must be a regular fixed-size file with the locked SHA256 identity")
    with Path(args.input_csv).open(newline="", encoding="utf-8") as stream:
        fixed = list(csv.DictReader(stream))
    with Path(args.upstream_csv).open(newline="", encoding="utf-8") as stream:
        original = list(csv.DictReader(stream))
    eligible = lambda rows: [(i, row) for i, row in enumerate(rows) if task.accepts(row)]
    if len(fixed) != task_plan["rows"] or eligible(fixed) != eligible(original):
        raise ValueError("selected task relation/source positions differ from its pinned upstream input")
    root = Path(args.output_dir).resolve()
    new_private_directory(root)
    projection_started = time.monotonic()
    manifest = prepare_movie_source(args.input_csv, root / "input", expected_csv_sha256=input_profile["csv_sha256"],
        max_rows=task_plan["rows"], role=input_profile["role"], task=task.name)
    projection_seconds = time.monotonic() - projection_started
    source = MovieRecordSource.from_manifest(manifest)
    source_report = source.preflight(FilterMethod.from_dict(plan["method"]))
    count = source_report["eligible_rows"]
    if count != task_plan["eligible_rows"]:
        raise ValueError("per-query call bound changed; re-review the budget")
    for name in ("units", "configs"):
        (root / name).mkdir(mode=0o700)
    emit = lambda name, value: write_private_json(root / name, value)
    emit("runtime-binding.json", runtime)
    emit("task-selection.json", selection)
    emit("task-definition.json", task.describe())
    emit("input-identities.json", dict(selected_profile=selection["input_profile"],
        selected_profile_verified=True, profile_declarations=catalog["input_profiles"],
        historical_files_overwritten=False, frozen_q3_authorization_reusable=False))
    emit("budget-proposal.json", dict(task=task.name, independently_authorized=False,
        preliminary_full_query_qualification=dict(queries=5, per_query_calls=count, call_upper_bound=5 * count),
        comparison=dict(queries=35, calibration_calls=10 * count, measured_calls=25 * count,
            call_upper_bound=35 * count, max_wall_s=35 * plan["design"]["per_query_supervisor_ceiling_s"] +
                plan["design"]["round_control_and_cleanup_allowance_s"]),
        total_with_one_full_qualification_per_backend=40 * count,
        comparison_completion_time_estimate=None,
        time_note="Existing per-query startup300/query480/supervisor810 and round30150 are stop ceilings, not a task-specific throughput estimate; qualify the new task before estimating completion time",
        current_allocation=0, qualification_is_separate_from_this_comparison_package=True,
        approved_4800_q3_calls_available=False, engineering_164_calls_available=False))
    emit("source.json", dict(source_report, preparation_only=True,
        offline_projection_seconds=projection_seconds, offline_projection_timing_scope="prepare_movie_source only; full input validation preceding projection is excluded",
        upstream_csv_sha256=digest(args.upstream_csv), eligible_full_rows_and_positions_equal=True,
        whole_csv_equal=digest(args.input_csv) == digest(args.upstream_csv),
        differing_full_rows=sum(a != b for a, b in zip(fixed, original, strict=True)),
        sembench_files_sha256={n: digest(checkout / n) for n in [
            "src/scenario/movie/runner/lotus_runner/lotus_runner.py",
            "src/scenario/movie/evaluation/evaluate.py", f"files/movie/query/gold_sql/Q{task.query}.sql"]}))
    order = []
    for point in range(plan["design"]["calibration_profiles_per_backend"]):
        for arm in (backends if point == 0 else list(reversed(backends))):
            order.append(("calibration", point + 1, arm, [arm["profiles"][point]]))
    for block in range(plan["design"]["measured_blocks"]):
        for arm in backends[block:] + backends[:block]:
            order.append(("measured", block + 1, arm, arm["profiles"]))
    units, commands = [], []
    for ordinal, (phase, block, arm, profiles) in enumerate(order, 1):
        query = f"{args.run_id}-{task.name}-{phase}-{block}-{arm['id']}"
        configurations = []
        for profile in profiles:
            name = f"configs/{query}-{profile['id']}.json"
            engine = dict(plan["model"]["engine"], **{k: runtime[k] for k in (
                "driver_python", "model_python", "model_path", "tokenizer_path")}, max_model_calls=count)
            resources = {k: v for k, v in profile.items() if k != "id"}
            if arm["id"] == "lotus-native":
                config = dict(schema="semloom.original_lotus_local_model_draft.v1", query_id=query,
                    status=arm["status"], task=task.name, method=plan["method"], generation=plan["generation"],
                    native_options=resources, requested_engine=engine,
                    source_manifest=str(manifest), sembench_checkout=str(checkout))
            else:
                resources.update(startup_timeout_s=plan["design"]["per_query_startup_timeout_s"],
                    query_timeout_s=plan["design"]["per_query_query_timeout_s"])
                if arm["id"] == "imlane":
                    config = dict(schema="imlane.filter.run.v1", mode="local-model", query_ids=[query],
                        task=task.name, method=plan["method"], generation=plan["generation"], engine=engine, resources=resources,
                        source_manifest=str(manifest), sembench_checkout=str(checkout), max_total_calls=count,
                        preflight_report=runtime["runtime_preflight_report"], method_python=runtime["imlane_method_python"],
                        author_root=runtime["imlane_author_root"], bridge_binary=runtime["imlane_bridge_binary"],
                        artifact_root=runtime["imlane_artifact_root"],
                        model_environment=dict(CUDA_VISIBLE_DEVICES=runtime["cuda_visible_devices"],
                            HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", LITELLM_LOCAL_MODEL_COST_MAP="True",
                            VLLM_WORKER_MULTIPROC_METHOD="spawn"),
                        owner_resource_limits=dict(cpu_cores=8,
                            rss_bytes=plan["shared_resources"]["max_sampled_owned_rss_bytes"],
                            shm_bytes=plan["shared_resources"]["max_owned_shm_bytes"]))
                else:
                    config = dict(schema="semloom.movie_records_run.v1", backend=arm["selector"], query_id=query,
                        task=task.name, method=plan["method"], generation=plan["generation"], engine=engine, resources=resources)
            emit(name, config)
            configurations.append(dict(profile=profile["id"], path=name, sha256=digest(root / name)))
        call_path = f"units/{query}.json"
        emit(call_path, dict(schema="semloom.formal_query_manifest.v1", query_id=query,
            task=task.name, ordinal=ordinal, phase=phase, block=block, backend=arm["id"],
            source_manifest_sha256=digest(manifest), input_identity="source.json",
            stages=[dict(model_role="main", algorithm_stage="filter", input_rows=count,
                model_call_upper_bound=count, calls=source.expected(query))],
            model_call_upper_bound=count, configurations=configurations,
            selected_profile=profiles[0]["id"] if phase == "calibration" else None,
            selection_gate="frozen calibration-selection.json required for measurement",
            scheduler_owner=arm["scheduler_owner"], qualification_state=arm["status"],
            lifecycle_scope="cold complete query; not resident", execution_authorized=False,
            reservation_state="not_allocated", result_state="not_run",
            raw_output=f"{args.run_id}-formal/{query}", raw_output_relative_to="package parent",
            budget_spent_on_failed_unknown=True))
        units.append(dict(query_id=query, ordinal=ordinal, phase=phase, block=block, backend=arm["id"],
            manifest=call_path, sha256=digest(root / call_path), max_calls=count))
        command = None
        checks = []
        if arm["id"] not in ("lotus-native", "imlane"):
            checks = [[runtime["driver_python"], "code/scripts/experiments/run_lotus_records_movie.py", "check",
                "--source-manifest", str(manifest), "--sembench-checkout", str(checkout),
                "--config", str(root / c["path"])] for c in configurations]
            # Data only, never executed here. Profile choice still needs an immutable selection.
            command = [runtime["driver_python"], "code/scripts/experiments/run_lotus_records_movie.py", "run",
                "--source-manifest", str(manifest), "--sembench-checkout", str(checkout), "--config",
                "SELECT_ONE_BOUND_CONFIG", "--output-dir", str(root / "raw" / query), "--execute-model"]
        commands.append(dict(query_id=query, no_model_checks=checks, qualification_run_argv_template=command,
            formal_run_argv=None,
            formal_campaign_argv_template=[runtime["driver_python"], str(REPO / "code/scripts/experiments/run_sembench_records_formal.py"),
                "run", "--package", str(root), "--authorization", "APPROVED_BOUND_AUTHORIZATION_JSON",
                "--output-dir", str(root.parent / (args.run_id + "-formal")), "--execute-model"],
            formal_worker_argv_template=[runtime["driver_python"], str(REPO / "code/scripts/experiments/run_sembench_records_formal.py"),
                "worker", "--package", str(root), "--unit", str(root / call_path),
                "--config", "ONE_CALIBRATION_SELECTED_BOUND_CONFIG", "--context", "CONTROLLER_WRITTEN_CONTEXT_JSON",
                "--output-dir", str(root.parent / (args.run_id + "-formal") / query), "--execute-model"],
            standalone_worker_authorized=False, blocked_by=plan["formal_gates"], cwd=str(REPO),
            environment=dict(PYTHONPATH=str(REPO / "code"), HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                LITELLM_LOCAL_MODEL_COST_MAP="True")))
    emit("schedule.json", dict(schema="semloom.formal_schedule.v1", run_id=args.run_id,
        execution_authorized=False, task=task.name, units=units, measured_queries_per_backend=5,
        call_bound_formula=f"5 backends * (2 calibration + 5 measured) * {count} Filter calls",
        calibration_calls=10 * count, measured_calls=25 * count, max_model_calls=len(units) * count,
        max_queries=len(units), warmup_calls=0, repair_reserve_calls=0,
        serial_unit_ceiling_s=plan["design"]["per_query_supervisor_ceiling_s"],
        max_round_wall_s=len(units) * plan["design"]["per_query_supervisor_ceiling_s"] +
            plan["design"]["round_control_and_cleanup_allowance_s"],
        allocated_calls=0, no_automatic_retry=True, historical_ledgers_unmodified=True))
    emit("commands.json", commands)
    emit("package-identity.json", dict(schema="semloom.formal_preparation_identity.v1",
        source_commit=plan["source_commit"], production_reference_commit=plan["production_reference_commit"],
        checked_out_commit=subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True).strip(),
        checked_out_dirty=bool(subprocess.check_output(["git", "-C", str(REPO), "status", "--porcelain"], text=True).strip()),
        versions=plan["versions"], sembench_commit=plan["sembench"]["commit"], task=task.describe(),
        input_profile=selection["input_profile"], input_csv_sha256=input_profile["csv_sha256"],
        sembench_checkout=str(checkout),
        runtime_source_lock_sha256=digest(HERE / "source.lock.json"),
        runtime_binding_status=runtime["binding_status"], model_identity=plan["model"],
        preparation_files_sha256={p.name: digest(p) for p in sorted(HERE.iterdir()) if p.is_file()},
        actual_model_calls=0, gpu_or_author_launches=0, budget_claims=0, formal_ready=False))
    emit("integrity.json", {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob("*")) if p.is_file()})
    return dict(status="prepared_not_authorized", units=len(units), model_calls_upper_bound=len(units) * count,
        actual_model_calls=0, formal_ready=False, output_dir=str(root))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("input-csv", "upstream-csv", "sembench-checkout", "runtime-json", "output-dir", "run-id"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--check-source", action="store_true", help="also require clean checkout descended from the reviewed base")
    parser.add_argument("--task-config", help="selection JSON; defaults to task.example.json (explicit historical Q3)")
    args = parser.parse_args()
    print(json.dumps(prepare(args)))


if __name__ == "__main__":
    main()
