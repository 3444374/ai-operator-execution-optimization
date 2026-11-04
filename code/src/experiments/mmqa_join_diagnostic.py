"""Fixed 24-attempt Join prompt diagnostic; never executes the complete Q2b query."""

import argparse
import base64
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import importlib
import json
from pathlib import Path
import subprocess
import sys
import time

from src.baselines.common.private_artifacts import (content_digest, file_identity, new_private_directory,
    open_private_text, publish_private_json, write_private_json)
from src.baselines.common.redact import redact_text, redact_json_values
from src.baselines.image.sembench_mmqa import MmqaDataset
from src.baselines.image.sembench_mmqa_compat import PROFILE_ID, validate_strategy_source
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.query_supervisor import supervise
from src.experiments.sembench_animals import configure_lm
from src.experiments.sembench_mmqa import validate_native_source, validate_profile
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.infrastructure.runner_lease import acquire_runner_lease, HOST_SCOPE_FINGERPRINT
from src.observability.request_gateway import GatewayRoute, ObservationGateway


SCHEMA = "semloom.mmqa_join_diagnostic.selection.v1"
TOTAL_ATTEMPTS = 24
ARMS = ("legacy-string", "enum-zs-cot")
INSTRUCTION = "{image} shows the logo of horse racetrack {Track}"
UNIT = "join-diagnostic"


def prepare_selection(dataset, image_names):
    preflight, value = dataset.preflight(), dataset.read()
    if (not isinstance(image_names, (list, tuple)) or len(image_names) != 2
            or any(type(n) is not str for n in image_names) or len(set(image_names)) != 2):
        raise ValueError("diagnostic requires exactly two explicit different image names")
    catalog = {r["name"]: r for r in value["images"]}
    if any(n not in catalog for n in image_names):
        raise ValueError("selected diagnostic image is outside the parent manifest")
    groups = {}
    rows = json.loads(dataset.table_frame(value)[["ID", "Track"]].to_json(orient="records"))
    for index, row in enumerate(rows):
        groups.setdefault(row["Track"], []).append(dict(row_index=index, ID=row["ID"]))
    if len(groups) != 6:
        raise ValueError("diagnostic requires all six distinct original Track values")
    result = dict(schema=SCHEMA, role=value["role"], parent_manifest=file_identity(dataset.manifest),
        parent_manifest_digest=dataset.expected_digest, source_sha256=preflight["source_sha256"],
        images=[catalog[n] for n in image_names],
        tracks=[dict(value=track, source_members=members) for track, members in groups.items()],
        arms=list(ARMS), pairs_per_arm=12, maximum_attempts=TOTAL_ATTEMPTS,
        concurrency=1, warmup=0, retries=0, extract_allowed=False,
        scope="two images by six distinct Tracks by two prompt strategies; diagnostic only, no SemBench score")
    result["sha256"] = content_digest(result)
    return result


def load_selection(dataset, path, expected_digest):
    value = json.loads(Path(path).read_text())
    payload = {k: v for k, v in value.items() if k != "sha256"}
    if value.get("sha256") != expected_digest or content_digest(payload) != expected_digest:
        raise ValueError("diagnostic selection digest differs")
    names = [r.get("name") for r in value.get("images", [])]
    if value != prepare_selection(dataset, names):
        raise ValueError("diagnostic selection differs from its parent inputs or fixed plan")
    return value


def prepare_matrix(dataset, selection, lm, max_prepared_bytes):
    import pandas as pd
    from lotus.dtype_extensions.image import ImageArray
    from lotus.templates import task_instructions as formatter
    enum_strategy = validate_strategy_source()
    data = Path(dataset.read()["data_path"])
    paths = [str(data / "images" / row["name"]) for row in selection["images"]]
    images = pd.Series(ImageArray(paths), name="image")
    tracks = pd.Series([r["value"] for r in selection["tracks"]], name="Track")
    left = formatter.df2multimodal_info(images.to_frame(), ["image"])
    right = formatter.df2multimodal_info(tracks.to_frame(), ["Track"])
    matrix, total = [], 0
    for i, image in enumerate(left):
        for j, doc in enumerate(formatter.merge_multimodal_info([image], right)):
            messages = {arm: formatter.filter_formatter(lm, doc, INSTRUCTION,
                strategy="zs-cot" if arm == "legacy-string" else enum_strategy) for arm in ARMS}
            if (messages[ARMS[0]][1:] != messages[ARMS[1]][1:]
                    or messages[ARMS[0]][0] == messages[ARMS[1]][0]):
                raise ValueError("diagnostic strategies must differ only in the system prompt")
            # Alternate first arm between pairs; both use the same single LM/service.
            for arm in ARMS if (i * 6 + j) % 2 == 0 else reversed(ARMS):
                prompt = messages[arm]
                total += len(json.dumps(prompt, ensure_ascii=False).encode())
                if total > max_prepared_bytes:
                    raise ValueError("diagnostic prepared messages exceed the declared maximum")
                matrix.append(dict(index=len(matrix), arm=arm, image=i, track=j,
                    image_name=selection["images"][i]["name"], Track=selection["tracks"][j]["value"],
                    messages=prompt, messages_sha256=content_digest(prompt)))
    return images, tracks, matrix, enum_strategy


@contextmanager
def forbid_extract():
    Extract = importlib.import_module("lotus.sem_ops.sem_extract").SemExtractDataFrame
    original = Extract.__call__
    def forbidden(*_args, **_kwargs):
        raise RuntimeError("Extract is prohibited in the Join-only diagnostic")
    Extract.__call__ = forbidden
    try:
        yield
    finally:
        Extract.__call__ = original


def run_diagnostic(dataset, selection, model, *, output, unit, tokenizer, profile,
                   max_tokens, max_request_bytes, max_prepared_bytes, max_result_bytes):
    if (unit.budget.limit != TOTAL_ATTEMPTS or unit.attempts or unit.remaining != TOTAL_ATTEMPTS
            or type(max_tokens) is not int or not 1 <= max_tokens <= 8192
            or any(type(v) is not int or v < 1 for v in (max_request_bytes, max_prepared_bytes, max_result_bytes))):
        raise ValueError("diagnostic requires a fresh total24 unit and finite token/byte maxima")
    if selection != prepare_selection(dataset, [r["name"] for r in selection["images"]]):
        raise ValueError("diagnostic selection no longer matches the parent")
    validate_native_source()
    validate_strategy_source()
    profile = validate_profile(profile, model, tokenizer, max_tokens)
    output = Path(output)
    new_private_directory(output)
    summary = dict(schema="semloom.mmqa_join_diagnostic.v1", status="failed",
        selection_sha256=selection["sha256"], selection=selection, profile=profile,
        arm_profiles={"legacy-string": "lotus1.2.4-unmapped-zs-cot-string",
                      "enum-zs-cot": PROFILE_ID}, maximum_attempts=TOTAL_ATTEMPTS,
        concurrency=1, warmup=0, retries=0, extract_calls=0, remote_compute_end_known=False,
        execution_owner="diagnostic_fixed_single_call_loop", method_owner="native_lotus_single_pair_sem_join",
        scope="Join prompt diagnostic only; no Extract, full Q2b result, quality score or performance comparison")
    state, results, failure = {}, [], None
    started = time.monotonic_ns()
    try:
        with open_private_text(output / "requests.jsonl") as requests, open_private_text(output / "responses.jsonl") as responses:
            def before(_route, body):
                value = json.loads(body)
                active = state.get("active")
                tokens = [value[k] for k in ("max_tokens", "max_completion_tokens") if k in value]
                if (active is None or state.get("sent") or len(body) > max_request_bytes
                        or value.get("messages") != active["messages"] or value.get("model") != model.model_id
                        or "response_format" in value or tokens != [max_tokens]
                        or value.get("temperature") != 0 or value.get("top_p") != 1):
                    raise ValueError("request is outside the active diagnostic Join pair")
                parameters = {k: v for k, v in value.items() if k != "messages"}
                if "parameters" in state and parameters != state["parameters"]:
                    raise ValueError("diagnostic generation parameters changed across arms")
                state["parameters"], state["sent"] = parameters, True
                attempt = unit.reserve(hashlib.sha256(body).hexdigest())
                requests.write(json.dumps(dict(active, attempt=attempt, body=value,
                    observed_monotonic_ns=time.monotonic_ns())) + "\n")

            def after(_route, body, response, status):
                if len(response) > max_result_bytes:
                    raise ValueError("diagnostic response exceeds the declared maximum")
                saved = response
                error_response = not 200 <= status < 300
                try:
                    text = response.decode("utf-8")
                    try:
                        structured = json.loads(text)
                    except json.JSONDecodeError:
                        structured = None
                    error_response = error_response or isinstance(structured, dict) and structured.get("error") is not None
                    if error_response:
                        saved = (json.dumps(redact_json_values(structured), ensure_ascii=False).encode("utf-8")
                                 if structured is not None else redact_text(text).encode("utf-8"))
                except UnicodeDecodeError:
                    if error_response:
                        saved = b'{"error_body_omitted":"non-UTF8 response; original identity retained"}'
                responses.write(json.dumps(dict(index=state["active"]["index"], status=status,
                    request_sha256=hashlib.sha256(body).hexdigest(), body_base64=base64.b64encode(saved).decode(),
                    original_response_sha256=hashlib.sha256(response).hexdigest(), original_response_bytes=len(response),
                    failure_copy_redacted=error_response,
                    observed_monotonic_ns=time.monotonic_ns())) + "\n")
                state["http_failed"] = error_response

            with ObservationGateway(routes=(GatewayRoute("join-diagnostic", "model", model.endpoint_url),),
                    trace_path=output / "http-trace.jsonl", request_timeout_s=model.timeout_ms / 1000,
                    before_forward=before, after_forward=after, preserve_http=True) as gateway:
                lm = configure_lm(replace(model, endpoint_url=gateway.endpoint_url("join-diagnostic", "model")),
                                  1, max_tokens, tokenizer)
                images, tracks, matrix, enum_strategy = prepare_matrix(dataset, selection, lm, max_prepared_bytes)
                native_join = importlib.import_module("lotus.sem_ops.sem_join").sem_join
                publish_private_json(output / "started.json", dict(started_ns=time.monotonic_ns()))
                with forbid_extract():
                    for active in matrix:
                        state.update(active=active, sent=False, http_failed=False)
                        i, j = active["image"], active["track"]
                        result = native_join(images.iloc[[i]], tracks.iloc[[j]], [i], [j], "image", "Track", lm,
                            INSTRUCTION, strategy="zs-cot" if active["arm"] == "legacy-string" else enum_strategy,
                            show_progress_bar=False)
                        if (not state["sent"] or state["http_failed"]
                                or len(result.filter_outputs) != 1 or len(result.all_raw_outputs) != 1):
                            raise ValueError("diagnostic native Join did not complete its one request")
                        expected_pairs = [(i, j)] if result.filter_outputs[0] else []
                        if [(a, b) for a, b, _ in result.join_results] != expected_pairs:
                            raise ValueError("diagnostic Join relation differs from native predicate")
                        results.append(dict(index=active["index"], arm=active["arm"], image_name=active["image_name"],
                            Track=active["Track"], decision=result.filter_outputs[0], raw=result.all_raw_outputs[0]))
                trace = gateway.snapshot()
                if (unit.attempts != TOTAL_ATTEMPTS or len(trace) != TOTAL_ATTEMPTS
                        or any(r["status"] != "completed" for r in trace)):
                    raise ValueError("diagnostic attempt coverage or HTTP outcomes differ")
                summary.update(status="completed", generation=state["parameters"])
                publish_private_json(output / "execution.json", dict(completed_ns=time.monotonic_ns()))
    except BaseException as error:
        failure = error
    finally:
        trace_path = output / "http-trace.jsonl"
        trace = [json.loads(s) for s in trace_path.read_text().splitlines()] if trace_path.exists() else []
        summary.update(application_seconds=(time.monotonic_ns() - started) / 1e9,
            attempts=unit.attempts, http_records=len(trace), results=results,
            known_http_completed=sum(r["status"] == "completed" for r in trace),
            unknown_or_failed_attempts=unit.attempts - sum(r["status"] == "completed" for r in trace),
            unconfirmed_responses=sum(r["forwarded"] and r["upstream_response_status"] is None for r in trace))
        if failure is not None:
            summary.update(status="failed", error_type=type(failure).__name__, error=redact_text(str(failure)))
        try:
            write_private_json(output / "summary.json", summary)
            write_private_json(output / "observation-footer.json", dict(status=summary["status"],
                files={n: file_identity(output / n) for n in ("requests.jsonl", "responses.jsonl", "http-trace.jsonl", "summary.json")
                       if (output / n).is_file()}))
        except BaseException as recording:
            if failure is None:
                raise
            failure.add_note("Diagnostic evidence recording also failed: " + type(recording).__name__)
            raise failure from recording
    if failure is not None:
        raise failure
    return summary


def parser():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--checkout", type=Path, required=True)
    mode = cli.add_mutually_exclusive_group(required=True)
    for name in ("prepare-selection", "preflight", "execute", "worker"):
        mode.add_argument("--" + name, action="store_true", help=argparse.SUPPRESS if name == "worker" else None)
    for name in ("manifest", "selection", "output", "model", "tokenizer", "model-profile", "lease-root"):
        cli.add_argument("--" + name, type=Path)
    for name in ("manifest-digest", "selection-digest", "repository-commit"):
        cli.add_argument("--" + name)
    cli.add_argument("--images", nargs=2)
    for name in ("max-attempts", "concurrency", "max-tokens", "max-request-bytes", "max-prepared-bytes", "max-result-bytes", "deadline-seconds"):
        cli.add_argument("--" + name, type=int)
    return cli


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    if args.manifest is None or args.manifest_digest is None:
        cli.error("parent manifest and digest are required")
    dataset = MmqaDataset(args.checkout, args.manifest, args.manifest_digest)
    if args.prepare_selection:
        if args.images is None or args.output is None:
            cli.error("prepare-selection requires two images and output JSON")
        selection = prepare_selection(dataset, args.images)
        write_private_json(args.output, selection)
        print(json.dumps(dict(selection_digest=selection["sha256"], maximum_attempts=TOTAL_ATTEMPTS)))
        return 0
    if args.selection is None or args.selection_digest is None:
        cli.error("selection and selection-digest are required")
    selection = load_selection(dataset, args.selection, args.selection_digest)
    if args.preflight:
        print(json.dumps(dict(status="input_preflight_passed", selection=selection, model_calls=0), indent=2))
        return 0
    required = (args.output, args.model, args.tokenizer, args.model_profile, args.lease_root, args.repository_commit,
        args.max_attempts, args.concurrency, args.max_tokens, args.max_request_bytes, args.max_prepared_bytes,
        args.max_result_bytes, args.deadline_seconds)
    if any(v is None for v in required):
        cli.error("execute requires exact source, model/profile, lease and all resource maxima")
    if args.max_attempts != TOTAL_ATTEMPTS or args.concurrency != 1 or not 1 <= args.deadline_seconds <= 1200:
        cli.error("diagnostic requires exactly total24, concurrency1 and deadline1..1200s")
    model = load_fixed_model_config(args.model)
    profile = json.loads(args.model_profile.read_text())
    validate_native_source()
    validate_strategy_source()
    validate_profile(profile, model, args.tokenizer, args.max_tokens)
    budget = AttemptBudget("mmqa-join-diagnostic", TOTAL_ATTEMPTS)
    if args.worker:
        ledger = CellBudgetLedger(args.output / "budget.sqlite", budget)
        unit = ledger.claim_shared_unit(UNIT)
        try:
            publish_private_json(args.output / "unit/unit-reserved.json", dict(unit_id=UNIT))
            run_diagnostic(dataset, selection, model, output=args.output / "unit/q0", unit=unit, tokenizer=args.tokenizer,
                profile=profile, max_tokens=args.max_tokens, max_request_bytes=args.max_request_bytes,
                max_prepared_bytes=args.max_prepared_bytes, max_result_bytes=args.max_result_bytes)
        finally:
            ledger.close_shared_unit(UNIT)
        return 0
    repo = Path(__file__).resolve().parents[3]
    commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=all", "--", "code"], text=True)
    if commit != args.repository_commit or dirty:
        raise ValueError("diagnostic requires the exact clean reviewed Git code identity")
    with acquire_runner_lease(args.lease_root, config_fingerprint=HOST_SCOPE_FINGERPRINT,
            repository_commit=commit, recover_stale=False):
        new_private_directory(args.output)
        (args.output / "unit").mkdir()
        ledger = CellBudgetLedger.create(args.output / "budget.sqlite", budget,
                                        deadline_utc=time.time() + args.deadline_seconds)
        ledger.reserve_unit(UNIT, TOTAL_ATTEMPTS)
        write_private_json(args.output / "run-config.json", dict(repository_commit=commit, selection=selection,
            profile=profile, maximum_attempts=TOTAL_ATTEMPTS, concurrency=1, warmup=0, retries=0,
            max_tokens=args.max_tokens, max_request_bytes=args.max_request_bytes,
            max_prepared_bytes=args.max_prepared_bytes, max_result_bytes=args.max_result_bytes,
            deadline_seconds=args.deadline_seconds))
        resolved = list(sys.argv[1:] if argv is None else argv)
        command = [sys.executable, "-B", str(repo / "code/scripts/experiments/run_mmqa_join_diagnostic.py"),
                   *("--worker" if v == "--execute" else v for v in resolved)]
        try:
            supervise(command, args.output, lambda: ledger.close_shared_unit(UNIT),
                      query_timeout_s=args.deadline_seconds, max_duration_s=args.deadline_seconds)
        finally:
            ledger.close_shared_unit(UNIT)
    print((args.output / "unit/q0/summary.json").read_text())
    return 0
