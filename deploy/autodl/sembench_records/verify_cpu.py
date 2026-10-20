#!/usr/bin/env python3
"""Check the finite preparation and full Q3 fixture parity, never model qualification."""

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import shutil
import sys
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO / "code"))

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.common.redact import redact_json_values, redact_text
from src.baselines.text.movie_records import MovieRecordSource
from src.experiments.lotus_records_movie import fixed_response_qualification, read_config
from src.experiments.sembench_records_formal import check as formal_check
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, VllmEngine


def load(path):
    return json.loads(Path(path).read_text())


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check(package, checkout, evidence):
    plan = load(HERE / "comparison.json")
    report = dict(schema="semloom.sembench_records_cpu_verification.v1", status="failed",
        real_model_calls=0, gpu_launches=0, author_launches=0, performance_claim=False,
        model_quality_qualification=False, checks=[])
    def require(value, name):
        if not value:
            raise AssertionError(name)
        report["checks"].append(name)
    new_private_directory(evidence)
    try:
        locked = load(HERE / "source.lock.json")
        require(all(sha(REPO / n) == h for n, h in locked["runtime_files_sha256"].items()),
            "reviewed production runtime source hashes")
        inventory = load(package / "integrity.json")
        require(set(inventory) == {str(p.relative_to(package)) for p in package.rglob("*")
            if p.is_file() and p.name != "integrity.json"}, "complete file inventory")
        require(all(sha(package / n) == h for n, h in inventory.items()), "all package SHA256 bindings")
        source = MovieRecordSource.from_manifest(package / "input/manifest.json")
        source.preflight(FilterMethod.from_dict(plan["method"]))
        source_manifest = load(package / "input/manifest.json")
        require(source_manifest["rows"] == 2000 and source_manifest["eligible_rows"] == 120,
            "2000 input rows and 120 predicate-eligible occurrences")
        require("scoreSentiment" not in source_manifest["columns"], "execution records contain no gold label column")
        require(load(package / "source.json")["eligible_full_rows_and_positions_equal"] is True,
            "upstream Q3 full records/source positions equal; full dataset identity remains distinct")
        schedule = load(package / "schedule.json")
        identity = load(package / "package-identity.json")
        require(identity["runtime_source_lock_sha256"] == sha(HERE / "source.lock.json")
            and all(sha(HERE / n) == h for n, h in identity["preparation_files_sha256"].items()),
            "preparation source/config identities remain fixed")
        require(schedule["execution_authorized"] is False and schedule["allocated_calls"] == 0,
            "preparation creates no formal authorization or budget claim")
        require(len(schedule["units"]) == 35, "35 distinct query units")
        ids, counts, blocks, slots = set(), Counter(), defaultdict(list), defaultdict(list)
        budget = 0
        for summary in schedule["units"]:
            unit = load(package / summary["manifest"])
            require(sha(package / summary["manifest"]) == summary["sha256"],
                "query manifest identity " + unit["query_id"])
            expected = source.expected(unit["query_id"])
            calls = unit["stages"][0]["calls"]
            if (calls != expected or len(calls) != 120 or unit["model_call_upper_bound"] != 120
                    or summary["max_calls"] != 120 or unit["source_manifest_sha256"] != sha(source.manifest_path)):
                raise AssertionError("stage identity/budget/input binding mismatch")
            if any(row["call_id"] in ids for row in calls):
                raise AssertionError("call identity reused across query owners")
            ids.update(row["call_id"] for row in calls)
            counts[unit["backend"], unit["phase"]] += 1
            if unit["phase"] == "measured":
                blocks[unit["block"]].append(unit["backend"])
                slots[unit["backend"]].append(len(blocks[unit["block"]]) - 1)
                if unit["selected_profile"] is not None:
                    raise AssertionError("measurement profile selected before calibration")
            for config in unit["configurations"]:
                path = package / config["path"]
                if sha(path) != config["sha256"]:
                    raise AssertionError("config SHA256 mismatch")
                value = load(path)
                engine = value.get("engine", value.get("requested_engine"))
                if EngineSpec.from_dict(engine).max_model_calls != 120:
                    raise AssertionError("per-engine cap mismatch")
                if value["method"] != plan["method"] or value["generation"] != plan["generation"]:
                    raise AssertionError("method/generation drift")
                if unit["backend"] not in ("lotus-native", "imlane"):
                    read_config(path)  # Actual three-backend CLI parser; no environment/model construction.
                elif unit["backend"] == "imlane":
                    RecordsResources(**value["resources"])
                    if value["query_ids"] != [unit["query_id"]] or value["max_total_calls"] != 120:
                        raise AssertionError("IMLane per-query reservation mismatch")
            budget += len(calls)
        require(budget == len(ids) == schedule["max_model_calls"] == 4200,
            "4200 exact unique call identities equal stage-derived global upper bound")
        require(all(counts[b, "calibration"] == 2 and counts[b, "measured"] == 5
            for b in ["lotus-native", "semloom-core", "ray-data", "daft", "imlane"]),
            "five backends each have two calibration and five measured queries")
        require(len(blocks) == 5 and all(len(v) == len(set(v)) == 5 for v in blocks.values())
            and all(sorted(v) == list(range(5)) for v in slots.values()), "Latin block order balances all five positions")
        require(schedule["max_round_wall_s"] == 35 * 810 + 1800 == 30150,
            "wall ceiling includes every unit and external control/cleanup allowance")
        require(all(c["formal_run_argv"] is None and c["blocked_by"] for c in load(package / "commands.json")),
            "no standalone unit command can bypass the authorized campaign")
        require(all(c["standalone_worker_authorized"] is False
            and c["formal_campaign_argv_template"][-1] == "--execute-model"
            and c["formal_worker_argv_template"][-1] == "--execute-model"
            for c in load(package / "commands.json")), "all five arms have explicit gated campaign/worker argv templates")
        readiness = formal_check(package)
        write_private_json(evidence / "formal-check.json", readiness)
        require(not any("adapter" in value for value in readiness["blockers"]),
            "both new formal adapter entries are connected; placeholder runtime remains unqualified")
        responses = [dict(row_id=row["row_id"], text="False", prompt_tokens=1, completion_tokens=1)
            for row in source.expected("cpu-original-q3")]
        write_private_json(evidence / "synthetic-responses.json", responses)
        # Explicit artificial responses qualify source/formatter/parser/COUNT plumbing only.
        with patch.object(VllmEngine, "__init__", side_effect=AssertionError("model construction prohibited")), \
                patch("litellm.batch_completion", side_effect=AssertionError("network inference prohibited")):
            result = fixed_response_qualification(source, checkout, FilterMethod.from_dict(plan["method"]),
                GenerationSpec.from_dict(plan["generation"]), responses, query_id="cpu-original-q3")
            write_private_json(evidence / "original-q3-fixture.json", result)
            require(result["inference_calls"] == 0 and result["status"] == "completed"
                and all(result["native_reference"][k] is True for k in (
                    "messages_equal", "decisions_equal", "count_equal", "lm_stats_equal")),
                "120-row original LOTUS accessor vs records messages/parser/COUNT/usage fixture parity")
            try:
                fixed_response_qualification(source, checkout, FilterMethod.from_dict(plan["method"]),
                    GenerationSpec.from_dict(plan["generation"]), responses[:-1], query_id="cpu-missing")
            except ValueError:
                report["checks"].append("missing synthetic response rejected before execution")
            else:
                raise AssertionError("missing response accepted")
        tampered = evidence / "tampered-source"
        shutil.copytree(package / "input", tampered)
        with (tampered / "records.csv").open("a") as stream:
            stream.write("\n")
        try:
            MovieRecordSource.from_manifest(tampered / "manifest.json")
        except ValueError:
            report["checks"].append("modified input bytes rejected before model/owner allocation")
        else:
            raise AssertionError("tampered input accepted")
        require(result["result"]["positive_review_cnt"] == 0,
            "all-False fixture remains an artificial result, not model quality success")
        report.update(status="passed", query_manifests=35, exact_call_identities=4200,
            full_original_fixture_occurrences=120, max_model_calls_proposed=4200,
            measured_n_per_backend=5, p99=None, p99_99=None,
            p99_reason="five complete measured query units per backend; zero real runs here",
            parser_scope="three existing records selectors; IMLane data classes only, no author ABI/runtime probe",
            fixture_sha256=sha(evidence / "original-q3-fixture.json"))
    except BaseException as error:
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        raise
    finally:
        write_private_json(evidence / "verification.json", redact_json_values(report))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--sembench-checkout", required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    arguments = parser.parse_args()
    report = check(arguments.package, arguments.sembench_checkout, arguments.evidence_dir)
    print(json.dumps({k: report[k] for k in ("status", "real_model_calls", "query_manifests", "exact_call_identities")}))
