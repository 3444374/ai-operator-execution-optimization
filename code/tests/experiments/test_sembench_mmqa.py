"""Finite Q2b inputs and unchanged native Join/JSON Extract using localhost only."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import content_digest, file_identity
from src.baselines.image.sembench_animals import image_identity
from src.baselines.image.sembench_mmqa import (MmqaDataset, SCHEMA, SEMBENCH_COMMIT,
    call_budget)


def fixture_dataset(root, checkout):
    from PIL import Image
    root.mkdir()
    (root / "images").mkdir()
    (root / "ap_warrior.csv").write_text("ID,Track\n10,fixture-keep\n20,fixture-drop\n")
    for name, color in (("a.png", "white"), ("b.png", "white"), ("c.png", "black")):
        Image.new("RGB", (12, 10), color).save(root / "images" / name)
    names = [p.name for p in (root / "images").iterdir()]
    value = dict(schema=SCHEMA, sembench_commit=SEMBENCH_COMMIT, role="software_fixture",
        data_path=str(root), table=file_identity(root / "ap_warrior.csv"),
        images=[dict(name=n, image=image_identity(root / "images" / n)) for n in names])
    value["sha256"] = content_digest(value)
    manifest = root / "manifest.json"
    manifest.write_text(json.dumps(value))
    return MmqaDataset(checkout, manifest, value["sha256"])


class MmqaInputsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.dataset = fixture_dataset(self.root / "input", self.root)

    def test_call_cap_covers_all_positive_join_including_extract(self):
        value = call_budget(25, 8)
        self.assertEqual((value["join"], value["extract_maximum"], value["total_maximum"]), (200, 200, 400))
        for counts in ((0, 3), (3, False), (3.0, 1)):
            with self.assertRaises(ValueError):
                call_budget(*counts)

    def test_fixture_cannot_claim_official_generated_inputs(self):
        value = json.loads(self.dataset.manifest.read_text())
        value["role"] = "upstream_generated_q2b_slice"
        value.pop("sha256")
        value["sha256"] = content_digest(value)
        self.dataset.manifest.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, "provenance"):
            MmqaDataset(self.root, self.dataset.manifest, value["sha256"]).read()

    def test_changed_table_is_rejected_before_native_calls(self):
        (self.root / "input/ap_warrior.csv").write_text("ID,Track\n10,changed\n")
        with self.assertRaisesRegex(ValueError, "table bytes"):
            self.dataset.read()

    def test_modified_image_is_rejected(self):
        from PIL import Image
        Image.new("RGB", (12, 10), "blue").save(self.root / "input/images/a.png")
        with self.assertRaisesRegex(ValueError, "pixels"):
            self.dataset.read()

    def test_added_image_cannot_silently_expand_cartesian_join(self):
        from PIL import Image
        Image.new("RGB", (12, 10), "red").save(self.root / "input/images/new.png")
        with self.assertRaisesRegex(ValueError, "enumeration"):
            self.dataset.read()

    def test_manifest_enumeration_is_checked_without_sorting_the_native_query(self):
        value = json.loads(self.dataset.manifest.read_text())
        value["images"].reverse()
        value.pop("sha256")
        value["sha256"] = content_digest(value)
        self.dataset.manifest.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, "enumeration"):
            MmqaDataset(self.root, self.dataset.manifest, value["sha256"]).read()


@contextmanager
def fixture_server(*, mode="selective", status=200, cot=False):
    calls, lock = [], threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            value = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with lock:
                calls.append(value)
            extracting = value.get("response_format") == {"type": "json_object"}
            content = (json.dumps(dict(color="white")) if extracting else
                       "Answer: " + str(mode == "positive" or mode == "selective" and "fixture-keep" in str(value["messages"])))
            if cot and not extracting:
                # Opposite labels in reasoning must not replace the final Answer.
                content = "Reasoning:\nBoth True and False are considered.\n\n" + content
            response = (dict(error=dict(message="software auth fixture", type="auth")) if status != 200 else
                dict(id="fixture", object="chat.completion", created=1, model="fixture-mmqa",
                    choices=[dict(index=0, message=dict(role="assistant", content=content), finish_reason="stop")],
                    usage=dict(prompt_tokens=2, completion_tokens=1, total_tokens=3)))
            raw = json.dumps(response).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1/chat/completions", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError("Q2b localhost fixture failed to stop")


@unittest.skipUnless(os.environ.get("SEMLOOM_TEST_SEMBENCH_ROOT"), "requires pinned SemBench and actual LOTUS1.2.4; no model")
class MmqaNativeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.checkout = Path(os.environ["SEMLOOM_TEST_SEMBENCH_ROOT"])
        self.dataset = fixture_dataset(self.root / "input", self.checkout)
        from tokenizers import Tokenizer, models
        self.tokenizer = self.root / "tokenizer.json"
        Tokenizer(models.WordLevel({"[UNK]": 0}, unk_token="[UNK]")).save(str(self.tokenizer))

    def execute(self, endpoint, *, max_prepared_bytes=8 * 1024 * 1024):
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        from src.experiments.sembench_mmqa import run_query
        self.ledger = CellBudgetLedger.create(self.root / "budget.sqlite", AttemptBudget("fixture-q2b", 12), deadline_utc=None)
        self.ledger.reserve_unit("q2b", 12)
        self.unit = self.ledger.claim_shared_unit("q2b")
        model = FixedModelConfig(endpoint, "fixture-mmqa", 5000)
        profile = dict(model=dict(id=model.model_id, revision="software-fixture", dtype="software-fixture"),
            generation=dict(temperature=0, top_p=1, max_tokens=32, retries=0),
            service_preprocess=dict(processor_revision="software-fixture", parameters=dict(no_model=True)),
            tokenizer_sha256=file_identity(self.tokenizer)["sha256"])
        try:
            return run_query(self.dataset, model, output=self.root / "result", unit=self.unit,
                tokenizer=self.tokenizer, profile=profile, concurrency=2, max_tokens=32,
                max_request_bytes=1024 * 1024, max_prepared_bytes=max_prepared_bytes,
                max_result_bytes=1024 * 1024)
        finally:
            self.ledger.close_shared_unit("q2b")

    def test_selective_original_join_only_releases_matches_to_json_extract_and_original_evaluator(self):
        with fixture_server() as (endpoint, calls):
            result = self.execute(endpoint)
        self.assertEqual((len(calls), result["model_calls"]), (9, 9))
        self.assertEqual([s["input_members"] for s in result["stages"]], [6, 3])
        self.assertEqual([s["output_rows"] for s in result["stages"]], [3, 3])
        self.assertEqual(sum(c.get("response_format") == {"type": "json_object"} for c in calls), 3)
        self.assertLessEqual(result["http_peak"], 2)
        self.assertEqual(result["unknown_or_failed_attempts"], 0)
        self.assertEqual(set(result["quality"]["original_metric"]), {"precision", "recall", "f1_score"})
        original = json.loads((self.root / "result/original-result.json").read_text())
        self.assertEqual(original["columns"], ["ID", "image_id", "color"])
        self.assertTrue(all(row[0] == 10 and row[2] == "white" for row in original["data"]))
        requests = [json.loads(s) for s in (self.root / "result/requests.jsonl").read_text().splitlines()]
        self.assertTrue(any(len(r["equivalent_members"]) > 1 for r in requests))
        footer = json.loads((self.root / "result/observation-footer.json").read_text())
        self.assertEqual(footer["status"], "completed")
        for name, identity in footer["files"].items():
            self.assertEqual(file_identity(self.root / "result" / name), identity)

    def test_all_positive_join_consumes_the_complete_conservative_cap(self):
        with fixture_server(mode="positive") as (endpoint, calls):
            result = self.execute(endpoint)
        self.assertEqual((len(calls), result["model_calls"], self.unit.remaining), (12, 12, 0))
        self.assertEqual([s["output_rows"] for s in result["stages"]], [6, 6])

    def test_empty_join_preserves_upstream_missing_color_error_without_fabricating_results(self):
        with fixture_server(mode="negative") as (endpoint, calls):
            with self.assertRaises(KeyError):
                self.execute(endpoint)
        self.assertEqual(len(calls), 6)
        self.assertFalse(any(c.get("response_format") == {"type": "json_object"} for c in calls))
        summary = json.loads((self.root / "result/summary.json").read_text())
        self.assertEqual((summary["status"], summary["model_calls"]), ("failed", 6))
        self.assertEqual(json.loads((self.root / "result/observation-footer.json").read_text())["status"], "failed")
        self.assertFalse((self.root / "result/original-result.json").exists())

    def test_http_failure_is_charged_once_and_never_releases_extract(self):
        with fixture_server(status=401) as (endpoint, calls):
            with self.assertRaises(Exception):
                self.execute(endpoint)
        summary = json.loads((self.root / "result/summary.json").read_text())
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(summary["model_calls"], len(calls))
        self.assertLessEqual(len(calls), 6)
        self.assertGreater(summary["unknown_or_failed_attempts"], 0)
        self.assertEqual([s["stage_id"] for s in summary["stages"]], ["join"])

    def test_native_observers_are_restored_after_failure(self):
        import importlib
        from lotus.models import LM
        join = importlib.import_module("lotus.sem_ops.sem_join").SemJoinDataframe
        extract = importlib.import_module("lotus.sem_ops.sem_extract").SemExtractDataFrame
        saved = join.__call__, extract.__call__, LM.__call__
        with fixture_server(status=401) as (endpoint, calls):
            with self.assertRaises(Exception):
                self.execute(endpoint)
        self.assertGreater(len(calls), 0, "the test must reach native HTTP, not pass on an import error")
        summary = json.loads((self.root / "result/summary.json").read_text())
        self.assertEqual(summary["model_calls"], len(calls))
        self.assertGreater(summary["unknown_or_failed_attempts"], 0)
        self.assertEqual((join.__call__, extract.__call__, LM.__call__), saved)

    def test_original_evaluator_checks_color_against_unchanged_full_gold(self):
        import pandas as pd
        from src.baselines.image.sembench_mmqa import evaluate_original, GOLD
        from src.baselines.image.sembench_animals import upstream_bytes
        gold = json.loads(upstream_bytes(self.checkout, GOLD))["ground_truth"]
        good = pd.DataFrame(gold, columns=["ID", "image_id", "color"])
        for label, result, expected in (("good", good, 1), ("wrong-color", good.assign(color="wrong-color"), 0)):
            with self.subTest(label=label):
                output = self.root / label
                output.mkdir()
                quality = evaluate_original(self.checkout, result, output)
                self.assertEqual(quality["original_metric"]["f1_score"], expected)
        with self.assertRaisesRegex(ValueError, "exactly ID/image_id/color"):
            evaluate_original(self.checkout, good[["ID", "image_id"]], self.root)

    def test_prepared_byte_limit_stops_before_any_http_dispatch(self):
        with fixture_server() as (endpoint, calls):
            with self.assertRaisesRegex(ValueError, "prepared message bytes"):
                self.execute(endpoint, max_prepared_bytes=1)
        self.assertEqual((len(calls), self.unit.attempts), (0, 0))
        self.assertEqual(json.loads((self.root / "result/summary.json").read_text())["status"], "failed")

    def test_required_event_sink_error_stops_before_http_and_preserves_failed_footer(self):
        from src.experiments.mmqa_q2b_observation import Q2bObservation
        with fixture_server() as (endpoint, calls), patch.object(Q2bObservation, "emit", side_effect=OSError("software sink failure")):
            with self.assertRaisesRegex(OSError, "software sink failure"):
                self.execute(endpoint)
        self.assertEqual((len(calls), self.unit.attempts), (0, 0))
        self.assertEqual(json.loads((self.root / "result/observation-footer.json").read_text())["status"], "failed")

    def test_supervisor_deadline_closes_budget_and_cleans_local_worker(self):
        import sys
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        from src.experiments.postgresql.query_supervisor import supervise
        root = self.root / "supervised"
        (root / "unit").mkdir(parents=True)
        (root / "unit/unit-reserved.json").write_text("{}")
        budget = CellBudgetLedger.create(root / "budget.sqlite", AttemptBudget("deadline-fixture", 1), deadline_utc=None)
        budget.reserve_unit("q2b", 1)
        unit = budget.claim_shared_unit("q2b")
        with self.assertRaises(TimeoutError):
            supervise([sys.executable, "-c", "import time; time.sleep(30)"], root,
                lambda: budget.close_shared_unit("q2b"), query_timeout_s=1,
                preparation_s=1, finish_s=1, grace_s=.1, max_duration_s=.2)
        report = json.loads((root / "supervisor.json").read_text())
        self.assertEqual(report["remaining_owned_pids"], [])
        self.assertFalse(report["remote_compute_end_known"])
        with self.assertRaisesRegex(Exception, "closed"):
            unit.reserve("0" * 64)


if __name__ == "__main__":
    unittest.main()
