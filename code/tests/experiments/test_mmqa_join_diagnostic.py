"""Exactly24 synthetic Join calls, transport stops, no Extract and unchanged Q2b."""

from collections import Counter
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib
import json
import os
from pathlib import Path
import runpy
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import content_digest, file_identity
from src.experiments.mmqa_join_diagnostic import (ARMS, TOTAL_ATTEMPTS, UNIT, forbid_extract,
    load_selection, prepare_selection, run_diagnostic)
from tests.experiments.test_mmqa_service_observer import full_fixture_dataset


@contextmanager
def diagnostic_server(*, fail_at=None, unknown=False):
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def do_POST(self):
            value = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append(value)
            number = len(calls)
            if number == fail_at and unknown:
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            status = 500 if number == fail_at else 200
            cot = "Let's think step by step." in value["messages"][0]["content"]
            content = "Reasoning:\nTrue is rejected.\n\nAnswer: False" if cot else "Answer: True"
            response = (dict(error=dict(message="fixture transport failure", type="fixture")) if status == 500 else
                dict(id=f"fixture-{number}", object="chat.completion", created=1, model="fixture-mmqa",
                    choices=[dict(index=0, message=dict(role="assistant", content=content), finish_reason="stop")],
                    usage=dict(prompt_tokens=2, completion_tokens=1, total_tokens=3)))
            raw = json.dumps(response).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1/chat/completions", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError("diagnostic fixture failed to stop")
    with socket.socket() as probe:
        if probe.connect_ex(("127.0.0.1", server.server_port)) == 0:
            raise RuntimeError("diagnostic fixture socket still accepts connections")


def diagnostic_dataset(root, checkout):
    dataset = full_fixture_dataset(root, checkout)
    table = root / "ap_warrior.csv"
    table.write_text("ID,Track\n" + "".join(f"{i},fixture-track-{i % 6}\n" for i in range(13)))
    value = json.loads(dataset.manifest.read_text())
    value["table"] = file_identity(table)
    value.pop("sha256")
    value["sha256"] = content_digest(value)
    dataset.manifest.write_text(json.dumps(value))
    return type(dataset)(checkout, dataset.manifest, value["sha256"])


class DiagnosticCLIErrorTests(unittest.TestCase):
    def test_new_cli_redacts_errors_and_retains_nonzero_exit(self):
        import io
        marker = "synthetic-diagnostic-sensitive-value"
        script = Path(__file__).resolve().parents[2] / "scripts/experiments/run_mmqa_join_diagnostic.py"
        with patch("sys.stderr", new_callable=io.StringIO) as stream, \
                patch("src.experiments.mmqa_join_diagnostic.main", side_effect=ValueError("password=" + marker)):
            with self.assertRaises(SystemExit) as caught:
                runpy.run_path(str(script), run_name="__main__")
            self.assertEqual(caught.exception.code, 1)
            self.assertNotIn(marker, stream.getvalue())
            self.assertIn("password=***", stream.getvalue())


@unittest.skipUnless(os.environ.get("SEMLOOM_TEST_SEMBENCH_ROOT"), "requires actual pinned LOTUS/SemBench; no model")
class DiagnosticNativeTests(unittest.TestCase):
    def setUp(self):
        from tokenizers import Tokenizer, models
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.checkout = Path(os.environ["SEMLOOM_TEST_SEMBENCH_ROOT"])
        self.dataset = diagnostic_dataset(self.root / "input", self.checkout)
        names = [r["name"] for r in self.dataset.read()["images"]][:2]
        self.selection = prepare_selection(self.dataset, names)
        self.tokenizer = self.root / "tokenizer.json"
        Tokenizer(models.WordLevel({"[UNK]": 0}, unk_token="[UNK]")).save(str(self.tokenizer))

    def execute(self, endpoint, *, cap=24, max_prepared_bytes=32 * 1024 * 1024, maximum=None):
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        model = FixedModelConfig(endpoint, "fixture-mmqa", 5000)
        profile = dict(model=dict(id=model.model_id, revision="software-fixture", dtype="software-fixture"),
            generation=dict(temperature=0, top_p=1, max_tokens=32, retries=0),
            service_preprocess=dict(processor_revision="software-fixture", parameters=dict(no_model=True)),
            tokenizer_sha256=file_identity(self.tokenizer)["sha256"])
        self.ledger = CellBudgetLedger.create(self.root / "budget.sqlite", AttemptBudget("diagnostic-fixture", cap), deadline_utc=None)
        self.ledger.reserve_unit(UNIT, cap if maximum is None else maximum)
        self.unit = self.ledger.claim_shared_unit(UNIT)
        try:
            return run_diagnostic(self.dataset, self.selection, model, output=self.root / "result", unit=self.unit,
                tokenizer=self.tokenizer, profile=profile, max_tokens=32, max_request_bytes=1024 * 1024,
                max_prepared_bytes=max_prepared_bytes, max_result_bytes=1024 * 1024)
        finally:
            self.ledger.close_shared_unit(UNIT)

    def test_exact24_same_inputs_and_generation_both_arms_no_extract_and_complete_footer(self):
        Extract = importlib.import_module("lotus.sem_ops.sem_extract").SemExtractDataFrame
        original = Extract.__call__
        with diagnostic_server() as (endpoint, calls), \
                patch("src.baselines.image.sembench_mmqa.run_original_query", side_effect=AssertionError("full Q2b prohibited")), \
                patch.object(Extract, "__call__", side_effect=AssertionError("Extract prohibited")) as extract:
            result = self.execute(endpoint)
            extract.assert_not_called()
            self.assertEqual(len(calls), 24)
            self.assertFalse(any("response_format" in row for row in calls))
            self.assertTrue(all({k: v for k, v in row.items() if k != "messages"} ==
                                {k: v for k, v in calls[0].items() if k != "messages"} for row in calls))
            for i in range(0, 24, 2):
                self.assertEqual(calls[i]["messages"][1:], calls[i + 1]["messages"][1:])
                self.assertNotEqual(calls[i]["messages"][0], calls[i + 1]["messages"][0])
        self.assertIs(Extract.__call__, original)
        self.assertEqual((result["attempts"], result["known_http_completed"], result["unknown_or_failed_attempts"]), (24, 24, 0))
        self.assertEqual(Counter(r["arm"] for r in result["results"]), {arm: 12 for arm in ARMS})
        by_pair = Counter((r["image_name"], r["Track"]) for r in result["results"])
        self.assertEqual((len(by_pair), set(by_pair.values())), (12, {2}))
        self.assertTrue(all(r["decision"] == (r["arm"] == "legacy-string") for r in result["results"]))
        footer = json.loads((self.root / "result/observation-footer.json").read_text())
        for name, expected in footer["files"].items():
            self.assertEqual(file_identity(self.root / "result" / name), expected)
        with self.assertRaisesRegex(Exception, "closed"):
            self.unit.reserve("0" * 64)

    def test_known_http_failure_stops_before_next_pair_or_arm_and_remains_charged(self):
        with diagnostic_server(fail_at=7) as (endpoint, calls):
            with self.assertRaises(Exception):
                self.execute(endpoint)
        self.assertEqual((len(calls), self.unit.attempts), (7, 7))
        value = json.loads((self.root / "result/summary.json").read_text())
        self.assertEqual((value["status"], value["known_http_completed"], value["unknown_or_failed_attempts"]), ("failed", 6, 1))
        self.assertEqual(len(value["results"]), 6)
        self.assertEqual(value["extract_calls"], 0)

    def test_unknown_response_stops_without_retry_or_refund(self):
        with diagnostic_server(fail_at=7, unknown=True) as (endpoint, calls):
            with self.assertRaises(Exception):
                self.execute(endpoint)
        value = json.loads((self.root / "result/summary.json").read_text())
        self.assertEqual((len(calls), self.unit.attempts, value["unconfirmed_responses"]), (7, 7, 1))
        self.assertEqual(value["unknown_or_failed_attempts"], 1)
        trace = [json.loads(s) for s in (self.root / "result/http-trace.jsonl").read_text().splitlines()]
        self.assertEqual(len(trace), 7)
        self.assertTrue(all(r["retry_count"] == 0 for r in trace))

    def test_extract_guard_restores_after_exception(self):
        Extract = importlib.import_module("lotus.sem_ops.sem_extract").SemExtractDataFrame
        original = Extract.__call__
        with self.assertRaisesRegex(RuntimeError, "Extract is prohibited"):
            with forbid_extract():
                Extract.__call__(object())
        self.assertIs(Extract.__call__, original)

    def test_unexpected_prompt_is_rejected_before_inference(self):
        join = importlib.import_module("lotus.sem_ops.sem_join")
        def unexpected(*args, **kwargs):
            lm = args[6]
            return lm([[dict(role="system", content="unexpected probe"), dict(role="user", content="wrong input")]])
        with diagnostic_server() as (endpoint, calls), patch.object(join, "sem_join", side_effect=unexpected):
            with self.assertRaises(Exception):
                self.execute(endpoint)
        self.assertEqual((len(calls), self.unit.attempts), (0, 0))

    def test_partial_allocation_cannot_start_inference(self):
        with diagnostic_server() as (endpoint, calls):
            with self.assertRaisesRegex(ValueError, "fresh total24"):
                self.execute(endpoint, maximum=12)
        self.assertEqual((len(calls), self.unit.attempts), (0, 0))

    def test_prepared_byte_limit_stops_with_failed_footer_and_zero_attempts(self):
        with diagnostic_server() as (endpoint, calls):
            with self.assertRaisesRegex(ValueError, "prepared messages"):
                self.execute(endpoint, max_prepared_bytes=1)
        self.assertEqual((len(calls), self.unit.attempts), (0, 0))
        self.assertEqual(json.loads((self.root / "result/observation-footer.json").read_text())["status"], "failed")

    def test_selection_binds_two_images_all_six_tracks_and_parent_identity(self):
        path = self.root / "selection.json"
        path.write_text(json.dumps(self.selection))
        self.assertEqual(load_selection(self.dataset, path, self.selection["sha256"]), self.selection)
        self.assertEqual((len(self.selection["images"]), len(self.selection["tracks"])), (2, 6))
        self.assertEqual(sum(len(r["source_members"]) for r in self.selection["tracks"]), 13)
        value = dict(self.selection, maximum_attempts=25)
        value.pop("sha256")
        value["sha256"] = content_digest(value)
        path.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, "fixed plan"):
            load_selection(self.dataset, path, value["sha256"])
        for images in (["missing.png", "also-missing.png"], [self.selection["images"][0]["name"]] * 2):
            with self.assertRaises(ValueError):
                prepare_selection(self.dataset, images)
