"""Complete 650-call native fixture and 325-attempt transport/accounting fixture."""

import asyncio
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import content_digest, file_identity
from src.baselines.image.sembench_animals import image_identity
from src.baselines.image.sembench_mmqa import MmqaDataset, SCHEMA, SEMBENCH_COMMIT
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from tests.experiments.test_processor_receipt import (fixed_profiles, install_receipt,
    load_receipt, service_stubs)


def full_fixture_dataset(root, checkout):
    from PIL import Image
    root.mkdir()
    (root / "images").mkdir()
    (root / "ap_warrior.csv").write_text("ID,Track\n" + "".join(f"{i},fixture-{i}\n" for i in range(13)))
    for i in range(25):
        Image.new("RGB", (12, 10), (i, 0, 0)).save(root / "images" / f"{i}.png")
    names = [p.name for p in (root / "images").iterdir()]
    value = dict(schema=SCHEMA, sembench_commit=SEMBENCH_COMMIT, role="software_fixture",
        data_path=str(root), table=file_identity(root / "ap_warrior.csv"),
        images=[dict(name=n, image=image_identity(root / "images" / n)) for n in names])
    value["sha256"] = content_digest(value)
    manifest = root / "manifest.json"
    manifest.write_text(json.dumps(value))
    return MmqaDataset(checkout, manifest, value["sha256"])


@contextmanager
def observed_service(root, *, failures=False):
    """Only the model/processor and renderer API shapes are stubbed; HTTP is real."""
    receipt, calls, lock = load_receipt(), [], threading.Lock()
    profiles = root / "profiles.jsonl"
    with patch.dict(os.environ, Q7_PROCESSOR_RECEIPT=str(profiles)), service_stubs() as stubs:
        install_receipt(receipt, stubs)
        fixed_profiles(stubs)
        rows = [json.loads(s) for s in profiles.read_text().splitlines()]
        if [r["kind"] for r in rows] != ["processor"] * 3 + ["loaded_service_model_config"]:
            raise RuntimeError("fixture service cannot accept POST before actual identity receipts")
        renderer = stubs.renderer()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                value = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with lock:
                    ordinal = len(calls) + 1
                    calls.append(value)
                identifiers = dict(image=[f"fixture-native-uuid-{ordinal}"])
                returned = renderer._process_mm_uuids({}, {}, identifiers, str(ordinal))
                if returned is not identifiers:
                    raise AssertionError("native UUID result identity changed")
                if failures and ordinal == 325:
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                status = 500 if failures and ordinal > 252 else 200
                extracting = value.get("response_format") == {"type": "json_object"}
                content = json.dumps(dict(color="white")) if extracting else "Answer: True"
                response = (dict(error=dict(message="fixture service error", type="fixture")) if status == 500 else
                    dict(id=f"fixture-{ordinal}", object="chat.completion", created=1, model="fixture-mmqa",
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
        thread = threading.Thread(target=server.serve_forever, name="fixture-profile-service")
        thread.start()
        endpoint = f"http://127.0.0.1:{server.server_port}/v1/chat/completions"
        try:
            yield endpoint, calls, profiles
        finally:
            server.shutdown()
            server.server_close()
            thread.join(5)
            if thread.is_alive():
                raise RuntimeError("fixture profile service did not stop")
    with socket.socket() as connection:
        if connection.connect_ex(("127.0.0.1", server.server_port)) == 0:
            raise RuntimeError("fixture service socket still accepts connections")


class MmqaServiceAccountingTests(unittest.TestCase):
    def test_325_attempts_keep_72_http_errors_and_one_unconfirmed_response_without_retry(self):
        import aiohttp
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = CellBudgetLedger.create(root / "budget.sqlite", AttemptBudget("fixture-accounting", 650), deadline_utc=None)
            ledger.reserve_unit("q2b", 650)
            unit = ledger.claim_shared_unit("q2b")
            def before(_route, body):
                unit.reserve(hashlib.sha256(body).hexdigest())
            try:
                with observed_service(root, failures=True) as (upstream, calls, profiles):
                    with ObservationGateway(routes=(GatewayRoute("q2b", "fixture", upstream),),
                            trace_path=root / "trace.jsonl", request_timeout_s=10,
                            before_forward=before, preserve_http=True) as gateway:
                        endpoint = gateway.endpoint_url("q2b", "fixture")
                        async def exercise():
                            limit = asyncio.Semaphore(4)
                            async with aiohttp.ClientSession() as client:
                                async def send(i):
                                    async with limit:
                                        try:
                                            async with client.post(endpoint, json=dict(model="fixture-mmqa", ordinal=i)) as response:
                                                await response.read()
                                                return response.status
                                        except aiohttp.ServerDisconnectedError:
                                            return "unconfirmed"
                                return await asyncio.gather(*(send(i) for i in range(325)))
                        outcomes = asyncio.run(exercise())
                        trace = gateway.snapshot()
                    self.assertEqual(len(calls), 325)
                    self.assertEqual(len(profiles.read_text().splitlines()), 4)
            finally:
                ledger.close_shared_unit("q2b")
            self.assertEqual((outcomes.count(200), outcomes.count(500), outcomes.count("unconfirmed")), (252, 72, 1))
            self.assertEqual((unit.attempts, len(trace)), (325, 325))
            self.assertEqual(sum(r["upstream_response_status"] == 200 for r in trace), 252)
            self.assertEqual(sum(r["upstream_response_status"] == 500 for r in trace), 72)
            unknown = [r for r in trace if r["upstream_response_status"] is None]
            self.assertEqual(len(unknown), 1)
            self.assertEqual(unknown[0]["error_type"], "ServerDisconnectedError")
            self.assertEqual(sum(r["status"] != "completed" for r in trace), 73)
            with self.assertRaises(BudgetExhausted):
                unit.reserve("0" * 64)
            self.assertEqual(unit.attempts, 325)
            self.assertEqual(ledger.snapshot()["allocated_requests"], 650)
            self.assertEqual(len((root / "trace.jsonl").read_text().splitlines()), 325)


@unittest.skipUnless(os.environ.get("SEMLOOM_TEST_SEMBENCH_ROOT"), "requires pinned SemBench and actual LOTUS1.2.4; no model")
class MmqaServiceNativeTests(unittest.TestCase):
    def test_original_325_join_and_325_extract_keep_all_request_traces_and_cleanup(self):
        from tokenizers import Tokenizer, models
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.experiments.sembench_mmqa import run_query
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = full_fixture_dataset(root / "input", Path(os.environ["SEMLOOM_TEST_SEMBENCH_ROOT"]))
            tokenizer = root / "tokenizer.json"
            Tokenizer(models.WordLevel({"[UNK]": 0}, unk_token="[UNK]")).save(str(tokenizer))
            ledger = CellBudgetLedger.create(root / "budget.sqlite", AttemptBudget("fixture-full-q2b", 650), deadline_utc=None)
            ledger.reserve_unit("q2b", 650)
            unit = ledger.claim_shared_unit("q2b")
            with observed_service(root) as (endpoint, calls, profiles):
                model = FixedModelConfig(endpoint, "fixture-mmqa", 10000)
                profile = dict(model=dict(id=model.model_id, revision="software-fixture", dtype="software-fixture"),
                    generation=dict(temperature=0, top_p=1, max_tokens=32, retries=0),
                    service_preprocess=dict(processor_revision="software-fixture", parameters=dict(no_model=True)),
                    tokenizer_sha256=file_identity(tokenizer)["sha256"])
                try:
                    result = run_query(dataset, model, output=root / "result", unit=unit,
                        tokenizer=tokenizer, profile=profile, concurrency=4, max_tokens=32,
                        max_request_bytes=1024 * 1024, max_prepared_bytes=32 * 1024 * 1024,
                        max_result_bytes=1024 * 1024)
                finally:
                    ledger.close_shared_unit("q2b")
                self.assertEqual(len(calls), 650)
                self.assertEqual(len(profiles.read_text().splitlines()), 4)
                self.assertEqual(sum(c.get("response_format") == {"type": "json_object"} for c in calls), 325)
            self.assertEqual((result["status"], result["model_calls"], result["unknown_or_failed_attempts"]), ("completed", 650, 0))
            self.assertEqual([s["input_members"] for s in result["stages"]], [325, 325])
            self.assertEqual([s["output_rows"] for s in result["stages"]], [325, 325])
            for name in ("requests.jsonl", "responses.jsonl", "http-trace.jsonl"):
                self.assertEqual(len((root / "result" / name).read_text().splitlines()), 650)
            footer = json.loads((root / "result/observation-footer.json").read_text())
            self.assertEqual(footer["status"], "completed")
            for name, identity in footer["files"].items():
                self.assertEqual(file_identity(root / "result" / name), identity)
            self.assertIn("original_metric", result["quality"])
            with self.assertRaises(BudgetExhausted):
                unit.reserve("0" * 64)
            self.assertEqual(unit.attempts, 650)
