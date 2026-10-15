"""Query identity, resource release and repeated native-library fixture calls."""

from contextlib import contextmanager
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import threading
import time
import unittest

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.shared_request_budget import CellBudgetLedger
from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup


@contextmanager
def _persistent_lotus_http_fixture():
    """HTTP/1.1 fixture whose peer ports expose actual cross-query reuse."""
    requests = []
    state = dict(active=0, peak=0, errors=[], closed=False)
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with lock:
                identity = "persistent-fixture-" + str(len(requests))
                requests.append(
                    dict(request=body, response_id=identity, peer_port=self.client_address[1])
                )
                state["active"] += 1
                state["peak"] = max(state["peak"], state["active"])
            try:
                raw = json.dumps(
                    dict(
                        id=identity,
                        object="chat.completion",
                        created=0,
                        model=body["model"],
                        choices=[
                            dict(
                                index=0,
                                message=dict(role="assistant", content="ok"),
                                finish_reason="stop",
                            )
                        ],
                        usage=dict(prompt_tokens=2, completion_tokens=1, total_tokens=3),
                    )
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError) as error:
                with lock:
                    state["errors"].append(type(error).__name__)
            finally:
                with lock:
                    state["active"] -= 1

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.05))
    thread.start()
    try:
        yield (
            "http://127.0.0.1:" + str(server.server_port) + "/v1/chat/completions",
            requests,
            state,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        state["closed"] = not thread.is_alive()
        if not state["closed"]:
            raise RuntimeError("persistent HTTP/1.1 fixture did not close")


@unittest.skipUnless(
    os.environ.get("SEMLOOM_PERSISTENT_HTTP_ARM"),
    "requires one selected pinned LOTUS/native arm in an isolated process",
)
@unittest.skipUnless(
    sys.platform.startswith("linux"),
    "full query observations require the verified Linux clock; no substituted metrics",
)
class PersistentLotusHttpLibraries(unittest.TestCase):
    def test_two_full_lotus_queries_reuse_the_owner_http_pool(self):
        from tokenizers import Tokenizer, models
        from src.baselines.common.private_artifacts import write_private_json
        from src.baselines.common.redact import redact_json_values
        from src.semantic_methods.lotus.sdk import validate_source

        validate_source()
        arm = os.environ["SEMLOOM_PERSISTENT_HTTP_ARM"]
        self.assertIn(
            arm,
            ("lotus-method-native-ray-data", "lotus-method-native-daft", "lotus-method-semloom"),
        )
        root = Path(os.environ["SEMLOOM_PERSISTENT_OUTPUT"])
        root.mkdir(mode=0o700, parents=True, exist_ok=False)
        tokenizer = root / "fixture-tokenizer.json"
        Tokenizer(models.WordLevel({"[UNK]": 0}, unk_token="[UNK]")).save(str(tokenizer))
        records, failure, group = [], None, None
        requests, fixture = [], dict(active=0, errors=[], closed=False)
        try:
            with _persistent_lotus_http_fixture() as (url, requests, fixture):
                ledger = CellBudgetLedger.create(
                    root / "budget.sqlite",
                    AttemptBudget("persistent-lotus-http", 8),
                    deadline_utc=time.time() + 240,
                )
                with PersistentAdapterGroup(
                    (arm,),
                    plan=SemanticMapPlan("Return ok.", "fixture", 16),
                    model=FixedModelConfig(url, "fixture", 5000),
                    ledger=ledger,
                    root=root / "group",
                    options=NativeGraphOptions(concurrency=4, num_threads=8, source_blocks=2),
                    physical=(
                        RayMapConfig(
                            "unused",
                            2,
                            2,
                            2**21 + 24,
                            2**23,
                            payload_backend="daft",
                            response_mode="full",
                        )
                        if arm == "lotus-method-semloom"
                        else None
                    ),
                    ray_temp_root=Path(os.environ["SEMLOOM_PERSISTENT_RAY_ROOT"]),
                    query_timeout_s=20,
                    owner_timeout_s=240,
                    tokenizer_path=tokenizer,
                    lotus_observation_mode="performance",
                ) as group:
                    owner = group.owners[arm]
                    lm, execution = owner.lm, owner.execution
                    for number in range(2):
                        values = tuple(
                            dict(
                                row_id=str(number) + "-" + str(i),
                                text="query " + str(number) + " item " + str(i),
                            )
                            for i in range(4)
                        )
                        before = len(requests)
                        query = root / ("q" + str(number))
                        result = group.run(
                            arm,
                            unit_id=query.name,
                            root=query,
                            load_source=lambda: iter(values),
                            phase="measurement",
                            reference_outputs={row["row_id"]: "ok" for row in values},
                            allowed_outputs=("ok",),
                        )
                        self.assertIs(owner.lm, lm)
                        self.assertIs(owner.execution, execution)
                        self.assertEqual(
                            (result["status"], result["rows"], result["actual_posts"]),
                            ("passed", 4, 4),
                        )
                        self.assertEqual(len(requests) - before, 4)
                        self.assertEqual(result["quality"]["correct"], 4)
                        self.assertEqual(
                            (lm.stats.cache_hits, lm.stats.operator_cache_hits), (0, 0)
                        )
                        for usage in (lm.stats.physical_usage, lm.stats.virtual_usage):
                            self.assertEqual(
                                (usage.prompt_tokens, usage.completion_tokens, usage.total_tokens),
                                ((number + 1) * 8, (number + 1) * 4, (number + 1) * 12),
                            )
                        traces = [
                            json.loads(line)
                            for line in (query / "http-trace.jsonl").read_text().splitlines()
                        ]
                        self.assertEqual(len(traces), 4)
                        self.assertEqual({row["query_id"] for row in traces}, {query.name})
                        self.assertEqual({row["upstream_pool_generation"] for row in traces}, {0})
                        self.assertTrue(all(row["status"] == "completed" for row in traces))
                        if number:
                            self.assertGreater(
                                sum(row["upstream_connection_reused_count"] for row in traces), 0
                            )
                            self.assertTrue(
                                {r["peer_port"] for r in requests[:before]}
                                & {r["peer_port"] for r in requests[before:]}
                            )
                        lifecycle = result["persistent_lifecycle"]
                        self.assertEqual(lifecycle["completed_queries"], number + 1)
                        self.assertFalse(lifecycle["result_cache"])
                        if execution is not None:
                            self.assertEqual(lifecycle["core_jobs"], 0)
                            self.assertFalse(any(lifecycle["core_usage"].values()))
                        self.assertEqual((query / "complete-responses.jsonl").read_text(), "")
                        records.append(
                            dict(
                                query=query.name,
                                actual_posts=4,
                                lifecycle=lifecycle,
                                lm_stats=asdict(lm.stats),
                                traces=traces,
                                quality=result["quality"],
                            )
                        )
                    for key in ("owner_id", "lm_id", "execution_id", "ray_session_id"):
                        self.assertEqual(records[0]["lifecycle"][key], records[1]["lifecycle"][key])
                closed = json.loads((root / "group/group-summary.json").read_text())
                self.assertEqual(closed["status"], "passed")
                self.assertFalse(closed["cleanup_errors"])
                self.assertTrue(owner._closed)
                self.assertIsNone(owner.gateway._thread)
                self.assertEqual((len(requests), fixture["active"], fixture["errors"]), (8, 0, []))
        except BaseException as error:
            failure = type(error).__name__ + ": " + str(error)
            raise
        finally:
            summary = root / "group/group-summary.json"
            write_private_json(
                root / "fixture.json",
                redact_json_values(
                    dict(
                        schema="semloom.persistent_lotus_http_fixture.v1",
                        arm=arm,
                        passed=failure is None,
                        actual_posts=len(requests),
                        real_model_requests=0,
                        original_lotus_sem_map=True,
                        queries=records,
                        errors=[] if failure is None else [failure],
                        fixture=fixture,
                        requests=requests,
                        group_cleanup=json.loads(summary.read_text()) if summary.exists() else None,
                        scope="two complete original LOTUS queries through the production persistent group; HTTP/1.1 CPU fixture",
                    )
                ),
            )
