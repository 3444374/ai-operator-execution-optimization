"""Opt-in original query/evaluator checks with real libraries and local HTTP only."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import threading
import time
import unittest
import sqlite3

from src.baselines.common.private_artifacts import write_private_json
from src.baselines.common.redact import redact_json_values
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup
from src.experiments.postgresql.sembench_queries import LOTUS_MOVIE_ARMS, MovieQuery
from tests.experiments.test_sembench_queries import fixture_dataset


@contextmanager
def movie_fixture_server(*, status=200, limit=120):
    records, lock = [], threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            value = json.loads(raw)
            with lock:
                if len(records) >= limit:
                    self.send_error(500, "software fixture allowance exhausted")
                    return
                counter = os.environ.get("SEMLOOM_MOVIE_POST_COUNTER")
                if counter:
                    with sqlite3.connect(counter, timeout=2) as connection:
                        connection.execute("BEGIN IMMEDIATE")
                        total, maximum = connection.execute(
                            "SELECT used, maximum FROM posts"
                        ).fetchone()
                        if total >= maximum:
                            self.send_error(500, "shared software fixture allowance exhausted")
                            return
                        connection.execute("UPDATE posts SET used=used+1")
                records.append(value)
            content = "True" if "fixture-positive" in str(value["messages"]) else "False"
            body = (
                dict(error=dict(message="software auth fixture", type="auth"))
                if status == 401
                else dict(
                    id="fixture",
                    object="chat.completion",
                    created=1,
                    model="fixture",
                    choices=[
                        dict(
                            index=0,
                            message=dict(role="assistant", content=content),
                            finish_reason="stop",
                        )
                    ],
                    usage=dict(prompt_tokens=2, completion_tokens=1, total_tokens=3),
                )
            )
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1/chat/completions", records
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError("Movie HTTP fixture did not stop")


@unittest.skipUnless(
    os.environ.get("SEMLOOM_MOVIE_ACTUAL_ARMS"),
    "requires explicit no-model native runtime and pinned SemBench checkout",
)
class MovieLibraryTests(unittest.TestCase):
    def test_original_three_queries_and_evaluators_preserve_occurrences_and_native_owners(self):
        arms = tuple(os.environ["SEMLOOM_MOVIE_ACTUAL_ARMS"].split(","))
        self.assertTrue(1 <= len(arms) <= 3 and set(arms) <= set(LOTUS_MOVIE_ARMS))
        output = Path(os.environ["SEMLOOM_MOVIE_FIXTURE_OUTPUT"])
        output.mkdir(parents=True, exist_ok=False)
        dataset = fixture_dataset(
            output / "workload", Path(os.environ["SEMLOOM_TEST_SEMBENCH_ROOT"])
        )
        completed = json.loads(os.environ.get("SEMLOOM_MOVIE_RESUME_COMPLETED", "{}"))
        queries = json.loads(os.environ.get("SEMLOOM_MOVIE_QUERY_IDS", "[1,2,3]"))
        self.assertTrue(queries and set(queries) <= {1, 2, 3})
        schedule = [(arm, q) for arm in arms for q in queries if q not in completed.get(arm, [])]
        expected_posts = sum(8 if q == 1 else 6 for _, q in schedule)
        ledger = CellBudgetLedger.create(
            output / "budget.sqlite",
            AttemptBudget("movie-software", expected_posts),
            deadline_utc=time.time() + 240,
        )
        options = NativeGraphOptions(
            **json.loads(os.environ.get("SEMLOOM_MOVIE_NATIVE_OPTIONS", "{}"))
        )
        ray_root = (
            Path(os.environ["SEMLOOM_MOVIE_RAY_ROOT"])
            if any(a in ("lotus-method-native-ray-data", "lotus-method-semloom") for a in arms)
            else None
        )
        results = []
        with movie_fixture_server(limit=expected_posts) as (endpoint, requests):
            with PersistentAdapterGroup(
                arms,
                plan=SemanticMapPlan("original Movie profile", "fixture", 8),
                model=FixedModelConfig(endpoint, "fixture", 10000),
                ledger=ledger,
                root=output / "group",
                options=options,
                physical=RayMapConfig("resolved", 2, 2, 2097152, 8388608)
                if "lotus-method-semloom" in arms
                else None,
                ray_temp_root=ray_root,
                query_timeout_s=20,
                owner_timeout_s=240,
                max_held_tasks=8,
                tokenizer_path=Path(os.environ["SEMLOOM_MOVIE_TOKENIZER"]),
                sembench_dataset=dataset,
            ) as group:
                for arm in arms:
                    owner_id = None
                    for q in queries:
                        if (arm, q) not in schedule:
                            continue
                        value = group.run(
                            arm,
                            unit_id=f"{arm}-q{q}",
                            root=output / (arm + "-q" + str(q)),
                            phase="qualification",
                            sembench_query=MovieQuery(dataset, q),
                        )
                        self.assertEqual(value["status"], "passed")
                        self.assertEqual(value["actual_posts"], 8 if q == 1 else 6)
                        self.assertEqual(value["execution"]["recorded_rows"], 1 if q == 3 else 5)
                        self.assertEqual(value["identity"]["original_query"]["query_id"], q)
                        self.assertTrue(value["identity"]["original_method_preserved"])
                        self.assertFalse(value["performance_qualified"])
                        audit = value["quality"]["classification_audit"]
                        self.assertEqual((audit["false_positive"], audit["false_negative"]), (1, 1))
                        self.assertEqual(audit["invalid"], 0)
                        self.assertEqual(audit["count_error_from_classifications"], 0)
                        if q == 3:
                            result = json.loads(
                                (output / (arm + "-q3") / "original-query-result.json").read_text()
                            )
                            self.assertEqual(result["dataframe"]["data"], [[5]])
                            self.assertEqual(
                                value["quality"]["original_metric"]["absolute_error"], 0
                            )
                        else:
                            self.assertEqual(value["quality"]["supplement"]["duplicates"], 1)
                            self.assertFalse(value["quality"]["supplement"]["limit_exceeded"])
                        life = value["persistent_lifecycle"]
                        if owner_id is None:
                            owner_id = (life["owner_id"], life["lm_id"])
                        self.assertEqual((life["owner_id"], life["lm_id"]), owner_id)
                        self.assertFalse(life["result_cache"])
                        if life["core_usage"] is not None:
                            self.assertTrue(all(v == 0 for v in life["core_usage"].values()))
                            self.assertEqual(life["core_jobs"], 0)
                        self.assertLessEqual(
                            value["execution"]["t_query_terminal_ns"], value["ended_ns"]
                        )
                        results.append(
                            dict(
                                arm=arm,
                                query_id=q,
                                status=value["status"],
                                posts=value["actual_posts"],
                                relation_rows=value["rows"],
                                result_rows=value["execution"]["recorded_rows"],
                                quality=value["quality"],
                                source=value["sembench_preflight"],
                                identity=value["identity"],
                                full_query_seconds=value["full_query_seconds"],
                                ready_query_seconds=value["execution"]["ready_query_seconds"],
                            )
                        )
                        write_private_json(
                            output / ("fixture-progress-" + str(len(results)) + ".json"),
                            redact_json_values(dict(models=0, posts=len(requests), cells=results)),
                        )
            self.assertEqual(len(requests), expected_posts)
            for request in requests:
                token_fields = [
                    request[name]
                    for name in ("max_tokens", "max_completion_tokens")
                    if name in request
                ]
                self.assertTrue(token_fields and all(value == 8 for value in token_fields))
                self.assertEqual(request["stop"], ["\n"])
                self.assertEqual(request["temperature"], 0)
                self.assertEqual(request["top_p"], 1)
                self.assertNotIn("scoreSentiment", str(request["messages"]))
                self.assertNotIn("_execution_row_id", str(request["messages"]))
        write_private_json(
            output / "fixture-verification.json",
            redact_json_values(
                dict(
                    passed=True,
                    fixture_posts=expected_posts,
                    models=0,
                    gpu_resources=0,
                    retries=0,
                    requested_queries=queries,
                    native_options=options.__dict__,
                    previously_completed=completed,
                    cells=results,
                    group_cleanup=json.loads((output / "group/group-summary.json").read_text()),
                )
            ),
        )


@unittest.skipUnless(
    os.environ.get("SEMLOOM_MOVIE_ERROR_FIXTURE"),
    "requires explicit no-model original LOTUS parser error check",
)
class MovieErrorLibraryTests(unittest.TestCase):
    def test_native_auth_error_poisoning_stops_reuse_and_keeps_first_parser_error(self):
        output = Path(os.environ["SEMLOOM_MOVIE_ERROR_FIXTURE"])
        output.mkdir(parents=True, exist_ok=False)
        dataset = fixture_dataset(
            output / "workload", Path(os.environ["SEMLOOM_TEST_SEMBENCH_ROOT"])
        )
        ledger = CellBudgetLedger.create(
            output / "budget.sqlite",
            AttemptBudget("movie-error", 8),
            deadline_utc=time.time() + 120,
        )
        with movie_fixture_server(status=401, limit=8) as (endpoint, requests):
            with PersistentAdapterGroup(
                (LOTUS_MOVIE_ARMS[0],),
                plan=SemanticMapPlan("original Movie profile", "fixture", 8),
                model=FixedModelConfig(endpoint, "fixture", 5000),
                ledger=ledger,
                root=output / "group",
                options=NativeGraphOptions(),
                query_timeout_s=20,
                owner_timeout_s=240,
                tokenizer_path=Path(os.environ["SEMLOOM_MOVIE_TOKENIZER"]),
                sembench_dataset=dataset,
            ) as group:
                with self.assertRaises(Exception) as failure:
                    group.run(
                        LOTUS_MOVIE_ARMS[0],
                        unit_id="auth",
                        root=output / "auth",
                        phase="qualification",
                        sembench_query=MovieQuery(dataset, 1),
                    )
                self.assertEqual(type(failure.exception).__name__, "AuthenticationError")
                summary = json.loads((output / "auth/summary.json").read_text())
                self.assertEqual(summary["status"], "failed")
                self.assertTrue(group.poisoned)
                before = len(requests)
                with self.assertRaisesRegex(RuntimeError, "persistent group stopped"):
                    group.run(
                        LOTUS_MOVIE_ARMS[0],
                        unit_id="after-error",
                        root=output / "after-error",
                        phase="qualification",
                        sembench_query=MovieQuery(dataset, 3),
                    )
                self.assertEqual(len(requests), before)
        write_private_json(
            output / "error-verification.json",
            dict(
                passed=True,
                model_requests=0,
                fixture_posts=len(requests),
                first_error="AuthenticationError",
                owner_poisoned=True,
                errors=summary["errors"],
                retry_posts=0,
                group_cleanup=json.loads((output / "group/group-summary.json").read_text()),
            ),
        )


if __name__ == "__main__":
    unittest.main()
