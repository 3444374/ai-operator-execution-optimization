"""Opt-in PG18.3 + real Ray CPU actors + real image decode, with zero model weights."""

from __future__ import annotations

import io
import json
import os
import socket
import struct
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from functools import partial
from pathlib import Path

from PIL import Image

from src.execution_provider.adapters.image_execution import ImageServiceConfig, build_image_execution
from src.execution_provider.image_gateway import ImageGateway
from src.execution_provider.wire import image
from src.execution_provider.wire.framing import encode_frame, read_frame
from src.scheduling.runtime.stage_broker import StageBrokerLimits
from tests.execution_provider.image_fixtures import PLAN, ray_workers, reference


def png(color):
    stream = io.BytesIO()
    Image.new("RGB", (2, 2), color).save(stream, format="PNG")
    return stream.getvalue()


def wait_until(predicate, seconds=10):
    end = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= end:
            raise AssertionError("fixture condition timed out")
        time.sleep(0.01)


@contextmanager
def image_service(root, mode, ray_api, **fixture_options):
    work = Path(tempfile.mkdtemp(prefix="gateway-", dir=root))
    socket_path = str(work / "provider.sock")
    stop, started = threading.Event(), threading.Event()
    failures, instances, events = [], [], []
    config = ImageServiceConfig(PLAN, mode, StageBrokerLimits(2 * 262144, 96, 8, 1, 1), 1, 1, 1, 12000)

    def run():
        listener = socket.socket(socket.AF_UNIX)
        pool, gateway = None, None
        try:
            if mode == "staged":
                pool = ray_workers(ray_api, **fixture_options)
            gateway = ImageGateway(config, max_jobs=2, max_connections=3, max_tasks=4,
                max_active_requests=1 if mode == "reference" else 2, frame_timeout_ms=15000,
                input_bytes=4 * 262144, result_bytes=4 * PLAN.result_bytes, observer=events.append,
                execution_factory=partial(build_image_execution, worker_pool=pool, ray_api=ray_api,
                                          reference=reference(**fixture_options)))
            instances.append(gateway)
            listener.bind(socket_path)
            listener.listen(3)
            started.set()
            gateway.serve(listener, stop, handler=None)
        except BaseException as failure:
            failures.append(failure)
            started.set()
        finally:
            listener.close()
            if gateway is not None:
                if not gateway.close():
                    failures.append(AssertionError("image gateway retained unknown work"))
            if pool is not None:
                for actor in (*pool.preprocessors, *pool.gpu_actors):
                    ray_api.kill(actor, no_restart=True)

    thread = threading.Thread(target=run)
    thread.start()
    assert started.wait(30), "image workers did not start"
    if not instances:
        thread.join(1)
        raise failures[0]
    try:
        yield socket_path, instances[0], events
    finally:
        # A held test computation must be allowed to finish before teardown.
        if fixture_options.get("release_path"):
            Path(fixture_options["release_path"]).touch()
        stop.set()
        instances[0].request_stop()
        thread.join(20)
        assert not thread.is_alive(), "gateway owner did not stop"
        (work / "events.json").write_text(json.dumps(events, indent=2))
    if failures:
        raise failures[0]
    assert instances[0].engine.capacity.usage().held_tasks == 0


@unittest.skipUnless(os.environ.get("SEMLOOM_IMAGE_PG_DSN") and os.environ.get("SEMLOOM_IMAGE_TEST_ROOT"),
                     "requires an explicitly isolated PG18.3/Ray fixture environment")
class ImagePostgresTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        import ray
        cls.psycopg, cls.ray = psycopg, ray
        cls.root = Path(os.environ["SEMLOOM_IMAGE_TEST_ROOT"])
        cls.root.mkdir(parents=True, exist_ok=False)
        cls.addClassCleanup(ray.shutdown)
        # CPU stand-ins only. No model weights, GPU allocation or external HTTP.
        ray.init(num_cpus=2, num_gpus=0, include_dashboard=False,
                 object_store_memory=128 * 1024 * 1024,
                 _temp_dir=os.environ["SEMLOOM_IMAGE_RAY_TEMP"],
                 _node_ip_address="127.0.0.1")
        cls.options = json.dumps(PLAN.to_record())
        cls.expression = f"ai_semantic.embed(image, '{cls.options}'::jsonb)"
        cls.schema = "image_f_" + cls.root.name
        with cls.psycopg.connect(os.environ["SEMLOOM_IMAGE_PG_DSN"], autocommit=True) as connection:
            assert connection.info.server_version == 180003
            connection.execute(cls.psycopg.sql.SQL("CREATE SCHEMA {}").format(cls.psycopg.sql.Identifier(cls.schema)))
            connection.execute(cls.psycopg.sql.SQL("SET search_path={},public").format(cls.psycopg.sql.Identifier(cls.schema)))
            connection.execute("CREATE TABLE source (id integer PRIMARY KEY, image bytea)")
            connection.execute("CREATE TABLE sink (id integer PRIMARY KEY, vector real[])")
            for row in ((1, png("red")), (2, None), (3, png("green")), (4, png("red")), (5, png("blue"))):
                connection.execute("INSERT INTO source VALUES (%s,%s)", row)

    @classmethod
    def tearDownClass(cls):
        cls.ray.shutdown()

    def connect(self, path, mode, *, window=2):
        connection = self.psycopg.connect(os.environ["SEMLOOM_IMAGE_PG_DSN"], autocommit=True)
        connection.execute(self.psycopg.sql.SQL("SET search_path={},public").format(self.psycopg.sql.Identifier(self.schema)))
        connection.execute("SELECT set_config('semloom_pg.provider_execution_profile',%s,false)", (f"image-{mode}",))
        connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)", (path,))
        connection.execute("SELECT set_config('semloom_pg.provider_window_tasks',%s,false)", (str(window),))
        connection.execute("SET semloom_pg.enable_total_window_budget=on")
        connection.execute("SET statement_timeout='15s'")
        return connection

    def test_reference_staged_types_identity_order_and_null(self):
        for mode in ("reference", "staged"):
            with self.subTest(mode=mode), image_service(self.root, mode, self.ray) as (path, gateway, events):
                with self.connect(path, mode) as connection:
                    plan = connection.execute(f"EXPLAIN (FORMAT JSON, COSTS OFF) SELECT id, {self.expression} FROM source").fetchone()[0][0]["Plan"]
                    self.assertEqual(plan["Semantic Spec Digest"], PLAN.digest)
                    self.assertEqual(plan["Physical Algorithm Digest"], image.physical_digest(mode))
                    result = connection.execute(f"SELECT id, {self.expression} FROM source").fetchall()
                    self.assertEqual(result, [(1, [1,0,0]), (2, None), (3, [0,1,0]), (4, [1,0,0]), (5, [0,0,1])])
                wait_until(lambda: gateway.engine.capacity.usage().held_tasks == 0)
                self.assertEqual(sum(item.get("event") == "accepted" for item in events), 4 if mode == "staged" else 0)

    def test_limit_and_null_do_not_speculate_invalid_later_images(self):
        with image_service(self.root, "staged", self.ray) as (path, gateway, events):
            with self.connect(path, "staged") as connection:
                connection.execute("CREATE TEMP TABLE demand (image bytea)")
                connection.execute("INSERT INTO demand VALUES (%s), (%s)", (png("red"), b"invalid"))
                self.assertEqual(connection.execute(f"SELECT {self.expression} FROM demand LIMIT 0").fetchall(), [])
                self.assertEqual(connection.execute(f"SELECT {self.expression} FROM demand LIMIT 1").fetchall(), [([1,0,0],)])
                self.assertEqual(connection.execute(f"SELECT {self.expression} FROM source WHERE id=2").fetchall(), [(None,)])
            self.assertEqual(sum(item.get("event") == "accepted" for item in events), 1)

    def test_insert_rollback_decode_error_and_old_text_recording(self):
        with image_service(self.root, "staged", self.ray) as (path, gateway, events):
            with self.connect(path, "staged") as connection:
                connection.execute("BEGIN")
                connection.execute(f"INSERT INTO sink SELECT id, {self.expression} FROM source")
                self.assertEqual(connection.execute("SELECT count(*) FROM sink").fetchone()[0], 5)
                connection.execute("ROLLBACK")
                self.assertEqual(connection.execute("SELECT count(*) FROM sink").fetchone()[0], 0)
                connection.execute("CREATE TEMP TABLE broken (id integer, image bytea)")
                connection.execute("INSERT INTO broken VALUES (1,%s), (2,%s)", (png("red"), b"invalid"))
                with self.assertRaises(self.psycopg.Error):
                    connection.execute(f"INSERT INTO sink SELECT id, {self.expression} FROM broken")
                self.assertEqual(connection.execute("SELECT count(*) FROM sink").fetchone()[0], 0)
                connection.execute("SET semloom_pg.provider_execution_profile='golden'")
                connection.execute("SET semloom_pg.gateway_socket=''")
                connection.execute("CREATE TEMP TABLE old_text (value text)")
                connection.execute("INSERT INTO old_text VALUES ('old')")
                self.assertEqual(connection.execute("SELECT ai_semantic.map(value) FROM old_text").fetchone()[0], "recorded:old")

    def test_cumulative_queries_release_jobs_and_result_storage(self):
        with image_service(self.root, "staged", self.ray) as (path, gateway, events):
            with self.connect(path, "staged") as connection:
                for _ in range(6):
                    self.assertEqual(len(connection.execute(f"SELECT {self.expression} FROM source").fetchall()), 5)
            wait_until(lambda: gateway.engine.capacity.usage().held_tasks == 0 and not gateway.engine.jobs.jobs)
            self.assertEqual(gateway.engine.backend.snapshot().active_blocks, 0)

    def test_pg_cancel_waits_for_remote_end_without_killing_shared_worker(self):
        entered, release = self.root / "model-entered", self.root / "model-release"
        with image_service(self.root, "staged", self.ray, entered_path=str(entered), release_path=str(release)) as (path, gateway, events):
            first = self.connect(path, "staged", window=1)
            errors = []

            def query():
                try:
                    first.execute(f"SELECT {self.expression} FROM source LIMIT 1").fetchall()
                except self.psycopg.Error as error:
                    errors.append(error.sqlstate)

            thread = threading.Thread(target=query)
            thread.start()
            wait_until(entered.exists)
            first.cancel()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, ["57014"])
            first.close()
            wait_until(lambda: any(item.get("event") == "cancel_requested" for item in events))
            self.assertGreater(gateway.engine.capacity.usage().active_requests, 0)
            self.assertGreater(gateway.engine.backend.snapshot().ready_held_bytes, 0)
            release.touch()
            wait_until(lambda: gateway.engine.capacity.usage().held_tasks == 0)
            with self.connect(path, "staged") as other:
                self.assertEqual(other.execute(f"SELECT {self.expression} FROM source LIMIT 1").fetchall(), [([1,0,0],)])

    def test_permissions_and_rls_keep_invisible_images_out_of_workers(self):
        role = "image_reader_" + self.root.name
        sql = self.psycopg.sql
        with image_service(self.root, "staged", self.ray) as (path, gateway, events):
            with self.connect(path, "staged") as connection:
                connection.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(role)))
                connection.execute(sql.SQL("GRANT USAGE ON SCHEMA {},ai_semantic TO {}").format(
                    sql.Identifier(self.schema), sql.Identifier(role)))
                connection.execute("CREATE TABLE protected (id integer, image bytea)")
                connection.execute("INSERT INTO protected VALUES (1,%s),(2,%s)", (png("red"), b"invisible-invalid"))
                connection.execute("ALTER TABLE protected ENABLE ROW LEVEL SECURITY")
                connection.execute("CREATE POLICY visible_image ON protected USING (id=1)")
                connection.execute(sql.SQL("GRANT SELECT(id,image) ON protected TO {}").format(sql.Identifier(role)))
                connection.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role)))
                self.assertEqual(connection.execute(f"SELECT id, {self.expression} FROM protected").fetchall(), [(1,[1,0,0])])
                connection.execute("RESET ROLE")
                connection.execute(sql.SQL("REVOKE SELECT(image) ON protected FROM {}").format(sql.Identifier(role)))
                connection.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role)))
                with self.assertRaises(self.psycopg.errors.InsufficientPrivilege):
                    connection.execute(f"SELECT {self.expression} FROM protected")
                connection.execute("RESET ROLE")
                connection.execute(sql.SQL("GRANT SELECT(image) ON protected TO {}").format(sql.Identifier(role)))
                connection.execute("REVOKE EXECUTE ON FUNCTION ai_semantic.embed(bytea,jsonb) FROM PUBLIC")
                try:
                    connection.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role)))
                    with self.assertRaises(self.psycopg.errors.InsufficientPrivilege):
                        connection.execute(f"SELECT {self.expression} FROM protected")
                finally:
                    connection.execute("RESET ROLE")
                    connection.execute("GRANT EXECUTE ON FUNCTION ai_semantic.embed(bytea,jsonb) TO PUBLIC")
            self.assertEqual(sum(item.get("event") == "accepted" for item in events), 1)

    def test_snapshot_and_prepared_plan_keep_typed_identity(self):
        with image_service(self.root, "staged", self.ray) as (path, gateway, events):
            with self.connect(path, "staged") as connection, self.connect(path, "staged") as writer:
                connection.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
                connection.execute(f"PREPARE image_prepared AS SELECT {self.expression} FROM source WHERE id=1")
                self.assertEqual(connection.execute("EXECUTE image_prepared").fetchone()[0], [1,0,0])
                writer.execute("UPDATE source SET image=%s WHERE id=1", (png("blue"),))
                try:
                    self.assertEqual(connection.execute("EXECUTE image_prepared").fetchone()[0], [1,0,0])
                    connection.execute("COMMIT")
                    self.assertEqual(connection.execute("EXECUTE image_prepared").fetchone()[0], [0,0,1])
                finally:
                    writer.execute("UPDATE source SET image=%s WHERE id=1", (png("red"),))

    def test_illegal_options_and_oversized_binary_fail_before_submission(self):
        with image_service(self.root, "staged", self.ray) as (path, gateway, events):
            with self.connect(path, "staged") as connection:
                for key, value in (("dimension", 0), ("dimension", 2.5), ("input_size", 1025),
                                   ("model_revision", "main"), ("dtype", "fp16"), ("extra", 1)):
                    options = json.dumps({**PLAN.to_record(), key: value})
                    with self.assertRaises(self.psycopg.Error):
                        connection.execute(f"SELECT ai_semantic.embed(image, '{options}'::jsonb) FROM source LIMIT 0")
                connection.execute("CREATE TEMP TABLE oversized (image bytea)")
                connection.execute("INSERT INTO oversized VALUES (%s)", (b"x" * 262145,))
                with self.assertRaises(self.psycopg.Error):
                    connection.execute(f"SELECT {self.expression} FROM oversized")
            self.assertEqual(sum(item.get("event") == "accepted" for item in events), 0)

    def test_pg_rejects_tampered_association_evidence_shape_and_nonfinite_float4(self):
        mutations = (
            lambda message: {**message, "sequence": "1"},
            lambda message: {**message, "semantic_spec_digest": "0" * 64},
            lambda message: {**message, "completion_evidence_digest": "0" * 64},
            lambda message: {**message, "output_hex": "00000000"},
            lambda message: {**message, "output_hex": "7f800000" * 3},
            lambda message: {**message, "extra": 0},
            lambda message: json.dumps(message)[:-1] + ',"sequence":"0"}',
        )
        for index, mutate in enumerate(mutations):
            with self.subTest(case=index):
                listener = socket.socket(socket.AF_UNIX)
                path = str(self.root / f"tamper-{index}.sock")
                listener.bind(path)
                listener.listen(1)
                listener.settimeout(5)
                failures = []

                def stub():
                    try:
                        connection, _ = listener.accept()
                        with connection:
                            connection.settimeout(5)
                            opened = read_frame(connection)
                            mode, window = image.validate_open(opened, PLAN)
                            connection.sendall(encode_frame(dict(type="opened", protocol_version=7,
                                **image.identities(PLAN, mode), max_inflight_tasks=window)))
                            task = image.validate_task(read_frame(connection), PLAN, mode, 0)
                            message = mutate(image.completion_message(PLAN, mode, 0, task.payload_digest,
                                                                      PLAN.encode_result([1,0,0])))
                            if isinstance(message, str):
                                data = message.encode()
                                connection.sendall(struct.pack("!I", len(data)) + data)
                            else:
                                connection.sendall(encode_frame(message))
                    except BaseException as error:
                        failures.append(error)

                thread = threading.Thread(target=stub)
                thread.start()
                try:
                    with self.connect(path, "reference") as connection:
                        with self.assertRaises(self.psycopg.Error):
                            connection.execute(f"SELECT {self.expression} FROM source LIMIT 1")
                finally:
                    thread.join(6)
                    listener.close()
                self.assertFalse(thread.is_alive())
                self.assertFalse(failures, failures)


if __name__ == "__main__":
    unittest.main()
