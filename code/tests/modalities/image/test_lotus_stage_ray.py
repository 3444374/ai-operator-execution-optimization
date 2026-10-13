"""Opt-in actual LOTUS -> Core -> Daft -> Ray -> localhost image fixture.

Reuse stage A's assets, preparation and static control. No model or GPU runs.
"""
from contextlib import ExitStack
from dataclasses import asdict
from importlib.util import find_spec
from io import BytesIO
import base64
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
import sys
from unittest.mock import patch

from PIL import Image
from src.baselines.common.private_artifacts import require_outside_git, write_private_json
from src.data.materializers.prepared_assets import AssetUse, PreparedAssetWindow
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, owned_map_worker_pool
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.experiments.process_sampling import ProcessSampler
from src.scheduling.core.session_contract import Usage
from src.scheduling.runtime.stage_broker import StageBrokerLimits
from src.semantic_methods.lotus.batch import LotusBatchExecutor, lotus_executor
from src.semantic_methods.lotus import batch as batch_module, sdk
from tests.modalities.image.test_lotus_stage_demand import StaticSharedPreparation, stage_assets
from tests.semantic_methods.test_lotus_adapter import fixture_server


REAL = (os.environ.get("SEMLOOM_STAGE_RAY_TEST") == "1" and
        all(find_spec(name) for name in ("lotus", "ray", "daft", "pyarrow", "psutil")))


def request_identity(body, names):
    parts = body["messages"][-1]["content"]
    text = " ".join(part["text"] for part in parts if part["type"] == "text")
    selected = [name for name in names if name in text]
    if len(selected) != 1:
        raise ValueError("fixture request lost its unique source identity")
    return ("filter" if "Claim:" in text else "map", selected[0])


def body_digest(body):
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


@unittest.skipUnless(REAL, "select preflight-checked actual Daft/Ray fixture; no installations")
class LotusStageRayTests(unittest.TestCase):
    def test_stage_a_on_existing_daft_ray_execution_and_error_cleanup(self):
        import ray
        sdk.validate_source(message_profile=sdk.IMAGE_TEXT_PROFILE)
        root = Path(os.environ["SEMLOOM_STAGE_RAY_ARTIFACT_ROOT"])
        require_outside_git(root)
        root.mkdir(mode=0o700, parents=True, exist_ok=False)
        paths, _, _, _ = stage_assets()
        names = tuple(Path(path).name for path in paths)
        current, records = {}, []
        started = time.monotonic()

        def reply(body):
            identity = request_identity(body, names)
            self.assertEqual(identity[0], current["stage"])
            if current["error"]:
                return 429, b'{"error":{"message":"controlled image fixture","type":"fixture"}}'
            parts = body["messages"][-1]["content"]
            uri = next(p["image_url"]["url"] for p in parts if p["type"] == "image_url")
            with Image.open(BytesIO(base64.b64decode(uri.split(",", 1)[1]))) as image:
                landscape = image.width >= image.height
            content = str(landscape) if identity[0] == "filter" else "landscape" if landscape else "portrait"
            response = dict(id="stage-demand", object="chat.completion", created=1, model=body["model"],
                choices=[dict(index=0, message=dict(role="assistant", content="Answer: " + content),
                              finish_reason="stop")],
                usage=dict(prompt_tokens=9, completion_tokens=3, total_tokens=12))
            return 200, json.dumps(response).encode()

        self.assertFalse(ray.is_initialized(), "never join an existing user cluster")
        ray_parent = Path(os.environ["SEMLOOM_STAGE_RAY_TEMP_PARENT"])
        require_outside_git(ray_parent)
        with tempfile.TemporaryDirectory(prefix="sr-", dir=ray_parent) as ray_root, \
             fixture_server(failure=reply, delay=lambda b: .05 if request_identity(b, names)[1] == names[0] else 0) as (base, calls):
            try:
                ray.init(num_cpus=2, num_gpus=0, include_dashboard=False, _temp_dir=ray_root)
                config = FixedModelConfig(base + "/chat/completions", "stage-image-fixture", 10000, "local-fixture")
                physical = RayMapConfig(ray.get_runtime_context().gcs_address, 2, 2,
                    2 * MAX_FRAME_BYTES, 4 * MAX_FRAME_BYTES, worker_pool="stage-" + root.name[-12:],
                    preparation=StageBrokerLimits(4 * MAX_FRAME_BYTES, 4 * MAX_FRAME_BYTES, 4, 1, 4),
                    response_mode="full")
                with owned_map_worker_pool(ray, config, 2, physical):
                    runtime_ready_s = time.monotonic() - started
                    for repeat, arm in ((0, "static_bytes"), (0, "consumer_bytes"),
                                        (1, "consumer_bytes"), (1, "static_bytes")):
                        current.update(stage="filter", error=False)
                        records.append(self.run_case(root, arm, repeat, config, physical, current, calls))
                    for arm in ("static_bytes", "consumer_bytes"):
                        current.update(stage="filter", error=True)
                        records.append(self.run_case(root, arm, 0, config, physical, current, calls))
                self.assertEqual(len(calls), 100)
                successful = records[:4]
                for record in successful[1:]:
                    for field in ("prepared_requests", "received_requests", "rows", "usage", "prepare_order"):
                        self.assertEqual(record[field], successful[0][field], field)
                self.assertTrue(all(r["prepare_calls"] == 17 for r in successful))
                self.assertTrue(all(r["copied_uri_bytes"] == 9736050 for r in successful))
                self.assertTrue(all(r["filter_survivors"] == 7 for r in successful))
                self.assertTrue(any(r["arrival_order_differs_from_source"] for r in successful))
                write_private_json(root / "summary.json", dict(status="passed", records=records,
                    fixture_posts=len(calls), real_model_calls=0, gpu_starts=0, ray_clusters=1,
                    runtime_ready_s=runtime_ready_s, total_wall_s=time.monotonic()-started,
                    memory_scope="50ms sampled process RSS; summed RSS is not unique physical memory",
                    request_scope="prepared payload bytes and canonical received JSON; raw HTTP wire bytes unavailable"))
            finally:
                ray.shutdown()
        self.assertFalse(ray.is_initialized())

    def run_case(self, root, arm, repeat, config, physical, current, calls):
        import lotus
        import pandas as pd
        from lotus.dtype_extensions import ImageArray
        from lotus.dtype_extensions.image import fetch_image
        from lotus.models import LM
        case = root / (arm + "-" + str(repeat) + ("-error" if current["error"] else ""))
        case.mkdir(mode=0o700)
        started = time.monotonic()
        paths, sources, keys, acquisition_s = stage_assets()
        names = tuple(Path(path).name for path in paths)
        uses = tuple(use for path in paths for use in (
            AssetUse("filter:" + Path(path).name, keys[path], source=sources[keys[path]]),
            AssetUse("map:" + Path(path).name, keys[path], source=sources[keys[path]], ready_after="filter-return")))
        window = PreparedAssetWindow(uses, max_consumers=24, host_bytes=4 * 1048576, clock=time.monotonic)
        static = StaticSharedPreparation(sources, 4 * 1048576)
        record = dict(arm=arm, repeat=repeat, error_case=current["error"], cache_bytes=4 * 1048576,
                      source_metadata_s=acquisition_s, prepare_order=[], prepared_requests=[],
                      responses=[], boundaries=[], timing=[], copied_uri_bytes=0)
        events, execution, selected = [], None, None
        batch_inputs = [None]
        first_call = len(calls)
        sampler = ProcessSampler(case / "rss.jsonl", {"driver":os.getpid()}, interval=.05,
                                 max_samples=12000, include_children=True)

        def rss_guard():
            import psutil
            driver = psutil.Process(os.getpid())
            measured = 0
            for process in (driver, *driver.children(recursive=True)):
                try:
                    measured += process.memory_info().rss
                except psutil.NoSuchProcess:
                    pass
            if measured > 4 * 1024**3:
                raise MemoryError("sampled driver and owned child RSS exceeds 4 GiB")
            return {}

        sampler.pid_provider = rss_guard

        def check_resources():
            if sampler.error:
                raise sampler.error

        def observe(event):
            events.append(dict(event, observed_ns=time.monotonic_ns(), query_stage=current["stage"]))

        def timed(function, label):
            def call(*args, **kwargs):
                before = time.monotonic_ns()
                try:
                    return function(*args, **kwargs)
                finally:
                    record["timing"].append(dict(stage=current["stage"], operation=label,
                        started_ns=before, ended_ns=time.monotonic_ns()))
            return call

        def prepare(path):
            record["prepare_order"].append(keys[path].content_digest)
            return fetch_image(path, "base64").encode("ascii")

        def get_image(array, index, image_type="Image"):
            self.assertEqual(image_type, "base64")
            path = str(array._data[index])
            key = keys[path]
            if arm == "static_bytes":
                payload = static.get(key, lambda: timed(prepare, "image_prepare")(path))
                uri = payload.decode("ascii")
                static.release(key)
            else:
                lease = window.acquire(current["stage"] + ":" + Path(path).name,
                    prepare=lambda: timed(prepare, "image_prepare")(path), prepared_bytes_upper_bound=1048576,
                    scratch_bytes_upper_bound=2 * sources[key].work.for_stage("prepare").units)
                self.assertIsNotNone(lease)
                uri = lease.payload.decode("ascii")
                window.release(lease)  # Exporter ended its read; URI/messages remain independently owned.
            record["copied_uri_bytes"] += len(uri)
            return uri

        def on_prepared(sequence, call):
            check_resources()
            body = json.loads(call.payload)
            record["prepared_requests"].append(dict(identity=request_identity(body, names), sequence=sequence,
                json_sha256=body_digest(body), prepared_payload_bytes=len(call.payload)))

        def on_response(sequence, response):
            check_resources()
            uris = {id(part["image_url"]["url"]):part["image_url"]["url"]
                    for messages,_ in batch_inputs[0] for message in messages
                    if isinstance(message["content"],list) for part in message["content"]
                    if part["type"] == "image_url"}
            record["responses"].append(dict(stage=current["stage"], sequence=sequence,
                status=response.status_code, usage=asdict(execution.engine.capacity.usage()),
                preparation=execution.engine.preparation.snapshot(),
                message_uri_bytes=sum(len(uri) for uri in uris.values()),
                known_python_uri_bytes=sum(sys.getsizeof(uri) for uri in uris.values()),
                cache_readers=window.snapshot().held_consumers))

        def boundary(stage):
            check_resources()
            state = execution.engine.preparation.snapshot()
            record["boundaries"].append(dict(stage=stage, usage=asdict(execution.engine.capacity.usage()),
                preparation=state, asset_cache=asdict(window.snapshot()) if arm == "consumer_bytes" else
                dict(prepared_bytes=sum(len(v) for v in static.cache.values()))))
            self.assertEqual(execution.engine.capacity.usage(), Usage())
            self.assertEqual(execution.engine.jobs.jobs, {})
            self.assertEqual(state["held_tasks"], 0)
            self.assertEqual(state["object_bytes"], 0)

        try:
            with sampler, ExitStack() as stack:
                execution = build_native_execution(config, physical=physical, max_tasks=4,
                                                   max_active_requests=2, observer=observe)
                record["execution_ready_s"] = time.monotonic() - started
                selected = LotusBatchExecutor(execution, config, query_id=case.name, operator_id="filter-map",
                    message_profile=sdk.IMAGE_TEXT_PROFILE, on_prepared=on_prepared, on_response=on_response,
                    on_batch=lambda data,_kwargs:batch_inputs.__setitem__(0,data))
                lm = LM("openai/stage-image-fixture", api_base=config.endpoint_url.removesuffix("/chat/completions"),
                    api_key="local-fixture", max_tokens=32, max_batch_size=1, timeout=10,
                    num_retries=0, max_retries=0)
                frame = pd.DataFrame({"image":ImageArray(paths), "asset_id":list(names)})
                stack.enter_context(lotus.settings.context(lm=lm, enable_cache=False))
                stack.enter_context(lotus_executor(lm, selected))
                stack.enter_context(patch.object(ImageArray, "get_image", get_image))
                original_prepare = sdk.prepare_call
                stack.enter_context(patch.object(sdk, "prepare_call", timed(original_prepare,"sdk_prepare")))
                stack.enter_context(patch.object(batch_module, "prepare_call", timed(original_prepare,"sdk_prepare")))
                stack.enter_context(patch.object(batch_module, "parse_lotus_response",
                                                timed(batch_module.parse_lotus_response,"sdk_parse")))
                query_started = time.monotonic()
                sampler.phase = "filter"
                try:
                    filtered = frame.sem_filter("The {image} is landscape oriented. Asset: {asset_id}.")
                except Exception as error:
                    if not current["error"]:
                        raise
                    self.assertEqual(getattr(error, "status_code", None), 429)
                    record["error_type"] = type(error).__name__
                    if arm == "static_bytes":
                        static.resolve(set())
                    else:
                        window.resolve_stage("filter-return", ())
                    batch_inputs[0] = None
                    boundary("filter-error-consumed")
                else:
                    self.assertFalse(current["error"])
                    record["filter_survivors"] = len(filtered)
                    record["filter_wall_s"] = time.monotonic() - query_started
                    survivors = {keys[str(value)] for value in filtered["image"].array._data}
                    if arm == "static_bytes":
                        static.resolve(survivors)
                    else:
                        window.resolve_stage("filter-return", tuple("map:"+name for name in filtered["asset_id"]))
                    batch_inputs[0] = None
                    boundary("filter-result-consumed")
                    current["stage"], sampler.phase = "map", "map"
                    map_started = time.monotonic()
                    mapped = filtered.sem_map("Return the orientation of {image}. Asset: {asset_id}.", suffix="_orientation")
                    record["map_wall_s"] = time.monotonic() - map_started
                    record["rows"] = mapped[["asset_id","_orientation"]].to_dict("records")
                    record["usage"] = {name:getattr(lm.stats.physical_usage,name)
                                       for name in ("prompt_tokens","completion_tokens","total_tokens")}
                    batch_inputs[0] = None
                    boundary("map-result-consumed")
                record["ready_query_wall_s"] = time.monotonic() - query_started
                record["task_wall_s"] = time.monotonic() - started
                self.assertEqual(selected.last_cleanup_errors, ())
                self.assertEqual(len(calls)-first_call, 12 if current["error"] else 19)
                self.assertTrue(all(item["usage"]["result_bytes"] > 0 for item in record["responses"]))
                sampler.phase = "cleanup"
                close_started = time.monotonic()
                record["close_confirmed"] = execution.close()
                record["close_s"] = time.monotonic() - close_started
                self.assertTrue(record["close_confirmed"])
                execution = None
                self.assertTrue(any(e["event"] == "ray_preparation_completed" for e in events))
                self.assertTrue(any(e["event"] == "ray_transport_closed" and e["confirmed"] for e in events))
                self.assertFalse(static.cache)
                self.assertEqual((window.snapshot().pending_consumers,window.snapshot().held_consumers,
                                  window.snapshot().prepared_bytes), (0,0,0) if arm == "consumer_bytes" else (24,0,0))
            received = [dict(identity=request_identity(body,names), json_sha256=body_digest(body))
                        for body in calls[first_call:]]
            expected = [dict(identity=r["identity"],json_sha256=r["json_sha256"]) for r in record["prepared_requests"]]
            self.assertEqual(sorted(received,key=lambda r:r["identity"]), sorted(expected,key=lambda r:r["identity"]))
            record["received_requests"] = sorted(received,key=lambda r:r["identity"])
            record["arrival_order_differs_from_source"] = received != expected
            record["prepare_calls"] = len(record["prepare_order"])
            record["resource_summary"] = sampler.summary()
            record["status"] = "passed"
            return record
        except BaseException as error:
            record.update(status="failed", failure_type=type(error).__name__)
            raise
        finally:
            if execution is not None:
                record["exception_close_confirmed"] = execution.close()
            record["lifecycle_wall_s"] = time.monotonic() - started
            write_private_json(case / "record.json", record)
            write_private_json(case / "events.json", events)
