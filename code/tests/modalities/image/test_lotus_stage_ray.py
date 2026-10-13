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
import re
import tempfile
import threading
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
from tests.modalities.image.stage_http_fixture import stage_http_fixture
from tests.modalities.image.stage_block_trace import StageBlockTrace


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


def assert_stage_limits(event):
    if "usage" in event:
        caps = dict(held_tasks=4,active_requests=2,active_work=2,input_bytes=4*1048576,result_bytes=4*1048576)
        assert all(0 <= event["usage"][name] <= cap for name,cap in caps.items()), event
    if "object_bytes" in event:
        assert 0 <= event["object_bytes"] <= 4*1048576, event
    prepared = event.get("preparation")
    if prepared:
        assert 0 <= prepared["held_tasks"] <= 4
        caps = dict(encoded_held_bytes=4*1048576,ready_held_bytes=4*1048576,
                    ready_held_work=4,prepare_inflight=1,model_inflight=4)
        assert all(0 <= prepared["stages"][name] <= cap for name,cap in caps.items()), prepared


def verify_preparation_borrow(stack, execution, record):
    original = execution.engine.preparation.try_prepare
    record["borrowed_inputs"] = []

    def checked(tasks):
        for task in tasks:
            accepted = execution.engine.capacity.records.get(task.key)
            assert accepted is not None and accepted.task is task.task and accepted.preparation_held
        count = original(tasks)
        record["borrowed_inputs"].extend(asdict(t.key) for t in tasks[:count])
        return count
    stack.enter_context(patch.object(execution.engine.preparation,"try_prepare",checked))


def guarded_sampler(path):
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
    return ProcessSampler(path,{"driver":os.getpid()},interval=.05,max_samples=12000,
                          include_children=True,pid_provider=rss_guard)


def close_for_record(execution, record):
    primary=sys.exception()
    try:
        record["exception_close_confirmed"]=execution.close()
    except BaseException as error:
        record["close_error_type"]=type(error).__name__
        if primary is None:
            record.update(status="failed",failure_type=type(error).__name__)
            raise
        primary.add_note("fixture execution close also failed: "+type(error).__name__)


def fixture_lm(config):
    from lotus.models import LM
    return LM("openai/"+config.model_id, api_base=config.endpoint_url.removesuffix("/chat/completions"),
              api_key=config.bearer_token,max_tokens=32,max_batch_size=1,
              timeout=config.timeout_ms/1000,num_retries=0,max_retries=0)


def listing_identity(body):
    text = "\n".join(p["text"] for p in body["messages"][-1]["content"] if p["type"] == "text")
    return ("filter" if "Claim:" in text else "map", re.search(r"«([^»]+)»",text).group(1))


def completion_bytes(body, content):
    return json.dumps(dict(id="stage-demand",object="chat.completion",created=1,model=body["model"],
        choices=[dict(index=0,message=dict(role="assistant",content="Answer: "+content),finish_reason="stop")],
        usage=dict(prompt_tokens=9,completion_tokens=3,total_tokens=12))).encode()


@unittest.skipUnless(REAL, "select preflight-checked actual Daft/Ray fixture; no installations")
class LotusStageRayTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get("SEMLOOM_STAGE_BLOCK_TRACE")=="1", "opt-in block-tail diagnostic")
    def test_fixed_small_blocks_against_existing_stage_rows(self):
        import ray
        sdk.validate_source(message_profile=sdk.IMAGE_TEXT_PROFILE)
        root=Path(os.environ["SEMLOOM_STAGE_RAY_ARTIFACT_ROOT"])
        require_outside_git(root);root.mkdir(mode=0o700,parents=True,exist_ok=False)
        paths,_,_,_,_=stage_assets()
        names=tuple(Path(path).name for path in paths)
        current=dict(stage="filter",error=False)
        def reply(body):
            kind,_=request_identity(body,names)
            parts=body["messages"][-1]["content"]
            uri=next(part["image_url"]["url"] for part in parts if part["type"]=="image_url")
            with Image.open(BytesIO(base64.b64decode(uri.split(",",1)[1]))) as image:
                landscape=image.width>=image.height
            return 200,completion_bytes(body,str(landscape) if kind=="filter" else "landscape" if landscape else "portrait")
        self.assertFalse(ray.is_initialized())
        with tempfile.TemporaryDirectory(prefix="sr-",dir=os.environ["SEMLOOM_STAGE_RAY_TEMP_PARENT"]) as ray_root, \
             stage_http_fixture(reply,lambda body:request_identity(body,names),max_posts=38) as (base,calls):
            try:
                ray.init(num_cpus=2,num_gpus=0,include_dashboard=False,_temp_dir=ray_root)
                config=FixedModelConfig(base+"/chat/completions","stage-image-fixture",10000,"local-fixture")
                physical=RayMapConfig(ray.get_runtime_context().gcs_address,2,2,2*MAX_FRAME_BYTES,4*MAX_FRAME_BYTES,
                    worker_pool="tail-"+root.name[-12:],response_mode="full",
                    preparation=StageBrokerLimits(4*MAX_FRAME_BYTES,4*MAX_FRAME_BYTES,4,1,4))
                from dataclasses import replace
                with owned_map_worker_pool(ray,config,2,physical):
                    records=[]
                    for size in (2,1):
                        current.update(stage="filter",error=False)
                        records.append(self.run_case(root,"static_bytes",size,config,
                                                     replace(physical,batch_rows=size),current,calls))
                self.assertEqual(len(calls),38)
                for field in ("prepared_requests","received_requests","rows","usage","prepare_order","copied_uri_bytes"):
                    self.assertEqual(records[0][field],records[1][field],field)
                write_private_json(root/"summary.json",dict(status="passed",records=records,fixture_posts=len(calls),
                    batch_rows=[2,1],real_model_calls=0,gpu_starts=0,ray_clusters=1,
                    scope="read-only block-tail software diagnostic; original Filter barrier and static asset arm"))
            finally:
                ray.shutdown()
                write_private_json(root/"http-receipts.json",list(calls))

    @unittest.skipIf(os.environ.get("SEMLOOM_STAGE_BLOCK_TRACE")=="1", "block trace selects its separate 38-POST diagnostic")
    def test_stage_a_on_existing_daft_ray_execution_and_error_cleanup(self):
        import ray
        sdk.validate_source(message_profile=sdk.IMAGE_TEXT_PROFILE)
        root = Path(os.environ["SEMLOOM_STAGE_RAY_ARTIFACT_ROOT"])
        require_outside_git(root)
        root.mkdir(mode=0o700, parents=True, exist_ok=False)
        paths, _, _, _, _ = stage_assets()
        names = tuple(Path(path).name for path in paths)
        current, records = {}, []
        started = time.monotonic()
        include_all_429=os.environ.get("SEMLOOM_STAGE_RAY_ALL_429","1")=="1"
        expected_posts=110 if include_all_429 else 86

        def reply(body):
            if current.get("listing"):
                stage, listing = listing_identity(body)
                if listing == "keep-a" and current.get("gate") is not None:
                    if not current["gate"].wait(5):
                        raise TimeoutError("fast unique response was not consumed before slow response")
                if current.get("mixed") and listing == "drop-b":
                    return 429,b'{"error":{"message":"controlled image fixture","type":"fixture"}}'
                return 200,completion_bytes(body,str(listing.startswith("keep")) if stage=="filter" else listing)
            identity = request_identity(body, names)
            self.assertEqual(identity[0], current["stage"])
            if current["error"]:
                return 429, b'{"error":{"message":"controlled image fixture","type":"fixture"}}'
            parts = body["messages"][-1]["content"]
            uri = next(p["image_url"]["url"] for p in parts if p["type"] == "image_url")
            with Image.open(BytesIO(base64.b64decode(uri.split(",", 1)[1]))) as image:
                landscape = image.width >= image.height
            content = str(landscape) if identity[0] == "filter" else "landscape" if landscape else "portrait"
            return 200,completion_bytes(body,content)

        self.assertFalse(ray.is_initialized(), "never join an existing user cluster")
        ray_parent = Path(os.environ["SEMLOOM_STAGE_RAY_TEMP_PARENT"])
        require_outside_git(ray_parent)
        with tempfile.TemporaryDirectory(prefix="sr-", dir=ray_parent) as ray_root, \
             stage_http_fixture(reply, lambda b:listing_identity(b) if current.get("listing") else
                                request_identity(b,names), max_posts=expected_posts) as (base, calls):
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
                    for arm in (("static_bytes", "consumer_bytes") if include_all_429 else ()):
                        current.update(stage="filter", error=True)
                        records.append(self.run_case(root, arm, 0, config, physical, current, calls))
                    self.assertEqual(len(calls),100 if include_all_429 else 76)
                    unique = self.run_listing_case(root,config,physical,current,calls,reply,mixed=False)
                    mixed = self.run_listing_case(root,config,physical,current,calls,reply,mixed=True)
                self.assertEqual(len(calls),expected_posts)
                successful = records[:4]
                for record in successful[1:]:
                    for field in ("prepared_requests", "received_requests", "rows", "usage", "prepare_order"):
                        self.assertEqual(record[field], successful[0][field], field)
                self.assertTrue(all(r["prepare_calls"] == 17 for r in successful))
                self.assertTrue(all(r["copied_uri_bytes"] == 9736050 for r in successful))
                self.assertTrue(all(r["filter_survivors"] == 7 for r in successful))
                # Orientation outputs alone are not an association oracle. A
                # separate unique-output flow below forces completion inversion.
                write_private_json(root / "summary.json", dict(status="passed", records=records,
                    unique_association=unique,mixed_sdk=mixed,
                    fixture_posts=len(calls), real_model_calls=0, gpu_starts=0, ray_clusters=1,
                    all_429_included=include_all_429,
                    runtime_ready_s=runtime_ready_s, total_wall_s=time.monotonic()-started,
                    memory_scope="50ms sampled process RSS; summed RSS is not unique physical memory",
                    request_scope="prepared payload bytes and canonical received JSON; raw HTTP wire bytes unavailable"))
            finally:
                ray.shutdown()
                write_private_json(root/"http-receipts.json",list(calls))
        self.assertFalse(ray.is_initialized())

    def run_case(self, root, arm, repeat, config, physical, current, calls):
        import lotus
        import pandas as pd
        from lotus.dtype_extensions import ImageArray
        from lotus.dtype_extensions.image import fetch_image
        case = root / (arm + "-" + str(repeat) + ("-error" if current["error"] else ""))
        case.mkdir(mode=0o700)
        started = time.monotonic()
        paths, sources, keys, snapshots, acquisition_s = stage_assets()
        names = tuple(Path(path).name for path in paths)
        uses = tuple(use for path in paths for use in (
            AssetUse("filter:" + Path(path).name, keys[path], source=sources[keys[path]]),
            AssetUse("map:" + Path(path).name, keys[path], source=sources[keys[path]], ready_after="filter-return")))
        window = PreparedAssetWindow(uses, max_consumers=24, host_bytes=4 * 1048576, clock=time.monotonic)
        static = StaticSharedPreparation(sources, 4 * 1048576)
        record = dict(arm=arm, repeat=repeat, error_case=current["error"], cache_bytes=4 * 1048576,
                      batch_rows=physical.batch_rows,
                      source_metadata_s=acquisition_s, prepare_order=[], prepared_requests=[],
                      source_snapshot_bytes=sum(len(s.encoded) for s in snapshots.values()),
                      responses=[], boundaries=[], timing=[], copied_uri_bytes=0)
        record["frontend_prepare_bytes"]=dict(encoded=0,rgb_nominal=0,uri_encoded=0)
        events, execution, selected = [], None, None
        batch_inputs = [None]
        first_call = len(calls)
        sampler = guarded_sampler(case / "rss.jsonl")

        def check_resources():
            if sampler.error:
                raise sampler.error

        def observe(event):
            assert_stage_limits(event)
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
            payload=snapshots[path].fetch(fetch_image).encode("ascii")
            record["frontend_prepare_bytes"]["encoded"]+=len(snapshots[path].encoded)
            record["frontend_prepare_bytes"]["rgb_nominal"]+=snapshots[path].rgb_bytes
            record["frontend_prepare_bytes"]["uri_encoded"]+=len(payload)
            return payload

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
                verify_preparation_borrow(stack, execution, record)
                trace=None
                if os.environ.get("SEMLOOM_STAGE_BLOCK_TRACE")=="1":
                    trace=StageBlockTrace(execution,lambda:current["stage"])
                    trace.attach(stack)
                record["execution_ready_s"] = time.monotonic() - started
                selected = LotusBatchExecutor(execution, config, query_id=case.name, operator_id="filter-map",
                    message_profile=sdk.IMAGE_TEXT_PROFILE, on_prepared=on_prepared, on_response=on_response,
                    on_batch=lambda data,_kwargs:batch_inputs.__setitem__(0,data))
                lm = fixture_lm(config)
                frame = pd.DataFrame({"image":ImageArray(paths), "asset_id":list(names)})
                stack.enter_context(lotus.settings.context(lm=lm, enable_cache=False))
                stack.enter_context(lotus_executor(lm, selected))
                stack.enter_context(patch.object(ImageArray, "get_image", get_image))
                original_prepare = sdk.prepare_call
                phase=dict(validating=False)
                record["sdk_preparation_bytes"]=dict(validate=0,execution=0)
                def measured_prepare(*args,**kwargs):
                    call=timed(original_prepare,"sdk_validate_row" if phase["validating"] else "sdk_prepare")(*args,**kwargs)
                    record["sdk_preparation_bytes"]["validate" if phase["validating"] else "execution"]+=len(call.payload)
                    return call
                original_validate=batch_module.validate_batch
                def measured_validate(*args,**kwargs):
                    phase["validating"]=True
                    try:
                        return timed(original_validate,"full_batch_validate")(*args,**kwargs)
                    finally:
                        phase["validating"]=False
                stack.enter_context(patch.object(sdk,"prepare_call",measured_prepare))
                stack.enter_context(patch.object(batch_module,"prepare_call",measured_prepare))
                stack.enter_context(patch.object(batch_module,"validate_batch",measured_validate))
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
                    expected = []
                    for path in paths:
                        with Image.open(BytesIO(snapshots[path].encoded)) as image:
                            if image.width >= image.height:
                                expected.append(dict(asset_id=Path(path).name,_orientation="Answer: landscape"))
                    self.assertEqual(record["rows"], expected)
                    record["usage"] = {name:getattr(lm.stats.physical_usage,name)
                                       for name in ("prompt_tokens","completion_tokens","total_tokens")}
                    self.assertEqual(record["usage"],dict(prompt_tokens=171,completion_tokens=57,total_tokens=228))
                    batch_inputs[0] = None
                    boundary("map-result-consumed")
                record["ready_query_wall_s"] = time.monotonic() - query_started
                record["task_wall_s"] = time.monotonic() - started
                self.assertEqual(selected.last_cleanup_errors, ())
                calls.wait_idle()
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
            received = [dict(identity=row["identity"],json_sha256=row["json_sha256"]) for row in calls[first_call:]]
            expected = [dict(identity=r["identity"],json_sha256=r["json_sha256"]) for r in record["prepared_requests"]]
            self.assertEqual(sorted(received,key=lambda r:r["identity"]), sorted(expected,key=lambda r:r["identity"]))
            record["received_requests"] = sorted(received,key=lambda r:r["identity"])
            record["arrival_order_differs_from_source"] = received != expected
            record["prepare_calls"] = len(record["prepare_order"])
            record["resource_summary"] = sampler.summary()
            if trace is not None:
                record["block_trace"]=trace.summary()
            record["status"] = "passed"
            return record
        except BaseException as error:
            record.update(status="failed", failure_type=type(error).__name__)
            raise
        finally:
            try:
                if execution is not None:
                    close_for_record(execution,record)
            finally:
                record["lifecycle_wall_s"] = time.monotonic() - started
                if 'trace' in locals() and trace is not None:
                    record["block_trace"]=trace.summary()
                write_private_json(case / "record.json", record)
                write_private_json(case / "events.json", events)

    def run_listing_case(self, root, config, physical, current, calls, reply, *, mixed):
        import httpx
        import lotus
        import pandas as pd
        from lotus.dtype_extensions import ImageArray
        from lotus.dtype_extensions.image import fetch_image
        paths,_,_,snapshots,_ = stage_assets()
        labels = ["keep-a","drop-b","keep-c","drop-d"]
        index = [10,20,30,40]
        record = dict(mixed=mixed,responses=[],prepared=[],native=[],events=[])
        current.update(listing=True,mixed=mixed,stage="map" if mixed else "filter",gate=None)

        def frame():
            return pd.DataFrame({"image":ImageArray(paths[:4]),"listing_text":labels},index=index)

        def get_image(array, row, image_type="Image"):
            return snapshots[str(array._data[row])].fetch(fetch_image,image_type)

        gate=threading.Event()
        before=len(calls)
        execution=None
        name="mixed-sdk" if mixed else "unique-association"
        sampler=guarded_sampler(root/(name+"-rss.jsonl"))
        def observe(event):
            if sampler.error:
                raise sampler.error
            assert_stage_limits(event)
            record["events"].append(dict(event,query_stage=current["stage"],observed_ns=time.monotonic_ns()))
        def response(sequence,raw):
            state=asdict(execution.engine.capacity.usage())
            record["responses"].append(dict(stage=current["stage"],sequence=sequence,status=raw.status_code,usage=state))
            if sequence==1:
                self.assertGreater(state["active_requests"],0)
                gate.set()
        def prepared(sequence,call):
            body=json.loads(call.payload)
            record["prepared"].append(dict(identity=listing_identity(body),json_sha256=body_digest(body)))
        try:
            with sampler, ExitStack() as stack:
                if mixed:
                    def native_send(client,request,**options):
                        body=json.loads(request.read())
                        record["native"].append(dict(identity=listing_identity(body),json_sha256=body_digest(body)))
                        status,data=reply(body)
                        return httpx.Response(status,content=data,request=request,headers={"content-type":"application/json"})
                    native_error=None
                    with lotus.settings.context(lm=fixture_lm(config),enable_cache=False), \
                         patch.object(ImageArray,"get_image",get_image), patch.object(httpx.Client,"send",native_send):
                        try:
                            frame().sem_map("Describe {listing_text} using {image}.",suffix="_decision")
                        except Exception as error:
                            native_error=error
                    self.assertIsNotNone(native_error)
                    self.assertEqual(getattr(native_error,"status_code",None),429)
                    self.assertEqual(len(record["native"]),4)
                current["gate"]=gate
                execution=build_native_execution(config,physical=physical,max_tasks=4,max_active_requests=2,observer=observe)
                verify_preparation_borrow(stack,execution,record)
                selected=LotusBatchExecutor(execution,config,query_id="mixed" if mixed else "unique",operator_id="association",
                    message_profile=sdk.IMAGE_TEXT_PROFILE,on_prepared=prepared,on_response=response)
                lm=fixture_lm(config)
                stack.enter_context(lotus.settings.context(lm=lm,enable_cache=False))
                stack.enter_context(lotus_executor(lm,selected))
                stack.enter_context(patch.object(ImageArray,"get_image",get_image))
                data=frame()
                if mixed:
                    with self.assertRaises(type(native_error)) as caught:
                        data.sem_map("Describe {listing_text} using {image}.",suffix="_decision")
                    self.assertEqual(getattr(caught.exception,"status_code",None),429)
                    self.assertEqual(sorted(record["prepared"],key=lambda r:r["identity"]),
                                     sorted(record["native"],key=lambda r:r["identity"]))
                    self.assertEqual(sorted(r["status"] for r in record["responses"]),[200,200,200,429])
                else:
                    filtered=data.sem_filter("{image} is consistent with {listing_text}.")
                    self.assertEqual(filtered.index.tolist(),[10,30])
                    self.assertEqual(filtered["listing_text"].tolist(),["keep-a","keep-c"])
                    gate.clear()
                    current["stage"]= "map"
                    mapped=filtered.sem_map("Describe {listing_text} using {image}.",suffix="_decision",return_raw_outputs=True)
                    self.assertEqual(mapped.index.tolist(),[10,30])
                    self.assertEqual(mapped["_decision"].tolist(),["Answer: keep-a","Answer: keep-c"])
                    map_order=[r["sequence"] for r in record["responses"] if r["stage"]=="map"]
                    self.assertLess(map_order.index(1),map_order.index(0))
                    self.assertEqual(lm.stats.physical_usage.total_tokens,72)
                    record["expected_indices"],record["expected_outputs"]=[10,30],["Answer: keep-a","Answer: keep-c"]
                first_stage="map" if mixed else "filter"
                order=[r["sequence"] for r in record["responses"] if r["stage"]==first_stage]
                self.assertLess(order.index(1),order.index(0))
                self.assertEqual(execution.engine.capacity.usage(),Usage())
                self.assertEqual(execution.engine.jobs.jobs,{})
                self.assertEqual(execution.engine.preparation.snapshot()["object_bytes"],0)
                self.assertEqual(selected.last_cleanup_errors,())
                calls.wait_idle()
                self.assertEqual(len(calls)-before,4 if mixed else 6)
                self.assertTrue(execution.close());execution=None
            received=[dict(identity=r["identity"],json_sha256=r["json_sha256"]) for r in calls[before:]]
            self.assertEqual(sorted(received,key=lambda r:r["identity"]),sorted(record["prepared"],key=lambda r:r["identity"]))
            record["received"]=list(calls[before:])
            record["status"]="passed"
            return record
        except BaseException as error:
            record.update(status="failed",failure_type=type(error).__name__)
            raise
        finally:
            gate.set()
            try:
                if execution is not None:
                    close_for_record(execution,record)
            finally:
                record["resource_summary"]=sampler.summary()
                write_private_json(root/(name+".json"),record)
