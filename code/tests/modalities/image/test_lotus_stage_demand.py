"""Real pinned LOTUS, unique existing assets, synthetic orientation responses.

No model runs. Select SEMLOOM_COCO_IMAGE_ROOT to exercise the historical twelve
assets; their immutable manifest hashes are checked before query preparation.
This is a data-organization diagnostic, not a native performance benchmark.
"""
from collections import Counter, OrderedDict
from contextlib import nullcontext
from importlib.util import find_spec
from io import BytesIO
import base64
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

from PIL import Image
from src.data.materializers.prepared_assets import (
    AssetUse, PreparedAssetKey, PreparedAssetWindow, PreparedSourceMetadata,
)
from src.semantic_methods.lotus.sdk import IMAGE_TEXT_PROFILE, IMAGE_SOURCE_HASHES, validate_source
from src.planning.work import StageWork, WorkDescriptor
from tests.modalities.image.stage_asset_snapshot import read_image_snapshot


PINNED = all(find_spec(name) is not None for name in ("lotus", "pandas", "httpx", "litellm"))
MANIFEST = Path(__file__).resolve().parents[4] / "experiments/results/postgresql/capacity_organization_image_validation_20260920/raw/asset-preparation.json"


def stage_assets():
    """Reuse the historical twelve assets, source versions and preparation bounds."""
    started = time.monotonic()
    manifest = json.loads(MANIFEST.read_text())["images"]["selected"]
    root = Path(os.environ["SEMLOOM_COCO_IMAGE_ROOT"])
    if len(manifest) != 12:
        raise ValueError("fixture requires the twelve unique historical assets")
    paths, sources, keys, snapshots = [], {}, {}, {}
    from PIL import __version__ as pillow_version
    for row in manifest:
        path = root / row["file"]
        snapshot = read_image_snapshot(path, row["sha256"], row["bytes"])
        key = PreparedAssetKey(row["sha256"], "lotus-rgb-png-base64:" +
                               IMAGE_SOURCE_HASHES["utils.py"] + ":pillow=" + pillow_version)
        paths.append(str(path))
        keys[str(path)] = key
        snapshots[str(path)] = snapshot
        work = WorkDescriptor((StageWork("source", len(snapshot.encoded), "encoded_bytes"),
                               StageWork("prepare", snapshot.rgb_bytes, "rgb_bytes"),
                               StageWork("model", 1, "complete_requests")), "model",
                              key.transform_signature, lower_primary_units=1, upper_primary_units=1)
        sources[key] = PreparedSourceMetadata(row["file"], row["sha256"], work,
                                              "historical-manifest+verified-bytes+image-header")
    if len(paths) != 12 or len(sources) != 12:
        raise ValueError("fixture requires the twelve unique historical assets")
    # This fixed source residency is separate from the auxiliary prepare cache.
    return paths, sources, keys, snapshots, time.monotonic() - started


class StaticSharedPreparation:
    """Fixed-order LRU with the same source bounds and actual barrier receipt.

    Last-use accounting is a strong ordinary static control. It knows exactly
    the same consumers/versions as the candidate and predicts no predicate.
    """
    def __init__(self, sources, host_bytes):
        self.sources, self.host_bytes = sources, host_bytes
        self.cache = OrderedDict()
        self.remaining = Counter({key: 2 for key in sources})
        self.prepare_order = []
        self.byte_seconds, self.last = 0.0, time.monotonic()

    def account(self):
        now = time.monotonic()
        self.byte_seconds += sum(len(value) for value in self.cache.values()) * (now - self.last)
        self.last = now

    def get(self, key, prepare):
        self.account()
        if key not in self.cache:
            required = 1048576 + 2 * self.sources[key].work.for_stage("prepare").units
            while self.cache and sum(len(v) for v in self.cache.values()) + required > self.host_bytes:
                self.cache.popitem(last=False)
            if required > self.host_bytes:
                raise BufferError("one preparation cannot fit static capacity")
            value = prepare()
            self.account()
            if len(value) > 1048576:
                raise BufferError("prepared asset exceeds declared capacity")
            self.cache[key] = value
            self.prepare_order.append(key.content_digest)
        self.cache.move_to_end(key)
        return self.cache[key]

    def release(self, key):
        self.account()
        self.remaining[key] -= 1
        if not self.remaining[key]:
            self.cache.pop(key, None)

    def resolve(self, survivors):
        for key in self.sources:
            if key not in survivors:
                self.release(key)


@unittest.skipUnless(PINNED and os.environ.get("SEMLOOM_COCO_IMAGE_ROOT"),
                     "existing COCO and pinned LOTUS not selected; no downloads")
class LotusStageDemandTest(unittest.TestCase):
    def test_original_filter_map_barrier_has_no_extra_organization_opportunity(self):
        import httpx
        import lotus
        import pandas as pd
        from lotus.dtype_extensions import ImageArray
        from lotus.dtype_extensions.image import fetch_image
        from lotus.models import LM
        validate_source(message_profile=IMAGE_TEXT_PROFILE)
        paths, metadata, keys, snapshots, metadata_acquisition_s = stage_assets()
        self.assertEqual(len(keys), 12)  # No added aliases or duplicated rows.
        self.assertEqual(len({key.content_digest for key in keys.values()}),12)
        all_records = []
        expected_calls = expected_rows = None
        for host_bytes in (4 * 1048576, 16 * 1048576):
            reference_order = None
            for arm in ("native", "static_uri", "static_bytes", "consumer_bytes"):
                uses = tuple(use for path in paths for use in (
                    AssetUse("filter:" + Path(path).name, keys[path], source=metadata[keys[path]]),
                    AssetUse("map:" + Path(path).name, keys[path], source=metadata[keys[path]],
                             ready_after="filter-return")))
                window = PreparedAssetWindow(uses, max_consumers=24, host_bytes=host_bytes,
                                             clock=time.monotonic)
                static = StaticSharedPreparation(metadata, host_bytes)
                stage = ["filter"]
                calls, prepared_order, boundaries = [], [], []
                copied_uri_bytes = [0]
                message_owners = []

                def prepare(path):
                    prepared_order.append(keys[path].content_digest)
                    uri = snapshots[path].fetch(fetch_image)
                    return uri if arm == "static_uri" else uri.encode("ascii")

                def get_image(array, index, image_type="Image"):
                    self.assertEqual(image_type, "base64")
                    path = str(array._data[index])
                    key = keys[path]
                    if arm in ("static_uri", "static_bytes"):
                        payload = static.get(key, lambda: prepare(path))
                        uri = payload if arm == "static_uri" else payload.decode("ascii")
                        static.release(key)
                    else:
                        consumer = stage[0] + ":" + Path(path).name
                        lease = window.acquire(consumer, prepare=lambda: prepare(path),
                            prepared_bytes_upper_bound=1048576,
                            scratch_bytes_upper_bound=2 * metadata[key].work.for_stage("prepare").units)
                        self.assertIsNotNone(lease)
                        uri = lease.payload.decode("ascii")
                        window.release(lease)
                    # Only the temporary bytes exporter stopped reading. The
                    # independent URI still belongs to the native messages.
                    if arm != "static_uri":
                        copied_uri_bytes[0] += len(uri)
                    return uri

                def send(client, request, **kwargs):
                    body = json.loads(request.read())
                    parts = body["messages"][-1]["content"]
                    uri = next(p["image_url"]["url"] for p in parts if p["type"] == "image_url")
                    with Image.open(BytesIO(base64.b64decode(uri.split(",", 1)[1]))) as image:
                        landscape = image.width >= image.height
                    text = " ".join(p["text"] for p in parts if p["type"] == "text")
                    is_filter = "Claim:" in text
                    self.assertEqual(stage[0], "filter" if is_filter else "map")
                    calls.append(dict(body_sha256=hashlib.sha256(json.dumps(body,sort_keys=True).encode()).hexdigest(),
                                      request_bytes=len(request.read()), stage=stage[0], uri_bytes=len(uri)))
                    response = dict(id="stage-demand",object="chat.completion",created=1,model=body["model"],
                        choices=[dict(index=0,message=dict(role="assistant",content="Answer: " +
                            (str(landscape) if is_filter else "landscape" if landscape else "portrait")),
                                      finish_reason="stop")],usage=dict(prompt_tokens=9,completion_tokens=3,total_tokens=12))
                    return httpx.Response(200,content=json.dumps(response).encode(),request=request,
                                          headers={"content-type":"application/json"})

                frame = pd.DataFrame({"image":ImageArray(paths), "asset_id":[Path(p).name for p in paths]})
                lm = LM("openai/stage-image-fixture",api_base="http://localhost/v1",api_key="fixture",
                        max_tokens=32,max_batch_size=1,timeout=2,num_retries=0,max_retries=0)
                original_process = lm._process_uncached_messages
                def process(data, *args, **kwargs):
                    def owned():
                        uris = {id(part["image_url"]["url"]):part["image_url"]["url"]
                            for messages,_ in data for message in messages if isinstance(message["content"],list)
                            for part in message["content"] if part["type"] == "image_url"}
                        # Count aliased strings once. Byte-cache values are a
                        # separate representation, even if their contents match.
                        representations = dict(uris)
                        if arm in ("static_uri","static_bytes"):
                            cached = static.cache.values()
                        elif arm == "consumer_bytes":
                            cached = window._assets.values()  # Read-only diagnostic of owned buffers.
                        else:
                            arrays = [frame["image"].array]
                            if stage[0] == "map":
                                arrays.append(filtered["image"].array)
                            cached = [value for array in arrays for value in array._cached_images.values()]
                        representations.update({id(value):value for value in cached})
                        return dict(uri_bytes=sum(len(value) for value in uris.values()),
                                    python_uri_bytes=sum(sys.getsizeof(value) for value in uris.values()),
                                    distinct_message_uris=len(uris),
                                    distinct_known_image_representation_bytes=sum(sys.getsizeof(value)
                                        for value in representations.values()),
                                    cache_payload_bytes=sum(len(value) for value in static.cache.values())
                                        if arm in ("static_uri","static_bytes") else
                                        window.snapshot().prepared_bytes if arm == "consumer_bytes" else None,
                                    cache_input_readers=window.snapshot().held_consumers
                                        if arm == "consumer_bytes" else None)
                    before = owned()
                    result = original_process(data,*args,**kwargs)
                    message_owners.append(dict(stage=stage[0],before=before,after=owned()))
                    return result
                cache_patch = (patch.object(ImageArray,"get_image",get_image) if arm != "native" else nullcontext())
                native_fetch = fetch_image
                def observe_native(image, image_type="Image"):
                    prepared_order.append(keys[str(image)].content_digest)
                    return snapshots[str(image)].fetch(native_fetch,image_type)
                native_patch = (patch("lotus.dtype_extensions.image.fetch_image",observe_native)
                                if arm == "native" else nullcontext())
                started = time.monotonic()
                with lotus.settings.context(lm=lm,enable_cache=False), patch.object(httpx.Client,"send",send), \
                     patch.object(lm,"_process_uncached_messages",process), cache_patch, native_patch:
                    filtered = frame.sem_filter("The {image} is landscape oriented. Asset: {asset_id}.")
                    survivors = {keys[str(value)] for value in filtered["image"].array._data}
                    if arm in ("static_uri", "static_bytes"):
                        static.resolve(survivors)
                    elif arm == "consumer_bytes":
                        window.resolve_stage("filter-return",tuple("map:" + name for name in filtered["asset_id"]))
                    boundaries.append(dict(stage="filter-return",logical_calls=len(calls),survivors=len(survivors)))
                    stage[0] = "map"
                    mapped = filtered.sem_map("Return the orientation of {image}. Asset: {asset_id}.",suffix="_orientation")
                query_s = time.monotonic() - started
                rows = mapped[["asset_id","_orientation"]].to_dict("records")
                if expected_calls is None:
                    expected_calls, expected_rows = calls, rows
                self.assertEqual(calls,expected_calls)
                self.assertEqual(rows,expected_rows)
                self.assertEqual(lm.stats.physical_usage.total_tokens,len(calls)*12)
                self.assertTrue(all(c["stage"] == "filter" for c in calls[:12]))
                self.assertTrue(all(c["stage"] == "map" for c in calls[12:]))
                if arm in ("static_uri", "static_bytes"):
                    if reference_order is None:
                        reference_order = prepared_order
                    self.assertEqual(prepared_order,reference_order)
                    static.account()
                    self.assertFalse(static.cache)
                elif arm == "consumer_bytes":
                    self.assertEqual(prepared_order,reference_order)
                    snapshot = window.snapshot()
                    self.assertEqual((snapshot.pending_consumers,snapshot.held_consumers,snapshot.prepared_bytes),(0,0,0))
                all_records.append(dict(arm=arm,host_cache_bytes=host_bytes if arm != "native" else None,
                    cache_cap_applied=arm != "native",logical_calls=len(calls),
                    prepare_calls=len(prepared_order),prepare_order=prepared_order,copied_uri_bytes=copied_uri_bytes[0],
                    request_bytes=sum(c["request_bytes"] for c in calls),stage_boundaries=boundaries,
                    diagnostic_query_wall_s=query_s,metadata_acquisition_s=metadata_acquisition_s,
                    source_snapshot_bytes=sum(len(snapshot.encoded) for snapshot in snapshots.values()),
                    message_owners=message_owners,
                    cache_byte_seconds=(static.byte_seconds if arm in ("static_uri","static_bytes") else
                        window.snapshot().resident_byte_seconds if arm == "consumer_bytes" else None)))
        output = os.environ.get("SEMLOOM_STAGE_DEMAND_OUTPUT")
        if output:
            Path(output).write_text(json.dumps(dict(records=all_records,unique_source_assets=12,
                real_model_calls=0,gpu_starts=0,query_responses="deterministic orientation fixture, not VLM quality",
                scope="mock SDK query wall time; cache payload byte-seconds exclude original messages, other copies and RSS"),indent=2)+"\n")


if __name__ == "__main__":
    unittest.main()
