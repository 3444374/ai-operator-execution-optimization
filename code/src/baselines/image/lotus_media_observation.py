"""Matched finite LOTUS media observation and optional representation retention."""

from contextlib import contextmanager
from io import BytesIO
import hashlib
import importlib
import importlib.metadata
import inspect
import json
from pathlib import Path
import time

from src.modalities.image.shared_media import SharedMediaRepresentation, media_key


class LotusMediaObservation:
    """Frontend owner only; no model scheduling or TaskPreparation authority."""

    def __init__(self, rows, stage_ids, *, retention_bytes, profile, record):
        if type(retention_bytes) is not int or not 0 <= retention_bytes <= 256 * 1024 * 1024:
            raise ValueError("media retention must be between zero and 256 MiB")
        self.rows, self.stage_ids, self.record = rows, tuple(stage_ids), record
        self.profile = profile
        self.module = importlib.import_module("lotus.dtype_extensions.image")
        self.original_fetch = self.module.fetch_image
        self.profile = dict(profile, media_recipe=dict(
            fetch_image_sha256=hashlib.sha256(inspect.getsource(self.original_fetch).encode()).hexdigest(),
            lotus=importlib.metadata.version("lotus-ai"), pillow=importlib.metadata.version("Pillow"),
            convert="RGB", encoding="PNG", resize="none", representation="ASCII data:image/png;base64"))
        plan = {}
        for row in rows:
            key = media_key(row["image"]["sha256"], self.profile)
            plan[key] = plan.get(key, ()) + tuple(stage + "/" + row["row_id"] for stage in self.stage_ids)
        # Zero is off. A one-byte positive MethodBudget can never retain a PNG URI.
        self.owner = SharedMediaRepresentation(plan, capacity_bytes=max(1, retention_bytes),
            max_entries=len(rows), max_item_bytes=8 * 1024 * 1024,
            max_consumers=len(rows) * len(self.stage_ids), retention_enabled=retention_bytes > 0)
        self.retention_bytes = retention_bytes
        self.stage, self.position, self.encoded = None, 0, 0
        self.read_validate_ns = self.encode_ns = self.prepare_ns = 0
        self.closed = False

    def begin(self, stage, identities):
        if self.stage is not None or stage not in self.stage_ids or identities != [r["row_id"] for r in self.rows]:
            raise ValueError("unexpected or overlapping original media consumer")
        self.stage, self.position = stage, 0

    def finish(self):
        if self.stage is None or self.position != len(self.rows):
            raise ValueError("media consumer did not cover the declared complete input")
        for row in self.rows:
            self.owner.finish(row["image"]["sha256"], self.profile, self.stage + "/" + row["row_id"])
        self.record(dict(event="consumer_terminal", stage_id=self.stage,
                         monotonic_ns=time.monotonic_ns(), cache=self.owner.snapshot()))
        self.stage = None

    def fetch(self, value, kind):
        from PIL import Image
        if self.stage is None or self.position >= len(self.rows) or kind != "base64":
            raise ValueError("media expansion outside its declared source consumer")
        row = self.rows[self.position]
        if str(value) != row["ImagePath"]:
            raise ValueError("original media expansion changed source occurrence order")
        start = time.monotonic_ns()
        raw = Path(value).read_bytes()
        if len(raw) != row["image"]["bytes"] or hashlib.sha256(raw).hexdigest() != row["image"]["sha256"]:
            raise ValueError("media source changed after original preflight")
        validated = time.monotonic_ns()
        encoded = False
        def produce(verified):
            nonlocal encoded
            begin = time.monotonic_ns()
            with Image.open(BytesIO(verified)) as image:
                result = self.original_fetch(image, kind)
            self.encode_ns += time.monotonic_ns() - begin
            self.encoded += 1
            encoded = True
            return result
        result = self.owner.borrow(raw, row["image"]["sha256"], self.profile,
                                   self.stage + "/" + row["row_id"], produce)
        end = time.monotonic_ns()
        self.read_validate_ns += validated - start
        self.prepare_ns += end - start
        self.position += 1
        self.record(dict(event="representation_ready", stage_id=self.stage, row_id=row["row_id"],
            source_sha256=row["image"]["sha256"], started_monotonic_ns=start,
            validated_monotonic_ns=validated, ready_monotonic_ns=end, encoded=encoded,
            uri_bytes=len(result), cache=self.owner.snapshot()))
        return result

    @contextmanager
    def observe(self):
        if self.module.fetch_image is not self.original_fetch:
            raise RuntimeError("another LOTUS media observer is already active")
        self.module.fetch_image = self.fetch
        try:
            yield self
        finally:
            self.module.fetch_image = self.original_fetch

    def snapshot(self):
        profile_json = json.dumps(self.profile,sort_keys=True,separators=(",",":"),allow_nan=False)
        return dict(retention_enabled=self.retention_bytes > 0,
            representation_profile=json.loads(profile_json),
            representation_profile_sha256=hashlib.sha256(profile_json.encode()).hexdigest(),
            configured_retention_bytes=self.retention_bytes, cache=self.owner.snapshot(),
            encodings=self.encoded, read_validate_ns=self.read_validate_ns,
            encode_ns=self.encode_ns, media_prepare_ns=self.prepare_ns, closed=self.closed,
            scope="frontend URI retention; LOTUS arrays/messages, encoder/SDK copies, RSS and HBM separate")

    def close(self):
        self.owner.close()  # fail closed if a consumer is active or still in the future
        self.closed = True
