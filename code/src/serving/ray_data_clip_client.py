"""NumPy-only workload function for Ray Data batches sent to Ray Serve."""
from __future__ import annotations

import uuid

import numpy as np


def rgb_batch(doc_ids, images):
    """Validate a complete native batch without importing model libraries."""
    ids = tuple(doc_ids)
    if (not ids or any(not isinstance(x, str) or not x for x in ids)
            or len(set(ids)) != len(ids) or len(images) != len(ids)):
        raise ValueError("image IDs must be nonempty, unique and aligned")
    arrays = list(images)
    if any(not isinstance(x, np.ndarray) or x.dtype != np.uint8
           or x.ndim != 3 or x.shape[-1] != 3 or min(x.shape[:2]) <= 0
           for x in arrays):
        raise TypeError("images must be nonempty uint8 HWC RGB arrays")
    return ids, arrays


def clip_service_batch(batch, *, handle, service_uid, call_id, timeout_s):
    """One ordinary synchronous Data task calls one unary service method."""
    ids, images = rgb_batch(batch["doc_id"], batch["image"])
    request_id = call_id + ":" + uuid.uuid4().hex
    response = handle.embed.remote(dict(doc_ids=ids, images=images,
        service_uid=service_uid, request_id=request_id))
    # This runs in a synchronous Data function, never inside an asyncio UDF.
    # A timeout leaves completion unknown; the owner must stop further reuse.
    reply = response.result(timeout_s=timeout_s)
    if (not isinstance(reply, dict) or reply.get("service_uid") != service_uid
            or reply.get("request_id") != request_id):
        raise ValueError("CLIP service identity or response association changed")
    result_ids = tuple(reply["doc_id"])
    matrix = reply["embedding"]
    if (len(result_ids) != len(ids) or len(set(result_ids)) != len(ids)
            or set(result_ids) != set(ids) or not isinstance(matrix, np.ndarray)
            or matrix.dtype != np.float32 or matrix.shape != (len(ids), 512)):
        raise ValueError("CLIP service must return complete aligned float32 vectors")
    output = {"doc_id": np.asarray(result_ids), "embedding": matrix}
    probe = reply.get("model_reuse")
    if probe is not None:
        for field, key in (("uid", "model_uid"), ("pid", "pid"),
                           ("embed_calls", "image_returns"), ("rows", "image_returned_rows")):
            output["_semloom_model_" + field] = np.repeat(probe[key], len(ids))
    return output
