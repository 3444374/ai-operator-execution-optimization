"""Native LOTUS sem_index / sem_search with explicit PIL images and row identity."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import threading
import time

import numpy as np
from PIL import Image

from .image_rm import timing_span

_owner = threading.Lock()


@dataclass(frozen=True)
class RetrievalQuery:
    query_id: str
    value: Image.Image | str
    expected_doc_id: str | None = None


def image_frame(doc_ids, images):
    import pandas as pd
    if not doc_ids or len(doc_ids) != len(images) or len(set(doc_ids)) != len(doc_ids):
        raise ValueError("gallery IDs and images must be nonempty, unique and aligned")
    if any(not isinstance(x, str) or not x for x in doc_ids):
        raise ValueError("gallery IDs must be nonempty strings")
    values = np.empty(len(images), dtype=object)
    for i, image in enumerate(images):
        if not isinstance(image, Image.Image) or image.mode != "RGB":
            raise TypeError("gallery column must contain RGB PIL images, never paths")
        values[i] = image
    return pd.DataFrame({"doc_id": list(doc_ids), "image": values})


def gallery_identity(frame, embedding_identity):
    rows = []
    for row in frame.itertuples(index=False):
        digest = hashlib.sha256(row.image.tobytes()).hexdigest()
        rows.append([row.doc_id, list(row.image.size), digest])
    payload = dict(schema="lotus.image_index.v1", rows=rows, embedding=embedding_identity)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return dict(payload, sha256=hashlib.sha256(encoded).hexdigest())


@contextmanager
def native_settings(rm, vs):
    """This qualified entry is serial; no claim about cross-query concurrency."""
    import lotus
    if not _owner.acquire(blocking=False):
        raise RuntimeError("another native image workflow owns this process")
    try:
        with lotus.settings.context(rm=rm, vs=vs, enable_cache=False, reranker=None):
            yield
    finally:
        _owner.release()


def run_workflow(frame, rm, queries, *, index_dir, embedding_identity, k=10,
                 mode="combined", max_gallery_rows=5000, report=None):
    """Run the unmodified native index and search, including native output assembly."""
    import pandas as pd
    from lotus.vector_store import FaissVS

    if mode not in ("combined", "index", "search"):
        raise ValueError("unknown native image workflow mode")
    if (type(k) is not int or k < 1 or type(max_gallery_rows) is not int
            or not 0 < len(frame) <= max_gallery_rows):
        raise ValueError("invalid K or gallery collection size")
    if not frame.index.equals(pd.RangeIndex(len(frame))):
        raise ValueError("native FAISS image gallery must use its original RangeIndex")
    if any(not isinstance(x, Image.Image) or x.mode != "RGB" for x in frame["image"]):
        raise TypeError("native image index must encode PIL images")
    if len(set(frame["doc_id"])) != len(frame):
        raise ValueError("gallery IDs must be unique")
    if mode != "index" and not queries:
        raise ValueError("native search needs at least one query")
    if len({x.query_id for x in queries}) != len(queries):
        raise ValueError("query IDs must be unique")
    for query in queries:
        if (not query.query_id or not isinstance(query.value, (Image.Image, str))
                or isinstance(query.value, Image.Image) and query.value.mode != "RGB"):
            raise TypeError("queries need an ID and RGB PIL image or text")
    index_dir = Path(index_dir)
    identity = gallery_identity(frame, embedding_identity)
    report = report if report is not None else {}
    report.update(schema="lotus.image_workflow.v1", mode=mode, status="running",
                  gallery_rows=len(frame), k=k, index_identity_sha256=identity["sha256"],
                  index_s=None, index_load_s=None, queries=[], native_cache=False)
    started = time.monotonic_ns()
    vs = FaissVS()  # Keep native Flat inner-product indexing/search and persistence.
    with native_settings(rm, vs):
        if mode in ("combined", "index"):
            if index_dir.exists():
                raise FileExistsError("refuse to overwrite a native image index")
            report["stage"] = "index"
            at = time.monotonic_ns()
            with timing_span("lotus.sem_index", scope="native_operator"):
                frame = frame.sem_index("image", str(index_dir))
            report["index_s"] = (time.monotonic_ns() - at) / 1e9
            report["index_matrix_sha256"] = hashlib.sha256((index_dir / "vecs").read_bytes()).hexdigest()
            report["index_file_sha256"] = hashlib.sha256((index_dir / "index").read_bytes()).hexdigest()
            identity.update(vecs_sha256=report["index_matrix_sha256"],
                            index_sha256=report["index_file_sha256"])
            with (index_dir / "identity.json").open("x") as stream:
                json.dump(identity, stream, sort_keys=True)
        else:
            stored = json.loads((index_dir / "identity.json").read_text())
            if stored["sha256"] != identity["sha256"]:
                raise ValueError("existing index does not match ordered gallery or embedding identity")
            for name, digest in (("vecs", stored["vecs_sha256"]), ("index", stored["index_sha256"])):
                if hashlib.sha256((index_dir / name).read_bytes()).hexdigest() != digest:
                    raise ValueError("existing native index contents changed")
            report["stage"] = "index_load"
            at = time.monotonic_ns()
            frame = frame.load_sem_index("image", str(index_dir))
            # Native sem_search performs the actual load; keep that cost in its time.
            report["index_load_s"] = (time.monotonic_ns() - at) / 1e9
        if mode != "index":
            for query in queries:
                report["stage"] = "query:" + query.query_id
                before = len(rm.observation.calls)
                at = time.monotonic_ns()
                record = dict(query_id=query.query_id,
                    kind="image" if isinstance(query.value, Image.Image) else "text", status="failed")
                try:
                    with timing_span("lotus.sem_search", scope="native_operator", query_id=query.query_id):
                        result = frame.sem_search("image", query.value, K=k, return_scores=True)
                        positions = [int(x) for x in result.index]
                        ids = result["doc_id"].tolist()
                        scores = [float(x) for x in result["vec_scores_sim_score"]]
                        record.update(observed_doc_ids=ids, observed_positions=positions,
                                      observed_scores=[x if np.isfinite(x) else repr(x) for x in scores])
                        if (len(ids) != min(k, len(frame)) or len(set(ids)) != len(ids)
                                or any(frame.iloc[i]["doc_id"] != x for i, x in zip(positions, ids))
                                or not np.isfinite(scores).all()):
                            raise ValueError("native retrieval output has invalid rows, rank or scores")
                        if query.expected_doc_id is not None and query.expected_doc_id not in ids:
                            raise ValueError("known image query did not retrieve its source image")
                    record.update(status="succeeded", doc_ids=ids, positions=positions, scores=scores)
                except BaseException as error:
                    record["error_type"] = type(error).__name__
                    raise
                finally:
                    record.update(elapsed_s=(time.monotonic_ns() - at) / 1e9,
                                  rm_calls=rm.observation.calls[before:])
                    report["queries"].append(record)
                    report["rm_calls"] = rm.observation.calls
    report.update(status="succeeded", stage="complete", native_workflow_s=(time.monotonic_ns() - started) / 1e9,
                  rm_calls=rm.observation.calls,
                  tail_quantiles={"p99": None, "p99_99": None,
                                  "reason": "qualification has too few independent complete queries"})
    return report
