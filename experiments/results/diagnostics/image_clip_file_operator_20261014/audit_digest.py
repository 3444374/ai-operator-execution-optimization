#!/usr/bin/env python3
"""Exercise the exact measured audit with synthetic vectors, without SDK imports."""
import argparse
import ast
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import sys
import types
from typing import Any

import numpy as np


def load_audit(source, expected_sha):
    raw = source.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha:
        raise ValueError("audit source differs from measured source mapping")
    tree = ast.parse(raw)
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name in ("EmbeddingCapture", "EmbeddingAudit")]
    assert len(classes) == 2
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    module = types.ModuleType("measured_image_audit")
    module.__dict__.update(np=np, hashlib=hashlib, dataclass=dataclass, field=field, Any=Any)
    sys.modules[module.__name__] = module
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future] + classes, type_ignores=[])), str(source), "exec"), module.__dict__)
    return module.EmbeddingAudit


def run(source_root, root):
    source_path = "code/src/modalities/image/execution.py"
    files = json.loads((root / "raw/source-files.json").read_text())["files"]
    expected = next(item["sha256"] for item in files if item["path"] == source_path)
    Audit = load_audit(source_root / source_path, expected)
    ids = ("image-a", "image-b", "image-c")
    vectors = np.asarray([[0.1, 0.2, 0.3], [0.25, -0.5, 0.75], [-0.125, 0.625, 0.375]], dtype=np.float32)

    def digest(keys, matrix, chunk_size=3):
        audit = Audit(frozenset(ids), dimension=3)
        for offset in range(0, len(keys), chunk_size):
            audit.add(tuple(keys[offset:offset+chunk_size]), matrix[offset:offset+chunk_size])
        return audit.finish()["embedding_digest_xor_rounded5"]

    baseline = digest(ids, vectors)
    zero = vectors.copy()
    zero[0, 0] = 0.0
    negative_zero = zero.copy()
    negative_zero[0, 0] = -0.0
    checks = {
        "row_order_invariant": digest(ids[::-1], vectors[::-1]) == baseline,
        "consumer_chunking_invariant": digest(ids, vectors, 1) == baseline,
        "lossless_float64_container_invariant": digest(ids, vectors.astype(np.float64)) == baseline,
        "big_endian_float32_container_invariant": digest(ids, vectors.astype(">f4")) == baseline,
        "signed_zero_can_change_hash_without_value_change": np.array_equal(zero, negative_zero) and digest(ids, zero) != digest(ids, negative_zero),
        "same_id_set_wrong_id_vector_association_changes_hash": digest(ids, vectors[::-1]) != baseline,
    }
    assert all(bool(value) for value in checks.values())
    return {
        "status": "passed_offline_source_behavior_checks",
        "source_sha256": expected,
        "numpy_version": np.__version__,
        "model_GPU_SDK_server_runs": 0,
        "checks": {key: bool(value) for key, value in checks.items()},
        "definition": "BLAKE2b128(UTF8(doc_id)+NUL+bytes(round(float32(row),5).astype('<f4'))), XOR across rows",
        "conclusion": "mere output order, consumer grouping and lossless dtype/endian representation cannot explain the observed mismatch; signed zero/rounding or numerical/input association differences remain possible; source contract matches, pointwise error and cause remain unverified",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.source_root, Path(__file__).resolve().parent), indent=2))
