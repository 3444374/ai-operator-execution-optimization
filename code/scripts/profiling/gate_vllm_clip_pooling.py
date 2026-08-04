#!/usr/bin/env python3
"""Run one offline vLLM CLIP pooling capability gate and write JSON evidence.

This is a capability/correctness gate, not a throughput benchmark.  It verifies
that the installed vLLM can load the requested CLIP model and return finite,
one-dimensional image/text embeddings.  A shell harness supplies the wall-clock
timeout because an in-process timeout cannot reliably terminate vLLM workers.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import platform
import sys
import time
from pathlib import Path
from typing import Any, Sequence

import numpy as np
from PIL import Image


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default="/root/autodl-tmp/models/clip-vit-base-patch32")
    parser.add_argument("--image", default="", help="Explicit image path")
    parser.add_argument(
        "--coco-glob",
        default="/root/autodl-tmp/data/raw/coco_train2017/*.jpg",
        help="Fallback image glob used only when --image is empty",
    )
    parser.add_argument("--texts", nargs="*", default=["a cat", "a scenic mountain landscape"])
    parser.add_argument("--expected-dimension", type=int, default=512)
    parser.add_argument("--json-out", required=True, type=Path)
    return parser.parse_args()


def resolve_image_path(explicit_path: str, fallback_glob: str) -> Path:
    """Resolve exactly one readable input image or fail before model startup."""

    if explicit_path:
        path = Path(explicit_path).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"--image is not a readable file: {path}")
        return path.resolve()
    matches = sorted(Path(path) for path in glob.glob(fallback_glob))
    if not matches:
        raise FileNotFoundError(f"--coco-glob matched no images: {fallback_glob}")
    return matches[0].resolve()


def extract_embedding(results: Sequence[Any]) -> np.ndarray:
    """Extract one embedding from vLLM ``LLM.embed`` output.

    vLLM 0.25 returns ``list[EmbeddingRequestOutput]`` where the vector is
    ``results[0].outputs.embedding``.  The fallback for a sequence-valued
    ``outputs`` keeps the gate compatible with older pooling output wrappers,
    without guessing arbitrary attribute names on the request object.
    """

    if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
        raise TypeError(f"LLM.embed must return a sequence, got {type(results).__name__}")
    if len(results) != 1:
        raise ValueError(f"expected one embedding result, got {len(results)}")
    request_output = results[0]
    output = getattr(request_output, "outputs", None)
    if output is None:
        raise RuntimeError(f"{type(request_output).__name__} has no outputs attribute")
    if not hasattr(output, "embedding") and isinstance(output, Sequence):
        if len(output) != 1:
            raise ValueError(f"expected one pooling output, got {len(output)}")
        output = output[0]
    value = getattr(output, "embedding", None)
    if value is None:
        visible = [name for name in dir(output) if not name.startswith("_")]
        raise RuntimeError(
            f"could not extract embedding from {type(output).__name__}; attrs={visible}"
        )
    embedding = np.asarray(value, dtype=np.float32)
    if embedding.ndim != 1:
        raise ValueError(f"expected a 1-D embedding, got shape={embedding.shape}")
    return embedding


def _embedding_summary(embedding: np.ndarray) -> dict[str, Any]:
    return {
        "dimension": int(embedding.shape[0]),
        "finite": bool(np.all(np.isfinite(embedding))),
        "norm": float(np.linalg.norm(embedding)),
        "min": float(np.min(embedding)),
        "max": float(np.max(embedding)),
    }


def _cosine(left: np.ndarray, right: np.ndarray) -> float:
    denominator = float(np.linalg.norm(left) * np.linalg.norm(right))
    return float(left @ right / denominator) if denominator > 0 else float("nan")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite gate evidence: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    temporary.replace(path)


def run_gate(args: argparse.Namespace) -> dict[str, Any]:
    image_path = resolve_image_path(args.image, args.coco_glob)
    model_path = Path(args.model).expanduser()
    if model_path.is_absolute() and not model_path.exists():
        raise FileNotFoundError(f"local model path does not exist: {model_path}")
    if args.expected_dimension <= 0:
        raise ValueError("--expected-dimension must be positive")

    started = time.perf_counter()
    from vllm import LLM, __version__ as vllm_version

    load_started = time.perf_counter()
    llm = LLM(
        model=args.model,
        runner="pooling",
        limit_mm_per_prompt={"image": 1},
        # Diagnostic gate only.  Eager mode isolates model/API capability from
        # CUDA-graph and torch.compile startup; it is not a formal-run setting.
        enforce_eager=True,
    )
    load_s = time.perf_counter() - load_started

    image_started = time.perf_counter()
    with Image.open(image_path) as image:
        image_result = llm.embed(
            {
                "prompt": "",
                "multi_modal_data": {"image": image.convert("RGB")},
            }
        )
    image_embedding = extract_embedding(image_result)
    image_embed_s = time.perf_counter() - image_started

    text_results: list[dict[str, Any]] = []
    for text in args.texts:
        text_started = time.perf_counter()
        text_embedding = extract_embedding(llm.embed(text))
        summary = _embedding_summary(text_embedding)
        summary.update(
            {
                "text": text,
                "embed_s": time.perf_counter() - text_started,
                "cosine_with_image": _cosine(image_embedding, text_embedding),
            }
        )
        text_results.append(summary)

    image_summary = _embedding_summary(image_embedding)
    checks = {
        "image_dimension_matches": image_summary["dimension"] == args.expected_dimension,
        "image_finite": image_summary["finite"],
        "text_dimensions_match": all(
            item["dimension"] == args.expected_dimension for item in text_results
        ),
        "text_finite": all(item["finite"] for item in text_results),
    }
    return {
        "schema_version": 1,
        "gate": "vllm_clip_offline_pooling_capability",
        "status": "pass" if all(checks.values()) else "fail",
        "boundary": "offline_vllm_pooling_model_and_api_capability",
        "not_a_benchmark": True,
        "model": args.model,
        "image": str(image_path),
        "expected_dimension": args.expected_dimension,
        "checks": checks,
        "image_embedding": image_summary,
        "text_embeddings": text_results,
        "timing_s": {
            "model_load": load_s,
            "image_embed": image_embed_s,
            "gate_total": time.perf_counter() - started,
        },
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "numpy": np.__version__,
            "vllm": vllm_version,
            "enforce_eager": True,
            "runner": "pooling",
            "selected_process_environment": {
                key: value
                for key, value in sorted(os.environ.items())
                if key.startswith("VLLM_") or key == "CUDA_VISIBLE_DEVICES"
            },
        },
    }


def main() -> int:
    args = parse_args()
    try:
        payload = run_gate(args)
    except Exception as exc:
        payload = {
            "schema_version": 1,
            "gate": "vllm_clip_offline_pooling_capability",
            "status": "error",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        try:
            _write_json(args.json_out, payload)
        except Exception as write_exc:
            print(f"failed to write error evidence: {write_exc}", file=sys.stderr, flush=True)
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr, flush=True)
        return 1
    _write_json(args.json_out, payload)
    print(json.dumps(payload, ensure_ascii=False), flush=True)
    return 0 if payload["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
