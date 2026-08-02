#!/usr/bin/env python
"""Compare CLIP preprocessing boundaries through the current actor contract.

Verifiable goal
---------------
Determine whether the CPU-preprocessing bottleneck observed with the historical
``CLIPProcessor(..., return_tensors="pt")`` path remains under the current
``ClipImagePreprocessor -> ClipTensorActor`` implementation and a torchvision
processor baseline. Variants are interleaved on the same image batches. The
script records raw repeats and embedding parity; it is a bounded motivation
profile, not a Daft/Ray end-to-end performance result.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.image import (  # noqa: E402
    ClipImagePreprocessor,
    ClipTensorActor,
    ImageEmbeddingBatch,
)
from src.modalities.image.clip import decode_rgb_image  # noqa: E402


@dataclass(frozen=True)
class ProcessorVariant:
    name: str
    processor: object
    output_kind: str
    exact_production_path: bool = False
    decode_backend: str = "pil"

    @property
    def backend(self) -> str:
        target = (
            self.processor.processor
            if self.exact_production_path
            else self.processor
        )
        value = getattr(target, "backend", None)
        return str(value) if value is not None else "legacy_or_unspecified"

    @property
    def processor_class(self) -> str:
        target = (
            self.processor.processor
            if self.exact_production_path
            else self.processor
        )
        return type(target).__name__

    def preprocess(self, encoded_images: list[bytes]):
        if self.exact_production_path:
            # This deliberately calls the reusable implementation unchanged.
            return self.processor.preprocess(encoded_images)
        if self.decode_backend == "pil":
            images = [decode_rgb_image(item) for item in encoded_images]
        elif self.decode_backend == "torchvision":
            import torch
            from torchvision.io import ImageReadMode, decode_image

            images = [
                decode_image(
                    torch.frombuffer(bytearray(item), dtype=torch.uint8),
                    mode=ImageReadMode.RGB,
                )
                for item in encoded_images
            ]
        else:
            raise ValueError(f"unsupported decode backend: {self.decode_backend}")
        output = self.processor(
            images=images,
            return_tensors=self.output_kind,
        )["pixel_values"]
        if self.output_kind == "np":
            return np.ascontiguousarray(output, dtype=np.float32)
        return output


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--model", required=True)
    parser.add_argument(
        "--pg-dsn",
        default=os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN", ""),
    )
    parser.add_argument("--table", default="image_documents")
    parser.add_argument("--id-column", default="doc_id")
    parser.add_argument("--image-column", default="image")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--batch-sizes", default="1,32,128")
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--seed", type=int, default=20260801)
    parser.add_argument("--dtype", choices=("float16", "float32", "bfloat16"), default="float16")
    parser.add_argument("--min-cosine", type=float, default=0.999)
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-manifest", required=True)
    return parser.parse_args()


def build_variants(model_path: str):
    from transformers import AutoImageProcessor

    production = ClipImagePreprocessor(model_path)
    variants = [
        ProcessorVariant(
            name="production_np",
            processor=production,
            output_kind="np",
            exact_production_path=True,
        ),
        ProcessorVariant(
            name="legacy_pt",
            processor=production.processor,
            output_kind="pt",
        ),
    ]
    skipped: dict[str, str] = {}
    try:
        torchvision_processor = AutoImageProcessor.from_pretrained(
            model_path,
            backend="torchvision",
        )
        variants.append(
            ProcessorVariant(
                name="torchvision_pil_pt",
                processor=torchvision_processor,
                output_kind="pt",
            )
        )
        variants.append(
            ProcessorVariant(
                name="torchvision_tensor_pt",
                processor=torchvision_processor,
                output_kind="pt",
                decode_backend="torchvision",
            )
        )
    except Exception as exc:  # noqa: BLE001 - unsupported backend is evidence
        message = f"{type(exc).__name__}: {exc}"
        skipped["torchvision_pil_pt"] = message
        skipped["torchvision_tensor_pt"] = message
    return variants, skipped


def _payload_work_units(payload, rows: int) -> int:
    shape = tuple(int(item) for item in payload.shape)
    if len(shape) != 4 or shape[0] != rows:
        raise ValueError(f"unexpected preprocessed tensor shape: {shape}")
    return rows * shape[-2] * shape[-1]


def _parity(candidate: np.ndarray, reference: np.ndarray) -> tuple[float, float, float]:
    dot = np.sum(candidate * reference, axis=1)
    denominator = np.linalg.norm(candidate, axis=1) * np.linalg.norm(
        reference,
        axis=1,
    )
    cosine = dot / np.maximum(denominator, np.finfo(np.float32).tiny)
    return (
        float(np.mean(cosine)),
        float(np.min(cosine)),
        float(np.max(np.abs(candidate - reference))),
    )


def main() -> None:
    args = parse_args()
    if not args.model.strip():
        raise SystemExit("--model must be a non-empty local path or model ID")
    if not args.pg_dsn:
        raise SystemExit("--pg-dsn is required (or set DATABASE_URL/PG_DSN)")
    if args.limit <= 0 or args.warmup < 0 or args.repeats <= 0:
        raise SystemExit("limit/repeats must be positive and warmup non-negative")

    import psycopg
    import torch
    import transformers

    from profile_image_clip_bottleneck import fetch_image_bytes, get_versions

    batch_sizes = [int(item) for item in args.batch_sizes.split(",") if item]
    if not batch_sizes or min(batch_sizes) <= 0:
        raise SystemExit("--batch-sizes must contain positive integers")

    conn = psycopg.connect(args.pg_dsn)
    versions = get_versions(conn)
    pool, db_fetch_s = fetch_image_bytes(
        conn,
        args.table,
        args.id_column,
        args.image_column,
        args.limit,
    )
    conn.close()
    if not pool:
        raise SystemExit("no image rows fetched")

    variants, skipped = build_variants(args.model)
    actor = ClipTensorActor(
        args.model,
        processor_revision=args.model,
        dtype=args.dtype,
        normalize=True,
    )
    gpu_name = torch.cuda.get_device_name(0)
    rng = random.Random(args.seed)
    raw_rows: list[dict[str, object]] = []

    for batch_size in batch_sizes:
        for repeat_index in range(-args.warmup, args.repeats):
            offset = ((repeat_index + args.warmup) * batch_size) % len(pool)
            selected = [pool[(offset + index) % len(pool)] for index in range(batch_size)]
            doc_ids = tuple(str(item[0]) for item in selected)
            encoded = [item[1] for item in selected]
            order = list(variants)
            rng.shuffle(order)
            iteration_results: dict[str, tuple[np.ndarray, dict[str, object]]] = {}

            for variant in order:
                prep_started = time.perf_counter()
                payload = variant.preprocess(encoded)
                cpu_preprocess_s = time.perf_counter() - prep_started
                work_units = _payload_work_units(payload, batch_size)
                batch = ImageEmbeddingBatch(
                    doc_ids=doc_ids,
                    payload=payload,
                    input_kind="preprocessed_tensor",
                    work_units=work_units,
                    work_unit="pixels",
                )
                torch.cuda.synchronize()
                actor_started = time.perf_counter()
                result = actor.embed(batch)
                torch.cuda.synchronize()
                actor_call_wall_s = time.perf_counter() - actor_started
                row = {
                    "phase": "warmup" if repeat_index < 0 else "formal",
                    "repeat_index": repeat_index,
                    "variant": variant.name,
                    "processor_class": variant.processor_class,
                    "processor_backend": variant.backend,
                    "processor_output_kind": variant.output_kind,
                    "batch_size": batch_size,
                    "rows": batch_size,
                    "cpu_preprocess_s": cpu_preprocess_s,
                    "actor_service_s": result.service_s,
                    "actor_call_wall_s": actor_call_wall_s,
                    "profiled_e2e_s": cpu_preprocess_s + actor_call_wall_s,
                    "images_per_s": batch_size / (cpu_preprocess_s + actor_call_wall_s),
                    "work_units": work_units,
                }
                iteration_results[variant.name] = (result.embeddings, row)

            reference = iteration_results["production_np"][0]
            for embeddings, row in iteration_results.values():
                mean_cosine, min_cosine, max_abs = _parity(embeddings, reference)
                row["cosine_to_production_mean"] = mean_cosine
                row["cosine_to_production_min"] = min_cosine
                row["max_abs_to_production"] = max_abs
                if repeat_index >= 0:
                    raw_rows.append(row)

    metadata = {
        "model_revision": args.model,
        "processor_revision": args.model,
        "dtype": args.dtype,
        "normalized": True,
        "dataset_rows": len(pool),
        "db_fetch_s": db_fetch_s,
        "batch_sizes": batch_sizes,
        "warmup": args.warmup,
        "repeats": args.repeats,
        "seed": args.seed,
        "variants": [item.name for item in variants],
        "skipped_variants": skipped,
        "transformers_version": transformers.__version__,
        "torch_version": torch.__version__,
        "gpu_name": gpu_name,
        **versions,
    }
    for row in raw_rows:
        row.update(
            metadata
            | {
                "batch_sizes": json.dumps(batch_sizes),
                "variants": json.dumps(metadata["variants"]),
            }
        )
        row["skipped_variants"] = json.dumps(skipped, sort_keys=True)

    out_csv = Path(args.out_csv)
    out_manifest = Path(args.out_manifest)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_manifest.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(raw_rows[0]),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(raw_rows)
    out_manifest.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")

    formal_non_reference = [
        row for row in raw_rows if row["variant"] != "production_np"
    ]
    min_cosine = min(float(row["cosine_to_production_min"]) for row in formal_non_reference)
    print(json.dumps(metadata, indent=2, sort_keys=True))
    print(f"rows={len(raw_rows)} csv={out_csv} manifest={out_manifest}")
    print(f"minimum embedding cosine to production_np={min_cosine:.6f}")
    if min_cosine < args.min_cosine:
        raise SystemExit(
            f"embedding parity gate failed: {min_cosine:.6f} < {args.min_cosine:.6f}"
        )


if __name__ == "__main__":
    main()
