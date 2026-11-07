#!/usr/bin/env python
"""Run comparable PostgreSQL -> data engine -> CLIP operator-E2E arms.

The arms keep the input table, model, GPU count, output validation, and timing
boundary fixed:

* ``daft_builtin_embed``: Daft built-in decode + ``embed_image`` AI Function;
  Daft owns batching, concurrency, backpressure, and scheduling.
* ``ray_data_staged``: native Ray Data SQL source and ``map_batches`` graph;
  Ray Data owns backpressure and actor scheduling.
* ``daft_native`` / ``daft_ray`` / ``daft_staged``: project-authored UDF
  references retained for mechanism diagnosis, not formal native baselines.
* ``project_ray``: the legacy arm identifier for SemLoom's bounded Ray CPU
  preprocessing actors feeding tensor-only GPU actors, with the source kept
  lazy and streamed from Daft.

This gate excludes pgvector writeback so that it measures the operator execution
path. A later system-E2E layer must add the same sink to every arm.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.image_clip.setup import ARMS, load_image_run_setup, validate_image_run_args
from src.experiments.image_clip.execution import ImageExecution, run_observed_image_execution
from src.experiments.image_clip.reporting import append_csv, build_image_result_row, build_image_manifest


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", choices=ARMS, required=True)
    parser.add_argument(
        "--pg-dsn",
        default=os.environ.get("DATABASE_URL") or os.environ.get("PG_DSN", ""),
    )
    parser.add_argument("--model", required=True)
    parser.add_argument("--processor", default="")
    parser.add_argument("--workload-name", default="coco_val2017")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument(
        "--dataset-passes",
        type=int,
        default=1,
        help=(
            "Logical passes over the selected unique images. Repeated rows receive "
            "pass-qualified execution IDs; --limit remains the unique-image count."
        ),
    )
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--warmup-rows", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument(
        "--input-size",
        type=int,
        default=224,
        help="Frozen square processor output used for HSE byte/work accounting",
    )
    parser.add_argument("--cpu-workers", type=int, default=4)
    parser.add_argument("--gpu-workers", type=int, default=2)
    parser.add_argument(
        "--daft-model-workers",
        type=int,
        default=0,
        help="Daft UDF actors; 0 means one actor per physical GPU",
    )
    parser.add_argument(
        "--source-shards",
        type=int,
        default=0,
        help="PostgreSQL lazy source shards; 0 follows model worker count",
    )
    parser.add_argument(
        "--source-cpu-threads",
        type=int,
        default=0,
        help="Daft native source/runner threads; 0 follows --cpu-workers",
    )
    parser.add_argument(
        "--max-active-batches",
        type=int,
        default=8,
        help="Project-Ray admission window; ignored by framework-native baseline arms",
    )
    parser.add_argument(
        "--project-execution-mode",
        choices=("direct_dependency", "hse_static"),
        default="direct_dependency",
        help=(
            "Project-Ray flow ownership. hse_static uses a real ready queue and hard "
            "byte/work limits; it does not enable SAOR dynamic decisions."
        ),
    )
    parser.add_argument(
        "--hse-encoded-bytes-limit",
        type=int,
        default=0,
        help="Encoded source bytes held by HSE; 0 derives a conservative workload cap",
    )
    parser.add_argument(
        "--hse-ready-bytes-limit",
        type=int,
        default=0,
        help="Prepared tensor bytes held by HSE; 0 derives from batch/window/FP32 NCHW",
    )
    parser.add_argument(
        "--hse-ready-work-limit",
        type=int,
        default=0,
        help="Prepared model work held by HSE; 0 derives from batch/window/pixels",
    )
    parser.add_argument(
        "--hse-prepare-inflight-limit",
        type=int,
        default=0,
        help="HSE CPU leases; 0 uses --cpu-workers",
    )
    parser.add_argument(
        "--hse-model-inflight-limit",
        type=int,
        default=0,
        help="HSE GPU leases; 0 uses --gpu-workers",
    )
    parser.add_argument(
        "--allow-non-native-diagnostic",
        action="store_true",
        help=(
            "Allow a project-authored Daft reference in a formal-phase diagnostic; "
            "it remains ineligible for baseline ranking"
        ),
    )
    parser.add_argument("--torch-intraop-threads", type=int, default=1)
    parser.add_argument("--torch-interop-threads", type=int, default=1)
    parser.add_argument("--dtype", choices=("float16", "float32", "bfloat16"), default="float16")
    parser.add_argument("--embedding-dimension", type=int, default=512)
    parser.add_argument(
        "--model-flops-per-image",
        type=float,
        default=0.0,
        help="Verified model FLOPs per image; 0 leaves estimated MFU blank",
    )
    parser.add_argument(
        "--gpu-peak-flops-per-s",
        type=float,
        default=0.0,
        help="Per-GPU peak FLOP/s matching dtype; 0 leaves estimated MFU blank",
    )
    parser.add_argument("--phase", choices=("gate", "warmup", "formal"), default="gate")
    parser.add_argument("--repeat-index", type=int, default=0)
    parser.add_argument("--gpu-sample-interval-s", type=float, default=0.5)
    parser.add_argument("--cpu-sample-interval-s", type=float, default=0.25)
    parser.add_argument(
        "--ray-address",
        default="",
        help=(
            "Connect to one externally managed Ray cluster. Required when multiple "
            "independent native image applications share the same physical resources; "
            "empty keeps the isolated single-job behavior."
        ),
    )
    parser.add_argument(
        "--formal-ready-file",
        default="",
        help="Optional per-job readiness marker written after the untimed warm-up.",
    )
    parser.add_argument(
        "--formal-start-barrier-file",
        default="",
        help="Optional JSON barrier containing start_epoch_s for coordinated jobs.",
    )
    parser.add_argument("--formal-start-offset-s", type=float, default=0.0)
    parser.add_argument("--formal-barrier-timeout-s", type=float, default=900.0)
    parser.add_argument(
        "--ray-data-autoscaling-actor-pools",
        action="store_true",
        help=(
            "Use Ray Data's native min=1/max=configured ActorPoolStrategy. "
            "This prevents independent multi-job graphs from gang-reserving all CPU slots."
        ),
    )
    parser.add_argument("--expected-source-doc-ids-sha256", default="")
    parser.add_argument("--expected-input-encoded-bytes", type=int, default=0)
    parser.add_argument(
        "--detailed-stage-timing",
        action="store_true",
        help="Synchronize CUDA stages to measure H2D/forward/D2H; use as a diagnostic arm",
    )
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-manifest", required=True)
    parser.add_argument(
        "--save-embeddings",
        default="",
        help=(
            "Diagnostic only: capture validated per-row embeddings to a compressed "
            ".npz plus a sidecar manifest. Supported for daft_builtin_embed and "
            "project_ray gate runs only. Capture adds driver memory/copy overhead, so "
            "its timing is invalid for performance comparison; empty default is a no-op."
        ),
    )
    parser.add_argument(
        "--embedding-output-contract",
        choices=("arm_default", "l2_normalized"),
        default="arm_default",
        help=(
            "Output contract inside the timed operator boundary. arm_default preserves "
            "each implementation's native behavior; l2_normalized charges any required "
            "normalization to that arm and is required for cross-system formal ranking."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    processor, embedding_target, embedding_sidecar, provenance = (
        validate_image_run_args(args)
    )

    import daft
    import ray
    import torch
    import transformers

    failure = None
    try:
        setup = load_image_run_setup(
            args,
            processor=processor,
            embedding_target=embedding_target,
            embedding_sidecar=embedding_sidecar,
            provenance=provenance,
        )
        execution = ImageExecution(
            args,
            setup,
            daft_module=daft,
            ray_module=ray,
        )
        observed = run_observed_image_execution(
            args,
            setup,
            execution,
            daft_module=daft,
            ray_module=ray,
            torch_module=torch,
            transformers_module=transformers,
        )
        row = build_image_result_row(
            args,
            setup,
            observed,
            daft_module=daft,
            ray_module=ray,
            torch_module=torch,
            transformers_module=transformers,
        )
        append_csv(Path(args.out_csv), row)
        manifest = build_image_manifest(args, setup, row)
        manifest_path = Path(args.out_manifest)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(manifest, sort_keys=True))
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            if ray.is_initialized():
                ray.shutdown()
        except BaseException as cleanup_error:
            if failure is None:
                raise
            failure.add_note(f"Ray SDK disconnect raised {type(cleanup_error).__name__}: {cleanup_error}")
            failure.image_cleanup_errors = (
                *getattr(failure, "image_cleanup_errors", ()), ("ray_sdk", cleanup_error),
            )


if __name__ == "__main__":
    main()
