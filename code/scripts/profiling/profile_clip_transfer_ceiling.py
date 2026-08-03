#!/usr/bin/env python3
"""Profile CLIP R0/R1/R2 compute and host-to-device transfer ceilings.

R0 reuses a GPU-resident tensor, R1 copies a pinned FP16 host tensor, and R2
matches the project boundary more closely: read-only pageable FP32 NumPy,
ownership copy, dtype conversion/H2D, then forward. The output is per-repeat
diagnostic evidence, not a database/system throughput baseline.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import subprocess
import sys
import time
from pathlib import Path

import numpy as np


CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.image.clip import extract_clip_image_features  # noqa: E402
from src.modalities.image.resource_sampling import NvidiaSmiSampler  # noqa: E402


MODES = ("r0_gpu_resident", "r1_pinned_fp16", "r2_pageable_fp32")
CSV_FIELDS = (
    "mode",
    "batch_size",
    "repeat_index",
    "wall_s",
    "host_ownership_copy_s",
    "h2d_cuda_s",
    "forward_cuda_s",
    "images_per_s",
    "host_input_bytes",
    "device_input_bytes",
    "logical_h2d_gbps",
    "output_sum",
    "output_norm_error",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--batch-sizes", default="16,64,256")
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--repeats", type=int, default=30)
    parser.add_argument("--seed", type=int, default=20260802)
    parser.add_argument("--device", default="cuda:0")
    parser.add_argument("--dtype", choices=("float16", "float32"), default="float16")
    parser.add_argument("--gpu-sample-interval-s", type=float, default=0.1)
    parser.add_argument("--out-csv", required=True, type=Path)
    parser.add_argument("--out-manifest", required=True, type=Path)
    args = parser.parse_args(argv)
    args.batch_sizes = tuple(int(item) for item in args.batch_sizes.split(","))
    if not args.batch_sizes or min(args.batch_sizes) <= 0:
        parser.error("--batch-sizes must contain positive integers")
    if args.warmup < 0 or args.repeats <= 0:
        parser.error("--warmup must be non-negative and --repeats positive")
    return args


def run_profile(args: argparse.Namespace) -> dict[str, object]:
    import torch
    from transformers import CLIPModel

    if not torch.cuda.is_available() or not args.device.startswith("cuda"):
        raise RuntimeError("transfer ceiling requires a CUDA device")
    dtype = torch.float16 if args.dtype == "float16" else torch.float32
    model = CLIPModel.from_pretrained(args.model).to(args.device, dtype=dtype).eval()
    rows: list[dict[str, object]] = []
    sampler = NvidiaSmiSampler(
        args.gpu_sample_interval_s,
        active_device_count=1,
    )
    sampler.start()
    for batch_size in args.batch_sizes:
        inputs = _make_inputs(batch_size, dtype=dtype, device=args.device)
        for mode in MODES:
            for _ in range(args.warmup):
                _measure(mode, model, inputs, batch_size, dtype, args.device)
        generator = random.Random(args.seed + batch_size)
        for repeat_index in range(1, args.repeats + 1):
            order = list(MODES)
            generator.shuffle(order)
            for mode in order:
                row = _measure(mode, model, inputs, batch_size, dtype, args.device)
                rows.append(
                    {"mode": mode, "batch_size": batch_size, "repeat_index": repeat_index, **row}
                )
    gpu_metrics = sampler.stop()
    _write_csv(args.out_csv, rows)
    manifest = {
        "schema_version": 1,
        "diagnostic_only": True,
        "timing": "cuda_events_plus_synchronized_wall; model_setup_excluded",
        "r0": "gpu_resident_model_input_then_forward",
        "r1": "pinned_fp16_nonblocking_h2d_then_forward",
        "r2": "readonly_pageable_fp32_ownership_copy_dtype_conversion_h2d_then_forward",
        "bandwidth_semantics": "logical_host_bytes_over_cuda_event_not_hardware_counter",
        "model": args.model,
        "device": args.device,
        "dtype": args.dtype,
        "batch_sizes": list(args.batch_sizes),
        "warmup": args.warmup,
        "repeats": args.repeats,
        "seed": args.seed,
        "rows": len(rows),
        "gpu": gpu_metrics,
        "topology": _command_output(["nvidia-smi", "topo", "-m"]),
        "git_commit": _command_output(["git", "rev-parse", "HEAD"]).strip(),
    }
    args.out_manifest.parent.mkdir(parents=True, exist_ok=True)
    args.out_manifest.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def _make_inputs(batch_size: int, *, dtype, device: str) -> dict[str, object]:
    import torch

    shape = (batch_size, 3, 224, 224)
    pageable = np.zeros(shape, dtype=np.float32)
    pageable.setflags(write=False)
    pinned = torch.zeros(shape, dtype=dtype, pin_memory=True)
    resident = torch.zeros(shape, dtype=dtype, device=device)
    return {"pageable": pageable, "pinned": pinned, "resident": resident}


def _measure(
    mode: str,
    model,
    inputs: dict[str, object],
    batch_size: int,
    dtype,
    device: str,
) -> dict[str, object]:
    import torch

    torch.cuda.synchronize()
    wall_started = time.perf_counter()
    ownership_copy_s = 0.0
    h2d_s = 0.0
    host_bytes = 0
    if mode == "r0_gpu_resident":
        pixel_values = inputs["resident"]
    else:
        if mode == "r1_pinned_fp16":
            host_tensor = inputs["pinned"]
            host_bytes = host_tensor.numel() * host_tensor.element_size()
        elif mode == "r2_pageable_fp32":
            copy_started = time.perf_counter()
            owned = inputs["pageable"].copy()
            ownership_copy_s = time.perf_counter() - copy_started
            host_tensor = torch.from_numpy(owned)
            host_bytes = owned.nbytes
        else:
            raise ValueError(f"unsupported mode: {mode}")
        h2d_started = torch.cuda.Event(enable_timing=True)
        h2d_ended = torch.cuda.Event(enable_timing=True)
        h2d_started.record()
        pixel_values = host_tensor.to(device=device, dtype=dtype, non_blocking=True)
        h2d_ended.record()
        h2d_ended.synchronize()
        h2d_s = h2d_started.elapsed_time(h2d_ended) / 1000.0

    forward_started = torch.cuda.Event(enable_timing=True)
    forward_ended = torch.cuda.Event(enable_timing=True)
    forward_started.record()
    with torch.inference_mode():
        output = extract_clip_image_features(
            model.get_image_features(pixel_values=pixel_values)
        ).float()
        output = output / output.norm(dim=-1, keepdim=True).clamp_min(1e-12)
    forward_ended.record()
    forward_ended.synchronize()
    forward_s = forward_started.elapsed_time(forward_ended) / 1000.0
    wall_s = time.perf_counter() - wall_started
    norms = output.norm(dim=-1)
    device_bytes = pixel_values.numel() * pixel_values.element_size()
    return {
        "wall_s": wall_s,
        "host_ownership_copy_s": ownership_copy_s,
        "h2d_cuda_s": h2d_s,
        "forward_cuda_s": forward_s,
        "images_per_s": batch_size / wall_s,
        "host_input_bytes": host_bytes,
        "device_input_bytes": device_bytes,
        "logical_h2d_gbps": host_bytes / h2d_s / 1e9 if h2d_s > 0 else "",
        "output_sum": float(output.sum().item()),
        "output_norm_error": float((norms - 1.0).abs().max().item()),
    }


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _command_output(command: list[str]) -> str:
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    return completed.stdout if completed.returncode == 0 else completed.stderr


def main() -> None:
    manifest = run_profile(parse_args())
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
