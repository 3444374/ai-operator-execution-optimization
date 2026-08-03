"""Framework-owned image execution graphs used as native baselines."""

from .daft import (
    build_daft_clip_embedder,
    build_daft_staged_clip_pipeline,
    run_daft_clip_baseline,
    run_daft_builtin_image_embedding,
    run_daft_staged_clip_baseline,
)
from .ray_data import build_ray_data_clip_pipeline, run_ray_data_clip_baseline

__all__ = [
    "build_daft_clip_embedder",
    "build_daft_staged_clip_pipeline",
    "build_ray_data_clip_pipeline",
    "run_daft_builtin_image_embedding",
    "run_daft_clip_baseline",
    "run_daft_staged_clip_baseline",
    "run_ray_data_clip_baseline",
]
