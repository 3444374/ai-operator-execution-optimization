#!/usr/bin/env python3
"""One CPU-only native Ray worker verifies observer loading before model calls."""
import json
import os
from pathlib import Path
import sys

import ray
from src.infrastructure.runtime_env import ray_runtime_env


def main():
    runtime = ray_runtime_env(Path(os.environ["CLIP_SOURCE_CODE_ROOT"]))
    # Ray env_vars only extends worker startup; no execution graph is changed.
    runtime["env_vars"]["SEMLOOM_CLIP_TRACE_DIR"] = os.environ["SEMLOOM_CLIP_TRACE_DIR"]
    ray.init(num_cpus=1, num_gpus=0, include_dashboard=False, runtime_env=runtime)
    try:
        @ray.remote(num_cpus=1, num_gpus=0)
        def check():
            from src.modalities.image.clip import FastClipImagePreprocessor, ClipTensorActor
            import clip_trace_hooks
            return {
                "pid": os.getpid(),
                "preprocess_observer": FastClipImagePreprocessor.preprocess is clip_trace_hooks.observed_preprocess,
                "model_observer": ClipTensorActor.embed is clip_trace_hooks.observed_embed,
                "trace_env_exists": bool(os.environ.get("SEMLOOM_CLIP_TRACE_DIR")),
                "model_constructed": False,
            }
        value = ray.get(check.remote())
        assert value["preprocess_observer"] and value["model_observer"] and value["trace_env_exists"]
        from src.modalities.image.clip import ClipTensorActor
        # Serialization only; no .remote() instantiation or model construction.
        value["typed_actor_serialized_bytes"] = len(ray.cloudpickle.dumps(ray.remote(num_cpus=1, num_gpus=1)(ClipTensorActor)))
        value["declared_GPU_for_actual_probe"] = 0
        Path(sys.argv[1]).write_text(json.dumps(value, indent=2) + "\n")
        print(json.dumps(value))
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
