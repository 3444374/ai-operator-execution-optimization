#!/usr/bin/env python3
"""vLLM CLIP pooling capability gate (A-line task 1).

Loads CLIP via vLLM OFFLINE pooling (no api_server to start/stop) and verifies the
image embedding is 512-d + finite, plus reports norms and image/text cosines as
descriptive signal. This is the correctness gate before 5K calibration and 60K
formal (per deploy/autodl/image_serving.md section 5.3 + vLLM official run_clip).

Invocation (runner="pooling", empty prompt for image-only) taken from the vLLM
run_clip example + the ab-line-research workflow; no --task embed (deprecated >0.20).
"""
from __future__ import annotations

import argparse
import glob

import numpy as np
from PIL import Image


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--model", default="/root/autodl-tmp/models/clip-vit-base-patch32")
    p.add_argument("--image", default="", help="image path; if empty, auto-find first coco jpg")
    p.add_argument("--coco-glob", default="/root/autodl-tmp/data/raw/coco_train2017/*.jpg")
    p.add_argument("--texts", nargs="*", default=["a cat", "a scenic mountain landscape"])
    return p.parse_args()


def extract_embedding(request_output) -> np.ndarray:
    """vLLM pooling output attribute name varies across versions; try the common ones."""
    output = request_output.outputs[0]
    for attr in ("embedding", "data", "embeddings"):
        value = getattr(output, attr, None)
        if value is not None:
            return np.asarray(value, dtype=np.float32)
    visible = [a for a in dir(output) if not a.startswith("_")]
    raise RuntimeError(f"could not extract embedding from {type(output).__name__}; attrs={visible}")


def main() -> None:
    args = parse_args()
    image_path = args.image or sorted(glob.glob(args.coco_glob))[0]
    print(f"image={image_path}", flush=True)
    print(f"model={args.model}", flush=True)

    from vllm import LLM

    llm = LLM(
        model=args.model,
        runner="pooling",
        limit_mm_per_prompt={"image": 1},
        # Capability gate only: skip vLLM first-run inductor compile + the ~50-size
        # cudagraph capture (hangs for minutes on a cold cache). Not used for formal runs.
        enforce_eager=True,
    )
    image_out = llm.embed(
        {"prompt": "", "multi_modal_data": {"image": Image.open(image_path).convert("RGB")}}
    )
    image_emb = extract_embedding(image_out)

    text_embs = []
    for text in args.texts:
        text_embs.append((text, extract_embedding(llm.embed(text))))

    def cos(a: np.ndarray, b: np.ndarray) -> float:
        return float(a @ b / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-12))

    norm = float(np.linalg.norm(image_emb))
    print(
        f"image_emb: dim={image_emb.shape[0]} finite={bool(np.all(np.isfinite(image_emb)))} "
        f"norm={norm:.4f} min={float(image_emb.min()):.4f} max={float(image_emb.max()):.4f}",
        flush=True,
    )
    for text, emb in text_embs:
        print(
            f"text '{text}': dim={emb.shape[0]} finite={bool(np.all(np.isfinite(emb)))} "
            f"norm={float(np.linalg.norm(emb)):.4f} cos(image)={cos(image_emb, emb):.4f}",
            flush=True,
        )

    dim_ok = image_emb.shape[0] == 512
    finite_ok = bool(np.all(np.isfinite(image_emb)))
    print(f"GATE_RESULT dim_ok={dim_ok} finite_ok={finite_ok}", flush=True)
    print("GATE_DONE", flush=True)


if __name__ == "__main__":
    main()
