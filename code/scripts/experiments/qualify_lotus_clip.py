#!/usr/bin/env python3
"""Small stock/cached CLIP content, preprocessing and output qualification."""
import argparse
import json
from importlib.metadata import version
from pathlib import Path
import sys
import time

CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
sys.path.insert(0, str(CODE_ROOT))

import numpy as np
from PIL import Image
from src.baselines.common.private_artifacts import require_outside_git
from src.baselines.common.redact import redact_text
from src.modalities.image.clip import configure_torch_thread_pools, decode_rgb_image
from src.modalities.image.file_source import load_image_file_manifest
from src.semantic_methods.lotus.image_backend import DirectImageExecutor
from src.semantic_methods.lotus.image_identity import parameter_digest, snapshot_files
from src.semantic_methods.lotus.image_rm import image_bytes
from src.semantic_methods.lotus.image_source import validate_image_source


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stock-model", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--image-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=12)
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cuda")
    args = parser.parse_args(argv)
    if not 1 <= args.rows <= 32:
        raise ValueError("initial model qualification is limited to 32 images")
    require_outside_git(args.output_dir)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    report = dict(schema="lotus.clip_identity.v1", qualified=False, model_dtype="float32", device=args.device,
                  rows=args.rows, status="preparing", atol=1e-5, rtol=1e-5)
    error = None
    direct = None
    try:
        report["lotus_source"] = validate_image_source()
        report["versions"] = {x: version(x) for x in ("lotus-ai", "sentence-transformers", "transformers", "torch", "numpy", "pillow", "faiss-cpu")}
        configure_torch_thread_pools(1, 1)
        from lotus.models import SentenceTransformersRM
        stock = SentenceTransformersRM(str(args.stock_model), device="cpu", normalize_embeddings=True)
        direct = DirectImageExecutor(model=str(args.model), device="cpu", dtype="float32", batch_size=32)
        report["stock"] = dict(repo="sentence-transformers/clip-ViT-B-32",
            revision="327ab6726d33c0e22f920c83f2ff9e4bd38ca37f", files=snapshot_files(args.stock_model))
        report["candidate"] = dict(repo="openai/clip-vit-base-patch32",
            revision="3d74acf9a28c67741b2f4f2ea7635f0aaf6f0268", files=snapshot_files(args.model))
        report["stock_parameters"] = parameter_digest(stock.transformer[0].model)
        report["candidate_parameters"] = parameter_digest(direct.actor._model)
        report["parameters_equal"] = report["stock_parameters"] == report["candidate_parameters"]
        if not report["parameters_equal"]:
            raise ValueError("stock and cached CLIP actual parameters differ")
        manifest = load_image_file_manifest(args.manifest, args.image_root)
        entries = manifest.entries[:args.rows]
        if len(entries) != args.rows:
            raise ValueError("source has fewer images than qualification requires")
        import hashlib
        images = []
        for entry in entries:
            raw = (manifest.root / entry.path).read_bytes()
            if hashlib.sha256(raw).hexdigest() != entry.sha256:
                raise ValueError("qualification image content changed")
            images.append(decode_rgb_image(raw))
        encoded = [image_bytes(x) for x in images]
        pixels_stock = stock.transformer[0].tokenize(images)["pixel_values"].numpy()
        pixels_direct = direct.processor.preprocess_decoded(images)
        report["png_fallback_pixels_equal"] = np.array_equal(
            pixels_direct, direct.processor.preprocess(encoded))
        report["effective_pixels_equal"] = np.array_equal(pixels_stock, pixels_direct)
        report["pixel_max_abs_error"] = float(np.max(np.abs(pixels_stock - pixels_direct)))
        texts = ["bird", "a photograph of a bird", "not-a-bird.jpg"]
        stock_tokens = stock.transformer[0].processor.tokenizer(texts, padding=True, return_tensors="np")
        candidate_tokens = direct.processor.processor.tokenizer(texts, padding=True, return_tensors="np")
        report["tokenizer_equal"] = all(np.array_equal(stock_tokens[x], candidate_tokens[x]) for x in stock_tokens)
        if (not report["effective_pixels_equal"] or not report["png_fallback_pixels_equal"]
                or not report["tokenizer_equal"]):
            raise ValueError("effective CLIP preprocessing or tokenizer differs")
        stock.transformer.to(args.device)
        direct.actor._model = direct.actor._model.to(args.device)
        direct.actor._device = args.device
        stock_values = stock(images)
        direct_values = direct.embed_batch(tuple(x.doc_id for x in entries), images).embeddings
        report["normalized_outputs_equivalent"] = bool(np.allclose(stock_values, direct_values, atol=1e-5, rtol=1e-5))
        report["embedding_max_abs_error"] = float(np.max(np.abs(stock_values - direct_values)))
        report["min_cosine"] = float(np.min(np.sum(stock_values * direct_values, axis=1)))
        first, renamed = images[0].copy(), images[0].copy()
        first.filename, renamed.filename = "bird.jpg", "different-name.jpg"
        changed = Image.new("RGB", first.size, "magenta")
        sentinels = stock([first, renamed, changed])
        report["same_pixels_different_names_equal"] = np.array_equal(sentinels[0], sentinels[1])
        report["changed_pixels_max_abs_difference"] = float(np.max(np.abs(sentinels[0] - sentinels[2])))
        report["changed_pixels_change_embedding"] = report["changed_pixels_max_abs_difference"] > 1e-3
        np.savez_compressed(args.output_dir / "vectors.npz", stock=stock_values, direct=direct_values,
                            doc_ids=np.asarray([x.doc_id for x in entries]))
        if (not report["normalized_outputs_equivalent"] or not report["same_pixels_different_names_equal"]
                or not report["changed_pixels_change_embedding"]):
            raise ValueError("CLIP numeric qualification or image-content controls failed")
        report.update(status="succeeded", qualified=True, manifest_sha256=manifest.sha256,
            stock_outer_batch_size=64, stock_encode_default_batch_size=32,
            decode="eager RGB PIL; PNG fallback checked separately", processor_backend="slow PIL",
            task_quality="self-image retrieval checked separately; captions/semantic relevance unavailable")
    except BaseException as exc:
        error = exc
        report.update(status="failed", error_type=type(exc).__name__, reason=redact_text(str(exc)))
    finally:
        if direct is not None:
            try:
                direct.close()
            except BaseException as exc:
                report["cleanup_error"] = type(exc).__name__
                from src.experiments.execution_timing import is_timing_error
                if error is None or (is_timing_error(error) and not is_timing_error(exc)):
                    if error is not None:
                        exc.add_note("Prior image timing failure: " + type(error).__name__)
                    error = exc
                    report.update(status="failed", qualified=False, error_type=type(exc).__name__, reason=redact_text(str(exc)))
                else:
                    error.add_note("Direct cleanup also raised " + type(exc).__name__)
        report["elapsed_s"] = time.perf_counter() - started
        try:
            (args.output_dir / "identity.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        except BaseException as exc:
            if error is None:
                error = exc
                report.update(status="failed", qualified=False, error_type=type(exc).__name__)
            else:
                error.add_note("identity evidence write also raised " + type(exc).__name__)
            print("identity evidence write failed: " + type(exc).__name__, file=sys.stderr)
    print(json.dumps({k: report.get(k) for k in ("status", "qualified", "parameters_equal", "effective_pixels_equal", "embedding_max_abs_error")}))
    return int(error is not None)


if __name__ == "__main__":
    sys.exit(main())
