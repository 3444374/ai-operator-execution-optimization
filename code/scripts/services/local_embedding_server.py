#!/usr/bin/env python3
"""Run a minimal OpenAI-compatible embedding endpoint for local profiling."""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local embedding endpoint for GPU-backed profile runs.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    parser.add_argument("--device", default="auto", choices=["auto", "cuda", "cpu"])
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--max-length", type=int, default=256)
    parser.add_argument("--normalize", action="store_true")
    return parser.parse_args()


class EmbeddingModel:
    def __init__(self, model_name: str, device: str, batch_size: int, max_length: int, normalize: bool):
        import torch
        from transformers import AutoModel, AutoTokenizer

        if device == "auto":
            device = "cuda" if torch.cuda.is_available() else "cpu"
        self.torch = torch
        self.device = device
        self.batch_size = batch_size
        self.max_length = max_length
        self.normalize = normalize
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.model.to(device)
        self.model.eval()
        self.embedding_dim = int(self.model.config.hidden_size)

    def embed(self, texts: list[str]) -> tuple[list[list[float]], int, float]:
        vectors = []
        token_count = 0
        started = time.perf_counter()
        with self.torch.inference_mode():
            for start in range(0, len(texts), self.batch_size):
                chunk = texts[start : start + self.batch_size]
                encoded = self.tokenizer(
                    chunk,
                    padding=True,
                    truncation=True,
                    max_length=self.max_length,
                    return_tensors="pt",
                )
                token_count += int(encoded["attention_mask"].sum().item())
                encoded = {key: value.to(self.device) for key, value in encoded.items()}
                output = self.model(**encoded)
                mask = encoded["attention_mask"].unsqueeze(-1)
                summed = (output.last_hidden_state * mask).sum(dim=1)
                counts = mask.sum(dim=1).clamp(min=1)
                pooled = summed / counts
                if self.normalize:
                    pooled = self.torch.nn.functional.normalize(pooled, p=2, dim=1)
                vectors.extend(pooled.detach().cpu().float().tolist())
        return vectors, token_count, time.perf_counter() - started


def make_handler(model: EmbeddingModel):
    class Handler(BaseHTTPRequestHandler):
        server_version = "LocalEmbeddingServer/0.1"

        def do_GET(self) -> None:
            if self.path == "/health":
                self.write_json(
                    200,
                    {
                        "status": "ok",
                        "device": model.device,
                        "embedding_dim": model.embedding_dim,
                    },
                )
                return
            self.write_json(404, {"error": "not_found"})

        def do_POST(self) -> None:
            if self.path != "/v1/embeddings":
                self.write_json(404, {"error": "not_found"})
                return
            try:
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                payload = json.loads(body.decode("utf-8"))
                inputs = payload.get("input")
                if isinstance(inputs, str):
                    texts = [inputs]
                elif isinstance(inputs, list) and all(isinstance(item, str) for item in inputs):
                    texts = inputs
                else:
                    self.write_json(400, {"error": "input must be a string or list of strings"})
                    return
                vectors, token_count, service_s = model.embed(texts)
                self.write_json(
                    200,
                    {
                        "object": "list",
                        "data": [
                            {
                                "object": "embedding",
                                "index": index,
                                "embedding": vector,
                            }
                            for index, vector in enumerate(vectors)
                        ],
                        "model": payload.get("model", "local"),
                        "usage": {"prompt_tokens": token_count, "total_tokens": token_count},
                        "local_metrics": {
                            "device": model.device,
                            "service_s": round(service_s, 6),
                            "embedding_dim": model.embedding_dim,
                        },
                    },
                )
            except Exception as exc:  # noqa: BLE001
                self.write_json(500, {"error": type(exc).__name__, "message": str(exc)})

        def log_message(self, format: str, *args: Any) -> None:
            print(f"{self.address_string()} - {format % args}")

        def write_json(self, status: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def main() -> None:
    args = parse_args()
    model = EmbeddingModel(args.model, args.device, args.batch_size, args.max_length, args.normalize)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(model))
    print(
        json.dumps(
            {
                "status": "started",
                "url": f"http://{args.host}:{args.port}/v1/embeddings",
                "model": args.model,
                "device": model.device,
                "embedding_dim": model.embedding_dim,
                "batch_size": args.batch_size,
            },
            indent=2,
        )
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
