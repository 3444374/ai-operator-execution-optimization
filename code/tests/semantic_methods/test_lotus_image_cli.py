"""CLI failure evidence must survive cleanup failures without hiding the cause."""
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace


def test_ray_startup_failure_and_shutdown_failure_are_both_retained(tmp_path, monkeypatch):
    path = Path(__file__).parents[2] / "scripts/experiments/run_lotus_image_retrieval.py"
    spec = importlib.util.spec_from_file_location("lotus_image_cli_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "validate_image_source", lambda: {})
    monkeypatch.setattr(module, "validate_embedding_identity", lambda *a, **k: {})
    monkeypatch.setattr(module, "version", lambda name: "fixture")
    monkeypatch.setattr(module, "configure_torch_thread_pools", lambda *args: {})
    class Stock:
        def __init__(self, *args, **kwargs):
            self.transformer = SimpleNamespace(to=lambda **kw: None)
    monkeypatch.setitem(sys.modules, "lotus.models", SimpleNamespace(SentenceTransformersRM=Stock))
    monkeypatch.setitem(sys.modules, "torch", SimpleNamespace(float32="float32"))
    def broken_startup(**kwargs):
        raise RuntimeError("image actor startup failed")
    def broken_shutdown():
        raise ValueError("Ray shutdown failed")
    monkeypatch.setattr(module, "SemLoomImageExecutor", broken_startup)
    monkeypatch.setitem(sys.modules, "ray", SimpleNamespace(is_initialized=lambda: False,
        init=lambda **kwargs: None, shutdown=broken_shutdown))
    identity = tmp_path / "identity.json"
    identity.write_text(json.dumps(dict(qualified=True, model_dtype="float32")))
    output = tmp_path / "output"
    args = ["--manifest", str(tmp_path / "unused-manifest.json"), "--image-root", str(tmp_path),
            "--stock-model", str(tmp_path), "--model", str(tmp_path), "--embedding-identity", str(identity),
            "--arm", "semloom_hse", "--index-dir", str(tmp_path / "index"), "--output-dir", str(output),
            "--ray-temp-dir", str(tmp_path / "ray")]
    assert module.main(args) == 1
    result = json.loads((output / "result.json").read_text())
    assert result["status"] == "failed"
    assert result["error_type"] == "RuntimeError"
    assert result["reason"] == "image actor startup failed"
    assert result["ray_cleanup_error"] == "ValueError"
    assert (output / "evidence-write.json").exists()
