"""CLI failure evidence must survive cleanup failures without hiding the cause."""
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import pytest


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


def load_cli():
    path = Path(__file__).parents[2] / "scripts/experiments/run_lotus_image_retrieval.py"
    spec = importlib.util.spec_from_file_location("lotus_image_codec_cli_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def base_args(tmp_path, arm="direct"):
    return ["--manifest", str(tmp_path / "manifest.json"), "--image-root", str(tmp_path),
            "--stock-model", str(tmp_path), "--model", str(tmp_path),
            "--embedding-identity", str(tmp_path / "identity.json"), "--arm", arm,
            "--index-dir", str(tmp_path / "index"), "--output-dir", str(tmp_path / "output")]


def test_cli_codec_defaults_and_inapplicable_flags_fail_before_work(tmp_path):
    cli = load_cli()
    args = base_args(tmp_path)
    assert cli.parser().parse_args(args).png_compress_level is None
    with pytest.raises(SystemExit):
        cli.parser().parse_args(args + ["--png-compress-level", "10"])
    with pytest.raises(ValueError, match="stock native PIL"):
        cli.main(base_args(tmp_path, "stock") + ["--png-compress-level", "1"])
    with pytest.raises(ValueError, match="require semloom_hse"):
        cli.main(args + ["--hse-encoded-bytes", "4"])
    assert not (tmp_path / "output").exists()


def test_codec_cli_source_error_survives_observation_and_close_errors(tmp_path, monkeypatch):
    cli = load_cli()
    for name in ("validate_image_source", "validate_embedding_identity"):
        monkeypatch.setattr(cli, name, lambda *a, **kw: {})
    monkeypatch.setattr(cli, "version", lambda name: "fixture")
    monkeypatch.setattr(cli, "configure_torch_thread_pools", lambda *a: {})
    class Stock:
        def __init__(self, *a, **kw):
            self.transformer = SimpleNamespace(to=lambda **k: None)
    monkeypatch.setitem(sys.modules, "lotus.models", SimpleNamespace(SentenceTransformersRM=Stock))
    monkeypatch.setitem(sys.modules, "torch", SimpleNamespace(float32="float32"))
    class Executor:
        @property
        def execution_observations(self):
            raise KeyError("observation failed")
        def close(self):
            raise ValueError("close failed")
    monkeypatch.setattr(cli, "DirectImageExecutor", lambda **k: Executor())
    received = []
    def rm_factory(*a, **kw):
        received.append(kw["png_compress_level"])
        return SimpleNamespace(observation=SimpleNamespace(calls=[], vectors={}))
    monkeypatch.setattr(cli, "make_image_rm", rm_factory)
    def fail_source(*a):
        raise RuntimeError("source first error")
    monkeypatch.setattr(cli, "load_image_file_manifest", fail_source)
    (tmp_path / "identity.json").write_text(json.dumps(dict(qualified=True, model_dtype="float32")))
    assert cli.main(base_args(tmp_path) + ["--png-compress-level", "1"]) == 1
    result = json.loads((tmp_path / "output/result.json").read_text())
    assert received == [1] and result["png_compress_level"] == 1
    assert result["error_type"] == "RuntimeError" and result["reason"] == "source first error"
    assert result["execution_observation_error"] == "KeyError" and result["cleanup_error"] == "ValueError"
