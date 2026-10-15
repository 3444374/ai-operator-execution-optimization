"""CLI failure evidence must survive cleanup failures without hiding the cause."""
import importlib.util
import asyncio
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from PIL import Image
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


def test_cli_defaults_and_inapplicable_pipeline_or_capacity_fail_before_work(tmp_path):
    cli = load_cli()
    args = base_args(tmp_path)
    assert cli.parser().parse_args(args).ray_image_pipeline is None
    assert cli.parser().parse_args(args).query_repeats == 1
    assert cli.parser().parse_args(args).observe_model_reuse is False
    with pytest.raises(SystemExit):
        cli.parser().parse_args(args + ["--ray-image-pipeline", "unknown"])
    with pytest.raises(ValueError, match="requires ray_data"):
        cli.main(args + ["--ray-image-pipeline", "staged"])
    with pytest.raises(ValueError, match="require semloom_hse"):
        cli.main(args + ["--hse-encoded-bytes", "4"])
    with pytest.raises(ValueError, match="dimensions"):
        cli.main(args + ["--query-repeats", "0"])
    with pytest.raises(ValueError, match="query repeats"):
        cli.main(args + ["--mode", "index", "--query-repeats", "5"])
    assert not (tmp_path / "output").exists()


def test_cli_source_error_survives_observation_and_close_errors(tmp_path, monkeypatch):
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
        received.append(kw)
        return SimpleNamespace(observation=SimpleNamespace(calls=[], vectors={}))
    monkeypatch.setattr(cli, "make_image_rm", rm_factory)
    def fail_source(*a):
        raise RuntimeError("source first error")
    monkeypatch.setattr(cli, "load_image_file_manifest", fail_source)
    (tmp_path / "identity.json").write_text(json.dumps(dict(qualified=True, model_dtype="float32")))
    assert cli.main(base_args(tmp_path)) == 1
    result = json.loads((tmp_path / "output/result.json").read_text())
    assert len(received) == 1 and result["input_policy"] == "backend_native"
    assert result["error_type"] == "RuntimeError" and result["reason"] == "source first error"
    assert result["execution_observation_error"] == "KeyError" and result["cleanup_error"] == "ValueError"


def observation_getter_fixture(tmp_path, monkeypatch, observation_error, *, source_error=None,
                              cleanup_failure=False):
    cli = load_cli()
    for name in ("validate_image_source", "validate_embedding_identity", "validate_executor_identity"):
        monkeypatch.setattr(cli, name, lambda *a, **k: {})
    monkeypatch.setattr(cli, "version", lambda name: "fixture")
    monkeypatch.setattr(cli, "configure_torch_thread_pools", lambda *a: {})
    class Stock:
        def __init__(self, *a, **k):
            self.transformer = SimpleNamespace(to=lambda **k: None)
    monkeypatch.setitem(sys.modules, "lotus.models", SimpleNamespace(SentenceTransformersRM=Stock))
    monkeypatch.setitem(sys.modules, "torch", SimpleNamespace(float32="float32"))
    calls = []
    class Executor:
        @property
        def execution_observations(self):
            calls.append("observation")
            raise observation_error
        def close(self):
            calls.append("close")
            if cleanup_failure:
                raise ValueError("close failed")
    def shutdown():
        calls.append("shutdown")
        if cleanup_failure:
            raise OSError("shutdown failed")
    monkeypatch.setitem(sys.modules, "ray", SimpleNamespace(is_initialized=lambda: False,
        init=lambda **k: None, shutdown=shutdown))
    monkeypatch.setattr(cli, "SemLoomImageExecutor", lambda **k: Executor())
    monkeypatch.setattr(cli, "make_image_rm", lambda *a, **k:
        SimpleNamespace(observation=SimpleNamespace(calls=[], vectors={})))
    payload = b"bounded source fixture"
    (tmp_path / "image.bin").write_bytes(payload)
    manifest = SimpleNamespace(root=tmp_path, sha256="0" * 64, entries=(SimpleNamespace(
        path="image.bin", sha256=hashlib.sha256(payload).hexdigest(), doc_id="a"),))
    def source(*a):
        if source_error is not None:
            raise source_error
        return manifest
    monkeypatch.setattr(cli, "load_image_file_manifest", source)
    monkeypatch.setattr(cli, "decode_rgb_image", lambda raw: Image.new("RGB", (4, 5), "red"))
    monkeypatch.setattr(cli, "image_frame", lambda *a: object())
    def workflow(*a, **k):
        k["report"].update(status="succeeded", index_s=0.0, queries=[])
    monkeypatch.setattr(cli, "run_workflow", workflow)
    (tmp_path / "identity.json").write_text(json.dumps(dict(qualified=True, model_dtype="float32")))
    result = cli.main(base_args(tmp_path, "semloom_hse") + ["--rows", "1",
        "--ray-temp-dir", str(tmp_path / "ray")])
    return result, json.loads((tmp_path / "output/result.json").read_text()), calls


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, asyncio.CancelledError, SystemExit])
@pytest.mark.parametrize("source_failed", [False, True])
def test_observation_getter_cancellation_keeps_primary_and_attempts_all_cleanup(
        tmp_path, monkeypatch, error_type, source_failed):
    cancellation = error_type("observation cancelled")
    source_error = RuntimeError("source first error") if source_failed else None
    exit_code, report, calls = observation_getter_fixture(tmp_path, monkeypatch, cancellation,
        source_error=source_error, cleanup_failure=True)
    primary = source_error if source_failed else cancellation
    assert exit_code == 1 and report["status"] == "failed"
    assert report["error_type"] == type(primary).__name__ and report["reason"] == str(primary)
    assert report["execution_observation_error"] == error_type.__name__
    assert report["cleanup_error"] == "ValueError" and report["ray_cleanup_error"] == "OSError"
    assert calls == ["observation", "close", "shutdown"]
    if source_failed:
        assert any("execution observations" in note and error_type.__name__ in note
                   for note in primary.__notes__)
    assert any("executor cleanup" in note and "ValueError" in note for note in primary.__notes__)
    assert any("Ray shutdown" in note and "OSError" in note for note in primary.__notes__)


def test_observation_getter_ordinary_diagnostic_preserves_success(tmp_path, monkeypatch):
    exit_code, report, calls = observation_getter_fixture(tmp_path, monkeypatch, KeyError("diagnostic"))
    assert exit_code == 0 and report["status"] == "succeeded"
    assert report["execution_observation_error"] == "KeyError" and report["ray_shutdown_returned"]
    assert calls == ["observation", "close", "shutdown"]


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, asyncio.CancelledError, SystemExit])
def test_observation_getter_cancellation_fails_even_when_all_cleanup_succeeds(
        tmp_path, monkeypatch, error_type):
    cancellation = error_type("observation cancelled")
    exit_code, report, calls = observation_getter_fixture(tmp_path, monkeypatch, cancellation)
    assert exit_code == 1 and report["status"] == "failed"
    assert report["error_type"] == error_type.__name__ and report["reason"] == str(cancellation)
    assert report["execution_observation_error"] == error_type.__name__
    assert report["ray_shutdown_returned"] and "cleanup_error" not in report
    assert "ray_cleanup_error" not in report
    assert calls == ["observation", "close", "shutdown"]


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, asyncio.CancelledError, SystemExit])
def test_observation_getter_cancellation_outranks_prior_timing_diagnostic(
        tmp_path, monkeypatch, error_type):
    timing = OSError("source timing diagnostic")
    timing._semloom_timing_failure = True
    cancellation = error_type("observation cancelled")
    exit_code, report, calls = observation_getter_fixture(tmp_path, monkeypatch, cancellation,
        source_error=timing, cleanup_failure=True)
    assert exit_code == 1 and report["error_type"] == error_type.__name__
    assert report["reason"] == str(cancellation) and report["status"] == "failed"
    assert calls == ["observation", "close", "shutdown"]
    assert any("Prior image timing failure: OSError" in note for note in cancellation.__notes__)
    assert any("executor cleanup" in note for note in cancellation.__notes__)
    assert any("Ray shutdown" in note for note in cancellation.__notes__)
