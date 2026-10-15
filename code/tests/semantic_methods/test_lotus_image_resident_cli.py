"""Cancellation must bypass model observations queued behind unfinished work."""
import asyncio
import hashlib
import json
import sys
from types import SimpleNamespace

from src.experiments import image_reuse
from tests.semantic_methods.test_lotus_image_cli import base_args, load_cli


def test_cancelled_query_does_not_poll_busy_workers_before_close(tmp_path, monkeypatch):
    cli = load_cli()
    events = []
    for name in ("validate_image_source", "validate_embedding_identity"):
        monkeypatch.setattr(cli, name, lambda *a, **k: {})
    monkeypatch.setattr(cli, "version", lambda name: "fixture")
    monkeypatch.setattr(cli, "configure_torch_thread_pools", lambda *a: {})
    monkeypatch.setattr(image_reuse, "observe_stock_model", lambda *a, **k: None)
    monkeypatch.setattr(image_reuse, "snapshot_models", lambda: [])

    class Transformer(list):
        def to(self, **kwargs):
            pass

    class Stock:
        def __init__(self, *args, **kwargs):
            self.transformer = Transformer([SimpleNamespace(model=object())])

    worker = SimpleNamespace(ready=SimpleNamespace(remote=lambda: "ready-reference"))
    executor = SimpleNamespace(pool=SimpleNamespace(gpu_actors=(worker,)),
        _unconfirmed_work=False, execution_observations=[], reuse_observations=[],
        close=lambda: events.append("close"))

    def get(refs):
        assert not executor._unconfirmed_work, "observations waited behind unfinished GPU work"
        events.append("ready")
        return [{"fixture": True}]

    ray = SimpleNamespace(is_initialized=lambda: False, init=lambda **k: None,
        get=get, shutdown=lambda: events.append("shutdown"))
    monkeypatch.setitem(sys.modules, "lotus.models", SimpleNamespace(SentenceTransformersRM=Stock))
    monkeypatch.setitem(sys.modules, "torch", SimpleNamespace(float32="float32"))
    monkeypatch.setitem(sys.modules, "ray", ray)
    monkeypatch.setattr(cli, "SemLoomImageExecutor", lambda **k: executor)
    monkeypatch.setattr(cli, "make_image_rm", lambda *a, **k:
        SimpleNamespace(observation=SimpleNamespace(calls=[], vectors={})))
    raw = b"fixture input, never a model image"
    (tmp_path / "image").write_bytes(raw)
    entry = SimpleNamespace(path="image", sha256=hashlib.sha256(raw).hexdigest(), doc_id="one")
    monkeypatch.setattr(cli, "load_image_file_manifest", lambda *a:
        SimpleNamespace(root=tmp_path, entries=(entry,), sha256="fixture"))
    monkeypatch.setattr(cli, "decode_rgb_image", lambda raw: object())
    monkeypatch.setattr(cli, "image_frame", lambda *a: object())

    def workflow(*args, **kwargs):
        kwargs["phase_observer"]("after_index_or_register")
        executor._unconfirmed_work = True
        raise asyncio.CancelledError("query fixture cancelled")

    monkeypatch.setattr(cli, "run_workflow", workflow)
    (tmp_path / "identity.json").write_text(json.dumps(dict(qualified=True, model_dtype="float32")))
    previous = str(tmp_path / "previous-reuse-directory")
    monkeypatch.setenv("SEMLOOM_IMAGE_REUSE_DIR", previous)
    args = base_args(tmp_path, "semloom_hse") + ["--rows", "1", "--query-repeats", "5",
        "--observe-model-reuse", "--ray-temp-dir", str(tmp_path / "ray")]
    assert cli.main(args) == 1
    result = json.loads((tmp_path / "output/result.json").read_text())
    assert result["error_type"] == "CancelledError"
    assert events == ["ready", "ready", "close", "shutdown"]
    assert "worker_models_unavailable" in result["model_reuse_observations"][-1]
    assert cli.os.environ["SEMLOOM_IMAGE_REUSE_DIR"] == previous


def test_phase_snapshots_exclude_live_models_from_another_owner_directory(tmp_path, monkeypatch):
    class Model:
        pass

    first_model, second_model = Model(), Model()
    first_directory, second_directory = tmp_path / "first", tmp_path / "second"
    monkeypatch.setenv("SEMLOOM_IMAGE_REUSE_DIR", str(first_directory))
    first = image_reuse.observe_model(first_model, role="fixture", device="cpu")
    monkeypatch.setenv("SEMLOOM_IMAGE_REUSE_DIR", str(second_directory))
    second = image_reuse.observe_model(second_model, role="fixture", device="cpu")
    assert [item["model_uid"] for item in image_reuse.snapshot_models()] == [second.model_uid]
    monkeypatch.setenv("SEMLOOM_IMAGE_REUSE_DIR", str(first_directory))
    assert [item["model_uid"] for item in image_reuse.snapshot_models()] == [first.model_uid]
