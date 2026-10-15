"""Model version and processor changes must not reuse a qualified index."""
import pytest
from types import SimpleNamespace

from src.modalities.image.contracts import EmbeddingSemantics
from src.semantic_methods.lotus.image_identity import file_hash, validate_embedding_identity, validate_executor_identity


def identity(root):
    (root / "pytorch_model.bin").write_bytes(b"fixture-weights")
    (root / "config.json").write_text('{"projection_dim":512}')
    (root / "preprocessor_config.json").write_text('{"size":224}')
    files = {name: file_hash(root / name) for name in ("pytorch_model.bin", "preprocessor_config.json", "config.json")}
    parameters = dict(sha256="a" * 64, parameter_tensors=1)
    return dict(schema="lotus.clip_identity.v1", qualified=True, parameters_equal=True,
                effective_pixels_equal=True, tokenizer_equal=True, normalized_outputs_equivalent=True,
                stock_parameters=parameters, candidate_parameters=parameters,
                versions={name: "fixture" for name in ("lotus-ai", "torch", "transformers", "sentence-transformers", "numpy", "pillow")},
                stock=dict(files=files), candidate=dict(files=files))


def test_processor_directory_needs_processor_files_without_weights(tmp_path, monkeypatch):
    monkeypatch.setattr("src.semantic_methods.lotus.image_identity.version", lambda name: "fixture")
    model = tmp_path / "model"
    model.mkdir()
    value = identity(model)
    processor = tmp_path / "processor"
    processor.mkdir()
    (processor / "preprocessor_config.json").write_bytes((model / "preprocessor_config.json").read_bytes())
    validate_embedding_identity(value, stock_model=model, candidate_model=model, processor=processor)
    (processor / "preprocessor_config.json").write_text('{"size":448}')
    with pytest.raises(ValueError, match="processor differs"):
        validate_embedding_identity(value, stock_model=model, candidate_model=model, processor=processor)


@pytest.mark.parametrize("change", ["versions", "parameters", "snapshot", "pixels", "version_drift"])
def test_unqualified_or_changed_identity_is_rejected(tmp_path, monkeypatch, change):
    monkeypatch.setattr("src.semantic_methods.lotus.image_identity.version", lambda name: "fixture")
    value = identity(tmp_path)
    if change == "versions":
        value["versions"] = {}
    elif change == "parameters":
        value["stock_parameters"] = value["candidate_parameters"] = {}
    elif change == "snapshot":
        (tmp_path / "pytorch_model.bin").write_bytes(b"changed")
    elif change == "pixels":
        value["effective_pixels_equal"] = False
    else:
        monkeypatch.setattr("src.semantic_methods.lotus.image_identity.version", lambda name: "changed")
    with pytest.raises(ValueError):
        validate_embedding_identity(value, stock_model=tmp_path)


@pytest.mark.parametrize("compute_dtype", ["float16", "bfloat16", "float32"])
def test_external_compute_precision_is_not_inferred_from_output_dtype(tmp_path, monkeypatch, compute_dtype):
    monkeypatch.setattr("src.semantic_methods.lotus.image_identity.version", lambda name: "fixture")
    value = identity(tmp_path)
    executor = SimpleNamespace(semantics=EmbeddingSemantics(str(tmp_path), str(tmp_path), 512),
        identity=dict(compute_dtype=compute_dtype, device="cuda", software={
            "torch": "fixture", "transformers": "fixture", "numpy": "fixture"},
            artifacts_sha256=dict(weights=file_hash(tmp_path / "pytorch_model.bin"),
                model_config=file_hash(tmp_path / "config.json"),
                processor_config=file_hash(tmp_path / "preprocessor_config.json"))))
    if compute_dtype == "float32":
        assert validate_executor_identity(executor, value, model=tmp_path, processor=None,
                                          dtype="float32", device="cuda") == executor.identity
    else:
        with pytest.raises(ValueError, match="compute dtype"):
            validate_executor_identity(executor, value, model=tmp_path, processor=None,
                                       dtype="float32", device="cuda")
