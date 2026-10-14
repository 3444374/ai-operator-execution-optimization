"""Content-pinned CLIP qualification identities, independent of model names."""
import hashlib
from importlib.metadata import version
import json
from pathlib import Path


def snapshot_files(root):
    root = Path(root)
    names = ("config.json", "preprocessor_config.json", "tokenizer_config.json",
             "special_tokens_map.json", "merges.txt", "vocab.json", "tokenizer.json",
             "modules.json", "config_sentence_transformers.json")
    result = {}
    for directory in (root, root / "0_CLIPModel"):
        if not directory.is_dir():
            continue
        for name in (*names, "model.safetensors", "pytorch_model.bin"):
            path = directory / name
            if path.is_file():
                result[path.relative_to(root).as_posix()] = file_hash(path)
    if not result or not any(x.endswith(("model.safetensors", "pytorch_model.bin")) for x in result):
        raise ValueError("CLIP snapshot is missing weights or configuration")
    return result


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parameter_digest(model):
    """Hash actual loaded parameters including both towers and projections."""
    digest = hashlib.sha256()
    count = 0
    for name, tensor in sorted(model.named_parameters()):
        array = tensor.detach().cpu().contiguous().numpy()
        header = json.dumps([name, list(array.shape), str(array.dtype)], separators=(",", ":")).encode()
        digest.update(len(header).to_bytes(8, "big"))
        digest.update(header)
        digest.update(array.tobytes())
        count += 1
    return {"sha256": digest.hexdigest(), "parameter_tensors": count}


def validate_embedding_identity(identity, *, stock_model, candidate_model=None, processor=None):
    if (identity.get("schema") != "lotus.clip_identity.v1" or identity.get("qualified") is not True
            or identity.get("parameters_equal") is not True
            or identity.get("effective_pixels_equal") is not True
            or identity.get("normalized_outputs_equivalent") is not True
            or identity.get("tokenizer_equal") is not True):
        raise ValueError("model content, preprocessing and numeric qualification are required")
    if identity["stock_parameters"] != identity["candidate_parameters"]:
        raise ValueError("stock and candidate actual CLIP parameters differ")
    parameters = identity["stock_parameters"]
    if (set(parameters) != {"sha256", "parameter_tensors"}
            or type(parameters["parameter_tensors"]) is not int or parameters["parameter_tensors"] < 1
            or not isinstance(parameters["sha256"], str) or len(parameters["sha256"]) != 64
            or any(x not in "0123456789abcdef" for x in parameters["sha256"])):
        raise ValueError("CLIP qualification needs a nonempty actual parameter digest")
    required_versions = {"lotus-ai", "torch", "transformers", "sentence-transformers", "numpy", "pillow"}
    if not required_versions.issubset(identity.get("versions", {})):
        raise ValueError("CLIP qualification must pin all math and preprocessing versions")
    for name, expected in identity["versions"].items():
        if version(name) != expected:
            raise ValueError("CLIP qualification software version changed: " + name)
    for name, root in (("stock", stock_model), ("candidate", candidate_model)):
        if root is not None and snapshot_files(root) != identity[name]["files"]:
            raise ValueError("CLIP snapshot contents differ from qualification: " + name)
    if processor is not None:
        expected = identity["candidate"]["files"]
        processor_files = {"preprocessor_config.json", "tokenizer_config.json", "special_tokens_map.json",
                           "merges.txt", "vocab.json", "tokenizer.json"}
        if any(not (Path(processor) / name).is_file() or file_hash(Path(processor) / name) != digest
               for name, digest in expected.items()
               if name in processor_files):
            raise ValueError("CLIP processor differs from qualification")
    return identity


def validate_executor_identity(executor, identity, *, model, processor, dtype, device):
    """Output dtype alone cannot establish the actual model compute precision."""
    evidence = getattr(executor, "identity", None)
    if (not isinstance(evidence, dict) or evidence.get("compute_dtype") != dtype
            or evidence.get("device") != device
            or executor.semantics.model_revision != str(Path(model).resolve())
            or executor.semantics.processor_revision != str(Path(processor or model).resolve())):
        raise ValueError("external executor compute dtype, device or model identity differs")
    root = Path(model)
    weights = "model.safetensors" if (root / "model.safetensors").exists() else "pytorch_model.bin"
    expected = dict(weights=identity["candidate"]["files"][weights],
                    model_config=file_hash(root / "config.json"),
                    processor_config=file_hash(Path(processor or model) / "preprocessor_config.json"))
    if evidence.get("artifacts_sha256") != expected:
        raise ValueError("external executor weights or effective processor identity differs")
    software = evidence.get("software", {})
    if not {"torch", "transformers", "numpy"}.issubset(software):
        raise ValueError("external executor must report math software versions")
    for name, observed in software.items():
        if version(name) != observed:
            raise ValueError("external executor software version differs: " + name)
    return evidence
