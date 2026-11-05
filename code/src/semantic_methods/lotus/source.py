"""Record installed LOTUS sources without imposing byte/version equality on execution.

The reference commit identifies the implementation used for existing qualification.
Installed package metadata and readable source bytes are observations, not a claim
that another release has passed the same semantic or performance checks.
"""

import hashlib
from importlib.metadata import PackageNotFoundError, distribution


LOTUS_COMMIT = "b1a85fd7a66fabed8a1585d44d7597d592b4433f"
FILTER_FILES = (
    "models/lm.py", "templates/task_instructions.py", "sem_ops/postprocessors.py",
    "cache.py", "settings.py", "nl_expression.py", "pricing.py", "sem_ops/sem_filter.py",
)
CHAIN_FILES = (*FILTER_FILES, "sem_ops/sem_map.py", "sem_ops/sem_extract.py", "types.py")
CASCADE_FILES = (*FILTER_FILES, "sem_ops/cascade_utils.py", "types.py", "utils.py")
IMAGE_FILES = (
    "models/rm.py", "models/sentence_transformers_rm.py", "dtype_extensions/image.py",
    "dtype_extensions/__init__.py", "sem_ops/sem_index.py", "sem_ops/sem_search.py",
    "sem_ops/load_sem_index.py", "vector_store/faiss_vs.py", "cache.py", "settings.py",
)
SDK_FILES = (
    "models/lm.py", "sem_ops/sem_map.py", "templates/task_instructions.py",
    "sem_ops/postprocessors.py", "cache.py", "settings.py", "nl_expression.py", "pricing.py",
)


def source_report(profile, lotus_files, *, provider_sources=False):
    """Report distribution provenance; unavailable metadata/files remain explicit.

    Normal request preparation does not call this observer. Imports and actual
    API/contract use establish the capabilities required by each execution path.
    """
    versions, hashes, unavailable = {}, {}, []
    for package in ("lotus-ai", "litellm", "openai"):
        try:
            installed = distribution(package)
        except PackageNotFoundError:
            versions[package] = None
            unavailable.append(package)
            continue
        versions[package] = installed.version
        if package == "lotus-ai":
            root, files = installed.locate_file("lotus"), lotus_files
        elif package == "litellm" and provider_sources:
            root = installed.locate_file("litellm")
            files = ("llms/openai/chat/gpt_transformation.py", "utils.py")
        else:
            continue
        for relative in files:
            name = relative if package == "lotus-ai" else "litellm/" + relative
            try:
                hashes[name] = hashlib.sha256((root / relative).read_bytes()).hexdigest()
            except OSError:
                unavailable.append(name)
    return dict(profile=profile, lotus_commit=None, reference_lotus_commit=LOTUS_COMMIT,
                versions=versions, source_hashes=hashes, unavailable_sources=unavailable,
                source_policy="record_only", source_kind="installed_distribution")
