"""Explicit Q2b strategy interpretation for the pinned LOTUS 1.2.4 SDK."""

from contextlib import contextmanager
import hashlib
import importlib
from importlib.metadata import version
from pathlib import Path

from src.semantic_methods.lotus.sdk import IMAGE_TEXT_PROFILE, validate_source


PROFILE_ID = "sembench-mmqa-q2b-lotus1.2.4-zs-cot-enum-v1"
STRATEGY_SOURCES = {
    "types.py": "73b0ebb4a8a9606cdec6df27353d57fb28ce6e2f5800c539caee892eb4f2cc6b",
    "sem_ops/sem_join.py": "9d1c1cc9af03ef4446c31cf607ed26dacef572c89d72746b0a351d8421edbd01",
}


def strategy_profile():
    return dict(id=PROFILE_ID, upstream_argument="zs-cot", lotus_version="1.2.4",
        sdk_strategy="ReasoningStrategy.ZS_COT", source_sha256=dict(STRATEGY_SOURCES),
        formatter="lotus.templates.task_instructions.filter_formatter",
        parser="lotus.sem_ops.postprocessors.filter_postprocess", native_default_boolean=True,
        scope="explicit upstream strategy intent; not byte parity with LOTUS 1.1.3")


def resolve_join_strategy(value):
    if type(value) is not str or value != "zs-cot":
        raise ValueError("Q2b compatibility only supports the original zs-cot argument")
    if version("lotus-ai") != "1.2.4":
        raise ValueError("Q2b strategy compatibility requires LOTUS 1.2.4")
    from lotus.types import ReasoningStrategy
    return ReasoningStrategy.ZS_COT


def validate_strategy_source():
    """Verify the common formatter/parser and the enum/Join implementation."""
    resolved = resolve_join_strategy("zs-cot")
    validate_source(message_profile=IMAGE_TEXT_PROFILE)
    root = Path(importlib.import_module("lotus").__file__).parent
    for relative, expected in STRATEGY_SOURCES.items():
        if hashlib.sha256((root / relative).read_bytes()).hexdigest() != expected:
            raise ValueError("Q2b strategy source differs at lotus/" + relative)
    return resolved


@contextmanager
def q2b_join_compatibility():
    """Map one original call in its isolated worker; restore on every exit."""
    resolved = validate_strategy_source()
    Join = importlib.import_module("lotus.sem_ops.sem_join").SemJoinDataframe
    original = Join.__call__
    if getattr(original, "_mmqa_q2b_compat", False) is True:
        raise RuntimeError("nested Q2b strategy compatibility requires an isolated process")

    def joined(accessor, other, instruction, *args, **kwargs):
        if args or kwargs != dict(strategy="zs-cot"):
            raise ValueError("unsupported Q2b strategy or additional Join options")
        return original(accessor, other, instruction, strategy=resolved)

    joined._mmqa_q2b_compat = True
    Join.__call__ = joined
    try:
        yield
    finally:
        Join.__call__ = original
