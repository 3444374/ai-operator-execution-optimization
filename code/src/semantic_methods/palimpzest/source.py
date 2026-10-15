"""Source checks for the private Palimpzest 0.8.2.sem_agg execution seams."""

import hashlib
from importlib import import_module
from pathlib import Path


PALIMPZEST_COMMIT = "6efc19841c49d727ce42f5c2dd55e7433601d2d9"
SOURCE_HASHES = {
    "constants.py": "75bafe22b5d7f6fee842e3b6b65ef93da765eb3d76942ca91d852f1ba3870fc9",
    "query/generators/generators.py": "88d05efa2004b83345d6c334a80c93cb76a8beaac9638006cd91ed8adff3c841",
    "query/processor/query_processor.py": "4f87df3db9ce5f6dc6f050c84f398cec8c3b0d41035881e7c25f172d89658238",
    "query/optimizer/plan.py": "54884deed593ec4a1113d04a61ad6abef3a15fb74468cf9a8a86c1c97d2a8f38",
    "query/execution/parallel_execution_strategy.py": "1a7e610795dd757da30a4c71e9decef839a2b4ae9f6eec011679f22a19c6604c",
    "query/operators/filter.py": "8e648657bc59f2ec57cc7f8e928ee5f193d9c4d7ffa8936461501882248797d8",
    "prompts/prompt_factory.py": "3d031960d659327dc4d668dd8abefa11e6e68a064f7d434b367e39aa396e060c",
}


def validate_source():
    """Reject a different source even when its package also reports version 0.8.2."""
    root = Path(import_module("palimpzest").__file__).resolve().parent
    for relative, expected in SOURCE_HASHES.items():
        actual = hashlib.sha256((root / relative).read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError(f"Palimpzest private adapter source differs at {relative}")
    return root
