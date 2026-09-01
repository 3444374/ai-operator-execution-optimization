"""Build one strict exact-SemFilter matched-reference calibration artifact."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[2]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.planning.semfilter_reference_calibration import (  # noqa: E402
    build_reference_calibration,
    load_json_document,
    validate_reference_calibration,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an exact-SemFilter reference calibration from offline training "
            "and held-out observations."
        )
    )
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        source = load_json_document(args.source.read_text(encoding="utf-8"))
        artifact = build_reference_calibration(source)
        validate_reference_calibration(artifact)
        encoded = json.dumps(
            artifact,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            indent=2,
        )
        with args.output.open("x", encoding="utf-8") as output:
            output.write(encoded)
            output.write("\n")
    except (OSError, ValueError) as exc:
        print(f"calibration artifact not written: {exc}", file=sys.stderr)
        return 2
    print(artifact["artifact_id"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
