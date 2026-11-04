#!/usr/bin/env python3
"""Prepare, verify, or observe the original finite MMQA Q2b query."""
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.baselines.common.redact import redact_text


def main():
    """Render propagated failures before the supervisor can persist stderr."""
    try:
        from src.experiments.sembench_mmqa import main as execute
        return execute()
    except SystemExit as error:
        if error.code is None or isinstance(error.code, int):
            raise
        sys.stderr.write(redact_text(str(error.code)) + "\n")
        return 1
    except BaseException as error:
        rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        sys.stderr.write(redact_text(rendered))
        return 130 if isinstance(error, KeyboardInterrupt) else 1

if __name__ == "__main__":
    raise SystemExit(main())
