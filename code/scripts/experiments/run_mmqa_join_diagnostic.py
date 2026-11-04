#!/usr/bin/env python3
"""Prepare, verify or execute the bounded Join-only dual-prompt diagnostic."""
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.baselines.common.redact import redact_text


def main():
    try:
        from src.experiments.mmqa_join_diagnostic import main as execute
        return execute()
    except SystemExit as error:
        if error.code is None or isinstance(error.code, int):
            raise
        sys.stderr.write(redact_text(str(error.code)) + "\n")
        return 1
    except BaseException as error:
        sys.stderr.write(redact_text("".join(traceback.format_exception(type(error), error, error.__traceback__))))
        return 130 if isinstance(error, KeyboardInterrupt) else 1


if __name__ == "__main__":
    raise SystemExit(main())
