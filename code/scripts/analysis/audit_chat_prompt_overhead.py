#!/usr/bin/env python3
"""Audit the frozen six-arm chat work-cost evidence from raw traces."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.saor.project_mechanism_formal import (  # noqa: E402
    EXPECTED_SCENARIOS,
)
from src.experiments.shared_vllm.config import (  # noqa: E402
    CompletionWorkCostConfig,
)
from src.experiments.shared_vllm.work_evidence import (  # noqa: E402
    audit_work_cost_matrix,
)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--expected-overhead", required=True, type=int)
    parser.add_argument("--expected-output-cap", required=True, type=int)
    parser.add_argument("--expected-requests-per-cell", type=int, default=1024)
    parser.add_argument("--phase", default="warmup")
    parser.add_argument("--repeat-index", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    args = _args()
    if (
        args.expected_overhead < 0
        or args.expected_output_cap <= 0
        or args.expected_requests_per_cell <= 0
    ):
        raise SystemExit("work-cost expectations must be non-negative/positive")
    result = audit_work_cost_matrix(
        args.matrix_root,
        work_cost=CompletionWorkCostConfig(
            protocol="chat_completions",
            prompt_token_overhead_per_request=args.expected_overhead,
            output_bound_source="fixed_output_cap",
            completion_max_tokens=args.expected_output_cap,
        ),
        expected_scenarios=EXPECTED_SCENARIOS,
        expected_phase=args.phase,
        expected_repeat_indexes=(args.repeat_index,),
        expected_requests_per_cell=args.expected_requests_per_cell,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
