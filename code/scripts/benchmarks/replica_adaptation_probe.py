"""Run the finite local Core/HTTP replica fixture; no model/server resources."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import platform
import sys

CODE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE_ROOT))
from src.experiments.replica_adaptation_probe import run_probe


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists")
    result = run_probe()
    result["python"] = platform.python_version()
    result["platform"] = platform.system()
    paths = ["src/scheduling/endpoint_routing/replica_adaptation.py", "src/execution_provider/adapters/model_replicas.py",
             "src/execution_provider/adapters/replica_feedback.py", "src/experiments/replica_adaptation_probe.py"]
    result["source_sha256"] = {p: hashlib.sha256((CODE_ROOT / p).read_bytes()).hexdigest() for p in paths}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(args.output, "wt", encoding="utf-8") as handle:
        json.dump(result, handle, separators=(",", ":"))
    print(json.dumps({k: result[k] for k in ("elapsed_s", "http_requests", "model_requests", "summary")}, indent=2))


if __name__ == "__main__":
    main()
