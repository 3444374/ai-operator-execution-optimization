"""Verify live TP1 replicas or opt-in native DP model ownership without model requests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


CODE_ROOT = Path(__file__).resolve().parents[2]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.execution_provider.adapters.model_replicas import ModelReplicaSet
from src.execution_provider.adapters.replica_identity import inspect_local_model_replicas


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replica-config", required=True, type=Path)
    parser.add_argument("--runtime-identity", action="append", required=True, metavar="ID=PATH")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--allow-shared-devices", action="store_true")
    args = parser.parse_args(argv)
    paths = {}
    for item in args.runtime_identity:
        if "=" not in item:
            parser.error("runtime identities use ID=PATH")
        endpoint, path = item.split("=", 1)
        if endpoint in paths:
            parser.error("runtime identities must be unique")
        paths[endpoint] = Path(path)
    try:
        result = inspect_local_model_replicas(ModelReplicaSet.load(args.replica_config), paths,
                                             independent_devices=not args.allow_shared_devices)
    except Exception:
        parser.exit(2, "model replica process/device verification failed\n")
    # Private evidence; do not overwrite an earlier observation or expose URLs/argv.
    with args.output.open("x", encoding="utf-8") as output:
        output.write(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps({name: result[name] for name in ("replica_count", "gpu_count", "independent_devices")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
