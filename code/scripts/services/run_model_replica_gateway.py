"""Serve existing PostgreSQL Map v6 sessions through a declared model replica set."""

from __future__ import annotations

import argparse
from functools import partial
from pathlib import Path
import sys


CODE_ROOT = Path(__file__).resolve().parents[2]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.execution_provider import server
from src.execution_provider.adapters.model_replicas import ModelReplicaSet, build_model_replica_execution
from src.execution_provider.adapters.ray_map_transport import RayMapConfig


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("--replica-config", required=True, type=Path)
    parser.add_argument("--replica-ray-config", action="append", default=[], metavar="ID=PATH")
    own, remaining = parser.parse_known_args(argv)
    selected = server.parse_args(remaining)
    if not selected.incremental_map or selected.fixed_model_config is None:
        parser.error("replica gateway requires --incremental-map and --fixed-model-config")
    if selected.map_transport_config or selected.organization_config or selected.job_compute_policy != "equal-share":
        parser.error("replica gateway uses its own factory and equal-share Job policy")
    try:
        directory = ModelReplicaSet.load(own.replica_config)
        physical = None
        if own.replica_ray_config:
            physical = {}
            for item in own.replica_ray_config:
                endpoint, path = item.split("=", 1)
                if endpoint in physical:
                    raise ValueError("repeated replica Ray configuration")
                physical[endpoint] = RayMapConfig.load(path)
    except (OSError, ValueError, TypeError):
        parser.error("invalid replica or replica Ray configuration")
    return server.main(remaining, incremental_execution_factory=partial(
        build_model_replica_execution, replicas=directory, physical=physical))


if __name__ == "__main__":
    raise SystemExit(main())
