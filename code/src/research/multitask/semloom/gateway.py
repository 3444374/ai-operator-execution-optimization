"""Opt-in multi-Job provider entry; transport and query ownership stay in SemLoom."""

from __future__ import annotations

import argparse
import sys
from functools import partial

from ....execution_provider import server
from ....execution_provider.adapters.incremental_execution import build_fixed_model_execution
from ....execution_provider.adapters.image_execution import build_image_execution
from ....scheduling.core.session_jobs import equal_share_job_budget
from .session import shared_compute_job_budget


def main(argv=None, *, incremental_execution_factory=None, **options):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--job-compute-policy", choices=("equal-share", "shared"),
                        default="equal-share")
    research, remaining = parser.parse_known_args(sys.argv[1:] if argv is None else argv)
    args = server.parse_args(remaining)
    if research.job_compute_policy == "shared" and (
        not args.incremental_map or incremental_execution_factory is not None
    ):
        raise SystemExit("shared compute requires incremental execution without a custom factory")
    if args.image_config and research.job_compute_policy != "equal-share":
        raise SystemExit("image configuration selects its own typed execution profile")
    if args.organization_config is not None:
        return server.main(remaining, incremental_execution_factory=incremental_execution_factory,
                           **options)
    allocate = (shared_compute_job_budget if research.job_compute_policy == "shared"
                else equal_share_job_budget)
    factory = incremental_execution_factory
    if args.map_transport_config is not None:
        if not args.incremental_map or not args.fixed_model_config or args.image_config or factory is not None:
            raise SystemExit("Ray Map transport requires fixed-model incremental Map without another factory")
        from ....execution_provider.adapters.ray_map_transport import RayMapConfig, ray_map_factory
        try:
            physical = RayMapConfig.load(args.map_transport_config)
        except (OSError, ValueError, TypeError):
            raise SystemExit("invalid Ray Map transport configuration") from None
        factory = partial(ray_map_factory(physical, options.get("remote_request_guard")),
                          allocate_job=allocate)
        # The existing transport factory now owns this parsed setting.
        forwarded = []
        skip_value = False
        for value in remaining:
            if skip_value:
                skip_value = False
            elif value == "--map-transport-config":
                skip_value = True
            elif not value.startswith("--map-transport-config="):
                forwarded.append(value)
        remaining = forwarded
    elif factory is None:
        if args.image_config:
            factory = partial(build_image_execution, allocate_job=shared_compute_job_budget)
        elif args.incremental_map:
            factory = partial(build_fixed_model_execution, allocate_job=allocate)
    return server.main(remaining, incremental_execution_factory=factory, **options)


if __name__ == "__main__":
    raise SystemExit(main())
