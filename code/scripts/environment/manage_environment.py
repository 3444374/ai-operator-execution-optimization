#!/usr/bin/env python3
"""Check, provision, or download one declared runtime capability at a time."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.infrastructure.environment import (  # noqa: E402
    check_environment,
    download_asset,
    install_missing_python,
    load_env_file,
    load_json_contract,
    missing_install_specs,
    report_payload,
    selected_group_names,
    write_json_atomic,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--assets-manifest", required=True, type=Path)
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="read-only preflight")
    check.add_argument("--machine-profile", required=True, type=Path)
    check.add_argument("--groups", required=True)
    check.add_argument("--json-out", type=Path)

    install = subparsers.add_parser("install-python", help="pip install missing selected groups")
    install.add_argument("--groups", required=True)
    install.add_argument("--python-executable", type=Path, default=Path(sys.executable))
    install.add_argument("--dry-run", action="store_true")

    download = subparsers.add_parser("download", help="download one declared asset")
    download.add_argument("--asset", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    file_environment = load_env_file(args.env_file)
    environment = {**file_environment, **os.environ}
    manifest = load_json_contract(args.assets_manifest, "runtime_assets")
    if args.command == "check":
        profile = load_json_contract(args.machine_profile, "machine_profile")
        groups = selected_group_names(args.groups)
        results = check_environment(profile, manifest, environment, groups)
        payload = report_payload(args.machine_profile, args.assets_manifest, groups, results)
        print(json.dumps(payload, indent=2, sort_keys=True))
        if args.json_out:
            write_json_atomic(args.json_out, payload)
        return 0 if payload["status"] == "ok" else 1
    if args.command == "install-python":
        groups = selected_group_names(args.groups)
        specs = missing_install_specs(manifest, groups)
        command = install_missing_python(args.python_executable, specs, dry_run=args.dry_run)
        print(json.dumps({"missing_specs": specs, "command": command, "dry_run": args.dry_run}))
        return 0
    if args.command == "download":
        target = download_asset(manifest, args.asset, environment)
        print(json.dumps({"asset": args.asset, "target": str(target), "status": "ready"}))
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
