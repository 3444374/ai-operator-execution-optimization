#!/usr/bin/env python3
"""Check, provision, or download one declared runtime capability at a time."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
REPOSITORY_ROOT = CODE_ROOT.parent
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.infrastructure.environment import (  # noqa: E402
    check_environment,
    download_asset,
    install_missing_python,
    load_env_file,
    load_json_contract,
    missing_install_specs,
    observe_machine,
    report_payload,
    select_machine_profile,
    selected_group_names,
    write_json_atomic,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        help=(
            "machine-local env file; defaults to AI_OPERATOR_ENV_FILE or "
            "~/.config/ai-operator/runtime.env"
        ),
    )
    parser.add_argument(
        "--assets-manifest",
        type=Path,
        default=REPOSITORY_ROOT / "deploy/runtime/assets.json",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="read-only preflight")
    check.add_argument(
        "--machine-profile",
        type=Path,
        help="explicit profile override; otherwise auto-detect from deploy/runtime/profiles",
    )
    check.add_argument(
        "--profiles-dir",
        type=Path,
        default=REPOSITORY_ROOT / "deploy/runtime/profiles",
    )
    check.add_argument("--groups", required=True)
    check.add_argument("--json-out", type=Path)

    install = subparsers.add_parser("install-python", help="pip install missing selected groups")
    install.add_argument("--groups", required=True)
    install.add_argument("--python-executable", type=Path, default=Path(sys.executable))
    install.add_argument("--dry-run", action="store_true")

    download = subparsers.add_parser("download", help="download one declared asset")
    download.add_argument("--asset", required=True)
    return parser.parse_args(argv)


def resolve_env_file(explicit: Path | None) -> Path:
    """Resolve one machine-local env file without placing secrets in the repository."""

    if explicit is not None:
        candidate = explicit.expanduser()
        if not candidate.is_file():
            raise FileNotFoundError(f"explicit runtime env file does not exist: {candidate}")
        return candidate
    configured = os.environ.get("AI_OPERATOR_ENV_FILE")
    if configured:
        candidate = Path(configured).expanduser()
        if not candidate.is_file():
            raise FileNotFoundError(
                f"AI_OPERATOR_ENV_FILE does not exist: {candidate}"
            )
        return candidate
    candidate = Path.home() / ".config/ai-operator/runtime.env"
    if candidate.is_file():
        return candidate
    raise FileNotFoundError(
        "no runtime env file found; pass --env-file or set AI_OPERATOR_ENV_FILE; "
        f"checked default: {candidate}"
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    env_file = resolve_env_file(args.env_file)
    file_environment = load_env_file(env_file)
    environment = {**file_environment, **os.environ}
    manifest = load_json_contract(args.assets_manifest, "runtime_assets")
    if args.command == "check":
        observation = observe_machine()
        if args.machine_profile is not None:
            profile_path = args.machine_profile
            profile = load_json_contract(profile_path, "machine_profile")
            profile_selection = "explicit"
        else:
            profile_paths = tuple(sorted(args.profiles_dir.glob("*.json")))
            profile_path, profile = select_machine_profile(profile_paths, observation)
            profile_selection = "automatic"
        groups = selected_group_names(args.groups)
        results = check_environment(profile, manifest, environment, groups)
        payload = report_payload(
            profile_path,
            args.assets_manifest,
            groups,
            results,
            observation=observation,
            profile_selection=profile_selection,
        )
        payload["env_file"] = str(env_file)
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
