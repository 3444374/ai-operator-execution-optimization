from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.infrastructure.environment import (
    check_environment,
    download_asset,
    load_env_file,
    selected_group_names,
)


class EnvironmentContractTests(unittest.TestCase):
    def test_env_file_expands_profile_roots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.env"
            path.write_text(
                "ROOT=/srv/ai\nMODEL_ROOT=${ROOT}/models\nMODEL_PATH=${MODEL_ROOT}/qwen\n",
                encoding="utf-8",
            )
            with mock.patch.dict("os.environ", {}, clear=True):
                resolved = load_env_file(path)
        self.assertEqual(resolved["MODEL_PATH"], "/srv/ai/models/qwen")

    def test_env_file_rejects_unresolved_reference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.env"
            path.write_text("MODEL_PATH=${MISSING}/qwen\n", encoding="utf-8")
            with mock.patch.dict("os.environ", {}, clear=True):
                with self.assertRaisesRegex(ValueError, "MISSING"):
                    load_env_file(path)

    def test_cpu_only_profile_checks_declared_asset(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            asset_path = root / "input.csv"
            asset_path.write_text("ok", encoding="utf-8")
            profile = {
                "schema_version": 1,
                "kind": "machine_profile",
                "platforms": [__import__("platform").system()],
                "minimum_python": [3, 10],
                "minimum_cpu_slots": 1,
                "required_environment": ["DATA_ROOT", "ARTIFACT_ROOT"],
                "disk": {"root_env": "ARTIFACT_ROOT", "minimum_free_gib": 0},
                "gpu": {"required": False},
            }
            manifest = {
                "schema_version": 1,
                "kind": "runtime_assets",
                "python_groups": {"test": []},
                "assets": [
                    {
                        "id": "fixture",
                        "kind": "http_file",
                        "groups": ["test"],
                        "target": "${DATA_ROOT}/input.csv",
                        "minimum_bytes": 2,
                    }
                ],
            }
            results = check_environment(
                profile,
                manifest,
                {"DATA_ROOT": str(root), "ARTIFACT_ROOT": str(root)},
                ("test",),
            )
        self.assertTrue(all(item.status == "ok" for item in results))

    def test_group_list_is_deduplicated(self) -> None:
        self.assertEqual(
            selected_group_names("core,ml-estimators,core"),
            ("core", "ml-estimators"),
        )

    def test_existing_asset_is_not_downloaded_again(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "ready.csv"
            target.write_text("ready", encoding="utf-8")
            manifest = {
                "assets": [
                    {
                        "id": "ready",
                        "kind": "http_file",
                        "groups": ["test"],
                        "target": "${DATA_ROOT}/ready.csv",
                        "url": "https://invalid.example/never-used",
                        "minimum_bytes": 5,
                    }
                ]
            }

            resolved = download_asset(manifest, "ready", {"DATA_ROOT": str(root)})

        self.assertEqual(resolved, target)

    def test_manual_asset_refuses_automatic_download(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = {
                "assets": [
                    {
                        "id": "licensed",
                        "kind": "manual",
                        "groups": ["test"],
                        "target": "${DATA_ROOT}/licensed",
                        "instructions": "accept provider terms",
                    }
                ]
            }
            with self.assertRaisesRegex(RuntimeError, "manual authorization"):
                download_asset(
                    manifest,
                    "licensed",
                    {"DATA_ROOT": directory},
                )


if __name__ == "__main__":
    unittest.main()
