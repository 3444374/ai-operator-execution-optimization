from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.infrastructure.environment import (
    MachineObservation,
    check_environment,
    download_asset,
    load_env_file,
    select_machine_profile,
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

    def test_machine_profile_selects_specific_match_before_generic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            generic = root / "generic.json"
            specific = root / "specific.json"
            generic.write_text(
                '{"schema_version":1,"kind":"machine_profile","name":"generic",'
                '"match":{"priority":0,"platforms":["Linux"],'
                '"minimum_gpu_count":1}}',
                encoding="utf-8",
            )
            specific.write_text(
                '{"schema_version":1,"kind":"machine_profile","name":"dual4090",'
                '"match":{"priority":100,"platforms":["Linux"],'
                '"minimum_gpu_count":2,"maximum_gpu_count":2,'
                '"gpu_name_regex":"4090"}}',
                encoding="utf-8",
            )
            observation = MachineObservation(
                machine_id="machine-test",
                platform="Linux",
                python="3.12.0",
                cpu_slots=16,
                gpu_names=("NVIDIA GeForce RTX 4090", "NVIDIA GeForce RTX 4090"),
                gpu_memory_mib=(24564, 24564),
                gpu_driver_versions=("570", "570"),
            )

            path, profile = select_machine_profile(
                (generic, specific), observation
            )

        self.assertEqual(path.name, "specific.json")
        self.assertEqual(profile["name"], "dual4090")

    def test_machine_profile_falls_back_to_generic_nvidia(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "generic.json"
            path.write_text(
                '{"schema_version":1,"kind":"machine_profile","name":"generic",'
                '"match":{"priority":0,"platforms":["Linux"],'
                '"minimum_gpu_count":1,"minimum_gpu_memory_mib":8000}}',
                encoding="utf-8",
            )
            observation = MachineObservation(
                machine_id="machine-test",
                platform="Linux",
                python="3.12.0",
                cpu_slots=8,
                gpu_names=("NVIDIA RTX A6000",),
                gpu_memory_mib=(49140,),
                gpu_driver_versions=("570",),
            )

            selected, _ = select_machine_profile((path,), observation)

        self.assertEqual(selected, path)


if __name__ == "__main__":
    unittest.main()
