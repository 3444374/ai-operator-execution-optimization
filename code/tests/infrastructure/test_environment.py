from __future__ import annotations

import http.client
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
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
            from src.infrastructure.cpu_allocation import CpuCapacity
            with mock.patch("src.infrastructure.environment.observe_cpu_capacity",
                            return_value=CpuCapacity(4, None, 4, "CPU_fixture")):
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

    def test_snapshot_download_owns_interrupted_and_cached_directories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'model'
            partial = target / '.cache/huggingface/download/weights.incomplete'
            partial.parent.mkdir(parents=True)
            partial.write_bytes(b'weights')
            manifest = {'assets': [{'id': 'fixture', 'kind': 'huggingface_snapshot',
                'repo_id': 'placeholder/model', 'target': '${MODEL_ROOT}/model',
                'minimum_bytes': 4}]}

            def download(**kwargs):
                if client.call_count == 1:
                    raise OSError('fixture interrupted')
                if partial.exists():
                    partial.replace(target / 'weights.bin')
                return str(target)

            client = mock.Mock(side_effect=download)
            with mock.patch.dict('sys.modules', {'huggingface_hub': SimpleNamespace(snapshot_download=client)}), \
                 mock.patch('src.infrastructure.environment.shutil.disk_usage',
                            return_value=SimpleNamespace(free=0)):
                with self.assertRaisesRegex(OSError, 'fixture interrupted'):
                    download_asset(manifest, 'fixture', {'MODEL_ROOT': directory})
                self.assertEqual(partial.read_bytes(), b'weights')
                self.assertEqual(download_asset(manifest, 'fixture', {'MODEL_ROOT': directory}), target)
                self.assertEqual((target / 'weights.bin').read_bytes(), b'weights')
                self.assertFalse(partial.exists())
                self.assertEqual(download_asset(manifest, 'fixture', {'MODEL_ROOT': directory}), target)
                self.assertEqual(client.call_count, 3)
                self.assertEqual(client.call_args_list, [mock.call(
                    repo_id='placeholder/model', repo_type='model', local_dir=target)] * 3)

    def test_interrupted_download_keeps_partial_and_can_resume(self) -> None:
        def response(wire):
            result = http.client.HTTPResponse(
                SimpleNamespace(makefile=lambda *args, **kwargs: io.BytesIO(wire))
            )
            result.begin()
            return result

        for status in (200, 206):
            with self.subTest(resume_status=status), tempfile.TemporaryDirectory() as directory:
                target = Path(directory) / 'asset.dat'
                partial = target.with_suffix('.dat.partial')
                manifest = {'assets': [{'id': 'fixture', 'kind': 'http_file',
                    'target': '${DATA_ROOT}/asset.dat', 'url': 'https://localhost/asset.dat',
                    'minimum_bytes': 4}]}
                first = response(b'HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabcde')
                body = b'abcdefghij' if status == 200 else b'fghij'
                headers = f'HTTP/1.1 {status} OK\r\nContent-Length: {len(body)}\r\n'
                if status == 206:
                    headers += 'Content-Range: bytes 5-9/10\r\n'
                second = response((headers + '\r\n').encode() + body)
                with mock.patch('src.infrastructure.environment.urllib.request.urlopen',
                                side_effect=[first, second]) as request:
                    with self.assertRaisesRegex(OSError, 'incomplete asset download'):
                        download_asset(manifest, 'fixture', {'DATA_ROOT': directory})
                    self.assertFalse(target.exists())
                    self.assertEqual(partial.read_bytes(), b'abcde')
                    self.assertTrue(first.isclosed())
                    self.assertEqual(download_asset(manifest, 'fixture', {'DATA_ROOT': directory}), target)
                self.assertEqual(target.read_bytes(), b'abcdefghij')
                self.assertFalse(partial.exists())
                self.assertTrue(second.isclosed())
                self.assertIsNone(request.call_args_list[0].args[0].get_header('Range'))
                self.assertEqual(request.call_args_list[1].args[0].get_header('Range'), 'bytes=5-')

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
