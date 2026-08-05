from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.infrastructure.runner_lease import (  # noqa: E402
    RunnerOwner,
    acquire_host_runner_lease,
    acquire_runner_lease,
)


class RunnerLeaseTests(unittest.TestCase):
    def test_host_scope_rejects_runner_with_different_output_dir(self) -> None:
        with TemporaryDirectory() as temp_dir:
            artifact_root = Path(temp_dir)
            first = acquire_host_runner_lease(
                artifact_root,
                repository_commit="abc123",
                owner=RunnerOwner("host-a", 11, "start-a", "owner-a"),
                process_alive=lambda pid: pid == 11,
            )
            self.addCleanup(first.release)

            with self.assertRaisesRegex(RuntimeError, "active runner"):
                acquire_host_runner_lease(
                    artifact_root,
                    repository_commit="abc123",
                    owner=RunnerOwner("host-a", 12, "start-b", "owner-b"),
                    process_alive=lambda pid: pid == 11,
                )

    def test_live_owner_rejects_second_runner(self) -> None:
        with TemporaryDirectory() as temp_dir:
            output = Path(temp_dir)
            lease = acquire_runner_lease(
                output,
                config_fingerprint="cfg-a",
                repository_commit="abc123",
                owner=RunnerOwner("host-a", 11, "start-a", "owner-a"),
                process_alive=lambda pid: pid == 11,
            )
            self.addCleanup(lease.release)

            with self.assertRaisesRegex(RuntimeError, "active runner"):
                acquire_runner_lease(
                    output,
                    config_fingerprint="cfg-a",
                    repository_commit="abc123",
                    owner=RunnerOwner("host-a", 12, "start-b", "owner-b"),
                    process_alive=lambda pid: pid == 11,
                )

    def test_stale_owner_requires_explicit_recovery(self) -> None:
        with TemporaryDirectory() as temp_dir:
            output = Path(temp_dir)
            self._write_stale_lease(output, config_fingerprint="cfg-a")

            with self.assertRaisesRegex(RuntimeError, "stale runner lease"):
                acquire_runner_lease(
                    output,
                    config_fingerprint="cfg-a",
                    repository_commit="abc123",
                    owner=RunnerOwner("host-a", 12, "start-b", "owner-b"),
                    process_alive=lambda _pid: False,
                )

            recovered = acquire_runner_lease(
                output,
                config_fingerprint="cfg-a",
                repository_commit="abc123",
                recover_stale=True,
                owner=RunnerOwner("host-a", 12, "start-b", "owner-b"),
                process_alive=lambda _pid: False,
            )
            self.addCleanup(recovered.release)

            self.assertEqual(
                recovered.recovered_owner["owner_token"],
                "owner-a",
            )

    def test_stale_recovery_rejects_config_mismatch(self) -> None:
        with TemporaryDirectory() as temp_dir:
            output = Path(temp_dir)
            self._write_stale_lease(output, config_fingerprint="cfg-a")

            with self.assertRaisesRegex(RuntimeError, "fingerprint"):
                acquire_runner_lease(
                    output,
                    config_fingerprint="cfg-b",
                    repository_commit="def456",
                    recover_stale=True,
                    owner=RunnerOwner("host-a", 12, "start-b", "owner-b"),
                    process_alive=lambda _pid: False,
                )

    @staticmethod
    def _write_stale_lease(
        output: Path,
        *,
        config_fingerprint: str,
    ) -> None:
        stale = {
            "hostname": "host-a",
            "pid": 11,
            "process_start_id": "start-a",
            "owner_token": "owner-a",
            "config_fingerprint": config_fingerprint,
            "repository_commit": "abc123",
            "started_epoch_s": 1.0,
        }
        (output / ".runner-lease.json").write_text(
            json.dumps(stale),
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
