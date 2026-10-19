"""Installed vLLM source identity checks used by native readiness."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from src.experiments.saor.vllm_0251_source_audit import audit_installed_vllm_0251


class VllmInstalledSourceAuditTest(unittest.TestCase):
    @staticmethod
    def _source_identity(root: Path) -> dict[str, object]:
        source_fields = {
            "config/scheduler.py": "vllm_source_config_scheduler_sha256",
            "v1/core/sched/scheduler.py": "vllm_source_scheduler_sha256",
            "v1/core/sched/async_scheduler.py": (
                "vllm_source_async_scheduler_sha256"
            ),
            "v1/core/sched/request_queue.py": (
                "vllm_source_request_queue_sha256"
            ),
            "v1/request.py": "vllm_source_request_sha256",
        }
        identity = {
            field: hashlib.sha256((root / relative).read_bytes()).hexdigest()
            for relative, field in source_fields.items()
        }
        dist = root.parent / "vllm-0.25.1.dist-info"
        identity.update({
            "vllm_metadata_sha256": hashlib.sha256(
                (dist / "METADATA").read_bytes()
            ).hexdigest(),
            "vllm_wheel_sha256": hashlib.sha256(
                (dist / "WHEEL").read_bytes()
            ).hexdigest(),
            "vllm_record_sha256": hashlib.sha256(
                (dist / "RECORD").read_bytes()
            ).hexdigest(),
        })
        return identity


    @staticmethod
    def _installed_source(root: Path) -> None:
        markers = {
            "config/scheduler.py": (
                "scheduler_cls AsyncScheduler "
                'Literal["fcfs", "priority"]'
            ),
            "v1/core/sched/scheduler.py": (
                "class Scheduler create_request_queue self.waiting"
            ),
            "v1/core/sched/async_scheduler.py": "class AsyncScheduler",
            "v1/core/sched/request_queue.py": (
                "class FCFSRequestQueue class SchedulingPolicy"
            ),
            "v1/request.py": "request_id client_index trace_headers",
        }
        for relative, content in markers.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        dist = root.parent / "vllm-0.25.1.dist-info"
        dist.mkdir()
        for name in ("METADATA", "WHEEL", "RECORD"):
            (dist / name).write_text(name, encoding="utf-8")


    def test_installed_source_never_passes_without_frozen_expected_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "site-packages/vllm"
            self._installed_source(root)
            audit = audit_installed_vllm_0251(
                package_root=root,
                installed_version="0.25.1",
            )
        self.assertEqual(audit["status"], "blocked_expected_identity_missing")
        self.assertTrue(audit["errors"])


    def test_installed_source_passes_only_exact_frozen_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "site-packages/vllm"
            self._installed_source(root)
            expected = self._source_identity(root)
            audit = audit_installed_vllm_0251(
                expected,
                package_root=root,
                installed_version="0.25.1",
            )
            self.assertEqual(audit["status"], "passed")
            (root / "v1/request.py").write_text(
                "request_id client_index trace_headers drift",
                encoding="utf-8",
            )
            drift = audit_installed_vllm_0251(
                expected,
                package_root=root,
                installed_version="0.25.1",
            )
        self.assertEqual(drift["status"], "blocked_source_drift")
        self.assertTrue(drift["errors"])


    def test_current_machine_installed_source_audit_fails_closed(self) -> None:
        audit = audit_installed_vllm_0251()
        self.assertIn(audit["status"], {
            "blocked_runtime_not_installed",
            "blocked_expected_identity_missing",
        })
        self.assertTrue(audit["errors"])


if __name__ == "__main__":
    unittest.main()
