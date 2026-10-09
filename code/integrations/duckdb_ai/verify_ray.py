#!/usr/bin/env python3
"""Run two bounded compiled SQL checks through actual Daft/Ray and local HTTP fixtures."""
from __future__ import annotations

import argparse
import os
import sys
import unittest
from pathlib import Path

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))

from tests.semantic_methods.test_duckdb_ai import DuckDBNativeLibraryTests
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.semantic_methods.duckdb_ai import DuckDBNativeTaskExecutor, DuckDBSemLoomBridge


class RayDuckDBChecks(DuckDBNativeLibraryTests):
    address = ''

    def common(self):
        config = FixedModelConfig(self.endpoint, 'fixture', 5000, bearer_token='EMPTY')
        physical = RayMapConfig(self.address, workers=1, batch_rows=2,
                                window_bytes=2 * 1024 * 1024, object_bytes=4 * 1024 * 1024)
        self.events = []
        self.execution = build_native_execution(config, physical=physical, max_tasks=4, max_active_requests=2,
                                               observer=self.events.append)
        self.adapter = DuckDBNativeTaskExecutor(self.execution, config)
        self.bridge = DuckDBSemLoomBridge(__import__('os').environ['DUCKDB_SEMLOOM_EXTENSION'], self.adapter)
        self.bridge.enable(self.connection)

    def tearDown(self):
        super().tearDown()
        self.assertTrue(any(e['event'] == 'ray_block_put' for e in self.events))
        self.assertTrue(any(e['event'] == 'ray_http_completed' for e in self.events))
        self.assertTrue(any(e['event'] == 'ray_transport_closed' and e['confirmed'] for e in self.events))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ray-temp', type=Path, required=True)
    args = parser.parse_args()
    if len(str(args.ray_temp.resolve()).encode()) > 40:
        parser.error('--ray-temp needs a short path for the Ray UNIX sockets')
    if hasattr(os, 'sched_getaffinity'):
        os.sched_setaffinity(0, set(sorted(os.sched_getaffinity(0))[:4]))
    import ray
    context = ray.init(num_cpus=4, num_gpus=0, include_dashboard=False,
                       _temp_dir=str(args.ray_temp.resolve()), object_store_memory=256 * 1024 * 1024)
    RayDuckDBChecks.address = context.address_info['address']
    try:
        suite = unittest.TestSuite(RayDuckDBChecks(name) for name in (
            'test_compiled_batch_bypasses_native_one_worker_and_guard',
            'test_patched_native_and_semloom_use_identical_complete_messages_and_return',
        ))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1
    finally:
        ray.shutdown()


if __name__ == '__main__':
    raise SystemExit(main())
