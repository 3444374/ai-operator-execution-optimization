"""Actual Q9 Arrow -> maintained provider -> Unix IPC -> software engine,256 rows."""
import asyncio
import hashlib
import json
import os
import sys
import time
import unittest
from pathlib import Path
import pyarrow as pa
from unittest.mock import patch
from tests.baselines.text.test_imlane_duckdb_model_ipc_worker import ModelWorkerTests
from integrations.imlane_duckdb import model_provider
from integrations.imlane_duckdb import q9_udf


class Q9IPC(ModelWorkerTests):

    async def test_full_q9_arrow_ipc(self):
        root = Path(os.environ['SEMLOOM_Q9_FIXTURE'])
        unit = self.bound_budget(256)
        await self.start()
        self.engine.release.set()
        self.engine.texts = {i: str(i % 5 + 1) for i in range(256)}
        prepared = json.loads((root / 'udf-test-prepared.json').read_text())
        rows = prepared['rows']
        calls = prepared['calls']
        messages = [json.loads(c['messages_json']) for c in calls]
        path = self.directory / 'messages.json'
        envelope = {
            row['occurrence_id']: dict(row, messages=message)
            for row, message in zip(rows, messages, strict=True)
        }
        path.write_text(json.dumps(dict(rows=envelope)))
        cfg = self.directory / 'udf.json'
        cfg.write_text(json.dumps(dict(
            provider_path=model_provider.__file__,
            provider_function='infer_batch',
            provider_mode='software_ipc',
            receipt_dir=str(self.directory),
            messages_path=str(path),
            messages_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        )))
        provider = self.directory / 'provider.json'
        sampling = json.loads((root / 'CONFIG.json').read_text())['sampling_params']
        provider.write_text(json.dumps(dict(
            schema=model_provider.CONFIG_SCHEMA,
            socket_path=self.config['socket_path'],
            model='fixture',
            sampling_params=sampling,
            request_timeout_s=20,
            deadline_monotonic=time.monotonic() + 20,
            receipt_dir=str(self.directory),
            provider_mode='software_ipc',
        )))
        columns = ('occurrence_id', 'id', 'reviewId', 'reviewText')
        table = pa.table({
            f'c{i}': [row[column] for row in rows]
            for i, column in enumerate(columns)
        })
        with patch.dict(
            os.environ,
            SEMLOOM_IMLANE_Q9_CONFIG=str(cfg),
            SEMLOOM_IMLANE_Q3_MODEL_CONFIG=str(provider),
        ):
            output = await asyncio.to_thread(q9_udf.infer_q9, table)
        self.assertEqual(output.column(0).to_pylist(), [float(i % 5 + 1) for i in range(256)])
        self.assertEqual([item[0][0] for item in self.engine.renderer.calls], messages)
        self.assertEqual(unit.attempts, 256)
        self.assertEqual(len(self.engine.calls), 256)
        overflow = await self.call(self.request(1))
        self.assertEqual(overflow['calls'][0]['error_type'], 'BudgetExhausted')
        self.assertFalse(overflow['calls'][0]['attempted'])
        self.assertEqual(len(self.engine.calls), 256)
        self.assertEqual(len({r['reviewId'] for r in rows}), 128)
        print(json.dumps(dict(status='passed',
            actual_q9_arrow=True,
            actual_model_provider=True,
            real_unix_ipc=True,
            software_engine_generates=256,
            permanent_budget_attempts=256,
            original_messages=256,
            business_ids=128,
            models=0,
            token_scope='separate exact-token test; software renderer token IDs here'),
            indent=2))


def main():
    suite = unittest.TestSuite([Q9IPC('test_full_q9_arrow_ipc')])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())

if __name__ == '__main__':
    main()
