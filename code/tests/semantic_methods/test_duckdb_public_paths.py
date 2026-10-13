"""Opt-in public persistent CLI checks with real SQL and deterministic HTTP responses."""
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest

from src.baselines.common.redact import redact_text
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.baselines.text.frameworks.semantic_map import application_prompt
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from tests.semantic_methods.test_duckdb_ai import _FixtureServer


ARMS = (
    'duckdb-adapted-native', 'duckdb-method-native-daft', 'duckdb-method-native-ray-data',
    'duckdb-method-semloom', 'duckdb-method-native-imlane', 'duckdb-method-native-imlane-ray',
)


@unittest.skipUnless(os.environ.get('DUCKDB_PUBLIC_ARM'),
                     'requires a pinned runtime, extension and an explicit public DuckDB arm')
class DuckDBPublicPaths(unittest.TestCase):
    def test_cli_reuses_connection_and_preserves_changed_duplicate_inputs(self):
        arm = os.environ['DUCKDB_PUBLIC_ARM']
        self.assertIn(arm, ARMS)
        root = Path(os.environ['DUCKDB_PUBLIC_OUTPUT'])
        root.mkdir(parents=True, exist_ok=False)
        instruction = 'Echo the complete input.'
        schedule, expected_payloads = [], []
        for number, count in enumerate((8, 8, 129)):
            values = [dict(row_id=f'query-{number}-row-{i}',
                           text=f'query-{number}-' + ('duplicate 中文\nline' if i % 3 == 0 else str(i)))
                      for i in range(count)]
            source, references = root / f'input-{number}.jsonl', root / f'references-{number}.json'
            source.write_text(''.join(json.dumps(v, ensure_ascii=False) + '\n' for v in values))
            references.write_text(json.dumps({v['row_id']: 'out:' + application_prompt(instruction, v['text'])
                                               for v in values}, ensure_ascii=False))
            schedule.append(dict(arm=arm, unit_id=f'public-{number}', input=str(source),
                                 references=str(references), phase=('qualification', 'warmup', 'measurement')[number]))
            expected_payloads.extend(application_prompt(instruction, v['text']) for v in values)
        (root / 'schedule.json').write_text(json.dumps(schedule))
        (root / 'plan.json').write_text(json.dumps(dict(instruction=instruction, model_id='fixture', max_tokens=16)))
        concurrency = 8 if arm.startswith('duckdb-method-native-imlane') else 4
        (root / 'options.json').write_text(json.dumps(asdict(NativeGraphOptions(concurrency=concurrency, num_threads=4))))
        CellBudgetLedger.create(root / 'budget.sqlite', AttemptBudget('duckdb-public-fixture', 145),
                                deadline_utc=time.time() + 360)
        server = _FixtureServer()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        verification = None
        try:
            endpoint = f'http://127.0.0.1:{server.server_port}/v1/chat/completions'
            (root / 'model.json').write_text(json.dumps(dict(endpoint_url=endpoint, model_id='fixture', timeout_ms=5000)))
            script = Path(__file__).resolve().parents[2] / 'scripts/baselines/run_persistent_native_adapter_query.py'
            command = [sys.executable, '-B', str(script), '--schedule', str(root / 'schedule.json'),
                       '--plan', str(root / 'plan.json'), '--model', str(root / 'model.json'),
                       '--options', str(root / 'options.json'), '--budget', str(root / 'budget.sqlite'),
                       '--budget-id', 'duckdb-public-fixture', '--max-attempts', '145',
                       '--output', str(root / 'group'), '--duckdb-library', os.environ['DUCKDB_SEMLOOM_EXTENSION'],
                       '--query-timeout-s', '60', '--owner-timeout-s', '300']
            if arm in ('duckdb-method-native-ray-data', 'duckdb-method-semloom'):
                command += ['--ray-address', os.environ['DUCKDB_PUBLIC_RAY_ADDRESS']]
            if arm == 'duckdb-method-semloom':
                (root / 'physical.json').write_text(json.dumps(asdict(RayMapConfig(
                    os.environ['DUCKDB_PUBLIC_RAY_ADDRESS'], 2, 2, 2**21 + 24, 2**23))))
                command += ['--ray-physical', str(root / 'physical.json')]
            if arm in ('duckdb-method-native-imlane', 'duckdb-method-native-imlane-ray'):
                mode = 'ray' if arm.endswith('-ray') else 'cpython'
                command += [f'--imlane-{mode}-config', os.environ['DUCKDB_PUBLIC_IMLANE_CONFIG']]
            try:
                result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                        timeout=330, env=dict(os.environ, CUDA_VISIBLE_DEVICES=''))
            except subprocess.TimeoutExpired as error:
                output = error.stdout or ''
                if isinstance(output, bytes):
                    output = output.decode('utf-8', errors='replace')
                (root / 'cli.log').write_text(redact_text(output))
                raise
            (root / 'cli.log').write_text(redact_text(result.stdout))
            self.assertEqual(result.returncode, 0, redact_text(result.stdout[-8000:]))
            summaries = [json.loads((root / f'group/query-{i}/summary.json').read_text()) for i in range(3)]
            lifecycles = [json.loads((root / f'group/query-{i}/persistent-query.json').read_text())['persistent_lifecycle']
                          for i in range(3)]
            for summary, count in zip(summaries, (8, 8, 129)):
                self.assertEqual(summary['status'], 'passed')
                self.assertEqual(summary['rows'], count)
                self.assertEqual(summary['actual_posts'], count)
                self.assertEqual(summary['preparation_model_posts'], 0)
                self.assertEqual(summary['quality']['correct'], count)
                self.assertEqual(summary['identity']['original_method_preserved'], True)
            for key in ('owner_id', 'duckdb_connection_id', 'execution_id', 'ray_session_id'):
                self.assertEqual(len({v[key] for v in lifecycles}), 1, key)
            for lifecycle in lifecycles:
                for key in ('owner_id', 'duckdb_connection_id'):
                    self.assertIsInstance(lifecycle[key], str)
                    self.assertTrue(lifecycle[key], key)
                if arm == 'duckdb-method-semloom':
                    self.assertIsInstance(lifecycle['execution_id'], str)
                    self.assertTrue(lifecycle['execution_id'])
                else:
                    self.assertIsNone(lifecycle['execution_id'])
                if arm in ('duckdb-method-native-ray-data', 'duckdb-method-semloom'):
                    self.assertIsInstance(lifecycle['ray_session_id'], str)
                    self.assertTrue(lifecycle['ray_session_id'])
                else:
                    self.assertIsNone(lifecycle['ray_session_id'])
                self.assertFalse(lifecycle['result_cache'])
            self.assertEqual([v['completed_queries'] for v in lifecycles], [1, 2, 3])
            with server.lock:
                records = list(server.records)
            prompts = [json.loads(body)['messages'][-1]['content'] for body, _ in records]
            self.assertCountEqual(prompts, expected_payloads)
            self.assertEqual(len(records), 145)
            group = json.loads((root / 'group/group-summary.json').read_text())
            self.assertEqual(group['status'], 'passed')
            verification = dict(
                arm=arm, query_rows=[8, 8, 129], fixture_posts=145, real_model_posts=0, gpu_resources=0,
                public_entry=str(script.name), reused_connection=True, changed_inputs_verified=True,
                fixture_body_sha256=[hashlib.sha256(body).hexdigest() for body, _ in records],
                evidence_scope='real libraries and public SQL/CLI behavior; model quality and performance untested',
                caller_owned_imlane_lifecycle=arm.startswith('duckdb-method-native-imlane'),
            )
        finally:
            server.shutdown()
            thread.join(timeout=2)
            deadline = time.monotonic() + 5
            while True:
                with server.lock:
                    active = server.active
                if active == 0 or time.monotonic() >= deadline:
                    break
                time.sleep(0.01)
            server.server_close()
            cleanup = dict(fixture_thread_stopped=not thread.is_alive(), final_active=active)
            (root / 'fixture-cleanup.json').write_text(json.dumps(cleanup, indent=2) + '\n')
        self.assertTrue(cleanup['fixture_thread_stopped'])
        self.assertEqual(cleanup['final_active'], 0)
        (root / 'public-verification.json').write_text(json.dumps(
            dict(verification, cleanup=cleanup), indent=2) + '\n')


if __name__ == '__main__':
    unittest.main()
