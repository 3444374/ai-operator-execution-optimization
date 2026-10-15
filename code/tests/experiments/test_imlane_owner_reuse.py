"""Opt-in original LOTUS/author IMLane owner checks; no model or GPU.

Requires an externally owned, verified Coordinator and reuse_bridge=True config.
Uses the existing production owner and author bridge session without a replacement executor.
"""
from dataclasses import asdict
import json
import os
from pathlib import Path
import sys
import time
import unittest

from src.baselines.common.private_artifacts import write_private_json
from src.baselines.common.redact import redact_json_values
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.postgresql.imlane_query import load_imlane_call_configs
from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup
from src.experiments.shared_request_budget import CellBudgetLedger
from tests.experiments.test_native_adapter_query import fixture_server
from tests.experiments.test_persistent_native_adapter_query import _persistent_lotus_http_fixture


@unittest.skipUnless(sys.platform.startswith('linux') and os.environ.get('SEMLOOM_IMLANE_OWNER_CONFIG'),
                     'requires Linux, fixed SDKs and an explicitly owned real author Coordinator')
class IMLaneOwnerAuthorLibraries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.semantic_methods.lotus.sdk import validate_source
        validate_source()
        value = json.loads(Path(os.environ['SEMLOOM_IMLANE_OWNER_CONFIG']).read_text())
        cls.mode = value['runtime']
        cls.config = load_imlane_call_configs({cls.mode: os.environ['SEMLOOM_IMLANE_OWNER_CONFIG']})[cls.mode]
        if cls.config.reuse_bridge is not True:
            raise ValueError('the explicit owner-reuse probe requires reuse_bridge=True')
        cls.arm = 'lotus-method-native-imlane' + ('-ray' if cls.mode == 'ray' else '')
        cls.root = Path(os.environ['SEMLOOM_IMLANE_OWNER_OUTPUT'])
        cls.root.mkdir(mode=0o700, parents=True, exist_ok=False)
        from tokenizers import Tokenizer, models
        cls.tokenizer = cls.root / 'fixture-tokenizer.json'
        Tokenizer(models.WordLevel({'[UNK]': 0}, unk_token='[UNK]')).save(str(cls.tokenizer))

    def group(self, root, url, posts):
        ledger = CellBudgetLedger.create(root / 'budget.sqlite', AttemptBudget('imlane-owner-' + root.name, posts),
                                        deadline_utc=time.time()+240)
        return PersistentAdapterGroup((self.arm,), plan=SemanticMapPlan('Return ok.', 'fixture', 16),
            model=FixedModelConfig(url, 'fixture', 5000), ledger=ledger, root=root / 'group',
            options=NativeGraphOptions(concurrency=8, num_threads=8), query_timeout_s=20, owner_timeout_s=240,
            tokenizer_path=self.tokenizer, imlane_call_configs={self.mode: self.config},
            lotus_observation_mode='performance')

    def query(self, group, root, name, rows):
        values = tuple(dict(row_id=name+'-'+str(i), text='fixture '+name+' row '+str(i)) for i in range(rows))
        return group.run(self.arm, unit_id=name, root=root / name, load_source=lambda: iter(values),
            phase='qualification', reference_outputs={r['row_id']: 'ok' for r in values}, allowed_outputs=('ok',))

    def test_two_queries_keep_one_original_bridge_context_and_close_it(self):
        from src.baselines.text.frameworks.imlane_calls import process_start_ticks, verify_coordinator
        root = self.root / 'healthy'; root.mkdir()
        records = []
        with _persistent_lotus_http_fixture() as (url, requests, fixture):
            with self.group(root, url, 8) as group:
                owner = group.owners[self.arm]
                graph = owner.imlane_calls
                self.assertIsNotNone(graph)
                lm = owner.lm
                facts = verify_coordinator(self.config, graph._imlane.delegate.config)
                bridge = session = None
                for number in range(2):
                    result = self.query(group, root, 'q'+str(number), 4)
                    self.assertIs(owner.imlane_calls, graph)
                    self.assertIs(owner.lm, lm)
                    self.assertEqual((result['status'], result['actual_posts'], result['quality']['correct']),
                                     ('passed', 4, 4))
                    report = dict(graph.last_close_report)
                    current = graph._imlane.delegate._session
                    if session is None:
                        session, bridge = current, current.process
                        ticks = process_start_ticks(bridge.pid)
                    self.assertIs(current, session)
                    self.assertIs(current.process, bridge)
                    self.assertEqual(process_start_ticks(bridge.pid), ticks)
                    self.assertIsNone(bridge.poll())
                    self.assertEqual(report['bridge_pid'], bridge.pid)
                    self.assertEqual(report['bridge_lifecycle'], 'session')
                    self.assertTrue(report['clean'] and report['reuse_allowed'] and report['author_batches_drained'])
                    self.assertFalse(report['bridge_stopped'] or report['external_coordinator_requires_cleanup'])
                    self.assertEqual(report['bridge_stats']['lifecycle']['context_creations'], int(number == 0))
                    self.assertEqual(report['bridge_stats']['lifecycle']['registrations'], int(number == 0))
                    self.assertEqual(session.batches, number+1)
                    for usage in (lm.stats.physical_usage, lm.stats.virtual_usage):
                        self.assertEqual((usage.prompt_tokens, usage.completion_tokens, usage.total_tokens),
                                         ((number+1)*8, (number+1)*4, (number+1)*12))
                    self.assertEqual((lm.stats.cache_hits, lm.stats.operator_cache_hits), (0, 0))
                    records.append(dict(query='q'+str(number), lifecycle=result['persistent_lifecycle'],
                        bridge_pid=bridge.pid, bridge_start_ticks=ticks, session_root=str(session.root),
                        namespace=graph._imlane.delegate.config['namespace'], report=report, stats=asdict(lm.stats)))
                self.assertNotEqual(records[0]['report']['batch_id'], records[1]['report']['batch_id'])
            self.assertEqual(bridge.returncode, 0)
            self.assertTrue(bridge.stdin.closed and bridge.stdout.closed)
            self.assertFalse(graph._imlane._uncertain.exists())
            self.assertEqual(verify_coordinator(self.config, graph._imlane.delegate.config), facts)
            cleanup = json.loads((root / 'group/group-summary.json').read_text())
            self.assertEqual(cleanup['status'], 'passed')
            self.assertFalse(cleanup['cleanup_errors'])
            self.assertEqual((len(requests), fixture['active'], fixture['errors']), (8, 0, []))
        self.assertTrue(fixture['closed'])
        write_private_json(root / 'fixture.json', redact_json_values(dict(passed=True, runtime=self.mode,
            actual_posts=8, real_model_requests=0, queries=records, group_cleanup=cleanup,
            bridge_returncode=bridge.returncode, external_coordinator_still_owned=True,
            author_context_identity='same bridge/session with reported creations 1 then 0; no invented context address',
            scope='real fixed-SDK LOTUS sem_map and real configured author DBEnd; author final exit verified by caller scope')))

    def test_http_429_poisons_group_and_stops_the_next_query_without_new_posts(self):
        root = self.root / 'http-error'; root.mkdir()
        with fixture_server(http_status=429) as (url, requests):
            with self.group(root, url, 2) as group:
                owner = group.owners[self.arm]
                graph = owner.imlane_calls
                from litellm.exceptions import RateLimitError
                with self.assertRaises(RateLimitError) as caught:
                    self.query(group, root, 'error', 2)
                self.assertEqual(caught.exception.status_code, 429)
                self.assertTrue(group.poisoned)
                self.assertEqual(len(requests), 2)
                failed_count = len(requests)
                report = dict(graph.last_close_report)
                self.assertTrue(report['clean'] and report['author_batches_drained'])
                self.assertFalse(report['external_coordinator_requires_cleanup'] or report['unconfirmed_work'])
                self.assertFalse(graph._imlane._uncertain.exists())
                bridge = graph._imlane.delegate._session.process
                with self.assertRaisesRegex(RuntimeError, 'stopped'):
                    self.query(group, root, 'forbidden', 1)
                self.assertEqual(len(requests), failed_count)
                self.assertIs(owner.imlane_calls, graph)
            self.assertEqual(bridge.returncode, 0)
            cleanup = json.loads((root / 'group/group-summary.json').read_text())
            self.assertEqual(cleanup['status'], 'failed')
            self.assertFalse(cleanup['cleanup_errors'])
        write_private_json(root / 'fixture.json', redact_json_values(dict(passed=True, expected_http_error=429,
            runtime=self.mode, actual_posts=2, real_model_requests=0, next_query_posts=0,
            original_sdk_error=type(caught.exception).__name__, graph_report=report,
            group_cleanup=cleanup, bridge_returncode=bridge.returncode,
            scope='complete HTTP error stays a value until original SDK; failed group never recovers automatically')))


if __name__ == '__main__':
    unittest.main()
