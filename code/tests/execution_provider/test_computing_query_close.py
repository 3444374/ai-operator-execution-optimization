"""Keep a computing CPU child charged until its owner supplies cleanup evidence."""
from copy import deepcopy
import json
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.semantic_methods.lotus import record_chain as chains
from tests.execution_provider import test_lotus_record_local_priority as priorities
from tests.execution_provider import test_lotus_record_local_query as queries
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.semantic_methods import test_lotus_chain_composition as plans


class ComputingQueryCloseTests(unittest.TestCase):
    setUp = queries.QueryOwnerTests.setUp
    config_for = queries.QueryOwnerTests.config_for
    events = queries.QueryOwnerTests.events
    owner = priorities.QueryPriorityTests.owner
    priority = priorities.QueryPriorityTests.priority

    def _check_outcome(self, outcome):
        for order in ('fifo', 'successor_first'):
            with self.subTest(order=order, outcome=outcome):
                plan, rows = plans.chain_for('MF'), plans.source_rows(size=1)
                completed = self.config.parent / ('complete-' + outcome + '-' + order)
                config = self.config_for(plan, rows)
                config['responses']['step_0|occ-0'].update(wait_file=str(completed), status=outcome)
                self.config.write_text(json.dumps(config))
                original_close = AsyncCoreModelActor.close
                original_abort = AsyncCoreModelActor.abort_core
                close_entered, abort_entered = threading.Event(), threading.Event()
                release, expired = threading.Event(), threading.Event()
                parsed = []
                interpret = chains.LotusRecordMethod.interpret_results

                def record_parse(method, results):
                    parsed.append(method.stage.stage_id)
                    return interpret(method, results)

                def hold_control(entered, original, actor, *args):
                    entered.set()
                    if not release.wait(5):
                        expired.set()
                        raise TimeoutError('CPU test did not release the control reply')
                    return original(actor, *args)

                def held_close(actor, timeout):
                    return hold_control(close_entered, original_close, actor, timeout)

                def held_abort(actor, key):
                    return hold_control(abort_entered, original_abort, actor, key)

                offset = len(self.events())
                options = {} if order == 'fifo' else self.priority()
                with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=queries.Process), \
                        patch.object(AsyncCoreModelActor, 'close', held_close), \
                        patch.object(AsyncCoreModelActor, 'abort_core', held_abort), \
                        patch.object(chains.LotusRecordMethod, 'interpret_results', record_parse):
                    owner = self.owner(plan, C=1, **options)
                    stream = owner.evaluate(iter(rows), plan, expected=rows, query_id=plan.query_id)
                    try:
                        until = time.monotonic() + 4
                        while not any(event['event'] == 'start' for event in self.events()[offset:]):
                            stream._tick()
                            self.assertLess(time.monotonic(), until, 'CPU child did not begin its held call')
                        self.assertFalse(any(event['event'] == 'end' for event in self.events()[offset:]))
                        held = owner.core.capacity.usage()
                        self.assertEqual((held.held_tasks, held.active_requests), (1, 1))
                        self.assertGreater(held.input_bytes, 0)
                        self.assertGreater(held.result_bytes, 0)
                        first = deepcopy(owner.close(timeout=.02))
                        self.assertEqual(first['status'], 'unknown')
                        self.assertEqual(first['core_usage']['held_tasks'], 1)
                        self.assertEqual(first['core_usage']['active_requests'], 1)
                        self.assertEqual(first['core_usage']['input_bytes'], held.input_bytes)
                        self.assertEqual(first['core_usage']['result_bytes'], held.result_bytes)
                        self.assertFalse(first['query_completed'])
                        self.assertFalse(any(event['event'] == 'end' for event in self.events()[offset:]))
                        self.assertEqual(parsed, [])

                        completed.touch()
                        until = time.monotonic() + 4
                        while not any(event['event'] == 'end' for event in self.events()[offset:]):
                            self.assertLess(time.monotonic(), until, 'CPU child did not produce its declared outcome')
                            time.sleep(.002)
                        release.set()
                        final = deepcopy(owner.close(timeout=3))
                        self.assertTrue(close_entered.is_set())
                        self.assertTrue(abort_entered.is_set())
                        self.assertFalse(expired.is_set())
                        self.assertEqual(final['status'], 'closed', final)
                        self.assertFalse(any(final['core_usage'].values()))
                        self.assertEqual(final['retained_remote_inputs'], 0)
                        self.assertTrue(final['producer_retirement']['confirmed'])
                        self.assertTrue(final['model_owner']['engine_close_report']['ipc_close']['process_exit_confirmed'])
                        self.assertEqual([(call['status'], call['total_tokens']) for call in owner.calls], [(outcome, 14)])
                        self.assertFalse(final['query_completed'])
                        self.assertEqual(stream.run.position, 0)
                        self.assertEqual(stream.run.stats, {})
                        self.assertEqual(parsed, [])
                        self.assertEqual(stream.pool.used, (0, 0))
                        events = self.events()[offset:]
                        self.assertEqual(sum(event['event'] == 'construct' for event in events), 1)
                        self.assertEqual(sum(event['event'] == 'start' for event in events), 1)
                        self.assertEqual(sum(event['event'] == 'end' for event in events), 1)
                        self.assertEqual(owner.close(timeout=3), final)
                        self.assertEqual(len(owner.calls), 1)
                    finally:
                        completed.touch()
                        release.set()
                        owner.close(timeout=3)

    def test_close_collects_actual_success_after_the_computing_child_stops(self):
        self._check_outcome('succeeded')

    def test_close_collects_actual_failure_after_the_computing_child_stops(self):
        self._check_outcome('failed')

    def test_close_keeps_unknown_outcome_after_the_computing_producer_stops(self):
        self._check_outcome('unknown')
