"""Service barriers are invariant to frame splitting and receipt delivery order."""
import asyncio
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from src.scheduling.core.models import AdmissionObservation
from src.scheduling.submission_control.request_scan import AscendingRequestController
from src.scheduling.submission_control.stage_work import SupplyState, default_request_feedback_options
from tests.experiments.movie_feedback_frontier import EventEngine, ServiceCohorts
from tests.serving.test_async_engine_process import row


class ServiceClockTests(unittest.TestCase):
    def service(self):
        directory = tempfile.TemporaryDirectory(prefix='service-clock-', dir='/tmp')
        self.addCleanup(directory.cleanup)
        service = ServiceCohorts(Path(directory.name)/'events.sock')
        self.addCleanup(service.close)
        return service

    def test_intermediate_frames_and_split_receipts_do_not_create_service_ticks(self):
        reports = []
        for cadence in ('coalesced', 'per_member', 'duplicates_and_split_receipts'):
            with self.subTest(cadence=cadence):
                service = self.service()
                controller = AscendingRequestController(8, replace(default_request_feedback_options(),
                    min_samples=1), clock=lambda: service.now)
                controller.reset(1, 96)
                completed, sequence, receipts = 0, 0, set()
                while completed < 96:
                    quota = controller.current_window
                    group = min(quota, 96-completed)
                    ids = [str(completed+i) for i in range(group)]
                    for request in ids:
                        sequence += 1
                        service.waiting[request] = sequence
                        if cadence != 'coalesced':
                            before = service.now
                            self.assertFalse(service.advance(receipts=receipts,
                                inflight=group, target=group, source_sequence=sequence-1))
                            self.assertEqual(service.now, before)
                    self.assertTrue(service.advance(receipts=receipts,
                        inflight=group, target=group, source_sequence=sequence))
                    observation = AdmissionObservation(service.now, True, group, group, 0, .1,
                        sample_age_s=1., completed_items_total=completed, pending_items=96-completed,
                        completed_input_work_total=11*completed,
                        completed_output_work_total=completed,
                        completed_computed_input_work_total=11*completed,
                        completed_cached_input_work_total=0, preemptions_total=0)
                    controller.update_supply(observation, SupplyState(service.now,
                        96-completed, 96, completed+group, 1,
                        upstream_pending=True, exhausted=False, active_requests=group))
                    service.release()
                    if cadence == 'duplicates_and_split_receipts':
                        for request in ids[:-1]:
                            receipts.add(request)
                            before = service.now
                            self.assertFalse(service.advance(receipts=receipts,
                                inflight=group, target=group, source_sequence=sequence+1))
                            self.assertEqual(service.now, before)
                    receipts.update(ids)
                    completed += group
                report = controller.report()
                self.assertGreater(report['accepted_steps'], 0, report)
                self.assertGreater(report['recommended_window'], 1)
                reports.append({key: report[key] for key in (
                    'accepted_steps', 'recommended_window', 'history', 'evaluations')})
        self.assertEqual(reports[0], reports[1])
        self.assertEqual(reports[0], reports[2])

    def test_new_native_frame_must_cover_actual_ready_ids_not_previous_query_frame(self):
        service = self.service()
        service.waiting = {'new-query-request': 12}
        self.assertFalse(service.advance(receipts=set(), inflight=1, target=1, source_sequence=11))
        self.assertEqual(service.now, 0.)
        self.assertTrue(service.advance(receipts=set(), inflight=1, target=1, source_sequence=12))
        self.assertEqual(service.now, 1.)

    def test_shrink_drains_real_admitted_cohort_and_tail_uses_remaining_work(self):
        service = self.service()
        service.waiting = {str(i): i+1 for i in range(4)}
        self.assertTrue(service.advance(receipts=set(), inflight=4, target=2, source_sequence=4))
        service.release()
        self.assertEqual(service.released, {'0', '1', '2', '3'})
        service.waiting = {'tail': 5}
        self.assertFalse(service.advance(receipts={'0', '1', '2'}, inflight=1, target=1, source_sequence=5))
        self.assertTrue(service.advance(receipts={'0', '1', '2', '3'}, inflight=1, target=1,
            source_sequence=5))
        self.assertEqual(service.history[-1]['requests'], ['tail'])


class ServiceEventTests(unittest.IsolatedAsyncioTestCase):
    async def connect_engine(self):
        directory = tempfile.TemporaryDirectory(prefix='service-event-', dir='/tmp')
        self.addCleanup(directory.cleanup)
        connected = asyncio.get_running_loop().create_future()
        def accept(reader, writer):
            connected.set_result((reader, writer))
        path = str(Path(directory.name)/'events.sock')
        server = await asyncio.start_unix_server(accept, path)
        self.addAsyncCleanup(server.wait_closed)
        self.addCleanup(server.close)
        engine = EventEngine(None, None, limits=None, path=path)
        self.addAsyncCleanup(engine.close)
        reader, writer = await asyncio.wait_for(connected, 2)
        self.addAsyncCleanup(writer.wait_closed)
        self.addCleanup(writer.close)
        return engine, reader, writer

    async def test_control_disconnect_fails_the_waiting_request_and_releases_its_gate(self):
        engine, reader, writer = await self.connect_engine()
        task = asyncio.create_task(engine.generate_prepared(row(), request_id='cpu-1',
            generation=None, deadline_monotonic=None))
        await asyncio.wait_for(reader.readline(), 2)  # Real request reached its gate.
        writer.close()
        await writer.wait_closed()
        with self.assertRaisesRegex(RuntimeError, 'event channel closed'):
            await asyncio.wait_for(task, 2)
        self.assertEqual(engine.gates, {})
        self.assertEqual(engine.running, 0)

    async def test_release_sets_native_service_clock_before_successful_reply(self):
        import json
        engine, reader, writer = await self.connect_engine()
        task = asyncio.create_task(engine.generate_prepared(row(), request_id='cpu-1',
            generation=None, deadline_monotonic=None))
        started = json.loads(await asyncio.wait_for(reader.readline(), 2))
        self.assertEqual(started['request_id'], 'cpu-1')
        self.assertFalse(task.done())
        self.assertEqual(engine.feedback_snapshot()['observed_at_s'], 0.)
        writer.write(b'{"at_s":1.0,"requests":["cpu-1"]}\n')
        await writer.drain()
        value = await asyncio.wait_for(task, 2)
        self.assertEqual((value['status'], value['prompt_computed_tokens'], value['prompt_cached_tokens']),
            ('succeeded', 11, 0))
        native = engine.feedback_snapshot()
        self.assertEqual((native['observed_at_s'], native['finished_total']), (1., 1))
        self.assertEqual(engine.gates, {})

    async def test_close_unblocks_a_ready_request_without_inventing_a_success(self):
        engine, reader, writer = await self.connect_engine()
        task = asyncio.create_task(engine.generate_prepared(row(), request_id='cpu-1',
            generation=None, deadline_monotonic=None))
        await asyncio.wait_for(reader.readline(), 2)
        await asyncio.wait_for(engine.close(), 2)
        with self.assertRaisesRegex(RuntimeError, 'engine closed'):
            await asyncio.wait_for(task, 2)
        self.assertEqual(engine.gates, {})
        self.assertEqual(engine.running, 0)


if __name__ == '__main__':
    unittest.main()
