"""Pending metadata reuse over the actual Core; no HTTP or model work."""

from dataclasses import replace
import gc
import json
import unittest
import weakref
from unittest.mock import patch

from src.execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
from src.scheduling.core.session_contract import Usage, TaskProfile, Uncertain
from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.scheduling.core import task_info
from tests.execution_provider.test_native_tasks import fixture


def task(sequence):
    return prepare_native_task(b'body', sequence, row_sequence=sequence,
                               call_id=f'call-{sequence}', max_result_bytes=512)


class TaskInfoReuseTests(unittest.TestCase):
    def setUp(self):
        self.execution, self.backend, _ = fixture(held_tasks=2, result_bytes=512)
        self.flow = NativeTaskSession(self.execution, 'query', 'map')

    def tearDown(self):
        self.flow.close()
        for key in tuple(self.backend.pending):
            self.backend.complete(key)
        self.execution.engine.advance()
        self.assertEqual(self.execution.engine.capacity.usage(), Usage())
        self.assertEqual(self.execution.engine.jobs.jobs, {})

    def test_same_pending_metadata_and_same_value_replacement_serialize_once(self):
        first, pending = task(0), task(1)
        with patch.object(task_info, 'asdict', wraps=task_info.asdict) as encode:
            self.assertEqual(self.flow.offer((first, pending)).accepted_prefix_count, 1)
            for _ in range(10):
                work = replace(pending.info.work,
                               stages=tuple(replace(stage) for stage in pending.info.work.stages))
                copy = replace(pending, info=replace(pending.info, work=work))
                self.assertEqual(self.flow.offer((copy,)).accepted_prefix_count, 0)
            self.assertEqual(encode.call_count, 2)

    def pending(self):
        pending = task(1)
        self.assertEqual(self.flow.offer((task(0), pending)).accepted_prefix_count, 1)
        return pending

    def test_new_invalid_suffix_rejects_even_cached_head_before_any_transfer(self):
        pending = self.pending()
        self.flow.advance(1)
        self.backend.complete(next(iter(self.backend.pending)), encode_full_response(
            FullModelResponse(200, (), b'ok')))
        delivery = self.flow.advance(1).deliveries[0]
        self.flow.release((delivery.lease_id,))
        bad = replace(task(2), info=replace(task(2).info, call_id=''))
        result = self.flow.offer((pending, bad))
        self.assertEqual((result.status, result.accepted_prefix_count), ('REJECTED', 0))
        self.assertEqual(self.execution.engine.capacity.usage(), Usage())
        self.assertEqual(self.flow.session._info_checks.retained_count, 0)

    def test_same_info_object_and_nested_stage_mutations_do_not_reuse_validation(self):
        for member, name, value in (('info', 'call_id', ''), ('stage', 'units', True),
                                    ('work', 'stages', [])):
            with self.subTest(member=member):
                pending = task(1)
                if not self.execution.engine.capacity.records:
                    self.flow.offer((task(0), pending))
                else:
                    self.flow.offer((pending,))
                target = {'info': pending.info, 'stage': pending.info.work.stages[0],
                          'work': pending.info.work}[member]
                before = self.execution.engine.capacity.usage()
                object.__setattr__(target, name, value)
                result = self.flow.offer((pending,))
                self.assertEqual((result.status, result.accepted_prefix_count), ('REJECTED', 0))
                self.assertEqual(self.execution.engine.capacity.usage(), before)
                self.assertEqual(self.flow.session._info_checks.retained_count, 0)

    def test_same_sequence_changed_valid_info_serializes_again(self):
        pending = self.pending()
        with patch.object(task_info, 'asdict', wraps=task_info.asdict) as encode:
            changed = replace(pending, info=replace(pending.info, call_id='replacement'))
            self.assertEqual(self.flow.offer((changed,)).accepted_prefix_count, 0)
            self.assertEqual(encode.call_count, 1)

    def test_float_signed_zero_change_rechecks_the_actual_metadata_size(self):
        first, pending = task(0), task(1)
        first = replace(first, info=replace(first.info, work=replace(first.info.work, deadline_s=0.0)))
        pending = replace(pending, info=replace(pending.info, work=replace(pending.info.work, deadline_s=0.0)))
        size = len(json.dumps(task_info.asdict(pending.info), ensure_ascii=False, allow_nan=False).encode())
        self.flow.session.limits = replace(self.flow.limits, metadata_bytes=size)
        self.assertEqual(self.flow.offer((first, pending)).accepted_prefix_count, 1)
        object.__setattr__(pending.info.work, 'deadline_s', -0.0)
        self.assertEqual(self.flow.offer((pending,)).status, 'REJECTED')

    def test_work_estimate_and_opaque_metadata_are_checked_on_cache_hit(self):
        pending = self.pending()
        with patch.object(task_info, 'asdict', wraps=task_info.asdict) as encode:
            result = self.flow.offer((replace(pending, estimated_work=2),))
            self.assertEqual(result.status, 'REJECTED')
            self.assertEqual(encode.call_count, 0)
        self.flow.offer((pending,))
        result = self.flow.offer((replace(pending, metadata=b'x' * self.flow.limits.metadata_bytes),))
        self.assertEqual(result.status, 'REJECTED')

    def test_resolved_profile_and_changed_unit_and_metadata_limit_are_checked(self):
        pending = self.pending()
        base = self.flow.session.spec
        self.flow.session.spec = replace(base, task_profiles=(TaskProfile('other', 'other-model'),))
        with patch.object(task_info, 'asdict', wraps=task_info.asdict) as encode:
            self.assertEqual(self.flow.offer((replace(pending, profile_name='other'),)).status, 'BACKPRESSURE')
            self.assertEqual(encode.call_count, 1)
        self.assertEqual(self.flow.offer((replace(pending, profile_name='unknown'),)).status, 'REJECTED')
        self.flow.offer((pending,))
        self.flow.session.spec = replace(base, work_unit='tokens')
        self.assertEqual(self.flow.offer((pending,)).status, 'REJECTED')
        self.flow.session.spec = base
        self.flow.offer((pending,))
        self.flow.session.limits = replace(self.flow.limits, metadata_bytes=1)
        self.assertEqual(self.flow.offer((pending,)).status, 'REJECTED')

    def test_current_job_work_budget_is_checked_without_serializing_same_info(self):
        from src.planning.work import StageWork, WorkDescriptor
        pending = prepare_native_task(b'body', 1, row_sequence=1, call_id='call-1',
            work=WorkDescriptor((StageWork('model', 2, 'work_units'),), 'model', 'fixture'), max_result_bytes=512)
        self.assertEqual(self.flow.offer((task(0), pending)).accepted_prefix_count, 1)
        capacity = self.execution.engine.capacity
        budget = capacity.job_limits[self.flow.session.spec.job_id]
        capacity.job_limits[self.flow.session.spec.job_id] = replace(budget, active_work=1)
        with patch.object(task_info, 'asdict', wraps=task_info.asdict) as encode:
            self.assertEqual(self.flow.offer((pending,)).status, 'REJECTED')
            self.assertEqual(encode.call_count, 0)

    def test_result_storage_remains_owned_until_release_then_pending_can_transfer(self):
        pending = self.pending()
        with patch.object(task_info, 'asdict', wraps=task_info.asdict) as encode:
            self.flow.advance(1)
            self.backend.complete(next(iter(self.backend.pending)), encode_full_response(
                FullModelResponse(200, (), b'ok')))
            delivery = self.flow.advance(1).deliveries[0]
            self.assertEqual(self.flow.offer((pending,)).accepted_prefix_count, 0)
            self.flow.release((delivery.lease_id,))
            self.assertEqual(self.flow.offer((pending,)).accepted_prefix_count, 1)
            self.assertEqual(encode.call_count, 0)
            self.assertEqual(self.flow.session._info_checks.retained_count, 0)

    def test_cancel_drops_pending_checks_without_sending_or_refunding_unknown_work(self):
        pending = self.pending()
        self.flow.advance(1)
        self.flow.request_cancel()
        self.assertEqual(self.flow.offer((pending,)).status, 'REJECTED')
        self.assertEqual(self.flow.session._info_checks.retained_count, 0)
        report = self.flow.close()
        self.assertEqual(report.uncertain_requests, 1)
        self.assertEqual(self.execution.engine.capacity.usage().active_requests, 1)
        self.execution.engine.advance()
        self.assertEqual(self.execution.engine.capacity.usage().active_requests, 1)

    def test_unknown_remote_failure_discards_checks_but_preserves_active_reservation(self):
        self.pending()
        self.flow.advance(1)
        key = next(iter(self.backend.pending))
        self.backend.events.append(Uncertain(key, self.backend.pending[key][0]))
        self.flow.advance(1)
        self.assertEqual(self.flow.session._info_checks.retained_count, 0)
        self.flow.close()
        for _ in range(3):
            self.execution.engine.advance()
        self.assertEqual(self.execution.engine.capacity.usage().active_requests, 1)

    def test_close_and_seal_drop_only_pending_checks(self):
        self.pending()
        self.flow.end_input()
        self.assertEqual(self.flow.session._info_checks.retained_count, 0)
        self.assertEqual(self.execution.engine.capacity.usage().held_tasks, 1)
        self.flow.close()
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            self.flow.offer((task(1),))

    def test_cache_keeps_no_task_info_objects_and_only_latest_unaccepted_metadata(self):
        pending = self.pending()
        info = weakref.ref(pending.info)
        work = weakref.ref(pending.info.work)
        stage = weakref.ref(pending.info.work.stages[0])
        del pending
        gc.collect()
        self.assertEqual((info(), work(), stage()), (None, None, None))
        self.assertEqual(self.flow.session._info_checks.retained_count, 1)
        self.flow.offer(())
        self.assertEqual(self.flow.session._info_checks.retained_count, 0)
