"""CPU-only event coordination, with the existing Core/actor/IPC fixtures."""
from contextlib import contextmanager
from dataclasses import asdict, replace
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters import lotus_chain_actor_backend as chain_backend
from src.execution_provider.adapters import records_query_runtime as query_runtime
from src.execution_provider.adapters import lotus_records_batches as ordinary_module
from src.execution_provider.adapters import lotus_chain_token_batches as packing
from src.semantic_methods.driver import MethodDriver
from src.scheduling.core.session_contract import Acceptance, BatchSubmission
from src.scheduling.runtime.async_backend import BoundedAsyncBackend
from tests.execution_provider import test_ready_state_preparation as state
from tests.execution_provider import test_lotus_async_token_batches as base


@contextmanager
def ordinary_event_optin():
    original = ordinary_module.PreparedRecordsCore
    created = []
    def construct(*args,**kwargs):
        if kwargs.get('interleaved_preparation'):
            kwargs['event_driven_preparation'] = True
        instance = original(*args,**kwargs)
        created.append(instance)
        return instance
    with patch.object(ordinary_module,'PreparedRecordsCore',construct):
        yield created


@contextmanager
def first_actor_ack_gate():
    """Hold one real fixture RPC reply; this is not a model-completion gate."""
    entered,release=threading.Event(),threading.Event()
    call=base.AsyncCoreModelActor.try_submit_prepared_batch
    first=[];lock=threading.Lock()
    def held(actor,payload):
        response=call(actor,payload)
        with lock:
            hold=not first
            if hold:first.append(True)
        if hold:
            entered.set()
            if not release.wait(5):
                raise TimeoutError('CPU fixture did not release actor ACK')
        return response
    with patch.object(base.AsyncCoreModelActor,'try_submit_prepared_batch',held):
        try:yield entered,release
        finally:release.set()


class EventPreparationTests(state.StatePreparationTests):
    def owner(self,plan,**options):
        enabled = options.pop('event_driven_preparation',True)
        owner = super().owner(plan,**options)
        owner._event_driven_preparation = enabled and owner._interleaved_preparation
        return owner

    def test_prevalidated_ordinary_entry_enables_candidate_and_delivers_all_receipts(self):
        with ordinary_event_optin() as created:
            super().test_prevalidated_ordinary_entry_enables_candidate_and_delivers_all_receipts()
        ordinary = created[-1]
        self.assertTrue(ordinary.event_driven_preparation)
        self.assertTrue(ordinary.owner.last_close_report['async_token_batches']['event_driven_preparation'])

    def test_ordinary_source_still_checks_late_illegal_payload_before_creating_owner(self):
        with ordinary_event_optin() as created:
            super().test_ordinary_source_still_checks_late_illegal_payload_before_creating_owner()
        self.assertTrue(created[-1].event_driven_preparation)
        self.assertIsNone(created[-1].owner)

    def test_event_flag_requires_boolean_and_the_existing_ordinary_optin(self):
        method = base.fixture.chain_for('F').stages[0]
        for enabled in (None,1,'yes'):
            with self.subTest(enabled=enabled),self.assertRaises(TypeError):
                ordinary_module.PreparedRecordsCore(method.method,None,method.generation,None,{},
                    dict(rolling_refill=True),None,interleaved_preparation=True,
                    event_driven_preparation=enabled)
        with self.assertRaises(ValueError):
            ordinary_module.PreparedRecordsCore(method.method,None,method.generation,None,{},
                dict(rolling_refill=True),None,event_driven_preparation=True)
        with self.assertRaises(TypeError):
            base.AsyncTokenBatchTests.owner(self,base.fixture.chain_for('M'),event_driven_preparation=True)

    def test_self_offers_do_not_poll_Core_or_rescan_an_unchanged_partial_prefix(self):
        plan,rows = base.fixture.chain_for('M'),base.fixture.source_rows(size=22)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan,rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]),patch.object(base.AsyncCoreModelActor,'_process_type',
                return_value=base.query_fixture.Process),first_actor_ack_gate() as (entered,ack):
            owner = self.owner(plan,R=11)
            stream = owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
            try:
                self.tick_until(stream,lambda: owner.core.capacity.usage().active_requests==1)
                self.assertTrue(entered.wait(2))
                backend = owner.transport.backend
                version,wake = backend.progress().generation,owner.core.wake.generation
                before = stream._phase_count
                # Hold the existing deadline at its prior Core check. No new
                # scheduler timeout or preparation count is introduced.
                clock = stream._core_due_at-owner.capacity.poll_interval_s
                with patch.object(query_runtime,'time',SimpleNamespace(monotonic=lambda:clock)), \
                        patch.object(stream.driver,'advance',wraps=stream.driver.advance) as advance, \
                        patch.object(packing._PreparedPrefix,'consider',autospec=True,
                            side_effect=packing._PreparedPrefix.consider) as consider:
                    while stream.driver.active_rows < owner.resources.max_active_batches-1:
                        stream._tick()
                    self.assertEqual(advance.call_count,0)
                    self.assertEqual(consider.call_count,stream._phase_count-before)
                self.assertEqual(backend.progress().generation,version)
                self.assertGreater(owner.core.wake.generation,wake)
                self.assertLessEqual(len(owner._token_organizer._estimates),11)
                ack.set()
                gate.touch()
                self.assertEqual(len(list(stream)),22)
            finally:
                ack.set();gate.touch();owner.close(timeout=3)

    def test_partial_ready_uses_other_RPC_permits_while_first_ack_is_delayed(self):
        for workers in (2,4):
            with self.subTest(workers=workers):
                plan,rows=base.fixture.chain_for('M'),base.fixture.source_rows(size=22)
                self.helper.config_for(plan,rows)
                packets,entries=[],[]
                submit=chain_backend._SectionTransport.try_submit_batch
                def packet(transport,tasks,endpoint):
                    packets.append((transport.owner._active._phase_count,len(tasks)))
                    return submit(transport,tasks,endpoint)
                with base.ThreadedRuntime([]),patch.object(base.AsyncCoreModelActor,'_process_type',
                        return_value=base.query_fixture.Process),first_actor_ack_gate() as (entered,ack), \
                        patch.object(chain_backend._SectionTransport,'try_submit_batch',packet):
                    actor_submit=base.AsyncCoreModelActor.try_submit_prepared_batch
                    def record(actor,payload):
                        entries.append(True)
                        return actor_submit(actor,payload)
                    owner=self.owner(plan,R=11)
                    # Explicit fixture deployment widths, not preparation quotas.
                    owner.resources=replace(owner.resources,model_rpc_workers=workers)
                    owner._actors.resources=replace(owner._actors.resources,model_rpc_workers=workers)
                    owner._actors.rpc_workers=workers
                    with patch.object(base.AsyncCoreModelActor,'try_submit_prepared_batch',record):
                        stream=owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
                        try:
                            self.tick_until(stream,lambda:len(entries)>1)
                            # RPC wrapper entry precedes the actual held reply.
                            self.assertTrue(entered.wait(2))
                            self.assertFalse(ack.is_set())
                            self.assertLess(packets[1][0],11)
                            ack.set()
                            self.assertEqual(len(list(stream)),22)
                            self.assertEqual(len({c['call_id'] for c in owner.calls}),22)
                        finally:
                            ack.set();owner.close(timeout=3)

    def test_live_compute_accumulates_partial_ready_until_real_resident_capacity(self):
        # The old name remains an inherited test seam; event mode now uses a
        # real handoff opportunity before reaching the resident bound.
        plan,rows=base.fixture.chain_for('M'),base.fixture.source_rows(size=22)
        gate=Path(str(self.helper.config)+'.release')
        config=self.helper.config_for(plan,rows)
        config['responses']['step_0|occ-0']['wait_file']=str(gate)
        self.helper.config.write_text(json.dumps(config))
        packets=[]
        submit=chain_backend._SectionTransport.try_submit_batch
        def observed(transport,tasks,endpoint):
            packets.append((transport.owner._active._phase_count,tuple(t.key.sequence for t in tasks)))
            return submit(transport,tasks,endpoint)
        with base.ThreadedRuntime([]),patch.object(base.AsyncCoreModelActor,'_process_type',
                return_value=base.query_fixture.Process),first_actor_ack_gate() as (entered,ack), \
                patch.object(chain_backend._SectionTransport,'try_submit_batch',observed):
            owner=self.owner(plan,R=11)
            stream=owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
            try:
                self.tick_until(stream,lambda:owner.core.capacity.usage().active_requests==1)
                self.assertTrue(entered.wait(2))
                clock=stream._core_due_at-owner.capacity.poll_interval_s
                with patch.object(query_runtime,'time',SimpleNamespace(monotonic=lambda:clock)):
                    while stream._phase_count<5:stream._tick()
                    self.assertEqual(len(packets),1)
                    accepted=threading.Event()
                    count=owner.transport._count_batch_ack
                    def settled(status,*args,**kwargs):
                        count(status,*args,**kwargs)
                        if status=='accepted':accepted.set()
                    with patch.object(owner.transport,'_count_batch_ack',settled):
                        ack.set()
                        self.assertTrue(accepted.wait(2))
                        # A result-poll RPC can briefly own the same permit.
                        # Keep this ready frontier fixed until that real slot
                        # becomes available, rather than preparing to R.
                        with patch.object(stream,'_fill',return_value=None):
                            self.tick_until(stream,lambda:len(packets)>1)
                    self.assertEqual(len(packets),2)
                self.assertLess(packets[1][0],11)
                self.assertGreater(len(packets[1][1]),1)
                self.assertFalse(any(e['event']=='end' and e.get('token')=='step_0|occ-0'
                                     for e in self.helper.events()))
                gate.touch()
                self.assertEqual(len(list(stream)),22)
            finally:
                ack.set();gate.touch();owner.close(timeout=3)

    def test_slow_method_consumer_retains_R_and_each_tick_still_advances_Core(self):
        # Override ed1's expectation of a full advance on every checkpoint.
        plan,rows = base.fixture.chain_for('M'),base.fixture.source_rows(size=7)
        self.helper.config_for(plan,rows)
        results,advance = MethodDriver.results,MethodDriver.advance
        hold,calls = [True],[]
        def delayed(driver,maximum=1):
            return () if hold[0] else results(driver,maximum)
        def observed(driver,maximum=1,**kwargs):
            calls.append(driver.active_rows)
            return advance(driver,maximum,**kwargs)
        with base.ThreadedRuntime([]),patch.object(base.AsyncCoreModelActor,'_process_type',
                return_value=base.query_fixture.Process),patch.object(MethodDriver,'results',delayed), \
                patch.object(MethodDriver,'advance',observed):
            owner = self.owner(plan,R=3)
            stream = owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
            try:
                self.tick_until(stream,lambda:len(owner.calls)==3 and owner.core.capacity.usage().active_requests==0)
                before = len(calls)
                for _ in range(4):
                    stream._tick()
                self.assertLessEqual(len(calls)-before,1)
                self.assertEqual(stream.driver.active_rows,3)
                self.assertEqual(stream._phase_count,3)
                hold[0]=False
                self.assertEqual(len(list(stream)),7)
                self.assertEqual(stream.pool.used,(0,0))
            finally:
                hold[0]=False;owner.close(timeout=3)

    def test_peak_observation_waits_for_progress_but_close_keeps_queued_reservations(self):
        plan,rows = base.fixture.chain_for('M'),base.fixture.source_rows(size=22)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan,rows)
        config['responses']['step_0|occ-0']['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]),patch.object(base.AsyncCoreModelActor,'_process_type',
                return_value=base.query_fixture.Process),first_actor_ack_gate() as (entered,ack):
            owner = self.owner(plan,R=11)
            stream = owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
            try:
                self.tick_until(stream,lambda:owner.core.capacity.usage().active_requests==1)
                self.assertTrue(entered.wait(2))
                reference = dict(stream.peak_core_usage)
                clock = stream._core_due_at-owner.capacity.poll_interval_s
                with patch.object(query_runtime,'time',SimpleNamespace(monotonic=lambda:clock)), \
                        patch.object(stream,'_record_core_usage',wraps=stream._record_core_usage) as observe:
                    while stream.driver.active_rows < 10:
                        stream._tick()
                        for key,value in asdict(owner.core.capacity.usage()).items():
                            reference[key] = max(reference[key],value)
                    self.assertEqual(observe.call_count,0)
                # The close snapshot precedes cancelling/removing queued work.
                gate.touch()
                ack.set()
                report = owner.close(timeout=3)
                self.assertEqual(report['peak_core_usage'],reference)
                self.assertEqual(report['peak_core_usage']['held_tasks'],10)
                self.assertFalse(report['query_completed'])
            finally:
                ack.set();gate.touch();owner.close(timeout=3)

    def test_final_peaks_match_per_checkpoint_reference_across_Filter_Map(self):
        plan,rows = base.fixture.chain_for('FM'),base.fixture.source_rows(size=22)
        self.helper.config_for(plan,rows)
        reference = {}
        tick = query_runtime._RecordLocalRows._tick
        def observed(stream):
            result = tick(stream)
            for key,value in asdict(stream.owner.core.capacity.usage()).items():
                reference[key] = max(reference.get(key,0),value)
            return result
        with base.ThreadedRuntime([]),patch.object(base.AsyncCoreModelActor,'_process_type',
                return_value=base.query_fixture.Process),patch.object(query_runtime._RecordLocalRows,'_tick',observed):
            owner = self.owner(plan,R=11)
            stream = owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
            try:
                actual = list(stream)
                self.assertEqual(owner.last_close_report['peak_core_usage'],reference)
                self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                self.assertTrue(actual)
            finally:
                owner.close(timeout=3)

    def test_result_burst_larger_than_Core_budget_fully_drains(self):
        plan,rows = base.fixture.chain_for('M'),base.fixture.source_rows(size=8)
        gate = Path(str(self.helper.config)+'.release')
        config = self.helper.config_for(plan,rows)
        for response in config['responses'].values():
            response['wait_file'] = str(gate)
        self.helper.config.write_text(json.dumps(config))
        with base.ThreadedRuntime([]),patch.object(base.AsyncCoreModelActor,'_process_type',
                return_value=base.query_fixture.Process):
            owner = self.owner(plan,R=8)
            owner.capacity = replace(owner.capacity,step_actions=2)
            stream = owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
            try:
                self.tick_until(stream,lambda:sum(e['event']=='start' for e in self.helper.events())==8)
                gate.touch()
                self.assertEqual(len(list(stream)),8)
                self.assertEqual(len({call['call_id'] for call in owner.calls}),8)
                self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                self.assertFalse(owner.transport.backend._slots)
            finally:
                gate.touch();owner.close(timeout=3)

    def test_one_atomic_backend_rejection_rolls_back_and_retries_without_duplicate_calls(self):
        plan,rows = base.fixture.chain_for('M'),base.fixture.source_rows(size=8)
        self.helper.config_for(plan,rows)
        rejected=[]
        submit = BoundedAsyncBackend.try_submit_batch
        def reject_once(backend,tasks,endpoint):
            if not rejected:
                rejected.append(tuple(task.key for task in tasks))
                return BatchSubmission(Acceptance.NOT_ACCEPTED)
            return submit(backend,tasks,endpoint)
        with base.ThreadedRuntime([]),patch.object(base.AsyncCoreModelActor,'_process_type',
                return_value=base.query_fixture.Process), \
                patch.object(BoundedAsyncBackend,'try_submit_batch',reject_once):
            owner=self.owner(plan,R=8)
            try:
                stream=owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
                self.assertEqual(len(list(stream)),8)
                self.assertEqual(len(owner.calls),8)
                self.assertEqual(len({call['call_id'] for call in owner.calls}),8)
                self.assertEqual(owner.core.capacity.usage().held_tasks,0)
                self.assertEqual(len(rejected),1)
            finally:
                owner.close(timeout=3)


class PrefixReplayTests(unittest.TestCase):
    setUp = base.AsyncTokenBatchTests.setUp

    def test_incremental_prefix_matches_stateless_members_reasons_and_byte_boundaries(self):
        plan,rows=base.fixture.chain_for('M'),base.fixture.source_rows(size=5)
        rows=[replace(row,values=dict(row.values,passage='中文 "quoted" \\ text')) for row in rows]
        self.helper.config_for(plan,rows)
        with base.ThreadedRuntime([]),patch.object(base.AsyncCoreModelActor,'_process_type',
                return_value=base.query_fixture.Process):
            owner=base.AsyncTokenBatchTests.owner(self,plan,R=5,C=None,rolling_refill=True)
            stream=owner.evaluate(iter(rows),plan,expected=rows,query_id=plan.query_id)
            try:
                stream._tick()
                for row in rows: stream._fill()
                stream.driver._submit(5)
                candidates=stream.session._records()
                costs=packing.PreparedTokenOrganizer(owner,base.CountingTokenizer(),None)
                entries=[costs._estimate(candidate) for candidate in candidates]
                first_two=sum(item[1]+item[2] for item in entries[:2])
                limits=owner._actors.limits
                extra,header,rpc_header=costs._header_sizes(candidates[0].key.session_id,entries[0][3][-1],limits)
                frame_two=max(header+sum(item[4] for item in entries[:2])+1,
                              rpc_header+sum(item[5] for item in entries[:2])+1)
                single=max(item[4]+extra for item in entries)
                for B,window,byte_cap,expected,reason in [
                    (first_two,5,None,2,'token_budget'),
                    (1,5,None,1,'token_budget'),
                    (1000000,3,None,3,'request_capacity'),
                    (1000000,5,frame_two,2,'frame_bytes')]:
                    with self.subTest(B=B,window=window,byte_cap=byte_cap):
                        owner.prepared_batching=base.PreparedBatching('token_budget',None,B)
                        bounded=limits if byte_cap is None else replace(limits,input_bytes=byte_cap,item_input_bytes=single)
                        with patch.object(owner._actors,'limits',bounded):
                            ref=packing.PreparedTokenOrganizer(owner,base.CountingTokenizer(),None)
                            inc=packing.PreparedTokenOrganizer(owner,base.CountingTokenizer(),None)
                            ref.request_window=inc.request_window=window
                            owner._event_driven_preparation=False
                            wanted=ref(candidates)
                            owner._event_driven_preparation=True
                            inc.refresh_prefix((),0)
                            for candidate in candidates: inc.append_ready((candidate,))
                            self.assertEqual(inc(candidates),wanted)
                            self.assertEqual(len(wanted),expected)
                            self.assertEqual(inc.history[-1]['reason'],reason)
                            self.assertEqual(inc.history,ref.history)
                changed=json.loads(candidates[1].task.payload)
                changed['generation']['max_tokens']+=1
                incompatible=list(candidates)
                incompatible[1]=replace(candidates[1],task=replace(candidates[1].task,
                    payload=json.dumps(changed).encode()))
                owner.prepared_batching=base.PreparedBatching('token_budget',None,1000000)
                inc=packing.PreparedTokenOrganizer(owner,base.CountingTokenizer(),None)
                inc.refresh_prefix((),0)
                for candidate in incompatible: inc.append_ready((candidate,))
                self.assertEqual(inc(incompatible),(candidates[0].key,))
                self.assertEqual(inc.history[-1]['reason'],'compatibility')
            finally:
                owner.close(timeout=3)
