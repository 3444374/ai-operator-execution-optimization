"""Ordinary Filter result adaptation over the existing async record-chain owner.

This maps a finite source declaration and its receipts; it adds no execution loop,
model owner, scheduling policy or native request replay. Raw ledger results become
iterable after the chain's normal traversal; execution still completes per member.
"""
from copy import deepcopy
from dataclasses import asdict
import threading
import weakref

from src.semantic_methods.lotus.record_chain import LotusRecordChain
from src.semantic_methods.lotus.record_types import RecordOccurrence, StageSpec
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, identity
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from .lotus_record_chain_core import SemLoomRecordChainCore
from .lotus_records_async_actor import AsyncRecordsActorOwner
from .records_runtime_identity import ModelRuntimeBinding, retired_close_metadata as _retired_metadata




class PreparedRecordsCore:
    def __init__(self, method, engine, generation, resources, profile, batching, limits, *, resident=False,
                 capacity=None, interleaved_preparation=False, event_driven_preparation=False):
        if type(resident) is not bool or resident and not batching.get('rolling_refill'):
            raise ValueError('resident token records require rolling_refill')
        from .records_profile import checked_preparation_options
        preparation = checked_preparation_options(batching, interleaved_preparation=interleaved_preparation,
                                                  event_driven_preparation=event_driven_preparation)
        self.interleaved_preparation = interleaved_preparation
        self.event_driven_preparation = event_driven_preparation
        self.resident = resident
        self.method = FilterMethod.from_dict(asdict(method))
        self.execution_profile = dict(profile, record_batching=deepcopy(batching))
        self.execution_profile['record_batching'].update(preparation)
        self.resources, self.engine, self.generation = resources, engine, generation
        self.capacity = capacity
        self.owner = self._active = None
        self._limits, self._closed = limits, False
        self._expected = {}
        self._delivered = set()
        self._thread = threading.get_ident()
        self._service_allocation = self._delivery = None
        self._runtime_binding = ModelRuntimeBinding()
        self._retired_report = self._service_retirement = None

    def _control(self):
        if threading.get_ident() != self._thread:
            raise RuntimeError('prepared records owner requires its single control thread')

    def bind_model_runtime(self, observed=None):
        self._control()
        self._runtime_binding.bind(observed)

    def _service_ready(self):
        self._service_allocation.observe_ready(self)

    @property
    def production_core_connected(self):
        return self.owner is not None and self.owner.production_core_connected

    def start(self):
        self._control()
        if self._closed:
            raise RuntimeError('record token owner is closing')
        if self.resident:
            try:
                if self.owner is None:
                    self.owner = self._new_owner(self._tokenizer())
                return self.owner.start()
            except BaseException as error:
                if self._service_allocation is not None:
                    self._startup_cleanup(error)
                raise
        return dict(no_session_model=True, model_startup_scope='inside finite record-chain evaluate',
                    record_batching=deepcopy(self.execution_profile['record_batching']))

    @property
    def model_ready(self):
        """None until this wrapper's existing query allocation has a model."""
        return None if self.owner is None else deepcopy(self.owner._actors.model_ready)

    def execution_metadata(self):
        return dict({} if self.owner is None else self.owner._actors.execution_metadata(),
            execution_profile=deepcopy(self.execution_profile),
            execution='record_local_sections', model_lifecycle=('resident model, independent queries' if self.resident
                else 'fresh finite query owner'),
            raw_ledger_delivery='after normal chain traversal; member compute completion remains independent')


    def evaluate(self, source, *, query_id, method=None, cancelled=None):
        self._control()
        if self._closed or self._active is not None and not (self.resident and self._active.finished):
            raise ValueError('record token owner has an active query or is closed')
        query_method = self.method if method is None else FilterMethod.from_dict(asdict(method))
        if query_method.model_name != self.method.model_name:
            raise ValueError('the query must retain the model identity')
        source.preflight(query_method)
        scan = source.scan_spec(query_method)
        scan.describe()
        expected = source.expected(query_id)
        CallLedger(expected, max(1, len(expected)))
        if self._service_allocation is not None:
            from .records_service_delivery import (
                OrdinaryDeliveryDeclaration, OrdinaryDeliverySession, declare_ordinary_delivery)
            delivery = self._service_allocation.plan.delivery
            if type(delivery) is OrdinaryDeliverySession:
                delivery = delivery.for_query(query_id)
            actual = declare_ordinary_delivery(query_id, expected, item_result_bytes=self.capacity.item_result_bytes)
            if type(delivery) is not OrdinaryDeliveryDeclaration or actual != delivery:
                raise ValueError('actual source identity differs from its service delivery declaration')
            if self._active is not None and self.has_unconsumed_results():
                raise RuntimeError('take unconsumed results before replacing their service query')
        else:
            delivery = None
        expected_by_id = {v['row_id']: v for v in expected}
        if len(expected_by_id) != len(expected):
            raise ValueError('finite source requires unique row occurrences')
        # Reuse the finite source declaration and its validation API. Its rows()
        # may reread/verify CSV; preserve values for the chain's source-binding audit.
        if not callable(getattr(source, 'rows', None)):
            raise ValueError('record token batches require a finite source.rows() declaration')
        records, seen = [], set()
        for raw in source.rows():
            if scan.predicate is not None and raw[scan.predicate[0]] != scan.predicate[1]:
                continue
            original = identity(query_id, raw['row_id'], raw['source_position'])
            if original != expected_by_id.get(raw['row_id']) or raw['row_id'] in seen:
                raise ValueError('finite source differs from its expected call declaration')
            seen.add(raw['row_id'])
            if any(type(raw[name]) is not str for name in scan.payload_columns):
                raise ValueError('record scan payload columns must retain text values')
            records.append(RecordOccurrence(raw['row_id'], raw['source_position'], raw['source_position'],
                {name: raw[name] for name in scan.payload_columns}))
        if len(records) != len(expected) or len({r.row_id for r in records}) != len(records):
            raise ValueError('finite source has missing or duplicate occurrences')
        chain = LotusRecordChain((StageSpec('filter', query_method, self.generation),), query_id=query_id)
        try:
            if self.owner is None:
                if self.resident:
                    self.start()
                else:
                    self.owner = self._new_owner(self._tokenizer() if records else None)
            stream = self.owner.evaluate(iter(records), chain, expected=records,
                                         columns=scan.payload_columns, query_id=query_id, cancelled=cancelled)
            self._expected, self._delivered, self._delivery = expected_by_id, set(), delivery
        except BaseException as error:
            self._startup_cleanup(error)
            raise
        self._active = _OrdinaryResults(self, stream)
        return self._active

    def _startup_cleanup(self, error):
        self._closed = True
        try:
            self.close(timeout=self.resources.startup_timeout_s)
        except BaseException as cleanup:
            error.add_note('prepared startup cleanup: ' + type(cleanup).__name__ + ': ' + str(cleanup))

    def _tokenizer(self):
        from transformers import AutoTokenizer
        return AutoTokenizer.from_pretrained(self.engine.tokenizer_path or self.engine.model_path,
                                             trust_remote_code=False, local_files_only=True)

    def _new_owner(self, tokenizer):
        batching = self.execution_profile['record_batching']
        actors = AsyncRecordsActorOwner.for_prepared(self.engine, self.generation, self.resources, limits=self._limits,
            **({} if self.capacity is None or self.capacity.budget is None else dict(capacity=self.capacity)))
        feedback = None
        request_feedback = None
        if 'feedback' in batching:
            from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
            feedback = SupplyFeedbackConfig.from_dict(batching['feedback'])
        if 'request_feedback' in batching:
            from src.scheduling.submission_control.feedback_options import ResourceRequestConfig
            request_feedback = ResourceRequestConfig(**batching['request_feedback'])
        owner = SemLoomRecordChainCore(self.engine, self.resources, execution='record_local_sections',
            actor_owner=actors, capacity=self.capacity,
            prepared_batching=(None if batching['prepared_batching'] is None
                               else PreparedBatching(**batching['prepared_batching'])),
            batch_concurrency=batching['C_batch'], batching_tokenizer=tokenizer, batch_feedback=feedback,
            rolling_refill=batching.get('rolling_refill', False), request_feedback=request_feedback,
            resident=self.resident,
            **({} if self.capacity is None or self.capacity.budget is None
               else dict(request_window=self.execution_profile['max_inflight'])),
            experimental_skip_token_packing=batching.get('experimental_skip_token_packing', False))
        owner._interleaved_preparation = self.interleaved_preparation
        owner._event_driven_preparation = self.event_driven_preparation
        if self._service_allocation is not None:
            self.owner = owner
            owner._service_wrapper_ref = weakref.ref(self)
            self._service_allocation.adopt_producer(self, owner)
            owner._service_runtime_binding = self._runtime_binding
            owner._service_ready_observer = self._service_ready
        return owner

    def _ordinary(self, receipt):
        expected = self._expected.get(receipt['row_id'])
        if (expected is None or any(receipt.get(k) != expected[k] for k in expected if k != 'call_id')):
            raise ValueError('chain receipt changed its ordinary Filter identity')
        value = dict(deepcopy(receipt), chain_call_id=receipt['call_id'], **expected)
        if getattr(self, '_delivery', None) is not None:
            from .records_service_delivery import check_ordinary_receipt
            check_ordinary_receipt(self._delivery, value)
        return value

    def has_unconsumed_results(self):
        self._control()
        return self.owner is not None and any(self._ordinary(c)['call_id'] not in self._delivered
                                             for c in self.owner.calls)

    def cleanup_confirmed(self):
        self._control()
        if not self._closed:
            return False
        if self.owner is None:
            return True
        identity = None if self._service_allocation is None else self._service_allocation._identity
        return self.owner._service_cleanup_confirmed(expected_identity=identity)

    def take_unconsumed_results(self, *, expected=None):
        self._control()
        if not self._closed and self._active is not None and not self._active.finished:
            raise RuntimeError('close the query before taking its retained receipts')
        report = self.last_close_report
        taken = [] if report is None else report.get('unconsumed_call_results', [])
        if expected is not None:
            from src.infrastructure.json_content import json_content_equal
            if type(expected) is not list or not json_content_equal(taken, expected):
                raise ValueError('retained receipts changed before consumer acceptance')
        previous_delivered = self._delivered
        active = self._active
        previous_report = None if active is None else active._report
        delivered = previous_delivered | {c['call_id'] for c in taken}
        updated_report = (None if previous_report is None
                          else dict(previous_report, unconsumed_call_results=[]))
        try:
            self._delivered = delivered
            if active is not None and updated_report is not None:
                active._report = updated_report
            # A caller without an expected snapshot first needs its only result
            # copy returned. Fallible cleanup belongs to a subsequent close.
            if (self._service_allocation is not None and self._closed
                    and (expected is not None or not taken)):
                self._retire_delivered_producer(report)
                self._service_allocation.settle(self)
            return taken
        except BaseException:
            if expected is None and taken:
                self._delivered = previous_delivered
                if active is not None:
                    active._report = previous_report
            raise

    def _retire_delivered_producer(self, report):
        if (self._service_allocation is None or self.owner is None or not self._closed
                or not self.cleanup_confirmed() or self.has_unconsumed_results()):
            return
        child = self.owner
        metadata = self._retired_report
        if metadata is None:
            metadata = _retired_metadata(report)
            metadata.update(payload_transferred=True, cleanup_confirmed=True,
                            calls_count=len(report.get('calls', [])), unconsumed_call_results=[])
        certificate = self._service_allocation.retire_producer(self, child)
        if self._service_retirement is not None and certificate is not self._service_retirement:
            raise RuntimeError('payload retirement cannot replace its original certificate')
        self._service_retirement, self._retired_report = certificate, metadata
        if self._active is not None:
            self._active._clear_iteration()
        child._service_retire_payloads(metadata, certificate)
        if not child._service_payloads_retired_confirmed():
            raise RuntimeError('original producer still retains private query payloads')
        self._active = self._delivery = None
        self._expected, self._delivered = {}, set()
        # Keep the original producer available until all wrapper payload
        # references are gone, so an interrupted close can resume its cleanup.
        self.owner = None

    @property
    def last_close_report(self):
        if self._retired_report is not None:
            return dict(deepcopy(self._retired_report), service_allocation=self._service_allocation.describe())
        if self.owner is None:
            value = (dict(status='closed', cleanup_errors=[], source_closed=True, unconsumed_call_results=[],
                record_batching=deepcopy(self.execution_profile['record_batching']),
                model_allocated=False, production_core_connected=False) if self._closed else None)
            if value is not None and self._service_allocation is not None:
                value['service_allocation'] = self._service_allocation.describe()
            return value
        report = self.owner.last_close_report
        if report is None:
            return None
        calls = [self._ordinary(c) for c in self.owner.calls]
        groups = {}
        for call in calls:
            member = call.get('transport_batch')
            if member is not None:
                key = tuple(member['batch_key'][n] for n in ('session_id', 'sequence'))
                groups.setdefault(key, []).append(dict(call_id=call['call_id'], chain_call_id=call['chain_call_id'],
                    member=deepcopy(member), core_task_key=call['core_task_key'],
                    submission_confirmed=call['submission_confirmed'], engine_request_id=call.get('engine_request_id'),
                    status=call['status'], model_end_monotonic=call.get('model_end_monotonic')))
        if report['query_completed']:
            actual, requested = report['async_token_batches'], self.execution_profile['record_batching']
            if requested.get('rolling_refill'):
                window = actual.get('C_request')
                valid_window = (actual.get('rolling_refill') is True and actual['concurrency'] is None
                    and (type(window) is int and 1 <= window <= requested['R'] if 'request_feedback' in requested
                         else window == requested['C_request']) and actual['peak_requests'] <= requested['R']
                    and actual['peak_batches'] <= requested['R'])
            else:
                upper = requested.get('feedback', {}).get('max_batches', requested['C_batch'])
                valid_window = (1 <= actual['concurrency'] <= upper and actual['peak_batches'] <= upper
                                if 'feedback' in requested else actual['concurrency'] == requested['C_batch'])
            if (actual['policy'] != requested['prepared_batching'] or not valid_window
                    or actual.get('experimental_skip_token_packing', False)
                       != requested.get('experimental_skip_token_packing', False)
                    or actual.get('interleaved_preparation', False)
                       != requested.get('interleaved_preparation', False)
                    or actual.get('event_driven_preparation', False)
                       != requested.get('event_driven_preparation', False)
                    or actual['request_limit'] != requested['R'] or sum(map(len, groups.values())) != len(self._expected)
                    or actual['data_batch_rpcs'] != len(groups)):
                raise ValueError('actual token batch execution differs from the selected factory configuration')
            for members in groups.values():
                size = len(members)
                if (sorted(m['member']['index'] for m in members) != list(range(size))
                        or any(m['member']['size'] != size or not m['submission_confirmed'] for m in members)):
                    raise ValueError('completed transport batch has incomplete or changed membership')
        snapshot = deepcopy({name: value for name, value in report.items() if name != 'calls'})
        value = dict(snapshot, calls=calls,
            record_batching=deepcopy(self.execution_profile['record_batching']),
            transport_batches=[dict(batch_key=list(key), receipt_members=members) for key,members in groups.items()],
            unconsumed_call_results=[c for c in calls if c['call_id'] not in self._delivered],
            source_closed=report['source_closed'])
        if self._service_allocation is not None:
            value['service_allocation'] = self._service_allocation.describe()
        return value

    @property
    def last_query_report(self):
        return self.last_close_report

    def close(self, *, timeout=None):
        self._control()
        self._closed = True
        active = self._active
        try:
            if self._service_allocation is not None and self.cleanup_confirmed():
                report = self.last_close_report
                self._retire_delivered_producer(report)
                self._service_allocation.settle(self)
                report['service_allocation'] = self._service_allocation.describe()
                return report
            if self.owner is not None:
                self.owner.close(timeout=timeout)
            if self._service_allocation is not None:
                report = self.last_close_report
                self._retire_delivered_producer(report)
                self._service_allocation.settle(self)
                report['service_allocation'] = self._service_allocation.describe()
                return report
            return self.last_close_report
        finally:
            if active is not None:
                active._clear_iteration()


class _OrdinaryResults:
    def __init__(self, owner, stream):
        self.owner, self.stream, self.results = owner, stream, None
        self.finished, self._report = False, None

    def __iter__(self):
        return self

    def _clear_iteration(self):
        self.results = self.stream = None

    def _completed_calls(self):
        """Borrow the inner EOF snapshot only after its data ownership drains."""
        inner, stream = self.owner.owner, self.stream
        report = inner.last_close_report
        if (stream is not inner._active or not stream.completed or stream.error is not None
                or report is not stream.last_close_report or report is None
                or report.get('status') != 'closed' or report.get('query_completed') is not True
                or report.get('source_closed') is not True or report.get('source_exhausted') is not True
                or report.get('model_retained') is not self.owner.resident
                or (report.get('model_owner') or {}).get('status')
                   != ('retained' if self.owner.resident else 'closed')):
            return None
        calls = report.get('calls')
        if (type(calls) is not list or len(calls) != len(self.owner._expected)
                or any(call.get('status') != 'succeeded' for call in calls)):
            return None
        # A successful report alone does not certify callbacks queued after it.
        # These existing observations read the actual query owners, without
        # changing any permit, acknowledgement or close responsibility.
        try:
            usage = asdict(inner.core.capacity.usage())
            rpc = inner._actors._data_rpc_progress()
            handoff = inner.transport.audit_handoff_report()
            if (any(usage.values()) or inner.transport.retained_refs()
                    or rpc.pending or rpc.inflight or rpc.batch_submits_pending or rpc.unconfirmed
                    or handoff['pending_receipts'] or handoff['deferred_core_acks']):
                return None
        except Exception:
            return None
        return calls

    def __next__(self):
        if self.owner._closed or self.finished:
            raise StopIteration
        if self.results is None:
            for _ in self.stream:
                pass  # The existing chain owns all per-member execution/progression.
            calls = self._completed_calls()
            self.results = iter(self.owner.owner.calls if calls is None else calls)
        try:
            value = self.owner._ordinary(next(self.results))
        except StopIteration:
            self.finished = True
            self._report = deepcopy(self.owner.last_close_report)
            self._clear_iteration()
            raise
        self.owner._delivered.add(value['call_id'])
        return value

    @property
    def production_core_connected(self):
        return self.owner.production_core_connected

    @property
    def execution_profile(self):
        return self.owner.execution_profile

    @property
    def last_close_report(self):
        if self.owner._service_allocation is not None and self.owner._active is self:
            return self.owner.last_close_report
        return deepcopy(self._report) if self._report is not None else self.owner.last_close_report

    def close(self):
        if self.finished and self.owner.resident:
            return self.last_close_report
        report = self.owner.close()
        self._report = deepcopy(report)
        return report
