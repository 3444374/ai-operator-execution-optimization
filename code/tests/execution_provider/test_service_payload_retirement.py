"""Service payload handoff on actual holder objects, without runtime startup."""
from copy import deepcopy
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import weakref

from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
from src.execution_provider.adapters.lotus_chain_token_batches import PreparedTokenOrganizer
from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
from src.execution_provider.adapters.records_query_runtime import _RecordLocalRows
from src.scheduling.core.session_policy import SessionPolicies
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec


class _TransferredQuery:
    _closed = True
    unconsumed = False

    def cleanup_confirmed(self):
        return True  # The fixture starts after producer cleanup is confirmed.

    def has_unconsumed_results(self):
        return self.unconsumed


class ServicePayloadRetirementTests(unittest.TestCase):
    def completed_owner(self):
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=2)
        engine = EngineSpec('/tmp/payload-model', '/tmp/model-python', '/tmp/driver-python')
        actors = AsyncRecordsActorOwner.for_prepared(engine, GenerationSpec(), resources)
        owner = SemLoomRecordChainCore(engine, resources, execution='record_local_sections',
            actor_owner=actors, rolling_refill=True)
        payload = dict(call_id='query:row', status='unknown', prompt_tokens=7, text='retained result')
        actors.model_ready = dict(engine_uid='model-instance', producer_owner_uid='producer',
            model_configuration=dict(context=8192))
        actors._model_close_ref = object()
        actors._model_close_report = dict(status='closed', engine_api_shutdown_confirmed=True,
            unconsumed_call_results=[payload])
        actors.last_close_report = dict(status='closed', model_ready=actors.model_ready,
            actor_kill_requests_confirmed=True, gpu_exit_confirmed=None,
            cleanup_errors=[], engine_close_report=actors._model_close_report,
            unconsumed_core_results=[dict(result=payload)], unconsumed_call_results=[payload])
        owner.last_close_report = dict(status='closed', calls=[payload],
            model_owner=actors.last_close_report, cleanup_errors=[])
        owner._raw_receipts, owner._contexts = {'row': payload}, {'row': payload}
        owner._root_run = owner.results = object()
        owner._source_expected = owner._current_relation = (payload,)
        owner._expected_bindings = owner._accountants = {'row': payload}
        owner._phase_parents = owner._phase_columns = (payload,)
        owner.phase_history = [payload]
        stream = object.__new__(_RecordLocalRows)
        for name in ('source', 'run', 'section', 'error', 'driver', 'session', 'pending',
                     'pool', '_output', '_phase_source', 'cancelled'):
            setattr(stream, name, payload)
        stream.owner = owner
        stream.bindings = stream._phase_bindings = stream._prepared = {'row': payload}
        stream.offered, stream._extract_results = {'row'}, [payload]
        stream.last_close_report = owner.last_close_report
        owner._active = stream
        # The closed transport's retained containers are the test input; no loop starts.
        transport = object.__new__(LotusChainActorBackend)
        transport._lock = threading.Lock()
        transport._outcomes = transport._requests = transport._refs = {'row': payload}
        transport._consumed_receipts, transport._bulk = {'row'}, {'batch': payload}
        owner.transport = transport
        organizer = PreparedTokenOrganizer(owner, None, None)
        organizer._prefix = organizer._prefix_context = payload
        organizer._estimates = {'row': payload}
        organizer._frontier, organizer._frontier_set = ['row'], {'row'}
        organizer._header_estimate = organizer._header_context = payload
        owner._token_organizer = organizer
        owner.core = SimpleNamespace(policies=SessionPolicies(None, None, None, 'fixture', organize=organizer))
        wrapper = _TransferredQuery()
        wrapper.owner = owner
        wrapper._retired_report = dict(status='closed', calls_count=1)
        wrapper._service_retirement = object()
        owner._service_wrapper_ref = weakref.ref(wrapper)
        return owner, wrapper, stream, organizer

    def retire(self, owner, wrapper):
        owner._service_retire_payloads(wrapper._retired_report, wrapper._service_retirement)

    def test_transferred_unknown_and_model_metadata_keep_their_original_meaning(self):
        owner, wrapper, stream, organizer = self.completed_owner()
        caller_report = owner.last_close_report
        caller_snapshot = deepcopy(caller_report)
        actor_report = owner._actors.last_close_report
        ready = owner._actors.model_ready
        self.retire(owner, wrapper)
        self.assertTrue(owner._service_payloads_retired_confirmed())
        self.assertTrue(stream.query_payloads_retired())
        self.assertEqual(caller_report, caller_snapshot)
        self.assertEqual(caller_report['calls'][0]['status'], 'unknown')
        self.assertEqual(actor_report['unconsumed_call_results'], caller_snapshot['calls'])
        self.assertIs(owner._actors.last_close_report['model_ready'], ready)
        self.assertEqual(ready['model_configuration'], dict(context=8192))
        self.assertIsNone(owner._actors._model_close_ref)
        self.assertTrue(all(not getattr(owner.transport, name) for name in (
            '_outcomes', '_requests', '_refs', '_consumed_receipts', '_bulk')))
        self.assertTrue(all(not getattr(organizer, name) for name in (
            '_prefix', '_prefix_context', '_estimates', '_frontier', '_frontier_set',
            '_header_estimate', '_header_context')))
        self.assertIsNone(owner.core.policies.organize)

    def test_unconsumed_results_keep_all_payload_holders_attached(self):
        owner, wrapper, stream, organizer = self.completed_owner()
        wrapper.unconsumed = True
        original = owner.last_close_report
        with self.assertRaisesRegex(RuntimeError, 'transfer'):
            self.retire(owner, wrapper)
        self.assertIs(owner.last_close_report, original)
        self.assertIs(owner._active, stream)
        self.assertIs(owner._token_organizer, organizer)
        self.assertTrue(owner.transport._outcomes)
        self.assertTrue(owner._actors.last_close_report['unconsumed_call_results'])

    def test_interrupted_component_cleanup_retries_the_same_transfer(self):
        for component, field in (('transport', '_bulk'), ('_actors', '_model_close_ref')):
            with self.subTest(component=component):
                owner, wrapper, stream, organizer = self.completed_owner()
                holder = getattr(owner, component)
                interrupted = []
                def read(actual):
                    return actual.__dict__[field]
                def store(actual, value):
                    actual.__dict__[field] = value
                    if actual is holder and not value and not interrupted:
                        interrupted.append(True)
                        raise KeyboardInterrupt('interrupted payload release')
                with patch.object(type(holder), field, property(read, store), create=True):
                    with self.assertRaises(KeyboardInterrupt):
                        self.retire(owner, wrapper)
                self.assertEqual(interrupted, [True])
                self.assertFalse(owner._service_payloads_retired_confirmed())
                if component == 'transport':
                    self.assertIs(owner._token_organizer, organizer)
                    self.assertTrue(owner._actors.last_close_report['unconsumed_call_results'])
                self.retire(owner, wrapper)
                self.assertTrue(owner._service_payloads_retired_confirmed())
                self.assertTrue(stream.query_payloads_retired())
                self.assertIs(owner._service_payloads_certificate, wrapper._service_retirement)
                self.assertEqual(owner.last_close_report['calls_count'], 1)
                self.assertEqual(owner._actors.last_close_report['model_ready']['engine_uid'], 'model-instance')


if __name__ == '__main__':
    unittest.main()
