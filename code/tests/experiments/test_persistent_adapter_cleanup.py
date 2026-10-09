"""Partial initialization and evidence failures retain the first query error."""
from contextlib import ExitStack
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.postgresql import persistent_native_adapter_query as persistent
from src.scheduling.core.session_contract import Usage


class SourceFailure(RuntimeError):pass
class SummaryFailure(OSError):pass
class LifecycleFailure(RuntimeError):pass
class CloseFailure(RuntimeError):pass


class Component:
    def __init__(self,*,enter_error=None,exit_error=None):
        self.enter_error,self.exit_error=enter_error,exit_error
        self.entered=self.exited=0
    def __enter__(self):
        self.entered+=1
        if self.enter_error is not None:raise self.enter_error
        return self
    def __exit__(self,*_):
        self.exited+=1
        if self.exit_error is not None:raise self.exit_error
    def endpoint_url(self,*_):return 'http://127.0.0.1:1/v1/chat/completions'
    def record(self,_):pass


class PersistentCleanupTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.root=Path(self.directory.name)
        self.events,self.gateway=Component(),Component()
        self.execution=SimpleNamespace(drain_timeout_s=.01,close=mock.Mock(return_value=True),
            engine=SimpleNamespace(capacity=SimpleNamespace(usage=lambda:Usage()),
                                   jobs=SimpleNamespace(jobs={})))
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.object(persistent,'BufferedEvents',return_value=self.events))
        self.stack.enter_context(mock.patch.object(persistent,'ObservationGateway',return_value=self.gateway))
        self.stack.enter_context(mock.patch.object(persistent,'build_native_execution',return_value=self.execution))

    def owner_group(self):
        return SimpleNamespace(root=self.root/'owner-group',physical=None,
            model=FixedModelConfig('http://127.0.0.1:1/v1/chat/completions','fixture',1000),
            plan=SemanticMapPlan('Return ok.','fixture',16),options=NativeGraphOptions(),
            query_timeout_s=3,ray_session_id=None,tokenizer_path=None)

    def owner(self):
        return persistent._ArmOwner(self.owner_group(),'fixed-map-semloom-local-diagnostic')

    def group(self):
        group=persistent.PersistentAdapterGroup(('fixed-map-semloom-local-diagnostic',),
            plan=SemanticMapPlan('Return ok.','fixture',16),
            model=FixedModelConfig('http://127.0.0.1:1/v1/chat/completions','fixture',1000),
            ledger=None,root=self.root/'group',query_timeout_s=3)
        group.root.mkdir()
        group.owners={'arm':SimpleNamespace(lifecycle=lambda:{'closed':True})}
        return group

    def test_gateway_enter_failure_exits_already_entered_events(self):
        primary=SourceFailure('gateway startup')
        self.gateway.enter_error=primary
        with self.assertRaises(SourceFailure) as caught:self.owner()
        self.assertIs(caught.exception,primary)
        self.assertEqual((self.events.entered,self.events.exited),(1,1))
        self.assertEqual(self.gateway.exited,0)
        self.execution.close.assert_not_called()

    def test_lm_failure_closes_execution_created_before_the_lm(self):
        primary=SourceFailure('LM validation')
        with mock.patch.object(persistent,'prepare_lotus_lm',side_effect=primary):
            with self.assertRaises(SourceFailure) as caught:
                persistent._ArmOwner(self.owner_group(),'lotus-method-semloom-local-diagnostic')
        self.assertIs(caught.exception,primary)
        self.execution.close.assert_called_once()
        self.assertEqual((self.events.exited,self.gateway.exited),(1,1))

    def test_initialization_error_survives_execution_close_failure(self):
        primary=SourceFailure('LM validation')
        self.execution.close.side_effect=CloseFailure('execution shutdown')
        with mock.patch.object(persistent,'prepare_lotus_lm',side_effect=primary):
            with self.assertRaises(SourceFailure) as caught:
                persistent._ArmOwner(self.owner_group(),'lotus-method-semloom-local-diagnostic')
        self.assertIs(caught.exception,primary)
        self.assertTrue(caught.exception.__notes__)
        self.assertEqual((self.events.exited,self.gateway.exited),(1,1))

    def test_normal_owner_close_runs_execution_close_once(self):
        owner=self.owner();owner.close();owner.close()
        self.execution.close.assert_called_once()
        self.assertEqual((self.events.exited,self.gateway.exited),(1,1))

    def test_owner_close_retains_first_cleanup_error_when_summary_also_fails(self):
        owner=self.owner();primary=CloseFailure('execution shutdown')
        self.execution.close.side_effect=primary
        with mock.patch.object(persistent,'write_private_json',side_effect=SummaryFailure('owner summary')):
            with self.assertRaises(CloseFailure) as caught:owner.close()
        self.assertIs(caught.exception,primary)
        self.assertEqual((self.events.exited,self.gateway.exited),(1,1))

    def test_owner_lifecycle_failure_is_recorded_before_summary_failure(self):
        owner=self.owner();primary=LifecycleFailure('owner lifecycle')
        with mock.patch.object(owner,'lifecycle',side_effect=primary), \
                mock.patch.object(persistent,'write_private_json',side_effect=SummaryFailure('owner summary')):
            with self.assertRaises(LifecycleFailure) as caught:owner.close()
        self.assertIs(caught.exception,primary)
        self.execution.close.assert_called_once()

    def test_execution_error_precedes_multiple_component_exit_errors(self):
        owner=self.owner();primary=CloseFailure('execution shutdown')
        self.execution.close.side_effect=primary
        self.gateway.exit_error=CloseFailure('gateway shutdown')
        self.events.exit_error=CloseFailure('events shutdown')
        with self.assertRaises(CloseFailure) as caught:owner.close()
        self.assertIs(caught.exception,primary)
        self.assertEqual((self.events.exited,self.gateway.exited),(1,1))

    def test_first_component_exit_error_precedes_later_component_error(self):
        owner=self.owner();primary=CloseFailure('gateway shutdown')
        self.gateway.exit_error=primary
        self.events.exit_error=CloseFailure('events shutdown')
        with self.assertRaises(CloseFailure) as caught:owner.close()
        self.assertIs(caught.exception,primary)
        self.execution.close.assert_called_once()

    def test_group_closes_all_owners_before_propagating_first_cleanup_error(self):
        group=self.group();first=CloseFailure('first exit');second=CloseFailure('second exit')
        closed=[]
        def close(name,error):
            closed.append(name)
            raise error
        group.stack.callback(group._close_errors.attempt,'second',lambda:close('second',second))
        group.stack.callback(group._close_errors.attempt,'first',lambda:close('first',first))
        with self.assertRaises(CloseFailure) as caught:group.__exit__(None,None,None)
        self.assertIs(caught.exception,first)
        self.assertEqual(closed,['first','second'])

    def assert_group_keeps_query_error(self,group,primary):
        with mock.patch.object(persistent.PersistentAdapterGroup,'__enter__',return_value=group):
            with self.assertRaises(SourceFailure) as caught:
                with group:raise primary
        self.assertIs(caught.exception,primary)
        self.assertTrue(caught.exception.__notes__)

    def test_query_error_is_not_replaced_by_summary_failure(self):
        group=self.group();primary=SourceFailure('query source')
        with mock.patch.object(persistent,'write_private_json',side_effect=SummaryFailure('group summary')):
            self.assert_group_keeps_query_error(group,primary)

    def test_query_error_is_not_replaced_by_lifecycle_failure(self):
        group=self.group();primary=SourceFailure('query source')
        group.owners['arm'].lifecycle=mock.Mock(side_effect=LifecycleFailure('group lifecycle'))
        self.assert_group_keeps_query_error(group,primary)

    def test_summary_failure_without_query_error_is_propagated(self):
        group=self.group();primary=SummaryFailure('group summary')
        with mock.patch.object(persistent,'write_private_json',side_effect=primary):
            with self.assertRaises(SummaryFailure) as caught:group.__exit__(None,None,None)
        self.assertIs(caught.exception,primary)

    def test_first_group_cleanup_error_survives_summary_failure(self):
        group=self.group();primary=CloseFailure('group shutdown')
        def close():raise primary
        group.stack.callback(close)
        with mock.patch.object(persistent,'write_private_json',side_effect=SummaryFailure('group summary')):
            with self.assertRaises(CloseFailure) as caught:group.__exit__(None,None,None)
        self.assertIs(caught.exception,primary)


if __name__=='__main__':unittest.main()
