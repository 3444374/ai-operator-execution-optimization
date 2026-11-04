"""Reproduce RPC entry before the actual held reply with controlled CPU delay."""
import ast
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import inspect
import io
import textwrap
import threading
import unittest
from unittest.mock import patch


ORIGINAL_METHOD_AST_SHA256 = '8aa327701ad8be9f225d7e3c62ea433945d34c406089f8757952b6fa1f735124'


def check_ack_order():
    from tests.execution_provider import test_event_state_preparation as candidate

    name = 'test_partial_ready_uses_other_RPC_permits_while_first_ack_is_delayed'
    current = ast.parse(textwrap.dedent(inspect.getsource(getattr(candidate.EventPreparationTests, name)))).body[0]
    previous = deepcopy(current)
    calls = [node for node in ast.walk(previous) if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)
        and node.func.value.id == 'entered' and node.func.attr == 'wait']
    if len(calls) != 1 or ast.literal_eval(calls[0].args[0]) != 2 or calls[0].keywords:
        raise ValueError('ACK fixture no longer has the verified single wait call')
    calls[0].func.attr, calls[0].args = 'is_set', []
    digest = lambda node: hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()
    if digest(previous) != ORIGINAL_METHOD_AST_SHA256:
        raise ValueError('derived pre-wait fixture differs from the verified original method AST')
    scope = dict(vars(candidate))
    exec(compile(ast.Module(body=[previous], type_ignores=[]), 'ack_order_before_wait', 'exec'), scope)
    Original = type('OriginalAckOrdering', (candidate.EventPreparationTests,), {name: scope[name]})

    @contextmanager
    def controlled_ack():
        may_reply = threading.Event()
        class Entered(threading.Event):
            def wait(self, timeout=None):
                may_reply.set()
                return super().wait(timeout)
        class Release(threading.Event):
            def set(self):
                may_reply.set()
                super().set()
        entered, release = Entered(), Release()
        actual = candidate.base.AsyncCoreModelActor.try_submit_prepared_batch
        first, lock = [], threading.Lock()
        def delayed(actor, payload):
            if not may_reply.wait(5):
                raise TimeoutError('CPU fixture did not allow the actual actor reply')
            response = actual(actor, payload)
            with lock:
                hold = not first
                if hold:
                    first.append(True)
            if hold:
                entered.set()
                if not release.wait(5):
                    raise TimeoutError('CPU fixture did not release its actual ACK')
            return response
        with patch.object(candidate.base.AsyncCoreModelActor, 'try_submit_prepared_batch', delayed):
            try:
                yield entered, release
            finally:
                release.set()

    outcomes = []
    for cls in (Original, candidate.EventPreparationTests):
        stream = io.StringIO()
        with patch.object(candidate, 'first_actor_ack_gate', controlled_ack):
            scope['first_actor_ack_gate'] = controlled_ack
            result = unittest.TextTestRunner(stream=stream, verbosity=2).run(unittest.TestSuite([cls(name)]))
        outcomes.append(dict(role='original' if cls is Original else 'candidate',
            run=result.testsRun, successful=result.wasSuccessful(),
            failures=[dict(id=test.id(), traceback=error) for test, error in result.failures],
            errors=[dict(id=test.id(), traceback=error) for test, error in result.errors]))
    assert not outcomes[0]['successful'] and len(outcomes[0]['failures']) == 2 and not outcomes[0]['errors']
    assert outcomes[1]['successful'] and not outcomes[1]['errors']
    return dict(schema='semloom.cpu_ack_order.v2',
        verified_original_source_commit='129c5555092facf3582266d1a17feebf0184f55f',
        original_method_AST_sha256=digest(previous), candidate_method_AST_sha256=digest(current),
        derived_fixture_changed_call_sites=1,
        scope='Same production async pipeline; only the verified fixture wait is replaced for the negative control.',
        outcomes=outcomes, production_code_changed_for_this_case=False,
        model_calls=0, real_Ray_runs=0, GPU_runs=0)
