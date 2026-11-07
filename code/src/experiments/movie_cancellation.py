"""Finite cancellation qualification over the ordinary Movie owner and supervisor.

This is a test entry, not a scheduling policy. Native API evidence is required;
an accepted Core request, a client cancel intent, and physical GPU exit alone
cannot establish that cancellation reached a live native request.
"""
from copy import deepcopy
from pathlib import Path
import json
import time
from unittest.mock import patch

from src.infrastructure.private_artifacts import new_private_directory, write_private_json
from src.infrastructure.redact import redact_json_values, redact_text
from src.experiments.native_query_lifecycle import _failure

QUALIFICATION = 'inflight_cancel_then_recover'
_EXPECTED_UNKNOWN = {'AbortNotConfirmed', 'CancelledError', 'ClosedWithoutConfirmedTerminal'}


def qualification_options(config):
    value = config['workload'].get('qualification')
    if value is None:
        return None
    if (value != QUALIFICATION or config['workload']['kind'] != 'movie_session'
            or config['observation']['mode'] != 'off'):
        raise ValueError('use the explicit Movie cancellation qualification with the optional resource observer off')
    bounds = config['observation']
    events, size = bounds.get('max_events_per_process'), bounds.get('max_event_bytes')
    if any(type(v) is not int or v <= 0 for v in (events, size)):
        raise ValueError('reuse explicit existing observer evidence bounds for the native sidecar')
    return dict(max_bytes=events*size, max_events=events)


def check_cancellation_plan(plan):
    from src.experiments.movie_records_session import check_plan
    checked = check_plan(plan)
    if (plan['backend'] != 'semloom-core' or plan['lifecycle'] != 'resident'
            or plan.get('rolling_refill') is not True
            or len(plan['queries']) != 2 or any(q['phase'] != 'measured' for q in plan['queries'])
            or plan.get('batch_feedback') is not None
            or type(plan['profile']) is not dict or plan['profile'].get('admission') is not None
            or plan['profile'].get('mode') != 'async'):
        raise ValueError('cancellation qualification requires two fixed async Core queries, without warmup or search')
    first, second = checked['queries']
    if (first['task'] != second['task'] or first['expected_calls'] < 2 or second['expected_calls'] < 1
            or checked['core_execution']['max_inflight'] < first['expected_calls']
            or plan['engine']['max_model_calls'] != checked['expected_calls']):
        raise ValueError('declare a fully in-flight cancellation unit, a recovery unit and an exact total call cap')
    return checked


def _present(identity, external):
    if type(identity) is not dict or identity.get('lookup_internal') is not False:
        return False
    mapped, present = identity.get('mapped_internal_ids'), identity.get('present_internal_ids')
    aliases = identity.get('external_id_by_internal')
    return (type(mapped) is list and len(mapped) == 1 and type(mapped[0]) is str
            and present == mapped and type(aliases) is dict and aliases.get(mapped[0]) == external)


def native_state(events, *, owner_uid, query_id, expected):
    """Order is from one native process; no cross-process clock subtraction."""
    bound, added, final, abort_enter, aborted = {}, set(), set(), set(), set()
    aliases, native_bindings = {}, {}
    for event in events:
        kind, key = event.get('event'), event.get('request_id')
        if type(key) is not str:
            key = None
        if kind == 'qualification_request':
            occurrence = tuple(event.get(name) for name in ('query_id', 'row_id', 'source_position', 'call_index'))
            if (type(key) is not str or not key.startswith(owner_uid + ':')
                    or event.get('query_id') != query_id or occurrence not in expected
                    or type(event.get('call_id')) is not str
                    or any(type(event.get(name)) is not int or event[name] < 0 for name in ('source_position','call_index'))):
                raise ValueError('native request binding differs from the declared query')
            if key in bound:
                raise ValueError('native request binding repeated or changed')
            bound[key], native_bindings[key] = expected[occurrence], event['call_id']
        elif kind == 'native_add_return' and key in bound:
            identity = event.get('native_identity')
            if _present(identity, key):
                added.add(key)
                for internal in identity['mapped_internal_ids']:
                    if internal in aliases and aliases[internal] != key:
                        raise ValueError('native alias belongs to another request')
                    aliases[internal] = key
        elif kind == 'native_final_observed':
            key = aliases.get(key, key)
            if key in bound and event.get('finished') is True:
                final.add(key)
        elif kind == 'native_abort_enter' and key in bound:
            identity = (event.get('native_identities') or {}).get(key)
            if event.get('internal') is False and key in added and key not in final and _present(identity, key):
                abort_enter.add(key)
        elif kind == 'native_abort_return' and key in abort_enter:
            aborted.add(key)
        elif kind in ('native_abort_error', 'native_add_error') and key in bound:
            raise ValueError('native API operation failed during cancellation qualification')
    if len(set(bound.values())) != len(bound):
        raise ValueError('multiple native requests bound to one source occurrence')
    return dict(live=sorted(added-final), aborted=sorted(aborted), binding=bound, native_binding=native_bindings)


class NativeGate:
    def __init__(self, directory, expected, query_id, *, max_bytes, max_events):
        if any(type(v) is not int or v <= 0 for v in (max_bytes, max_events)):
            raise ValueError('explicit positive native evidence bounds required')
        self.directory, self.query_id = Path(directory), query_id
        self.identities = {tuple(row[name] for name in ('query_id', 'row_id', 'source_position', 'call_index')):
                           row['call_id'] for row in expected}
        self.expected = set(self.identities.values())
        if len(self.identities) != len(expected) or len(self.expected) != len(expected):
            raise ValueError('cancellation source occurrences must be unique')
        self.max_bytes, self.max_events = max_bytes, max_events
        self.ready = None
        self.triggered = False
        self.trigger_ns = None

    def state(self, *, complete=False):
        ready = self.ready() if self.ready is not None else None
        if not ready:
            return dict(live=[], aborted=[], binding={})
        pid, uid = ready.get('pid'), ready.get('producer_owner_uid')
        if type(pid) is not int or pid <= 0 or type(uid) is not str or not uid:
            raise ValueError('original model readiness lacks its process/producer identity')
        path = self.directory / ('native-' + str(pid) + '.jsonl')
        if not path.exists():
            return dict(live=[], aborted=[], binding={})
        if path.is_symlink() or path.stat().st_size > self.max_bytes:
            raise ValueError('native evidence exceeds its owned bounded file')
        raw = path.read_bytes()
        if len(raw) > self.max_bytes:
            raise ValueError('native evidence grew beyond its bound')
        if raw and not raw.endswith(b'\n'):
            if complete:
                raise ValueError('native evidence contains an incomplete final record')
            raw = raw[:raw.rfind(b'\n')+1]
        events = [json.loads(line) for line in raw.splitlines()]
        if len(events) > self.max_events or any(type(e) is not dict or e.get('pid') != pid for e in events):
            raise ValueError('native evidence count or process identity changed')
        return native_state(events, owner_uid=uid, query_id=self.query_id, expected=self.identities)

    def __call__(self):
        if not self.triggered:
            state = self.state()
            if len(state['live']) == len(self.expected):
                self.triggered = True
                self.trigger_ns = time.monotonic_ns()
        return self.triggered


def _clean_phase(phase):
    if phase.get('cleanup_failure') or phase.get('observer_errors'):
        raise ValueError('the phase contains an additional cleanup or observation failure')
    owners = phase.get('owners', [])
    if len(owners) != 1 or owners[0].get('status') != 'closed':
        raise ValueError('the phase owner did not confirm cleanup')
    close = owners[0].get('close') or {}
    storage = close.get('qualification_method_storage') or {}
    model = (close.get('receipts') or {}).get('model') or {}
    actor = model.get('model_owner') or {}
    if (close.get('status') != 'closed' or close.get('cleanup_errors')
            or model.get('status') != 'closed' or model.get('cleanup_errors')
            or model.get('transport_closed') is not True or model.get('retained_remote_inputs') != 0
            or not model.get('core_usage') or any(model['core_usage'].values())
            or actor.get('data_rpc_pending') != 0 or actor.get('data_rpc_unconfirmed') != 0
            or storage.get('runs') != 0 or storage.get('logical_bytes') != 0):
        raise ValueError('Core, bytes, delivery or RPC ownership remains unconfirmed')
    return model


def _records(phase):
    query = phase['queries'][0]
    return ((query.get('backend_report') or {}).get('calls') or {}).get('records', [])


def _exception_evidence(error):
    # Lifecycle receipts can precede recording/cleanup notes added to the primary
    # exception. Capture the actual propagated exception after that work finishes.
    return dict(_failure(error), type_module=type(error).__module__, type_qualname=type(error).__qualname__)


def _phase_receipts(directory, phase):
    """Require original lifecycle publications, not just this wrapper's copies."""
    def read(name):
        path = directory/name
        try:
            if path.is_symlink():
                raise ValueError('phase receipt must not be a symlink')
            return json.loads(path.read_text())
        except (OSError, ValueError) as error:
            raise ValueError('original phase receipt is missing or invalid: '+name) from error
    expected = redact_json_values(phase)
    if read('session.json') != expected:
        raise ValueError('original session receipt differs from the retained phase')
    query = expected['queries'][0]
    published = read('query-0.json')
    # The original query receipt precedes owner close. Late call receipts may
    # update only its backend accounting; require that final publication too.
    if ({k:v for k,v in published.items() if k != 'backend_report'} !=
            {k:v for k,v in query.items() if k != 'backend_report'}
            or read('owner-0/query-0/query-accounting-final.json') != query['backend_report']):
        raise ValueError('original query/final accounting receipts differ from the phase')
    ready = read('owner-ready-0.json')
    owner = expected['owners'][0]
    if (ready.get('session_id') != expected['session_id'] or ready.get('owner_id') != owner['owner_id']
            or ready.get('ready') != owner['ready']):
        raise ValueError('original readiness receipt differs from the phase owner')


def validate_cancel(phase, gate, *, raised):
    if (not isinstance(raised, dict) or raised.get('type_module') != 'builtins'
            or raised.get('type_qualname') != 'InterruptedError'
            or raised.get('error_type') != 'InterruptedError' or raised.get('notes') != []):
        raise ValueError('only an expected cancellation without additional failures permits recovery')
    model = _clean_phase(phase)
    state = gate.state(complete=True)
    aborted_calls = {state['binding'][key] for key in state['aborted']}
    if not gate.triggered or aborted_calls != gate.expected:
        raise ValueError('in-flight native cancellation was not covered; no recovery or retry permitted')
    query = phase['queries'][0]
    failures = (phase.get('failure') or {}, query.get('failure') or {})
    if (phase.get('status') != 'failed' or query.get('status') != 'failed'
            or query.get('checkpoint_failure')
            or any(f.get('error_type') != 'InterruptedError' or f.get('notes') != []
                   or f.get('error') != raised.get('error') for f in failures)):
        raise ValueError('the original query cancellation did not propagate')
    records = _records(phase)
    if {r['call_id'] for r in records} != gate.expected or len(records) != len(gate.expected):
        raise ValueError('cancelled call accounting is missing or duplicated')
    for row in records:
        native_key = next(key for key in state['aborted'] if state['binding'][key] == row['call_id'])
        if state['native_binding'][native_key] != row.get('chain_call_id', row['call_id']):
            raise ValueError('native and final ledger call identities differ')
        if row['status'] == 'unknown':
            if row.get('engine_terminal_confirmed') is not False or row.get('error_type') not in _EXPECTED_UNKNOWN:
                raise ValueError('unexpected unknown outcome; cancellation does not excuse other failures')
        elif row['status'] != 'succeeded':
            raise ValueError('unexpected failed call in the cancellation unit')
    if any(r['status'] == 'unknown' for r in records) and not (model.get('producer_retirement') or {}).get('confirmed'):
        raise ValueError('unknown resources lack exact-owner producer retirement')
    return dict(schema='semloom.movie_cancellation.v1', status='passed',
                native_api_inflight_call_ids=sorted(aborted_calls),
                expected_unknown_call_ids=sorted(r['call_id'] for r in records if r['status'] == 'unknown'),
                trigger_ns=gate.trigger_ns, trigger_clock='driver time.monotonic_ns; consumption boundary',
                native_clock='one model process event order; native API state, not proof of an executing GPU kernel',
                resource_retired=True, outcome_upgraded=False)


def run_movie_cancellation(plan, context, output, *, observer=None):
    from src.experiments import movie_records_session as session
    from src.baselines.text.movie_records import MovieRecordSource
    from src.experiments.movie_cancellation_probe import instrument
    checked = check_cancellation_plan(plan)
    bounds = context['cancellation_probe']
    root = Path(output)
    new_private_directory(root)
    trace = root / 'native-cancel'
    new_private_directory(trace)
    first = plan['queries'][0]
    expected = MovieRecordSource.from_manifest(first['manifest']).expected(first['query_id'])
    gate = NativeGate(trace, expected, first['query_id'], **bounds)
    report = dict(schema='semloom.movie_cancellation_run.v1', status='failed', queries=[],
                  performance_claim=False, automatic_retries=0, owner_reuse_after_cancel=False)
    query = session._MovieDriver.query
    close = session._MovieDriver.close
    def cancellable(driver, spec, emit):
        evaluate = driver.owner.evaluate
        gate.ready = lambda: driver.owner.model_ready
        with patch.object(driver.owner, 'evaluate', side_effect=lambda *a, **kw: evaluate(*a, **kw, cancelled=gate)):
            return query(driver, spec, emit)
    def phase_plan(index):
        value = deepcopy(plan)
        value['queries'] = [plan['queries'][index]]
        value['engine']['max_model_calls'] = checked['queries'][index]['expected_calls']
        return value
    def observed_close(driver):
        result = close(driver)
        used = driver.owner.owner._active.pool.used
        result['qualification_method_storage'] = dict(runs=used[0], logical_bytes=used[1],
                                                       scope='existing Method pool declarations; not measured RSS')
        return result
    def run_phase(index):
        with patch.object(session._MovieDriver, 'close', observed_close):
            return session.run_movie_session(phase_plan(index), context,
                root/('cancel' if index == 0 else 'recovery'), observer=observer)
    def preserve(index, phase):
        report['cancel_phase' if index == 0 else 'recovery_phase'] = phase
        if phase.get('queries'):
            report['queries'].append(phase['queries'][0])
            write_private_json(root/('query-'+str(index)+'.json'), phase['queries'][0])
    try:
        phase = None
        with instrument(trace), patch.object(session._MovieDriver, 'query', cancellable):
            try:
                phase = run_phase(0)
            except BaseException as error:
                report['cancel_exception'] = _exception_evidence(error)
                phase = getattr(error, 'session_report', None)
                if phase is not None:
                    preserve(0, phase)
                if type(error) is not InterruptedError or phase is None:
                    raise
        if 'cancel_phase' not in report:
            preserve(0, phase)
        qualification = validate_cancel(phase, gate, raised=report.get('cancel_exception'))
        _phase_receipts(root/'cancel', phase)
        report['cancellation_qualification'] = qualification
        try:
            recovery = run_phase(1)
        except BaseException as error:
            failed = getattr(error, 'session_report', None)
            if failed is not None:preserve(1, failed)
            raise
        preserve(1, recovery)
        _clean_phase(recovery)
        _phase_receipts(root/'recovery', recovery)
        recovered = _records(recovery)
        if recovery['status'] != 'completed' or len(recovered) != checked['queries'][1]['expected_calls'] or any(r['status'] != 'succeeded' for r in recovered):
            raise ValueError('the fresh recovery owner did not complete every declared call')
        cancel_uid = phase['owners'][0]['ready']['engine_uid']
        recovery_uid = recovery['owners'][0]['ready']['engine_uid']
        if cancel_uid == recovery_uid:
            raise ValueError('cancelled owner was reused instead of retired')
        report.update(status='completed', recovery_owner_new=True)
    except BaseException as error:
        report['error_type'] = type(error).__name__
        report['error'] = redact_text(str(error))
        report['failure'] = _exception_evidence(error)
        if not isinstance(error, Exception):
            raise
    finally:
        write_private_json(root/'session.json', report)
    return report


def verify_cancellation_cell(config, root, counts):
    """Guard-side recheck; model outcomes and conservative charges stay untouched."""
    from src.baselines.text.movie_records import MovieRecordSource
    bounds = qualification_options(config)
    if bounds is None:
        raise ValueError('ordinary cells cannot accept expected cancellation unknowns')
    checked = check_cancellation_plan(config['movie_plan'])
    root = Path(root)/'session'
    value = json.loads((root/'session.json').read_text())
    phase = value['cancel_phase']
    spec = config['movie_plan']['queries'][0]
    expected = MovieRecordSource.from_manifest(spec['manifest']).expected(spec['query_id'])
    gate = NativeGate(root/'native-cancel', expected, spec['query_id'], **bounds)
    gate.ready = lambda: phase['owners'][0]['ready']
    gate.trigger_ns = value['cancellation_qualification']['trigger_ns']
    gate.triggered = type(gate.trigger_ns) is int and gate.trigger_ns > 0
    qualification = validate_cancel(phase, gate, raised=value.get('cancel_exception'))
    recovery = value['recovery_phase']
    _clean_phase(recovery)
    records = _records(recovery)
    all_records = _records(phase) + records
    recovery_spec = config['movie_plan']['queries'][1]
    expected_recovery = MovieRecordSource.from_manifest(recovery_spec['manifest']).expected(recovery_spec['query_id'])
    expected_unknown = qualification['expected_unknown_call_ids']
    if (value['status'] != 'completed' or value.get('recovery_owner_new') is not True
            or recovery['status'] != 'completed' or any(r['status'] != 'succeeded' for r in records)
            or len(records) != checked['queries'][1]['expected_calls']
            or {r['call_id'] for r in records} != {r['call_id'] for r in expected_recovery}
            or phase['owners'][0]['ready']['engine_uid'] == recovery['owners'][0]['ready']['engine_uid']
            or len({r['call_id'] for r in all_records}) != checked['expected_calls']
            or counts.get('observed_records') != checked['expected_calls'] or counts.get('missing_claimed') != 0
            or counts.get('failed') != 0 or counts.get('unknown_records') != len(expected_unknown)
            or counts.get('succeeded') + len(expected_unknown) != checked['expected_calls']):
        raise ValueError('cancellation/recovery evidence does not reconcile with the unchanged call ledger')
    for index, item in enumerate((phase, recovery)):
        _phase_receipts(root/('cancel' if index == 0 else 'recovery'), item)
        published = json.loads((root/('query-'+str(index)+'.json')).read_text())
        if published != item['queries'][0]:
            raise ValueError('published call accounting differs from the verified phase evidence')
    return qualification
