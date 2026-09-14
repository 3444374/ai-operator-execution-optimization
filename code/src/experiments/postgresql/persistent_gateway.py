"""Finite sequential Map queries sharing one observed production gateway.

The group owns service startup/shutdown and one durable POST reservation. Each
query owns a PG connection and a projection of the unchanged group event files.
This is an experiment adapter, not a new scheduler or production query API.
"""
from contextlib import ExitStack
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import threading
import time

from src.baselines.common.private_artifacts import new_private_directory, write_private_json, open_private_text
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.experiments.process_sampling import ProcessSampler
from .cell_evidence import CellErrors
from .map_query_recording import record_pg_query
from .query_evaluation import evaluate, read_events
from .query_execution import pg_gateway_command, prepare_pg_query
from .query_inputs import QueryInputs
from .query_workloads import load_manifest
from .runtime_helpers import owned_child_process, wait_for_path


def complete_events(path, offset):
    """Read a bounded complete JSONL suffix; never advance past a partial write."""
    if not path.exists():
        return [], offset
    size = path.stat().st_size
    if size < offset or size - offset > 64 * 1048576:
        raise ValueError('event projection exceeds bounds or source was truncated')
    with path.open('rb') as source:
        source.seek(offset)
        data = source.read(size - offset)
    end = data.rfind(b'\n') + 1
    return [json.loads(line) for line in data[:end].splitlines()], offset + end


def settled_query(events, sessions):
    """Require one actual Job drain and the matching socket session termination."""
    drains = [e for e in events if e['event'] == 'core_job_drained']
    starts = [e for e in sessions if e['event'] == 'session_start']
    ends = [e for e in sessions if e['event'] == 'session_end']
    if max(len(drains), len(starts), len(ends)) > 1:
        raise ValueError('projection contains overlapping queries')
    if not drains or not starts or not ends:
        return False
    if (not isinstance(drains[0].get('usage'), dict) or not drains[0]['usage']
            or any(drains[0]['usage'].values()) or starts[0]['session_id'] != ends[0]['session_id']
            or ends[0]['termination'] != 'returned'):
        raise ValueError('query did not settle its Job and session')
    return True


class PersistentMapGateway:
    def __init__(self, config, *, plan, manifest_path, model_path, ledger, root, query_count):
        if (config.arm != 'pg' or config.task != 'map' or config.movie_id is not None
                or type(query_count) is not int or not 1 <= query_count <= 32):
            raise ValueError('persistent experiment requires finite full-scan PG Map queries')
        self.config, self.plan, self.ledger = config, plan, ledger
        self.manifest_path, self.model_path, self.root = map(Path, (manifest_path, model_path, root))
        self.manifest = load_manifest(self.manifest_path)
        if self.manifest['rows'] < 1 or load_fixed_model_config(self.model_path).model_id != plan.model_id:
            raise ValueError('persistent group requires nonempty input and the declared model')
        self.query_count, self.completed, self.attempted = query_count, [], 0
        self.offsets = {'events.jsonl': 0, 'sessions.jsonl': 0}
        self.lock, self.stack = threading.Lock(), ExitStack()
        self.failed, self.reserved, self.running = False, False, False
        self.summary = dict(status='failed', config=asdict(config), query_count=query_count,
            manifest_sha256=self.manifest['sha256'], semantic_reference_sha256=plan.digest,
            budget_scope='one group reservation; never refunded or independently reset per query')

    def __enter__(self):
        new_private_directory(self.root)
        started = time.monotonic_ns()
        try:
            self.ledger.reserve_unit(self.config.unit_id, self.manifest['rows'] * self.query_count)
            self.reserved = True
            command, self.socket = pg_gateway_command(self.config, self.plan, self.model_path, self.ledger, self.root)
            write_private_json(self.root/'gateway-command.json', command)
            env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[3])+os.pathsep+os.environ.get('PYTHONPATH',''))
            self.gateway = self.stack.enter_context(owned_child_process(command, self.root, 'gateway', env, None))
            wait_for_path(self.socket, self.gateway)
            self.running = True
            self.summary.update(gateway_pid=self.gateway.pid, startup_started_ns=started,
                startup_ready_ns=time.monotonic_ns())
            write_private_json(self.root/'startup.json', self.summary)
            return self
        except BaseException:
            self.__exit__(*__import__('sys').exc_info())
            raise

    def run_query(self, unit_id, *, dsn, pg_log, trace_flow=False, peer_pids=None):
        started = time.monotonic_ns()
        if not self.lock.acquire(blocking=False):
            raise ValueError('persistent experiment only supports sequential queries')
        try:
            if not self.running or self.failed or self.attempted >= self.query_count:
                raise ValueError('persistent group is closed, failed, or exhausted')
            config = replace(self.config, unit_id=unit_id)
            root = self.root/unit_id
            new_private_directory(root)
            self.attempted += 1
            errors = CellErrors()
            summary = dict(status='failed', config=asdict(config), manifest_sha256=self.manifest['sha256'],
                semantic_reference_sha256=self.plan.digest, query_preparation_started_ns=started,
                performance_qualified=False, budget_group=self.config.unit_id)
            try:
                manifest = load_manifest(self.manifest_path)
                if manifest != self.manifest:
                    raise ValueError('persistent input identity changed')
                inputs = QueryInputs(manifest['kind'], config.table, manifest['rows'],
                                     manifest['max_source_bytes'], manifest['max_input_bytes'], None)
                if config.organization_config is not None:
                    write_private_json(root/'organization.json', json.loads((self.root/'organization.json').read_text()))
                import psycopg
                with psycopg.connect(dsn, autocommit=True) as connection:
                    statement = prepare_pg_query(config, inputs, self.plan, connection, self.socket, root)
                    if trace_flow:
                        connection.execute('SET semloom_pg.test_flow_trace=on')
                        write_private_json(root/'flow-settings.json', dict(pause_input=-1,pause_ms=0,ready_first=False,
                            clock_source=time.get_clock_info('monotonic').implementation))
                    pids = dict(peer_pids or {}, consumer=os.getpid(), pg_backend=connection.info.backend_pid)
                    if self.gateway.pid not in pids.values():
                        pids['gateway'] = self.gateway.pid
                    with ProcessSampler(root/'query-rss.jsonl', pids) as sampler:
                        pg_log = Path(pg_log)
                        offset = pg_log.stat().st_size
                        try:
                            summary['execution'] = record_pg_query(connection, statement, root/'q0',
                                max_rows=inputs.max_rows, max_result_bytes=inputs.max_rows*70000,
                                flush_rows=64, query_timeout_s=config.query_timeout_s)
                        finally:
                            with pg_log.open('rb') as source, open_private_text(root/'q0-producer.log') as out:
                                source.seek(offset)
                                for line in source: out.write(line.decode())
                        self._project(root)
                    summary['resources'] = dict(processes=sampler.summary(), gateway_alive_after_query=self.gateway.poll() is None,
                        service_lifecycle=str(self.root/'summary.json'))
                if self.gateway.poll() is not None:
                    raise ValueError('persistent gateway exited between queries')
                summary['t_execution_cleanup_ns'] = time.monotonic_ns()
                summary['evaluation'] = evaluate(config, inputs, self.plan, self.manifest_path, root, None)
                if summary['evaluation']['actual_posts'] != self.manifest['rows']:
                    raise ValueError('query POST count differs from group allocation')
                summary['status'] = 'passed'
                self.completed.append(unit_id)
            except BaseException as failure:
                self.failed = True
                errors.record('query', failure)
            finally:
                summary.update(errors=errors.details, ended_ns=time.monotonic_ns())
                write_private_json(root/'summary.json', summary)
            errors.raise_if_failed()
            return summary
        finally:
            self.lock.release()

    def _project(self, root):
        start = dict(self.offsets)
        deadline = time.monotonic() + 10
        while True:
            events, end = complete_events(self.root/'events.jsonl', start['events.jsonl'])
            sessions, session_end = complete_events(self.root/'sessions.jsonl', start['sessions.jsonl'])
            if settled_query(events, sessions):
                break
            if self.gateway.poll() is not None or time.monotonic() >= deadline:
                raise TimeoutError('gateway did not persist a settled query projection')
            time.sleep(.01)
        config_events = [e for e in events if e['event'] == 'core_map_organization_config']
        if not self.completed:
            self.static_events = config_events
        elif config_events:
            raise ValueError('gateway organization unexpectedly initialized again')
        projected = events if not self.completed else self.static_events + events
        for name, records in (('events.jsonl', projected), ('sessions.jsonl', sessions)):
            with open_private_text(root/name) as out:
                for event in records: out.write(json.dumps(event)+'\n')
        self.offsets = {'events.jsonl': end, 'sessions.jsonl': session_end}
        write_private_json(root/'event-projection.json', dict(source_group=str(self.root), start_bytes=start,
            end_bytes=self.offsets, static_configuration_reference='first query interval in unchanged group events',
            added_reference_events=len(self.static_events) if self.completed else 0))

    def __exit__(self, kind, value, traceback):
        errors = CellErrors()
        if value is not None: errors.record('body', value)
        errors.attempt('gateway_close', self.stack.close)
        self.running = False
        if self.reserved:
            errors.attempt('budget_close', lambda: self.ledger.close_shared_unit(self.config.unit_id))
        self.summary.update(completed_queries=self.completed, attempted_queries=self.attempted,
            ended_ns=time.monotonic_ns(), gateway_exit=getattr(getattr(self, 'gateway', None), 'returncode', None),
            socket_removed=not getattr(self, 'socket', self.root/'g.sock').exists())
        def audit():
            events = read_events(self.root/'events.jsonl')
            closed = [e for e in events if e['event']=='core_transport_closed']
            self.summary['transport_close'] = closed
            if (len(closed) != 1 or not closed[0].get('closed') or any(closed[0]['usage'].values())
                    or self.summary['gateway_exit'] != 0 or not self.summary['socket_removed']
                    or len(self.completed) != self.query_count or self.failed
                    or sum(e['event']=='request' for e in events) != self.manifest['rows']*self.query_count
                    or sum(e['event']=='core_job_drained' for e in events) != self.query_count):
                raise ValueError('persistent group did not complete and close cleanly')
            self.summary['status'] = 'passed'
        if value is None: errors.attempt('group_audit', audit)
        self.summary['errors'] = errors.details
        errors.attempt('summary_write', lambda: write_private_json(self.root/'summary.json', self.summary))
        if value is None: errors.raise_if_failed()
        return False
