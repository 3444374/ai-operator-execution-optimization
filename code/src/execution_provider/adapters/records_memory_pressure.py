"""Bind an independent kernel interval to its actual model-actor RPC and query."""
import math
from threading import Lock

from src.observability.metrics.memory_pressure import MEMORY_CLOCK, MEMORY_PRESSURE_SCHEMA
from .records_native_source import PRIVATE_CLOCK


def _time(value):
    try:
        return type(value) in (int, float) and math.isfinite(value) and value >= 0
    except OverflowError:
        return False


class MemoryPressureBinding:
    """One query's safety source; RPCs cannot refresh a cached source sample."""
    def __init__(self, engine_uid, session_id, *, started_at_s, max_age_s):
        if (type(engine_uid) is not str or not engine_uid or type(session_id) is not int or session_id < 0
                or not _time(started_at_s) or not _time(max_age_s) or not max_age_s > 0):
            raise ValueError('memory feedback requires the actual query identity and finite source age')
        self.engine_uid, self.session_id = engine_uid, session_id
        self.started_at_s, self.max_age_s = started_at_s, max_age_s
        self._lock = Lock()
        self._uid = self._hierarchy = self._sequence = self._payload = self._mapped = None
        self._rejected = False
        self._current = None
        self._reason = 'not_sampled'

    def observe(self, packet, *, requested_at_s, received_at_s):
        with self._lock:
            self._current = None
            try:
                if (type(packet) is not dict or packet.get('engine_uid') != self.engine_uid
                        or type(packet.get('session_id')) is not int or packet['session_id'] != self.session_id
                        or packet.get('clock_domain') != PRIVATE_CLOCK
                        or not _time(requested_at_s) or not _time(received_at_s) or requested_at_s > received_at_s):
                    raise ValueError('actor_or_query_not_bound')
                sample = packet.get('sample')
                sources = sample.get('sources') if type(sample) is dict else None
                host = sources.get('host') if type(sources) is dict else None
                value = host.get('memory_pressure') if type(host) is dict else None
                if (type(value) is not dict or value.get('schema') != MEMORY_PRESSURE_SCHEMA
                        or value.get('clock_domain') != MEMORY_CLOCK
                        or type(value.get('pid')) is not int or value['pid'] <= 0
                        or type(packet.get('actor_pid')) is not int or value['pid'] != packet['actor_pid']
                        or type(value.get('source_uid')) is not str or not value['source_uid']
                        or type(value.get('hierarchy_uid')) is not str or not value['hierarchy_uid']
                        or type(value.get('source_sequence')) is not int or value['source_sequence'] < 1
                        or not _time(value.get('observed_at_s')) or not _time(packet.get('captured_at_s'))
                        or value['observed_at_s'] > packet['captured_at_s']):
                    raise ValueError('kernel_source_not_bound')
                uid, hierarchy, sequence = value['source_uid'], value['hierarchy_uid'], value['source_sequence']
                if self._uid is None:
                    self._uid, self._hierarchy = uid, hierarchy
                elif (uid, hierarchy) != (self._uid, self._hierarchy):
                    self._rejected = True
                if self._rejected:
                    raise ValueError('kernel_source_changed_in_query')
                pressure = value.get('pressure')
                status = value.get('status')
                if status == 'observed':
                    interval, start = value.get('interval_s'), value.get('interval_started_at_s')
                    if (type(pressure) is not bool or not _time(interval) or not 0 < interval <= self.max_age_s
                            or not _time(start) or value['observed_at_s']-start != interval):
                        raise ValueError('kernel_interval_unknown')
                elif (pressure is not None or type(status) is not str
                        or value.get('interval_s') is not None or value.get('interval_started_at_s') is not None):
                    raise ValueError('invalid_kernel_source_state')
                payload = (value['observed_at_s'], status, pressure,
                           value.get('interval_started_at_s'), value.get('interval_s'))
                if self._sequence is not None and sequence < self._sequence:
                    raise ValueError('out_of_order_kernel_sample')
                if sequence == self._sequence:
                    if payload != self._payload:
                        self._rejected = True
                        raise ValueError('conflicting_kernel_sample')
                    mapped = self._mapped
                else:
                    if self._payload is not None and value['observed_at_s'] <= self._payload[0]:
                        raise ValueError('kernel_source_time_did_not_advance')
                    mapped = requested_at_s-(packet['captured_at_s']-value['observed_at_s'])
                    if not _time(mapped) or mapped < self.started_at_s:
                        raise ValueError('kernel_sample_predates_query')
                    self._sequence, self._payload, self._mapped = sequence, payload, mapped
                self._current = dict(safety_sequence=sequence, safety_observed_at_s=mapped,
                    memory_pressure=pressure)
                self._reason = status
            except (KeyError, TypeError, ValueError, OverflowError) as error:
                self._reason = str(error) if type(error) is ValueError else 'kernel_frame_unavailable'

    def permit_fields(self, engine_uid, session_id):
        with self._lock:
            if engine_uid != self.engine_uid or session_id != self.session_id:
                return {}
            return dict(self._current or {})

    def report(self):
        with self._lock:
            return dict(enabled=True, status=self._reason, source_uid=self._uid,
                engine_uid=self.engine_uid, session_id=self.session_id,
                hierarchy_uid=self._hierarchy, sample=None if self._current is None else dict(self._current),
                scope='model actor visible cgroup hierarchy; not driver/Ray RSS or physical capacity')
