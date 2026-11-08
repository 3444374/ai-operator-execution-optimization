"""Read kernel memory stalls and reclaim events; never acquire or grant RAM."""
import hashlib
import math
import os
from pathlib import Path
from threading import Lock
import time
import uuid

from src.infrastructure.cgroup import controller_hierarchy, read_optional


MEMORY_PRESSURE_SCHEMA = 'semloom.cgroup_memory_pressure.v1'
MEMORY_CLOCK = 'process.time.monotonic seconds'
_EVENTS = ('low', 'high', 'max', 'oom', 'oom_kill', 'oom_group_kill')


def _counter(value):
    if not value.isascii() or not value.isdigit():
        raise ValueError('kernel memory counter must be a non-negative integer')
    return int(value)


def _pairs(lines):
    result = {}
    for line in lines.splitlines():
        key, raw = line.split()
        if key in result:
            raise ValueError('duplicate kernel memory counter')
        result[key] = _counter(raw)
    return result


def _pressure(lines):
    result = {}
    for line in lines.splitlines():
        name, *raw = line.split()
        if name not in ('some', 'full') or name in result:
            raise ValueError('invalid kernel memory pressure row')
        fields = dict(field.split('=', 1) for field in raw)
        if len(fields) != len(raw):
            raise ValueError('duplicate kernel memory pressure field')
        result[name] = _counter(fields['total'])
    if set(result) != {'some', 'full'} or result['full'] > result['some']:
        raise ValueError('incomplete kernel memory pressure totals')
    return result


def _capture(proc_root):
    version, directories = controller_hierarchy('memory', proc_root=proc_root)
    if version != 2 or not directories:
        raise ValueError('cgroup v2 memory pressure is unavailable')
    identity, counters = [], []
    for level, directory in enumerate(directories):
        stat = directory.stat()
        identity.append((stat.st_dev, stat.st_ino))
        pressure = _pressure((directory/'memory.pressure').read_text())
        events = read_optional(directory/'memory.events')
        if events is None:
            # The actual hierarchy root has PSI but no memory controller files.
            if level != len(directories)-1 or read_optional(directory/'memory.max') is not None:
                raise ValueError('cgroup memory events are unavailable')
            events = {}
        else:
            events = _pairs(events)
            if not set(_EVENTS[:-1]) <= events.keys():
                raise ValueError('incomplete cgroup memory events')
        counters.append(dict(psi_some_us=pressure['some'], psi_full_us=pressure['full'],
            **{'events_'+key: events[key] for key in _EVENTS if key in events}))
    after_version, after_directories = controller_hierarchy('memory', proc_root=proc_root)
    after = tuple((p.stat().st_dev, p.stat().st_ino) for p in after_directories)
    if after_version != version or tuple(identity) != after:
        raise ValueError('cgroup memory membership changed during observation')
    return tuple(identity), tuple(counters)


class CgroupMemoryPressureSource:
    """One locked interval source for the current process's visible hierarchy.

    A false value means no observed stall/event increment in that interval. It
    proves neither free capacity nor absence of future allocation failures.
    First reads, errors, migration and counter reversals stay unknown.
    """
    def __init__(self, *, proc_root=Path('/proc/self'), clock=time.monotonic):
        self.proc_root, self.clock = proc_root, clock
        self._lock = Lock()
        self._uid = uuid.uuid4().hex
        self._pid = os.getpid()
        self._sequence = 0
        self._previous = None

    def __call__(self):
        with self._lock:
            self._sequence += 1
            value = dict(schema=MEMORY_PRESSURE_SCHEMA, source_uid=self._uid,
                source_sequence=self._sequence, pid=os.getpid(), pressure=None, status='unavailable',
                clock_domain=MEMORY_CLOCK,
                scope='current process visible cgroup hierarchy; may include siblings; no RAM grant or RSS ceiling')
            try:
                before = self.clock()
                identity, counters = _capture(self.proc_root)
                observed = self.clock()
                if (type(before) not in (int, float) or type(observed) not in (int, float)
                        or not math.isfinite(before) or not math.isfinite(observed) or not 0 <= before <= observed):
                    raise ValueError('invalid memory source clock')
                value.update(observed_at_s=observed, read_started_at_s=before,
                    hierarchy_uid=hashlib.sha256(repr(identity).encode()).hexdigest(),
                    counters=[dict(row) for row in counters])
                previous = self._previous
                self._previous = (identity, counters, observed)
                changed = os.getpid() != self._pid or previous is not None and previous[0] != identity
                if changed:
                    self._pid = os.getpid()
                    self._uid = uuid.uuid4().hex
                    value['source_uid'] = self._uid
                    value['status'] = 'membership_changed'
                elif previous is None:
                    value['status'] = 'baseline_priming'
                elif before <= previous[2]:
                    raise ValueError('memory source interval is nonpositive or overlaps')
                elif any(set(old) != set(new) or any(new[key] < old[key] for key in old)
                        for old, new in zip(previous[1], counters)):
                    self._uid = value['source_uid'] = uuid.uuid4().hex
                    value['status'] = 'counter_reversed_or_shape_changed'
                else:
                    increments = [{key: new[key]-old[key] for key in new}
                        for old, new in zip(previous[1], counters)]
                    value.update(status='observed', pressure=any(any(row.values()) for row in increments),
                        interval_started_at_s=previous[2], interval_s=observed-previous[2], increments=increments)
            except (OSError, ValueError, TypeError, KeyError, OverflowError) as error:
                self._previous = None
                value.update(status='unavailable:'+type(error).__name__, pressure=None)
            return value
