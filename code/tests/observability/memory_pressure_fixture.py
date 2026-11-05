"""Temporary kernel-shaped files; no writes to the actual cgroup filesystem."""
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from src.observability.metrics.memory_pressure import CgroupMemoryPressureSource


class KernelMemoryFixture:
    def __enter__(self):
        self.cleanup = ExitStack()
        self.root = Path(self.cleanup.enter_context(TemporaryDirectory()))
        self.mount = self.root/'cgroup'
        self.leaf = self.mount/'actor'
        self.leaf.mkdir(parents=True)
        self.proc = self.root/'proc'
        self.proc.mkdir()
        (self.proc/'cgroup').write_text('0::/actor\n')
        (self.proc/'mountinfo').write_text(f'1 0 0:1 / {self.mount} rw - cgroup2 cgroup rw\n')
        self.write(self.mount)
        self.write(self.leaf)
        (self.leaf/'memory.max').write_text('max\n')
        self.now = 10000.
        self.cleanup.enter_context(patch('src.infrastructure.cgroup.sys', SimpleNamespace(platform='linux')))
        self.source = CgroupMemoryPressureSource(proc_root=self.proc, clock=self.clock)
        return self

    def clock(self):
        self.now += .125
        return self.now

    @staticmethod
    def write(directory, *, some=0, full=0, **events):
        (directory/'memory.pressure').write_text(
            f'some avg10=0.00 avg60=0.00 avg300=0.00 total={some}\n'
            f'full avg10=0.00 avg60=0.00 avg300=0.00 total={full}\n')
        (directory/'memory.events').write_text(''.join(
            f'{name} {events.get(name, 0)}\n' for name in ('low','high','max','oom','oom_kill','oom_group_kill')))

    def __exit__(self, *error):
        self.cleanup.close()


def resource_packet(value, *, engine='CPU-engine', session=7):
    return dict(engine_uid=engine, session_id=session, actor_pid=value['pid'],
        captured_at_s=value.get('observed_at_s', 10000.)+.03125,
        clock_domain='private_child_and_model_actor.monotonic',
        sample=dict(sources=dict(host=dict(memory_pressure=value)), values={}))
