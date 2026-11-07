"""Bounded disk-only process RSS sampling; labels distinguish query and evaluation."""
import os
from pathlib import Path
import threading
import time

from src.baselines.common.private_artifacts import open_private_text
import json


class ProcessSampler:
    def __init__(self, path: Path, pids, *, interval=.2, max_samples=40000,
                 include_children=False, pid_provider=None):
        import psutil
        if interval < .05 or max_samples < 1:
            raise ValueError('invalid sample limits')
        self.processes = {label: psutil.Process(pid) for label, pid in pids.items()}
        self.interval, self.limit, self.path = interval, max_samples, path
        self.include_children, self.pid_provider = include_children, pid_provider
        self.identities, self.missing = {}, set()
        self.phase, self.error = 'preparation', None
        self.stop = threading.Event()
        self.count = 0
        self.peaks = {}

    def __enter__(self):
        self.thread = threading.Thread(target=self._run, name='experiment-rss', daemon=True)
        self.thread.start()
        return self

    def _run(self):
        import psutil
        try:
            with open_private_text(self.path) as stream:
                while not self.stop.is_set():
                    if self.count >= self.limit:
                        raise ValueError('resource sample limit reached')
                    phase = self.phase
                    current = dict(self.processes)
                    if self.include_children:
                        for process in self.processes.values():
                            try:
                                for child in process.children(recursive=True):
                                    current['child:'+str(child.pid)] = child
                            except psutil.NoSuchProcess:
                                pass
                    if self.pid_provider is not None:
                        for label, value in self.pid_provider().items():
                            pid, created = value
                            try:
                                process = psutil.Process(pid)
                                if process.create_time() != created:
                                    self.missing.add(label)
                                    continue
                                current[label] = process
                            except psutil.NoSuchProcess:
                                self.missing.add(label)
                    values, cpu = {}, {}

                    for label, process in current.items():
                        try:
                            created = process.create_time()
                            identity = str(process.pid)+':'+str(created)
                            if identity not in self.identities and len(self.identities)>=4096:
                                raise ValueError('resource process identity limit reached')
                            self.identities[identity] = dict(pid=process.pid,created_at=created,
                                name=process.name(),label=label)
                            values[label] = process.memory_info().rss
                            times = process.cpu_times()
                            cpu[label] = times.user+times.system
                        except psutil.NoSuchProcess:
                            values[label] = None
                    for label, value in values.items():
                        if value is not None:
                            key = phase + ':' + label
                            self.peaks[key] = max(self.peaks.get(key, 0), value)
                    stream.write(json.dumps(dict(monotonic_ns=time.monotonic_ns(), phase=phase,
                                                 rss_bytes=values,cpu_seconds=cpu,
                                                 summed_rss_bytes=sum(v for v in values.values() if v is not None))) + '\n')
                    self.count += 1
                    self.stop.wait(self.interval)
        except BaseException as error:
            self.error = error

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(5)
        if self.thread.is_alive():
            raise TimeoutError('resource sampler did not stop')
        if self.error:
            raise self.error

    def summary(self):
        return dict(samples=self.count, interval_seconds=self.interval, peak_rss_bytes=self.peaks,
                    processes=self.identities,discovered_but_unobserved=sorted(
                        self.missing.difference(value['label'] for value in self.identities.values())),
                    include_children=self.include_children,
                    summed_rss_is_unique_physical_memory=False,
                    scope='sampled process RSS, separate from logical reservation; sub-interval peaks may be missed')
