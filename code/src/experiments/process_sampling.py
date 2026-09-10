"""Bounded disk-only process RSS sampling; labels distinguish query and evaluation."""
import os
from pathlib import Path
import threading
import time

from src.baselines.common.private_artifacts import open_private_text
import json


class ProcessSampler:
    def __init__(self, path: Path, pids, *, interval=.2, max_samples=40000):
        import psutil
        if interval < .05 or max_samples < 1:
            raise ValueError('invalid sample limits')
        self.processes = {label: psutil.Process(pid) for label, pid in pids.items()}
        self.interval, self.limit, self.path = interval, max_samples, path
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
                    values = {}
                    for label, process in self.processes.items():
                        try:
                            values[label] = process.memory_info().rss
                        except psutil.NoSuchProcess:
                            values[label] = None
                    for label, value in values.items():
                        if value is not None:
                            key = phase + ':' + label
                            self.peaks[key] = max(self.peaks.get(key, 0), value)
                    stream.write(json.dumps(dict(monotonic_ns=time.monotonic_ns(), phase=phase,
                                                 rss_bytes=values)) + '\n')
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
                    scope='sampled process RSS, separate from logical reservation; sub-interval peaks may be missed')
