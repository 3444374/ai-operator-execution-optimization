"""A finite owner for optional CPU preparation of already known image bytes.

Only RGB preparation runs here. Semantic calls and consumer eligibility remain
with the caller. Pending-item capacity is not a process-memory guarantee.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import time

from .representations import decode_rgb


@dataclass(frozen=True)
class PreparedImage:
    size: tuple[int, int]
    pixels: bytes
    wall_ns: int
    cpu_ns: int


def prepare_rgb(encoded):
    wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
    size, pixels = decode_rgb(encoded)
    return PreparedImage(size, pixels, time.perf_counter_ns()-wall, time.thread_time_ns()-cpu)


def preparation_value(probability, prepare_ns, overlap_ns, opportunity_ns):
    saved = probability * min(prepare_ns, overlap_ns)
    wasted = (1-probability) * prepare_ns
    return dict(prepare=saved > wasted + opportunity_ns,
                expected_hidden_ns=saved, expected_unused_ns=wasted, opportunity_ns=opportunity_ns)


class ImagePreparationAhead:
    def __init__(self, *, max_pending, workers):
        if type(max_pending) is not int or max_pending < 1:
            raise ValueError('positive pending preparation capacity required')
        self.executor = ThreadPoolExecutor(max_workers=workers)
        self.max_pending, self.pending, self.closed = max_pending, {}, False
        self.counts = dict(submitted=0, consumed=0, discarded=0, cancelled_before_start=0,
                           consumed_cpu_ns=0, unused_cpu_ns=0, consume_wait_ns=0)
        self.discard_errors = []

    def submit(self, key, encoded):
        if key in self.pending:
            raise ValueError('preparation occurrence is already pending')
        if len(self.pending) >= self.max_pending:
            return False
        self.pending[key] = self.executor.submit(prepare_rgb, encoded)
        self.counts['submitted'] += 1
        return True

    def take(self, key):
        future = self.pending[key]
        started = time.perf_counter_ns()
        try:
            result = future.result()
        finally:
            del self.pending[key]
            self.counts['consume_wait_ns'] += time.perf_counter_ns()-started
        self.counts['consumed'] += 1
        self.counts['consumed_cpu_ns'] += result.cpu_ns
        return result

    def discard(self, key):
        future = self.pending[key]
        if future.cancel():
            self.counts['cancelled_before_start'] += 1
        else:
            try:
                self.counts['unused_cpu_ns'] += future.result().cpu_ns
            except Exception as error:
                # Failed work on an unchosen branch has no semantic consumer.
                self.discard_errors.append(type(error).__name__)
        del self.pending[key]
        self.counts['discarded'] += 1

    def close(self):
        if self.closed:
            return
        for key in tuple(self.pending):
            self.discard(key)
        self.executor.shutdown(wait=True)
        self.closed = True

    def snapshot(self):
        return dict(self.counts, pending=len(self.pending), closed=self.closed,
                    discard_errors=list(self.discard_errors))
