"""Event-stepped CPU service for Movie policy wiring tests, never a benchmark.

One ready cohort costs one declared logical service tick. Actual native frames,
Core receipts and result IPC still run; their polling cadence cannot add ticks.
The private socket releases exact request IDs, never controller decisions.
"""
import asyncio
from contextlib import contextmanager
import json
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

from src.serving.backends.async_engine_process import AsyncEngineProcess, serve
from src.serving.backends.vllm_async_engine import NativeStats
from tests.serving.resource_feedback_fixture import FixtureEngine as NativeEngine


class ServiceCohorts:
    """Single driver thread owns the service clock and the receipt barrier."""

    def __init__(self, path):
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(str(path))
        self.server.listen(1)
        self.server.setblocking(False)
        self.peer = None
        self.input = b''
        self.output = b''
        self.now = 0.
        self.waiting = {}
        self.released = set()
        self.pending = ()
        self.history = []
        self.receipts = set()

    def poll(self):
        if self.peer is None:
            try:
                self.peer, _ = self.server.accept()
                self.peer.setblocking(False)
            except BlockingIOError:
                return
        while True:
            try:
                value = self.peer.recv(65536)
            except BlockingIOError:
                break
            if not value:
                break
            self.input += value
        while b'\n' in self.input:
            line, self.input = self.input.split(b'\n', 1)
            event = json.loads(line)
            request = event['request_id']
            if request in self.waiting or request in self.released:
                raise AssertionError('CPU service request started twice')
            self.waiting[request] = event['sequence']
        while self.output:
            try:
                sent = self.peer.send(self.output)
            except BlockingIOError:
                break
            if not sent:
                raise AssertionError('CPU service release channel closed')
            self.output = self.output[sent:]

    def advance(self, *, receipts, inflight, target, source_sequence):
        self.released.difference_update(receipts)
        # A partially delivered cohort is not a new service interval. Wait for
        # its real receipt prefix, then for all currently admitted members to
        # reach their gates. Shrink transitions can drain more than target.
        if (self.pending or self.released or not self.waiting
                or len(self.waiting) != inflight or inflight < target
                or source_sequence is None
                or source_sequence < max(self.waiting.values())):
            return False
        self.pending = tuple(self.waiting)
        self.now += 1
        self.history.append(dict(at_s=self.now, requests=list(self.pending),
            delivered_receipts=len(receipts), source_sequence=source_sequence))
        return True

    def release(self):
        if not self.pending:
            return
        requests, self.pending = self.pending, ()
        for request in requests:
            del self.waiting[request]
        self.released.update(requests)
        self.output += (json.dumps(dict(at_s=self.now, requests=requests))+'\n').encode()
        self.poll()

    def close(self):
        if self.peer is not None:
            self.peer.close()
        self.server.close()


class EventEngine(NativeEngine):
    """Same CPU replies/work counters, released by service events, no sleeps."""

    def __init__(self, spec, generation, *, limits, path):
        super().__init__(spec, generation, limits=limits)
        self.service_time = 0.
        self.stats = NativeStats(self.engine_uid, clock=lambda: self.service_time)
        self.emit()
        self.connected = asyncio.Event()
        self.control_error = None
        self.control = asyncio.create_task(self.listen(path))

    async def listen(self, path):
        try:
            reader, self.writer = await asyncio.open_unix_connection(path)
            self.connected.set()
            while line := await reader.readline():
                value = json.loads(line)
                self.service_time = value['at_s']
                for request in value['requests']:
                    gate = self.gates.get(request)
                    if gate is not None:
                        gate.set()  # An abort may already have drained it.
            if not self.closed:
                raise RuntimeError('CPU service event channel closed before engine close')
        except Exception as error:
            self.control_error = error
            self.connected.set()
            for gate in self.gates.values():
                gate.set()

    async def generate_prepared(self, row, **options):
        from tests.serving.async_engine_fixture import FixtureEngine as Replies
        await self.connected.wait()
        if self.control_error is not None:
            raise self.control_error
        request = options['request_id']
        gate = self.gates[request] = asyncio.Event()
        self.running += 1
        self.emit()
        self.writer.write((json.dumps(dict(request_id=request,
            sequence=self.stats.sequence))+'\n').encode())
        await self.writer.drain()
        try:
            await gate.wait()
            if self.control_error is not None:
                raise self.control_error
            value = await Replies.generate_prepared(self, row, **options)
            value.update(text='True' if row['source_position'] % 2 == 0 else 'False',
                prompt_cached_tokens=0, prompt_computed_tokens=value['prompt_tokens'])
            return value
        finally:
            self.gates.pop(request, None)
            self.running -= 1
            self.emit(finished=True)

    async def close(self, timeout=10):
        result = await super().close(timeout=timeout)
        self.control_error = RuntimeError('CPU service engine closed')
        self.connected.set()
        self.control.cancel()
        try:
            await self.control
        except asyncio.CancelledError:
            pass
        if hasattr(self, 'writer'):
            self.writer.close()
            await self.writer.wait_closed()
        return result


@contextmanager
def logical_request_frontier():
    from src.execution_provider.adapters import lotus_chain_token_batches as batches
    from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
    from src.scheduling.core.models import AdmissionObservation
    from src.scheduling.submission_control.request_scan import AscendingRequestController
    from tests.experiments.movie_token_resident_fixture import with_actual_cpu_pid

    with tempfile.TemporaryDirectory(prefix='movie-service-', dir='/tmp') as directory:
        path = str(Path(directory)/'events.sock')
        service = ServiceCohorts(path)

        class Process(AsyncEngineProcess):
            @staticmethod
            def _command(spec, descriptors):
                return [sys.executable, str(Path(__file__).resolve()),
                        *map(str, descriptors), path]

        class Frontier:
            def __init__(self, sampler, **kwargs):
                self.sampler = sampler
                self.enabled = self.is_running = True

            def latest(self, inflight):
                service.poll()
                feedback = self.sampler.__self__
                snapshot = self.sampler()
                remaining = feedback.owner._expected_count-len(service.receipts)
                fresh = service.advance(receipts=service.receipts, inflight=inflight,
                    target=min(feedback._window(), remaining),
                    source_sequence=None if snapshot is None else snapshot.source_sequence)
                return AdmissionObservation(service.now, fresh, inflight,
                    None if snapshot is None else snapshot.running,
                    None if snapshot is None else snapshot.waiting,
                    None if snapshot is None else snapshot.kv_usage,
                    sample_age_s=None if snapshot is None else service.now-snapshot.source_observed_at_s,
                    preemptions_total=None if snapshot is None else snapshot.preemptions_total)

            def pause(self): self.enabled = False
            def resume(self):
                self.enabled = True
                # A successful previous query already drained these real
                # requests. Its receipt dictionary is deliberately per-query.
                if service.waiting or service.pending:
                    raise AssertionError('CPU service query left gated requests')
                service.released.clear()
            def close(self): self.is_running = False

        original = batches.BatchSupplyFeedback.update
        def at_frontier(feedback, stream):
            feedback.next_poll = 0
            with feedback.owner._receipt_lock:
                service.receipts = {v['engine_request_id'] for k, v in feedback.owner._raw_receipts.items()
                    if k.session_id == feedback.session_id and v.get('status') == 'succeeded'}
            try:
                return original(feedback, stream)
            finally:
                # Let the unmodified controller observe the completed prefix
                # before any member of this next cohort can return a result.
                service.release()

        try:
            with patch.object(batches, 'time', SimpleNamespace(monotonic=lambda: service.now)), \
                    patch('src.execution_provider.adapters.records_native_sampler.time',
                        SimpleNamespace(monotonic=lambda: service.now)), \
                    patch('src.scheduling.submission_control.request_scan.AscendingRequestController',
                        lambda maximum, config: AscendingRequestController(
                            maximum, config, clock=lambda: service.now)), \
                    patch('src.scheduling.runtime.observations.NonBlockingMetricsObservationProvider', Frontier), \
                    patch.object(batches.BatchSupplyFeedback, 'update', at_frontier), \
                    patch.object(AsyncCoreModelActor, '_process_type', return_value=with_actual_cpu_pid(Process)):
                yield service
        finally:
            service.close()


if __name__ == '__main__':
    asyncio.run(serve(*map(int, sys.argv[1:4]),
        engine_factory=lambda *args, **kwargs: EventEngine(*args, **kwargs, path=sys.argv[4])))
