"""Inherited private pipes carry NativeTaskSession calls to one Core owner.

Only trusted child processes created by the caller receive these pipe ends. The
bounded pickle frames are not a network protocol or an untrusted-file decoder.
Each reader has one pending RPC; blocked pipe I/O never runs on the Core owner.
"""

from concurrent.futures import Future, TimeoutError
from dataclasses import asdict, dataclass, field, replace
import multiprocessing as mp
from multiprocessing.connection import wait
import pickle
import queue
import threading
import time

from .native_tasks import NativeTaskSession


MAX_BRIDGE_FRAME_BYTES = 8 * 1024 * 1024


def _encode(value):
    raw = pickle.dumps(value, protocol=5)
    if len(raw) > MAX_BRIDGE_FRAME_BYTES:
        raise ValueError('native bridge frame exceeds byte limit')
    return raw


@dataclass
class _Peer:
    label: str
    connection: object
    client_connection: object
    expected_tasks: int | None
    flow: object = None
    opened: bool = False
    closed: bool = False
    accepted: int = 0
    cancel: threading.Event = field(default_factory=threading.Event)
    disconnected: threading.Event = field(default_factory=threading.Event)
    stopped: threading.Event = field(default_factory=threading.Event)
    reader: object = None
    watcher: object = None


class NativeTaskBridge:
    """One flow per independently registered query; all Core calls stay on owner.

    The execution factory runs inside the owner thread. A process sentinel is
    required to reap delivery leases after unexpected EOF; EOF alone only asks
    for cancellation. Accepted remote compute survives either signal until its
    authoritative terminal. Normal close retains NativeTaskSession's consumer
    promise that delivery bytes have been discarded/released.
    """

    def __init__(self, execution_factory, queries, *, poll_s=.005, observer=None, context=None):
        if not queries or len(set(queries)) != len(queries):
            raise ValueError('bridge queries must have distinct identities')
        if any(type(k) is not str or not k or type(v) not in (int, type(None))
               or (v is not None and v < 0) for k, v in queries.items()):
            raise ValueError('invalid query declaration')
        if not 0 < poll_s <= .1:
            raise ValueError('invalid owner poll interval')
        context = context or mp.get_context('spawn')
        self._peers = {}
        for label, count in queries.items():
            owner, client = context.Pipe(duplex=True)
            self._peers[label] = _Peer(label, owner, client, count)
        self._factory, self._poll_s, self._observer = execution_factory, poll_s, observer
        self._requests = queue.Queue(maxsize=len(queries) + 1)
        self._ready, self._stop, self._finished = (threading.Event() for _ in range(3))
        self._startup_error = self._runtime_error = None
        self._last_snapshot = None
        self._execution = None
        self._owner_id = None
        self._thread = threading.Thread(target=self._run, name='native-query-core-owner', daemon=True)

    def start(self):
        self._thread.start()
        if not self._ready.wait(10) or self._startup_error is not None:
            raise RuntimeError('native bridge startup failed') from self._startup_error
        for peer in self._peers.values():
            peer.reader = threading.Thread(target=self._read, args=(peer,), daemon=True,
                                           name='native-query-pipe-' + peer.label)
            peer.reader.start()
        return self

    def client_connection(self, query_id):
        return self._peers[query_id].client_connection

    def bind_process(self, query_id, process):
        peer = self._peers[query_id]
        if peer.watcher is not None or process.pid is None:
            raise ValueError('bind one started frontend process per query')
        def stopped():
            wait([process.sentinel])
            peer.stopped.set()
            peer.cancel.set()
        peer.watcher = threading.Thread(target=stopped, daemon=True, name='native-query-stop-proof')
        peer.watcher.start()

    def request_cancel(self, query_id):
        self._peers[query_id].cancel.set()

    def _read(self, peer):
        expected = 1
        try:
            while not self._finished.is_set():
                raw = peer.connection.recv_bytes(MAX_BRIDGE_FRAME_BYTES)
                request = pickle.loads(raw)
                if (type(request) is not tuple or len(request) != 3 or type(request[0]) is not int
                        or request[0] != expected or type(request[1]) is not str
                        or type(request[2]) is not tuple):
                    raise ValueError('invalid native bridge request')
                future = Future()
                self._requests.put_nowait((peer, request[1], request[2], future))
                del raw, request
                while True:
                    try:
                        reply = future.result(timeout=.1)
                        break
                    except TimeoutError:
                        if self._finished.is_set():
                            raise RuntimeError('native bridge owner stopped')
                peer.connection.send_bytes(_encode((expected, *reply)))
                expected += 1
                del reply, future
        except (EOFError, OSError, ValueError, RuntimeError, queue.Full, pickle.UnpicklingError):
            peer.disconnected.set()
            peer.cancel.set()
        finally:
            peer.connection.close()

    def _emit(self, event, peer=None):
        if self._observer:
            self._observer(dict(event=event, query_id=peer.label if peer else None,
                                owner_thread=self._owner_id, t=time.monotonic()))

    def _invoke(self, peer, operation, args):
        if operation == 'open':
            query_id, operator_id = args
            if query_id != peer.label or peer.opened or peer.cancel.is_set():
                raise ValueError('query connection cannot open that flow')
            peer.flow = NativeTaskSession(self._execution, peer.label, operator_id)
            peer.opened = True
            self._emit('query_open', peer)
            return peer.flow.limits
        if not peer.opened or peer.closed:
            raise RuntimeError('native query flow is unavailable')
        flow = peer.flow
        if operation == 'offer':
            if len(args) != 1:
                raise ValueError('invalid offer arguments')
            tasks = args[0]
            if peer.expected_tasks is not None and peer.accepted + len(tasks) > peer.expected_tasks:
                raise ValueError('offer exceeds declared query work')
            result = flow.offer(tasks)
            peer.accepted += result.accepted_prefix_count
            return result
        if operation == 'advance':
            if len(args) != 1 or type(args[0]) is not int or args[0] < 1:
                raise ValueError('invalid delivery count')
            # Reserve room for the delivery's bounded metadata/pickle envelope;
            # one owner reply cannot contain a full held_tasks response batch.
            per_delivery = flow.limits.item_result_bytes + flow.limits.metadata_bytes + 4096
            maximum = (MAX_BRIDGE_FRAME_BYTES - 4096) // per_delivery
            if maximum < 1:
                raise ValueError('one declared result cannot fit a bridge frame')
            return flow.advance(min(args[0], maximum))
        if operation == 'release':
            return flow.release(*args)
        if operation == 'end_input':
            if args or (peer.expected_tasks is not None and peer.accepted != peer.expected_tasks):
                raise ValueError('query input is not complete')
            flow.end_input()
            self._execution.engine.seal_job(flow.job)
            self._emit('query_input_sealed', peer)
            return None
        if operation == 'close':
            if len(args) != 1 or type(args[0]) is not bool:
                raise ValueError('invalid consumer-close arguments')
            result = flow.close(clean=args[0])
            peer.closed = True
            self._emit('query_consumer_closed', peer)
            return result
        if operation == 'cancel':
            if args:
                raise ValueError('invalid cancellation arguments')
            peer.cancel.set()
            flow.request_cancel()
            return None
        raise ValueError('unknown native bridge operation')

    def _snapshot(self):
        engine = self._execution.engine
        return dict(owner_thread=self._owner_id, usage=asdict(engine.capacity.usage()), error=engine.error,
                    jobs=len(engine.jobs.jobs), sessions=len(engine._sessions),
                    uncertain_requests=sum(r.phase == 'UNCERTAIN' for r in engine.capacity.records.values()),
                    leased_results=sum(r.phase == 'LEASED' for r in engine.capacity.records.values()),
                    ready_results=sum(r.phase == 'READY' for r in engine.capacity.records.values()),
                    peers={p.label: dict(opened=p.opened, closed=p.closed, accepted=p.accepted,
                                         disconnected=p.disconnected.is_set(), stop_proven=p.stopped.is_set())
                           for p in self._peers.values()})

    def _tick(self):
        for peer in self._peers.values():
            if peer.flow is not None and not peer.closed:
                if peer.cancel.is_set():
                    peer.flow.request_cancel()
                if peer.stopped.is_set():
                    peer.flow.close(clean=False)
                    peer.closed = True
                    self._emit('frontend_stop_reaped', peer)
        self._execution.engine.advance()
        self._last_snapshot = self._snapshot()

    def _run(self):
        try:
            self._owner_id = threading.get_ident()
            self._execution = self._factory()
            self._last_snapshot = self._snapshot()
        except BaseException as error:
            self._startup_error = error
            self._ready.set()
            self._finished.set()
            return
        self._ready.set()
        try:
            while True:
                try:
                    peer, op, args, future = self._requests.get(timeout=self._poll_s)
                except queue.Empty:
                    future = None
                if future is not None:
                    try:
                        value = self._snapshot() if peer is None else self._invoke(peer, op, args)
                        future.set_result((True, value))
                        del value
                    except Exception as error:
                        if peer is not None:
                            peer.cancel.set()
                        future.set_result((False, type(error).__name__))
                    # Transport copies may outlive a lease only until the reader
                    # sends this reply. The owner retains no previous RPC body.
                    del args, peer, op
                self._tick()
                if self._stop.is_set() and not self._execution.engine.capacity.records and not self._execution.engine.jobs.jobs:
                    if not self._execution.close():
                        raise RuntimeError('native bridge backend close unconfirmed')
                    break
        except BaseException as error:
            self._runtime_error = error
        finally:
            self._last_snapshot = self._snapshot()
            self._finished.set()

    def snapshot(self, timeout_s=2):
        if self._finished.is_set():
            return self._last_snapshot
        future = Future()
        self._requests.put((None, 'snapshot', (), future), timeout=timeout_s)
        ok, result = future.result(timeout=timeout_s)
        if not ok:
            raise RuntimeError('native bridge snapshot failed')
        return result

    def close(self, timeout_s=5):
        for peer in self._peers.values():
            peer.cancel.set()
        self._stop.set()
        self._thread.join(timeout_s)
        if self._thread.is_alive():
            return False  # Neither consumer disappearance nor accepted work was confirmed gone.
        for peer in self._peers.values():
            peer.connection.close()
        if self._runtime_error is not None:
            raise RuntimeError('native bridge owner failed') from self._runtime_error
        return self._last_snapshot is not None and not any(self._last_snapshot['usage'].values())


class NativeTaskClient:
    """A query-bound synchronous caller, created inside each frontend process."""

    def __init__(self, connection, query_id, *, timeout_s=120):
        if not 0 < timeout_s <= 120:
            raise ValueError('invalid native bridge timeout')
        self.connection, self.query_id, self.timeout_s = connection, query_id, timeout_s
        self._sequence, self._lock = 1, threading.Lock()

    def _call(self, operation, *args):
        if not self._lock.acquire(blocking=False):
            raise RuntimeError('one frontend control thread must own the pipe')
        try:
            sequence = self._sequence
            self.connection.send_bytes(_encode((sequence, operation, args)))
            if not self.connection.poll(self.timeout_s):
                raise TimeoutError('native query owner did not respond')
            reply = pickle.loads(self.connection.recv_bytes(MAX_BRIDGE_FRAME_BYTES))
            if type(reply) is not tuple or len(reply) != 3 or reply[0] != sequence or type(reply[1]) is not bool:
                raise ValueError('native bridge reply identity mismatch')
            self._sequence += 1
            if not reply[1]:
                raise RuntimeError('native query operation failed: ' + str(reply[2]))
            return reply[2]
        finally:
            self._lock.release()

    def open_session(self, query_id, operator_id):
        limits = self._call('open', query_id, operator_id)
        return RemoteNativeTaskSession(self, limits)


class RemoteNativeTaskSession:
    """The existing consumer API; every reservation still belongs to Core."""

    def __init__(self, client, limits):
        self.client, self.limits = client, limits

    def offer(self, tasks):
        # Bound the transport frame without changing or splitting any row.
        if not tasks:
            return self.client._call('offer', ())
        prefix = len(tasks)
        while prefix:
            try:
                _encode((self.client._sequence, 'offer', (tuple(tasks[:prefix]),)))
                break
            except ValueError:
                prefix -= 1
        if not prefix:
            raise ValueError('one complete request exceeds native bridge frame limit')
        result = self.client._call('offer', tuple(tasks[:prefix]))
        return replace(result, status='BACKPRESSURE', reason='bridge_frame_bytes') if prefix < len(tasks) else result

    def advance(self, max_deliveries=1):
        return self.client._call('advance', max_deliveries)

    def release(self, leases):
        return self.client._call('release', tuple(leases))

    def end_input(self):
        return self.client._call('end_input')

    def request_cancel(self):
        return self.client._call('cancel')

    def wait(self, progress, timeout_s=None):
        if not progress.has_immediate_work:
            # Core/backend continue on the owner while this frontend waits.
            time.sleep(self.limits.poll_interval_s if timeout_s is None else timeout_s)

    def close(self, *, clean=False):
        return self.client._call('close', clean)
