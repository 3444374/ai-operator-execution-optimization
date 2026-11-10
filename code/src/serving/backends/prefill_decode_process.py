"""Opt-in text prefill/decode placement using the native NIXL pull protocol.

Two persistent processes on one host may use the same or different GPUs. Each
complete conversation crosses P then D; native vLLM owns KV blocks and transfer.
This first implementation serializes logical requests and does not migrate an
already-running decode or split a multimodal encoder.
"""

from dataclasses import dataclass, replace
from pathlib import Path
import threading
import time

from .staged_generation_process import ChainEngineProcess
from .vllm_protocol import EngineSpec, GenerationSpec, _prepared, result_record
from .vllm_sync_process import _verified_terminal


@dataclass(frozen=True)
class NixlPlacement:
    cuda_visible_devices: str
    side_channel_port: int

    def __post_init__(self):
        if (not isinstance(self.cuda_visible_devices, str)
                or not self.cuda_visible_devices.strip()
                or ',' in self.cuda_visible_devices):
            raise ValueError('each native stage requires one explicit CUDA device')
        if type(self.side_channel_port) is not int or not 1 <= self.side_channel_port <= 65535:
            raise ValueError('side_channel_port must be a valid TCP port')

    def environment(self):
        return dict(CUDA_VISIBLE_DEVICES=self.cuda_visible_devices,
                    VLLM_NIXL_SIDE_CHANNEL_HOST='127.0.0.1',
                    VLLM_NIXL_SIDE_CHANNEL_PORT=str(self.side_channel_port))


class PrefillDecodeProcess:
    """Same-model text handoff; the caller still owns GPU allocation and deadline.

    Results retain both physical receipts. The P completion is discarded, while
    D receives the original full prompt and original generation specification.
    Native send expiry or closing P releases a handoff never consumed by D.
    """

    def __init__(self, engine: EngineSpec, generation: GenerationSpec, control_dir,
                 deadline, startup_timeout_s, *, prefill: NixlPlacement,
                 decode: NixlPlacement, kv_buffer_device='cuda'):
        if prefill.side_channel_port == decode.side_channel_port:
            raise ValueError('native stages require different side-channel ports')
        if engine.kv_transfer_config is not None:
            raise ValueError('the stage owner supplies its NIXL connector configuration')
        if kv_buffer_device not in ('cuda', 'cpu'):
            raise ValueError('kv_buffer_device must be cuda or cpu')
        self.generation, self.deadline = generation, deadline
        self.control = Path(control_dir)
        self.control.mkdir(parents=True, exist_ok=True)
        self.prefill = self.decode = None
        self._lock = threading.Lock()
        self._closed = False
        self.last_close_report = None
        configured = replace(engine, kv_transfer_config=dict(
            kv_connector='NixlConnector', kv_role='kv_both',
            kv_buffer_device=kv_buffer_device, kv_load_failure_policy='fail'))
        try:
            self.prefill = ChainEngineProcess(configured, replace(generation, max_tokens=1),
                self.control, deadline, startup_timeout_s,
                worker_env=prefill.environment(), worker_log=self.control / 'prefill.log')
            self.decode = ChainEngineProcess(configured, generation,
                self.control, deadline, startup_timeout_s,
                worker_env=decode.environment(), worker_log=self.control / 'decode.log')
        except BaseException as error:
            cleanup = self.close()
            if cleanup['status'] != 'closed':
                error.add_note('native stage cleanup remains unconfirmed; inspect process reports')
            raise

    def ready(self):
        return dict(mode='native_nixl_prefill_decode', request_policy='serial_complete_requests',
                    prefill=self.prefill.ready_report, decode=self.decode.ready_report)

    def _stopped(self):
        return (self._closed or (self.control / 'stop').exists()
                or self.deadline is not None and time.monotonic() >= self.deadline)

    @staticmethod
    def _receipt(values, expected, process):
        if (not isinstance(values, list) or len(values) != 1
                or not _verified_terminal(values[0], expected, process.ready_report['engine_uid'])):
            raise ValueError('native stage receipt does not identify the complete request')
        return values[0]

    def run_prepared(self, rows, *, generation=None):
        effective = self.generation if generation is None else generation
        if not isinstance(effective, GenerationSpec):
            raise TypeError('generation must be GenerationSpec')
        identities, _ = _prepared(rows)
        if not self._lock.acquire(blocking=False):
            raise RuntimeError('native stages already have an owner call')
        results = []
        try:
            for row, identity in zip(rows, identities, strict=True):
                started = time.monotonic()
                receipts = {}
                result = result_record(identity, engine_uid='native_nixl_prefill_decode',
                    start=started, end=started, status='failed', submitted=False,
                    error_type='StoppedBeforeSubmission')
                if not self._stopped():
                    try:
                        receipts['prefill'] = self.prefill.run_prepared([row],
                            generation=replace(effective, max_tokens=1),
                            kv_transfer_params=[dict(do_remote_decode=True, do_remote_prefill=False)])
                        producer = self._receipt(receipts['prefill'], identity, self.prefill)
                        if producer['status'] != 'succeeded':
                            result = dict(producer, text=None)
                        else:
                            transfer = producer.get('kv_transfer_params')
                            if (not isinstance(transfer, dict)
                                    or transfer.get('do_remote_prefill') is not True
                                    or not transfer.get('remote_engine_id')
                                    or not transfer.get('remote_request_id')
                                    or not transfer.get('remote_block_ids')):
                                raise ValueError('prefill did not return a usable native KV descriptor')
                            if not self._stopped():
                                receipts['decode'] = self.decode.run_prepared([row],
                                    generation=effective, kv_transfer_params=[transfer])
                                consumer = self._receipt(receipts['decode'], identity, self.decode)
                                result = dict(consumer)
                                if (consumer['status'] == 'succeeded'
                                        and consumer['prompt_token_ids_json'] != producer['prompt_token_ids_json']):
                                    raise ValueError('native stage prompt token IDs differ')
                    except Exception as error:
                        result.update(status='unknown', text=None,
                                      error_type=type(error).__name__)
                        self.cancel()
                result.update(stage_receipts=receipts,
                    stage_elapsed_s=time.monotonic() - started,
                    execution_mode='native_nixl_prefill_decode')
                results.append(result)
                if result['status'] != 'succeeded':
                    self.cancel()
            return results
        finally:
            self._lock.release()

    def cancel(self):
        """Stop both private owners; a stop request is not an exit receipt."""
        (self.control / 'stop').touch()

    def close(self, timeout=30.0):
        from .vllm_protocol import _number
        _number(timeout, 'close timeout', low=0, inclusive_low=False)
        deadline = time.monotonic() + timeout
        self._closed = True
        self.cancel()
        reports = {}
        owners = [('decode', self.decode), ('prefill', self.prefill)]
        for index, (name, owner) in enumerate(owners):
            if owner is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    reports[name] = dict(status='unknown', process_exit_confirmed=False)
                else:
                    reports[name] = owner.close(timeout=remaining / (len(owners) - index))
        self.last_close_report = dict(
            status='closed' if all(r['status'] == 'closed' for r in reports.values()) else 'unknown',
            stages=reports, gpu_exit_confirmed=None)
        return self.last_close_report
