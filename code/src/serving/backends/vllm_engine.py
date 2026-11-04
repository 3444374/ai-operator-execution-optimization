"""Finite synchronous vLLM batches for prepared, independent LOTUS calls.

This module does not import LOTUS or own a model process. The caller must run it
with ``model_python`` and supervise that process for deadlines and physical exit.
"""

from __future__ import annotations

from dataclasses import asdict
import importlib.metadata
import json
import os
import sys
import threading
import time
import uuid

from .vllm_protocol import (
    VLLM_VERSION,
    MAX_LOGPROBS,
    IDENTITY_FIELDS,
    _PROBE,
    _integer,
    _number,
    _string,
    GenerationSpec,
    _probe_python,
    _prefix,
    EngineSpec,
    _validate_current_runtime,
    _token_ids,
    _chat_logprobs,
    _prepared,
    result_record,
    result_from_output,
    sampling_params,
)


class ModelContextLengthExceeded(ValueError):
    """A fully rendered request does not fit this actual model context."""


class VllmEngine:
    """One isolated model instance; each chat call is a finite synchronous batch."""

    constructor_policy = 'native_optional_capacity'

    def _capacity_kwargs(self, engine):
        return {name: getattr(engine, name) for name in
                ('max_model_len', 'max_num_seqs', 'max_num_batched_tokens')
                if getattr(engine, name) is not None}

    def __init__(self, engine: EngineSpec, generation: GenerationSpec):
        if not isinstance(engine, EngineSpec) or not isinstance(generation, GenerationSpec):
            raise TypeError("engine and generation must use their typed specs")
        generation_kwargs = generation.kwargs()
        identity = engine.validate_local()
        _validate_current_runtime(identity)
        capacity_kwargs = self._capacity_kwargs(engine)
        requested_context = capacity_kwargs.get('max_model_len')
        if requested_context is not None and generation.max_tokens > requested_context:
            raise ValueError("max_tokens exceeds max_model_len")
        # Local directories plus offline flags forbid fallback model/tokenizer downloads.
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"
        import vllm

        if (getattr(vllm, "__version__", None) != VLLM_VERSION
                or importlib.metadata.version("vllm") != VLLM_VERSION):
            raise RuntimeError(f"local kernel requires exactly vllm=={VLLM_VERSION}")
        from vllm.sampling_params import StructuredOutputsParams

        self._sampling_type = vllm.SamplingParams
        self._structured_outputs_type = StructuredOutputsParams
        self._sampling = self._new_sampling(generation_kwargs)
        if engine.chat_template is not None:
            from vllm.entrypoints.chat_utils import load_chat_template

            # LLM loads this after constructing its engine; preflight the same fixed API first.
            load_chat_template(engine.chat_template)
        self.spec, self.generation = engine, generation
        self.engine_uid = uuid.uuid4().hex
        self._closed = False
        self._close_attempt = 0
        self._shutdown_failed = False
        self._context_observed = None
        self._max_model_len = requested_context
        self._constructor_parameters = dict(capacity_kwargs)
        self._max_model_calls = engine.max_model_calls
        self._reserved_model_calls = 0
        self._lock = threading.Lock()
        self._versions = {"vllm": VLLM_VERSION, "python": sys.version.split()[0]}
        if engine.enable_mfu_metrics:
            from src.observability.metrics.mfu_trace import install
            install(self.engine_uid)
        self._llm = vllm.LLM(
            model=engine.model_path, tokenizer=engine.tokenizer_path or engine.model_path,
            dtype=engine.dtype, **capacity_kwargs,
            gpu_memory_utilization=engine.gpu_memory_utilization, seed=engine.seed,
            chat_template=engine.chat_template, tensor_parallel_size=1,
            pipeline_parallel_size=1, distributed_executor_backend="uni",
            trust_remote_code=False, generation_config="vllm",
            max_logprobs=MAX_LOGPROBS, logprobs_mode="raw_logprobs",
            **({"enable_mfu_metrics": True, "disable_log_stats": False}
               if engine.enable_mfu_metrics else {}),
        )
        self._mfu_initial = self.mfu_snapshot() if engine.enable_mfu_metrics else None

    def mfu_snapshot(self):
        from src.observability.metrics.mfu import prometheus_snapshot, unavailable
        if not self.spec.enable_mfu_metrics:
            return dict(status="disabled")
        try:
            return prometheus_snapshot(self._llm.get_metrics(), self.engine_uid)
        except Exception as error:
            return unavailable("optional local metrics: " + type(error).__name__)

    def _new_sampling(self, values: dict):
        return sampling_params(values, self._sampling_type, self._structured_outputs_type)

    def _context_report(self):
        from .model_context import model_context_record
        try:
            native_context = self._llm.model_config.max_model_len
        except AttributeError as error:
            raise RuntimeError('native model context is unavailable at synchronous readiness') from error
        observed = model_context_record(self.spec, self.engine_uid, native_context)
        if self.generation.max_tokens > observed['max_model_len']:
            raise ValueError('max_tokens exceeds resolved model context')
        if self._context_observed is not None and self._context_observed != observed:
            raise RuntimeError('native model context changed during this engine lifetime')
        self._context_observed = dict(observed)
        self._max_model_len = observed['max_model_len']
        return dict(observed)

    def ready(self) -> dict:
        if self._shutdown_failed and not self._closed:
            raise RuntimeError('model engine cleanup is uncertain; retry close before reuse')
        from src.observability.metrics.mfu import configuration_snapshot
        from src.observability.metrics.vllm import effective_capacity_snapshot

        observed = self._context_report()
        return {
            "engine_uid": self.engine_uid, "pid": os.getpid(), "python": sys.executable,
            "prefix": sys.prefix, "versions": dict(self._versions),
            "model_config": asdict(self.spec), "generation": asdict(self.generation),
            "constructor_policy": self.constructor_policy,
            "constructor_parameters": dict(self._constructor_parameters),
            "native_capacity": effective_capacity_snapshot(
                getattr(self._llm.llm_engine, 'vllm_config', None),
                source='constructed LLM.llm_engine.vllm_config; not free capacity'),
            "closed": self._closed, "batch_mode": "finite_synchronous_chat",
            "model_context": observed,
            "max_model_calls": self._max_model_calls,
            "reserved_model_calls": self._reserved_model_calls,
            "max_logprobs": MAX_LOGPROBS, "logprobs_mode": "raw_logprobs",
            **({"native_mfu_initial": self._mfu_initial} if self.spec.enable_mfu_metrics else {}),
            **({"native_mfu_configuration": configuration_snapshot(self._llm.llm_engine.vllm_config)}
               if self.spec.enable_mfu_metrics else {}),
        }

    def _result(self, identity: dict, start: float, end: float, *, status: str,
                submitted: bool, error_type: str | None = None) -> dict:
        return result_record(identity, engine_uid=self.engine_uid, start=start, end=end,
                             status=status, submitted=submitted, error_type=error_type)

    def _prepare_native(self, conversations, generation, context):
        if self._llm.model_config.runner_type != 'generate':
            raise ValueError('synchronous text inference requires a generative model')
        prompts = tuple(self._llm._preprocess_chat(conversations))
        if len(prompts) != len(conversations):
            raise ValueError('native chat rendering changed request cardinality')
        expected = []
        for prompt in prompts:
            if not isinstance(prompt, dict) or prompt.get('type') != 'token':
                raise ValueError('synchronous text inference requires rendered token inputs')
            ids = _token_ids(prompt.get('prompt_token_ids'), 'rendered prompt token IDs')
            if not ids:
                raise ValueError('native chat renderer did not provide token IDs')
            if len(ids) + generation.max_tokens > context['max_model_len']:
                raise ModelContextLengthExceeded('rendered prompt and output cap exceed resolved model context')
            expected.append(ids)
        return prompts, expected

    def _invoke_native(self, prompts, sampling):
        self._llm._render_and_add_requests(prompts=iter(prompts), params=[sampling] * len(prompts))
        return self._llm.wait_for_completion(use_tqdm=False)

    def run_prepared(self, rows: list[dict], *,
                     generation: GenerationSpec | None = None,
                     deadline_monotonic: float | None = None) -> list[dict]:
        """Render once in vLLM; in-flight stopping belongs to the supervisor."""
        if deadline_monotonic is not None:
            _number(deadline_monotonic, "deadline_monotonic", low=0.0,
                    inclusive_low=False)
        effective_generation = self.generation if generation is None else generation
        if not isinstance(effective_generation, GenerationSpec):
            raise TypeError("generation must be a GenerationSpec or None")
        generation_kwargs = effective_generation.kwargs()
        identities, conversations = _prepared(rows)
        if self._closed:
            raise RuntimeError("model engine is closed")
        if self._shutdown_failed:
            raise RuntimeError("model engine cleanup is uncertain; retry close before reuse")
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("model engine is already running or closing")
        try:
            context = self._context_report()
            if effective_generation.max_tokens > context['max_model_len']:
                raise ValueError('max_tokens exceeds resolved model context')
            sampling = self._new_sampling(generation_kwargs)
            start = time.monotonic()
            if deadline_monotonic is not None and start >= deadline_monotonic:
                results = [self._result(item, start, start, status="failed", submitted=False,
                                        error_type="DeadlineExceeded") for item in identities]
                for result in results:
                    result["deadline_expired"] = True
                return results
            if not rows:
                return []
            if (self._max_model_calls is not None
                    and self._reserved_model_calls + len(rows) > self._max_model_calls):
                raise RuntimeError("max_model_calls exceeded; entire batch was not submitted")
            try:
                native_inputs, expected_ids = self._prepare_native(conversations, effective_generation, context)
            except Exception as error:
                end = time.monotonic()
                return [self._result(item, start, end, status='failed', submitted=False,
                                     error_type=type(error).__name__) for item in identities]
            if expected_ids is not None and deadline_monotonic is not None:
                checked_at = time.monotonic()
                if checked_at >= deadline_monotonic:
                    results = [self._result(item, start, checked_at, status='failed', submitted=False,
                                           error_type='DeadlineExceeded') for item in identities]
                    for result in results:
                        result['deadline_expired'] = True
                    return results
            # Each row is one request; reserve before submission, retaining unknown/failure.
            self._reserved_model_calls += len(rows)
            try:
                outputs = self._invoke_native(native_inputs, sampling)
            except Exception as exc:
                end = time.monotonic()
                results = [self._result(item, start, end, status="unknown", submitted=False,
                                        error_type=type(exc).__name__) for item in identities]
                for result in results:
                    result["chat_invoked"] = expected_ids is None
                    result['native_submission_invoked'] = True
                    result["deadline_expired"] = (
                        deadline_monotonic is not None and end >= deadline_monotonic
                    )
                return results
            end = time.monotonic()
            request_ids = ([getattr(item, "request_id", None) for item in outputs]
                           if isinstance(outputs, (list, tuple)) else [])
            if (not isinstance(outputs, (list, tuple)) or len(outputs) != len(rows)
                    or any(not isinstance(item, str) or not item for item in request_ids)
                    or len(set(request_ids)) != len(request_ids)):
                results = [self._result(item, start, end, status="unknown", submitted=True,
                                        error_type="ResultAssociationError") for item in identities]
                for result in results:
                    result["deadline_expired"] = (
                        deadline_monotonic is not None and end >= deadline_monotonic
                    )
                return results
            results = []
            for index, (identity, output) in enumerate(zip(identities, outputs, strict=True)):
                result = result_from_output(
                    identity, output, engine_uid=self.engine_uid, start=start, end=end,
                    generation=effective_generation, get_tokenizer=lambda: self._llm.get_tokenizer(),
                )
                if (expected_ids is not None and result['status'] == 'succeeded'
                        and json.loads(result['prompt_token_ids_json']) != expected_ids[index]):
                    result.update(status='failed', text=None, error_type='PromptTokenMismatch')
                results.append(result)
            if deadline_monotonic is not None and end >= deadline_monotonic:
                for result in results:
                    result.update(status="failed", text=None, deadline_expired=True,
                                  error_type="DeadlineExceeded")
            return results
        finally:
            self._lock.release()

    def close(self, timeout: float = 10.0) -> dict:
        """Report engine API shutdown only; physical process/GPU exit needs supervision."""
        _number(timeout, "timeout", low=0.0, inclusive_low=False)
        if self.spec.enable_mfu_metrics and not self._closed:
            self._mfu_final = self.mfu_snapshot()
        report = {
            "status": "closed" if self._closed else "unknown", "engine_uid": self.engine_uid,
            "pid": os.getpid(), "engine_api_shutdown_confirmed": self._closed,
            "gpu_exit_confirmed": None, "cleanup_scope": "engine_core_api",
            "attempt": self._close_attempt, "error_type": None,
            "max_model_calls": self._max_model_calls,
            "reserved_model_calls": self._reserved_model_calls,
            **({"native_mfu_initial": self._mfu_initial, "native_mfu_final": self._mfu_final}
               if self.spec.enable_mfu_metrics else {}),
        }
        if self._closed:
            return report
        if not self._lock.acquire(blocking=False):
            return {**report, "error_type": "EngineBusy"}
        try:
            self._close_attempt += 1
            report["attempt"] = self._close_attempt
            core = self._llm.llm_engine.engine_core
            finalizer = getattr(core, "_finalizer", None)
            retry_detached = self._shutdown_failed and getattr(finalizer, "alive", True) is False
            core.shutdown(timeout=timeout)
            if retry_detached:
                # 0.25.1 MPClient can detach before a failed cleanup, making a retry a no-op.
                report["error_type"] = "ShutdownNotConfirmed"
                return report
            self._closed = True
            return {**report, "status": "closed", "engine_api_shutdown_confirmed": True}
        except Exception as exc:
            self._shutdown_failed = True
            return {**report, "error_type": type(exc).__name__}
        finally:
            self._lock.release()
