"""One isolated vLLM 0.25.1 instance with independent final-result requests.

This opt-in component owns no scheduler, operator parser, Ray runtime or GPU
lease. The caller bounds its inputs/results and supervises physical shutdown.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import importlib.metadata
import json
import os
import sys
import time
import uuid

from . import vllm_engine as common


@dataclass(frozen=True)
class AsyncEngineLimits:
    max_inflight: int = 8
    input_bytes: int = 16 * 1024**2
    item_input_bytes: int = 2 * 1024**2
    result_slots: int = 8
    result_bytes: int = 16 * 1024**2
    item_result_bytes: int = 2 * 1024**2
    command_slots: int = 8
    control_slots: int = 16
    control_frame_bytes: int = 64 * 1024
    feedback_interval_s: float = 0.0

    def __post_init__(self):
        for name, value in vars(self).items():
            if name == "feedback_interval_s":
                common._number(value, name, low=0)
                continue
            if type(value) is not int or value <= 0:
                raise ValueError(name + " must be a positive integer")
        if self.input_bytes < self.item_input_bytes or self.result_bytes < self.item_result_bytes:
            raise ValueError("total bytes must fit one reserved item")
        if self.item_result_bytes < 4096 or self.control_frame_bytes < 4096:
            raise ValueError("result and control frames require at least 4096 bytes")


def result_record(identity, *, engine_uid, start, end, status, submitted, error_type=None):
    """Call the owner's shared constructor; never maintain a second usage schema."""
    return common.result_record(identity, engine_uid=engine_uid, start=start, end=end,
                                status=status, submitted=submitted, error_type=error_type)


class VllmAsyncEngine:
    """Single-event-loop engine; generation is per request, not per drained batch."""

    def __init__(self, engine: common.EngineSpec, generation: common.GenerationSpec,
                 *, limits: AsyncEngineLimits | None = None):
        self.spec = common.EngineSpec.from_dict(asdict(engine))
        self.generation = common.GenerationSpec.from_dict(asdict(generation))
        self.limits = AsyncEngineLimits() if limits is None else limits
        if type(self.limits) is not AsyncEngineLimits:
            raise TypeError("limits must be AsyncEngineLimits")
        values = self.generation.kwargs()
        if self.generation.max_tokens > self.spec.max_model_len:
            raise ValueError("max_tokens exceeds max_model_len")
        identity = self.spec.validate_local()
        common._validate_current_runtime(identity)
        # These functions are extracted by the single shared-engine owner.
        if not callable(getattr(common, "result_from_output", None)) or not callable(
            getattr(common, "result_record", None)
        ):
            raise RuntimeError("the shared engine result conversion is required")
        os.environ["HF_HUB_OFFLINE"] = os.environ["TRANSFORMERS_OFFLINE"] = "1"
        import vllm
        if (getattr(vllm, "__version__", None) != common.VLLM_VERSION
                or importlib.metadata.version("vllm") != common.VLLM_VERSION):
            raise RuntimeError("async local kernel requires exactly vllm==0.25.1")
        from vllm.engine.arg_utils import AsyncEngineArgs
        from vllm.entrypoints.chat_utils import load_chat_template
        from vllm.renderers import ChatParams
        from vllm.sampling_params import RequestOutputKind, StructuredOutputsParams
        from vllm.utils.mistral import is_mistral_tokenizer
        from vllm.v1.engine.async_llm import AsyncLLM

        self._sampling_type = vllm.SamplingParams
        self._structured_outputs_type = StructuredOutputsParams
        self._final_only = RequestOutputKind.FINAL_ONLY
        self._sampling(values)  # Validate default sampling before model allocation.
        template = load_chat_template(self.spec.chat_template)
        self.engine_uid = uuid.uuid4().hex
        self._closed = self._unhealthy = self._shutdown_failed = False
        self._closing_task = None
        self._requests = {}
        self._reserved_model_calls = self._input_bytes = 0
        self._loop = asyncio.get_running_loop()
        self._feedback = NativeStats(self.engine_uid, enable_mfu_metrics=self.spec.enable_mfu_metrics) \
            if self.limits.feedback_interval_s or self.spec.enable_mfu_metrics else None
        if self.spec.enable_mfu_metrics:
            from src.observability.metrics.mfu_trace import install
            install(self.engine_uid)
        self._llm = AsyncLLM.from_engine_args(AsyncEngineArgs(
            model=self.spec.model_path, tokenizer=self.spec.tokenizer_path or self.spec.model_path,
            dtype=self.spec.dtype, max_model_len=self.spec.max_model_len,
            max_num_seqs=self.spec.max_num_seqs,
            max_num_batched_tokens=self.spec.max_num_batched_tokens,
            gpu_memory_utilization=self.spec.gpu_memory_utilization, seed=self.spec.seed,
            tensor_parallel_size=1, pipeline_parallel_size=1,
            distributed_executor_backend="uni", trust_remote_code=False,
            generation_config="vllm", max_logprobs=common.MAX_LOGPROBS,
            logprobs_mode="raw_logprobs", enable_log_requests=False, disable_log_stats=True,
            **({"enable_mfu_metrics": True} if self.spec.enable_mfu_metrics else {}),
        ), **({"stat_loggers": [lambda config, engine_index=0: self._feedback]}
              if self._feedback is not None else {}))
        self._chat_params = ChatParams(
            chat_template=template, chat_template_content_format="auto",
            chat_template_kwargs=dict(add_generation_prompt=True, continue_final_message=False,
                tools=None, tokenize=(is_mistral_tokenizer(self._llm.renderer.tokenizer)
                    or self._llm.model_config.enable_prompt_embeds)),
        )

    def _control(self):
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("async engine requires its owning event loop")

    def _sampling(self, values):
        params = common.VllmEngine._new_sampling(self, values)
        params.output_kind = self._final_only
        return params

    def ready(self):
        from src.observability.metrics.mfu import configuration_snapshot
        from src.observability.metrics.vllm import effective_capacity_snapshot
        return dict(engine_uid=self.engine_uid, pid=os.getpid(), python=sys.executable,
                    vllm_version=common.VLLM_VERSION, batch_mode="independent_final_results",
                    closed=self._closed, max_model_calls=self.spec.max_model_calls,
                    reserved_model_calls=self._reserved_model_calls,
                    native_abort_is_terminal_confirmation=False,
                    **({"native_capacity": effective_capacity_snapshot(getattr(self._llm, "vllm_config", None))}
                       if self._feedback is not None else {}),
                    **({"native_mfu_initial": self.mfu_snapshot(),
                        "native_mfu_configuration": configuration_snapshot(self._llm.vllm_config)}
                       if self.spec.enable_mfu_metrics else {}))

    async def generate_prepared(self, row, *, request_id, generation=None,
                                deadline_monotonic):
        self._control()
        common._number(deadline_monotonic, "deadline_monotonic", low=0, inclusive_low=False)
        common._string(request_id, "request_id")
        if len(request_id.encode()) > 256:
            raise ValueError("request_id exceeds 256 bytes")
        effective = self.generation if generation is None else generation
        if not isinstance(effective, common.GenerationSpec):
            raise TypeError("generation must be GenerationSpec")
        effective = common.GenerationSpec.from_dict(asdict(effective))
        if effective.max_tokens > self.spec.max_model_len:
            raise ValueError("max_tokens exceeds max_model_len")
        sampling = self._sampling(effective.kwargs())
        identities, conversations = common._prepared([row])
        identity = identities[0]
        size = len(json.dumps(row, ensure_ascii=False, allow_nan=False).encode())
        if (self._closed or self._closing_task is not None or self._unhealthy
                or request_id in self._requests):
            raise RuntimeError("async engine is closed, unhealthy or owns this request")
        if (len(self._requests) >= self.limits.max_inflight
                or size > self.limits.item_input_bytes
                or self._input_bytes + size > self.limits.input_bytes):
            raise RuntimeError("async engine input capacity exhausted")
        started = time.monotonic()
        if started >= deadline_monotonic:
            result = result_record(identity, engine_uid=self.engine_uid, start=started,
                end=started, status="failed", submitted=False, error_type="DeadlineExceeded")
            result.update(deadline_expired=True, engine_terminal_confirmed=True)
            return result
        state = dict(entered=False, abort_requested=False, uncertain=False, output=None)
        self._requests[request_id] = state
        self._input_bytes += size
        confirmed = False

        async def execute():
            _, prompts = await self._llm.renderer.render_chat_async(conversations, self._chat_params)
            if len(prompts) != 1 or prompts[0].get("type") != "token":
                raise ValueError("async text engine requires one rendered token input")
            expected_ids = common._token_ids(prompts[0].get("prompt_token_ids"), "rendered token IDs")
            if not expected_ids:
                raise ValueError("chat renderer did not provide token IDs")
            state["expected_ids"] = expected_ids
            expired = time.monotonic() >= deadline_monotonic
            if state["abort_requested"] or expired or self._closing_task is not None:
                result = result_record(identity, engine_uid=self.engine_uid, start=started,
                    end=time.monotonic(), status="failed", submitted=False,
                    error_type="DeadlineExceeded" if expired else "CancelledBeforeSubmission")
                result.update(engine_terminal_confirmed=True, deadline_expired=expired)
                return result
            if (self.spec.max_model_calls is not None
                    and self._reserved_model_calls >= self.spec.max_model_calls):
                raise ValueError("max_model_calls exceeded before generation")
            self._reserved_model_calls += 1
            state["entered"] = True
            async for output in self._llm.generate(prompts[0], sampling, request_id):
                if output.finished is True:
                    state["output"] = output
            if state["output"] is None:
                raise RuntimeError("generate ended without a final output")
            return self._convert(identity, state["output"], request_id, expected_ids,
                                 effective, started, deadline_monotonic)

        work = state["task"] = asyncio.create_task(execute())
        work.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        try:
            done, _ = await asyncio.wait((work,), timeout=max(0, deadline_monotonic - time.monotonic()))
            if not done:
                # AsyncLLM catches cancellation and awaits ABORT. Notify the
                # caller now; retain this bounded task and all unknown ownership.
                raise TimeoutError("request deadline reached")
            result = work.result()
            confirmed = result["status"] != "unknown"
            return result
        except (Exception, asyncio.CancelledError) as error:
            state["abort_requested"] = True
            if not work.done():
                work.cancel()
            confirmed = not state["entered"] and work.done()
            result = result_record(identity, engine_uid=self.engine_uid, start=started,
                end=time.monotonic(), status="failed" if confirmed else "unknown",
                submitted=False, error_type=type(error).__name__)
            if state["output"] is not None:
                result = self._convert(identity, state["output"], request_id, state["expected_ids"],
                                       effective, started, deadline_monotonic)
                result.update(status="unknown", text=None, error_type=type(error).__name__)
                confirmed = False
            result["engine_terminal_confirmed"] = confirmed
            result["deadline_expired"] = isinstance(error, TimeoutError)
            return result
        finally:
            if confirmed:
                self._requests.pop(request_id, None)
                self._input_bytes -= size
            else:
                state["uncertain"] = self._unhealthy = True

    def _convert(self, identity, output, request_id, expected_ids, generation, start, deadline):
        result = common.result_from_output(identity, output,
            engine_uid=self.engine_uid, start=start, end=time.monotonic(),
            generation=generation, get_tokenizer=self._llm.get_tokenizer)
        # vLLM 0.25.1 RequestOutput reports actual prefix-cache hits per request.
        # Keep missing observation missing; this does not change usage/billing.
        cached = getattr(output, "num_cached_tokens", None)
        prompt = result.get("prompt_tokens")
        valid = (type(cached) is int and type(prompt) is int and 0 <= cached <= prompt
                 and not getattr(output, "kv_transfer_params", None))
        result["prompt_cached_tokens"] = cached if valid else None
        result["prompt_computed_tokens"] = prompt - cached if valid else None
        result["input_work_source"] = "vllm_request_output_cache_tokens" if valid else None
        try:
            associated = (output.request_id == request_id and common._token_ids(
                output.prompt_token_ids, "returned prompt token IDs") == expected_ids)
        except (AttributeError, TypeError, ValueError):
            associated = False
        if not associated:
            result.update(status="unknown", text=None, error_type="ResultAssociationError")
        # v0.25.1 synthesizes abort output before sending ABORT to the core.
        if result.get("finish_reason") == "abort":
            result.update(status="unknown", text=None, error_type="AbortNotConfirmed")
        if time.monotonic() >= deadline:
            result.update(status="failed" if result["status"] != "unknown" else "unknown",
                          text=None, deadline_expired=True, error_type="DeadlineExceeded")
        result["engine_terminal_confirmed"] = result["status"] != "unknown"
        return result

    async def abort(self, request_id):
        self._control()
        state = self._requests.get(request_id)
        if state is None:
            return dict(requested=False, engine_terminal_confirmed=False)
        state["abort_requested"] = True
        if state["entered"]:
            await self._llm.abort(request_id)
        return dict(requested=True, engine_terminal_confirmed=False)

    async def close(self, timeout=10.0):
        self._control()
        common._number(timeout, "close timeout", low=0, inclusive_low=False)
        deadline = time.monotonic() + timeout
        if self._closing_task is None:
            self._closing_task = asyncio.create_task(asyncio.to_thread(self._llm.shutdown, timeout=timeout))
        try:
            await asyncio.wait_for(asyncio.shield(self._closing_task), timeout)
            # MPClient finalizer detachment after a failed first shutdown cannot
            # make a later no-op retry into affirmative cleanup evidence.
            if self._shutdown_failed:
                raise RuntimeError("a prior shutdown attempt remains unconfirmed")
            self._closed = True
            pending = [state["task"] for state in self._requests.values() if not state["task"].done()]
            for task in pending:
                task.cancel()
            if pending:
                _, pending = await asyncio.wait(pending, timeout=max(0, deadline - time.monotonic()))
            return dict(status="unknown" if pending else "closed", engine_api_shutdown_confirmed=True,
                        gpu_exit_confirmed=None, uncertain_requests=len(self._requests),
                        pending_request_tasks=len(pending))
        except Exception as error:
            # A timeout leaves the same shutdown task running. Its eventual
            # return can confirm that attempt; an actual exception cannot.
            if not isinstance(error, TimeoutError):
                self._shutdown_failed = True
            return dict(status="unknown", engine_api_shutdown_confirmed=False,
                        gpu_exit_confirmed=None, uncertain_requests=len(self._requests),
                        error_type=type(error).__name__)


    def feedback_snapshot(self):
        return None if self._feedback is None else self._feedback.snapshot()


    def mfu_snapshot(self):
        return (dict(status="disabled") if not self.spec.enable_mfu_metrics
                else self._feedback.mfu.snapshot())


class NativeStats:
    """Constant-size vLLM 0.25.1 stat logger; no I/O in the output handler."""

    def __init__(self, engine_uid, *, clock=time.monotonic, enable_mfu_metrics=False):
        self.engine_uid, self.clock = engine_uid, clock
        self.sequence = self.preemptions = self.finished = self.tokens = 0
        self.latest = None
        from src.observability.metrics.mfu import NativeMfuTotals
        self.mfu = NativeMfuTotals(engine_uid, clock=clock) if enable_mfu_metrics else None

    def record(self, scheduler_stats, iteration_stats, mm_cache_stats=None, engine_idx=0):
        if self.mfu is not None:
            self.mfu.record(scheduler_stats, iteration_stats, engine_idx)
        if iteration_stats is not None:
            self.preemptions += iteration_stats.num_preempted_reqs
            self.tokens += iteration_stats.num_generation_tokens
            self.finished += sum(getattr(item.finish_reason, "name", str(item.finish_reason)).lower() != "abort"
                                 for item in iteration_stats.finished_requests)
        if scheduler_stats is not None:
            self.sequence += 1
            self.latest = dict(engine_uid=self.engine_uid, sequence=self.sequence,
                observed_at_s=self.clock(), running=scheduler_stats.num_running_reqs,
                waiting=scheduler_stats.num_waiting_reqs, kv_usage=scheduler_stats.kv_cache_usage,
                preemptions_total=self.preemptions, finished_total=self.finished,
                generation_tokens_total=self.tokens)

    def log_engine_initialized(self):
        pass

    def log(self):
        pass

    def snapshot(self):
        return None if self.latest is None else dict(self.latest)
