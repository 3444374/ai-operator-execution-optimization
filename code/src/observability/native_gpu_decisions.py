"""Passive, CPU-only decision capture for the vLLM 0.25.1 v1 runner.

The caller qualifies the installed source and owns the event sink. This module
does not import an engine, copy device data, or change execution decisions.
"""
from contextlib import contextmanager
from functools import wraps
import hashlib
import math


def cpu_counts(value):
    if isinstance(value, (list, tuple)):
        values = list(value)
    elif type(value).__module__.startswith("numpy"):
        values = value.tolist()
    elif (type(value).__module__.startswith("torch")
          and getattr(getattr(value, "device", None), "type", None) == "cpu"):
        values = value.tolist()
    else:
        return None
    return values if all(type(v) is int and v >= 0 for v in values) else None


def _members(runner, counts):
    ids = runner.input_batch.req_ids
    if counts is not None and len(ids) == len(counts):
        return dict(zip(ids, counts))
    return None


def _arguments(names, args, kwargs):
    return dict(zip(names, args), **kwargs)


_CASCADE_ARGS = ("num_scheduled_tokens", "num_computed_tokens", "num_common_prefix_blocks")
_GRAPH_ARGS = ("num_tokens", "num_reqs", "num_scheduled_tokens_np", "max_num_scheduled_tokens",
               "use_cascade_attn", "allow_microbatching", "force_eager", "force_uniform_decode",
               "force_has_lora", "force_num_active_loras", "num_encoder_reqs")


def _cascade(runner, args, kwargs, result):
    inputs = _arguments(_CASCADE_ARGS, args, kwargs)
    return dict(
        scheduled_tokens=_members(runner, cpu_counts(inputs['num_scheduled_tokens'])),
        computed_tokens=_members(runner, cpu_counts(inputs['num_computed_tokens'])),
        common_prefix_blocks=inputs['num_common_prefix_blocks'],
        cascade_selected=result is not None,
        cascade_prefix_lengths=result,
    )


def _graph(runner, args, kwargs, result):
    inputs = _arguments(_GRAPH_ARGS, args, kwargs)
    # Slot 3 may contain a device tensor: never inspect or serialize it.
    mode, descriptor, microbatching, _, _ = result
    return dict(
        scheduled_tokens=_members(runner, cpu_counts(inputs['num_scheduled_tokens_np'])),
        prompt_token_counts=_members(runner, cpu_counts(runner.input_batch.num_prompt_tokens[:inputs['num_reqs']])),
        computed_before=_members(runner, cpu_counts(runner.input_batch.num_computed_tokens_cpu[:inputs['num_reqs']])),
        num_tokens=inputs['num_tokens'], num_reqs=inputs['num_reqs'],
        padded_tokens=descriptor.num_tokens, padded_reqs=descriptor.num_reqs,
        cudagraph_mode=mode.name, use_cascade_attn=inputs['use_cascade_attn'],
        force_eager=inputs.get('force_eager', False),
        num_encoder_reqs=inputs.get('num_encoder_reqs', 0), should_ubatch=microbatching,
    )


@contextmanager
def observe_decisions(runner, scheduler_output, sequence, emit):
    """Wrap two native calls for one execute; preserve returns and exceptions.

    Only primitive CPU metadata is retained. Native worker execution is serial;
    this context is not intended for concurrent calls on the same runner.
    """
    def record(event, **fields):
        try:
            emit(event, observer_execute_id=sequence, **fields)
        except Exception:
            pass  # Sink loss is also checked through its close receipt.

    def unknown(operation, error):
        record("native_decision_unknown", operation=operation, error_type=type(error).__name__)

    def make_wrapper(original, event, extract):
        @wraps(original)
        def observed(*args, **kwargs):
            result = original(*args, **kwargs)
            try:
                record(event, **extract(runner, args, kwargs, result))
            except Exception as error:
                unknown(event, error)
            return result
        return observed

    installed = []
    try:
        try:
            record("native_schedule_metadata",
                   common_prefix_blocks=scheduler_output.num_common_prefix_blocks,
                   total_scheduled_tokens=scheduler_output.total_num_scheduled_tokens,
                   encoder_input_counts={k: len(v) for k, v in scheduler_output.scheduled_encoder_inputs.items()},
                   new_request_computed_tokens={r.req_id: r.num_computed_tokens for r in scheduler_output.scheduled_new_reqs},
                   cascade_enabled=runner.cascade_attn_enabled,
                   microbatching_enabled=runner.parallel_config.use_ubatching)
        except Exception as error:
            unknown("scheduler_metadata", error)
        try:
            for name, event, extract in (
                ("_compute_cascade_attn_prefix_lens", "native_cascade_decision", _cascade),
                ("_determine_batch_execution_and_padding", "native_graph_decision", _graph),
            ):
                original = getattr(runner, name)
                existed = name in vars(runner)
                saved = vars(runner).get(name)

                setattr(runner, name, make_wrapper(original, event, extract))
                installed.append((name, existed, saved))
        except Exception as error:
            unknown('install', error)
        yield
    finally:
        for name, existed, saved in reversed(installed):
            if existed:
                setattr(runner, name, saved)
            else:
                delattr(runner, name)


@contextmanager
def observe_encoder_cache(runner, sequence, emit):
    """Observe native cache references using tensor metadata, never tensor values."""
    def snapshot(action):
        try:
            entries = [dict(representation_key=hashlib.sha256(str(key).encode()).hexdigest(),
                            representation_object_id=str(id(value)), shape=list(value.shape), dtype=str(value.dtype),
                            logical_bytes=math.prod(value.shape) * value.element_size())
                       for key, value in runner.encoder_cache.items()]
            event = 'native_encoder_cache_snapshot'
            fields = dict(action=action, cache_entries=entries, retained_entries=len(entries),
                          logical_bytes=sum(e['logical_bytes'] for e in entries),
                          scope='native runner cache references and logical tensor bytes; aliases and allocator memory are separate')
        except Exception as error:
            event = 'native_decision_unknown'
            fields = dict(operation='encoder_cache_metadata', error_type=type(error).__name__)
        try:
            emit(event, observer_execute_id=sequence, **fields)
        except Exception:
            pass  # The optional sink cannot change native execution.
    snapshot('before_execute')
    try:
        yield
    finally:
        snapshot('after_execute')
