"""Passive, CPU-only decision capture for the vLLM 0.25.1 v1 runner.

The caller qualifies the installed source and owns the event sink. This module
does not import an engine, copy device data, or change execution decisions.
"""
from contextlib import contextmanager
from functools import wraps


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


def _count(value):
    return value if type(value) is int and value >= 0 else None


def _bool(value):
    return value if type(value) is bool else None


def _members(runner, counts):
    ids = getattr(getattr(runner, "input_batch", None), "req_ids", None)
    if (counts is not None and isinstance(ids, (list, tuple)) and len(ids) == len(counts)
            and all(type(v) is str for v in ids) and len(set(ids)) == len(ids)):
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
    lengths = None
    if isinstance(result, (list, tuple)):
        lengths = [cpu_counts(group) for group in result]
        if any(group is None for group in lengths):
            lengths = None
    computed = cpu_counts(inputs.get("num_computed_tokens"))
    return dict(
        scheduled_tokens=_members(runner, cpu_counts(inputs.get("num_scheduled_tokens"))),
        computed_tokens=_members(runner, computed),
        common_prefix_blocks=cpu_counts(inputs.get("num_common_prefix_blocks")),
        cascade_selected=False if result is None else (True if lengths is not None else None),
        cascade_prefix_lengths=lengths,
    )


def _graph(runner, args, kwargs, result):
    inputs = _arguments(_GRAPH_ARGS, args, kwargs)
    # Slot 3 may contain a device tensor: never inspect or serialize it.
    mode, descriptor, microbatching = result[:3]
    mode_name = getattr(mode, "name", None)
    if mode_name not in ("NONE", "PIECEWISE", "FULL"):
        mode_name = None
    return dict(
        scheduled_tokens=_members(runner, cpu_counts(inputs.get("num_scheduled_tokens_np"))),
        num_tokens=_count(inputs.get("num_tokens")),
        num_reqs=_count(inputs.get("num_reqs")),
        padded_tokens=_count(getattr(descriptor, "num_tokens", None)),
        padded_reqs=_count(getattr(descriptor, "num_reqs", None)),
        cudagraph_mode=mode_name,
        use_cascade_attn=_bool(inputs.get("use_cascade_attn")),
        force_eager=_bool(inputs.get("force_eager", False)),
        num_encoder_reqs=_count(inputs.get("num_encoder_reqs", 0)),
        should_ubatch=_bool(microbatching),
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

    installed = []
    try:
        try:
            encoder = getattr(scheduler_output, "scheduled_encoder_inputs", None)
            encoder_counts = ({k: len(v) for k, v in encoder.items()}
                              if isinstance(encoder, dict)
                              and all(type(k) is str and isinstance(v, (list, tuple))
                                      for k, v in encoder.items()) else None)
            record("native_schedule_metadata",
                   common_prefix_blocks=cpu_counts(getattr(scheduler_output, "num_common_prefix_blocks", None)),
                   total_scheduled_tokens=_count(getattr(scheduler_output, "total_num_scheduled_tokens", None)),
                   encoder_input_counts=encoder_counts,
                   cascade_enabled=_bool(getattr(runner, "cascade_attn_enabled", None)),
                   microbatching_enabled=_bool(getattr(getattr(runner, "parallel_config", None), "use_ubatching", None)))
        except Exception as error:
            unknown("scheduler_metadata", error)
        for name, event, extract in (
            ("_compute_cascade_attn_prefix_lens", "native_cascade_decision", _cascade),
            ("_determine_batch_execution_and_padding", "native_graph_decision", _graph),
        ):
            try:
                original = getattr(runner, name)
                existed = name in vars(runner)
                saved = vars(runner).get(name)

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

                setattr(runner, name, make_wrapper(original, event, extract))
                installed.append((name, existed, saved))
            except Exception as error:
                unknown(name, error)
        yield
    finally:
        for name, existed, saved in reversed(installed):
            try:
                if existed:
                    setattr(runner, name, saved)
                else:
                    delattr(runner, name)
            except Exception as error:
                unknown("restore_" + name, error)
