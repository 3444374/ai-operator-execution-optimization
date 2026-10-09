"""Two ordinary LOTUS Maps with a real per-row dependency, over external input."""

from dataclasses import dataclass
import json
import base64
from copy import deepcopy

from ..continuation import Continue, Final, Request
from .sdk import check_model_config, decode_lotus_response, prepare_call, validate_source


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


@dataclass(frozen=True)
class LotusMapStage:
    instruction: str
    output_column: str
    system_prompt: str | None = None

    def __post_init__(self):
        if (type(self.instruction) is not str or not self.instruction
                or type(self.output_column) is not str or not self.output_column):
            raise ValueError("Map requires an instruction and output column")
        if self.system_prompt is not None and type(self.system_prompt) is not str:
            raise ValueError("invalid Map system prompt")


def _validate_stages(stages):
    from lotus.nl_expression import parse_cols
    if type(stages) is not tuple or len(stages) != 2 or any(type(s) is not LotusMapStage for s in stages):
        raise ValueError("LOTUS continuation requires exactly two Map stages")
    if stages[0].output_column == stages[1].output_column:
        raise ValueError("Map output columns must differ")
    if stages[0].output_column not in parse_cols(stages[1].instruction):
        raise ValueError("the second Map must consume the first Map result")


def map_messages(lm, row, stage):
    import pandas as pd
    from lotus.nl_expression import nle2str, parse_cols
    from lotus.templates.task_instructions import df2multimodal_info, map_formatter
    columns = parse_cols(stage.instruction)
    if any(c not in row for c in columns):
        raise ValueError("Map references a missing input column")
    docs = df2multimodal_info(pd.DataFrame([row]), columns)
    return map_formatter(lm, docs[0], nle2str(stage.instruction, columns), system_prompt=stage.system_prompt)


def staged_two_map(frame, lm, stages):
    """Unmodified LOTUS calls preserve each stage's whole-frame completion wait."""
    import lotus
    validate_source()
    _validate_stages(stages)
    if lotus.settings.enable_cache:
        raise ValueError("two-Map paired execution requires cache disabled")
    with lotus.settings.context(lm=lm):
        for stage in stages:
            frame = frame.sem_map(stage.instruction, suffix=stage.output_column,
                                  system_prompt=stage.system_prompt, return_raw_outputs=True)
    return frame


class LotusTwoMapMethod:
    """MethodDriver owns live rows; callbacks only prepare and parse native Map work."""

    def __init__(self, lm, stages, *, model_config, max_response_bytes=65536, postprocessor=None):
        import lotus
        from lotus.sem_ops.postprocessors import map_postprocess
        validate_source()
        _validate_stages(stages)
        if postprocessor is not None and postprocessor is not map_postprocess:
            raise ValueError("custom postprocessors require a separate method adapter")
        if lotus.settings.enable_cache:
            raise ValueError("incremental two-Map requires cache disabled; operator caching is whole-frame")
        if type(max_response_bytes) is not int or max_response_bytes <= 0:
            raise ValueError("invalid response byte limit")
        self.lm, self.stages, self.max_response_bytes = lm, stages, max_response_bytes
        self.model_config = model_config
        self.call_kwargs = deepcopy(lm.kwargs)
        if self.call_kwargs.get("logprobs", False):
            self.call_kwargs.setdefault("top_logprobs", 10)

    def _request(self, state):
        stage = self.stages[state["stage"]]
        messages = map_messages(self.lm, state["row"], stage)
        call = prepare_call(self.lm, messages, self.call_kwargs)
        check_model_config(call, self.model_config)
        return Continue(Request("lotus-map", call.payload, 1, self.max_response_bytes), encode(state))

    def start(self, value):
        import lotus
        if lotus.settings.enable_cache:
            raise ValueError("incremental two-Map requires cache disabled")
        row = json.loads(value)
        if type(row) is not dict or any(type(k) is not str or type(v) is not str for k, v in row.items()):
            raise ValueError("two-Map external rows must contain text columns")
        if any(s.output_column in row for s in self.stages):
            raise ValueError("Map output would overwrite an input column")
        return self._request({"stage": 0, "row": row, "raw_responses": []})

    def resume(self, state, result):
        from lotus.sem_ops.postprocessors import map_postprocess
        current = json.loads(state)
        full, response = decode_lotus_response(result)
        try:
            # LM.__call__ updates usage before choice extraction, including parse failures.
            self.lm._update_stats(response, is_cached=False)
            raw = self.lm._get_top_choice(response)
            parsed = map_postprocess([raw], self.lm, False)
        except Exception as error:
            error.full_response = full
            raise
        stage = self.stages[current["stage"]]
        current["row"][stage.output_column] = parsed.outputs[0]
        current["row"]["raw_output" + stage.output_column] = parsed.raw_outputs[0]
        current["raw_responses"].append(base64.b64encode(result).decode("ascii"))
        if current["stage"] == 0:
            current["stage"] = 1
            return self._request(current)
        return Final(encode({"row": current["row"], "raw_responses": current["raw_responses"]}))
