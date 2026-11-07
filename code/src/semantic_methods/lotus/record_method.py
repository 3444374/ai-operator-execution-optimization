"""Native LOTUS formatting, parsing and accounting for one record stage.

Methods prepare complete requests and interpret results; their execution owner
chooses batching, admission and model lifetime.
"""

import json
import os
import time

from .engine import _FilterFormatter, _input_instruction, engine_response, validate_engine_protocol
from .records import FilterMethod
from .record_types import ExtractMethod, MapMethod, StageSpec, stage_identity


class LotusRecordMethod:
    """Original ordinary formatter/parser and accounting for one named stage."""

    def __init__(self, stage):
        import lotus
        from lotus.models import LM
        from lotus.templates import task_instructions
        from lotus.types import SerializationFormat

        self.stage = StageSpec.from_dict(stage.describe())
        if lotus.settings.enable_cache or lotus.settings.serialization_format != SerializationFormat.DEFAULT:
            raise ValueError("record methods require caching disabled and DEFAULT serialization")
        self.method = self.stage.method
        if type(self.method) in (FilterMethod, MapMethod):
            self.columns, self.instruction = _input_instruction(
                self.method.instruction, self.method.resolved_input_columns)
        generation = self.stage.generation
        options = dict(max_tokens=generation.max_tokens, temperature=generation.temperature,
                       top_p=generation.top_p, stop=list(generation.stop),
                       presence_penalty=generation.presence_penalty,
                       frequency_penalty=generation.frequency_penalty)
        if generation.seed is not None:
            options["seed"] = generation.seed
        if generation.logprobs is not None:
            options.update(logprobs=True, top_logprobs=generation.logprobs)
        self.lm = LM(self.method.model_name, **options)
        self.instructions = task_instructions
        if type(self.method) is FilterMethod:
            self._filter_formatter = _FilterFormatter(self.method, self.lm)
            self.columns = self._filter_formatter.columns
            self.instruction = self._filter_formatter.instruction
        elif type(self.method) is ExtractMethod:
            self.columns = list(self.method.input_columns)
            self.instruction = None
        if not self.columns:
            raise ValueError("a record method must reference an input column")

    def validate_rows(self, records, *, allow_nulls=False):
        """Check ordinary input/settings even when a caller already prepared it."""
        import lotus
        from lotus.types import SerializationFormat

        if lotus.settings.enable_cache or lotus.settings.serialization_format != SerializationFormat.DEFAULT:
            raise ValueError("record methods require caching disabled and DEFAULT serialization")
        if type(allow_nulls) is not bool:
            raise TypeError("allow_nulls must be a boolean")
        for record in records:
            if any(name not in record.values or not (type(record.values[name]) is str
                   or allow_nulls and record.values[name] is None) for name in self.columns):
                raise ValueError("ordinary record methods require present text input columns")

    def prepare_rows(self, records, *, query_id, allow_nulls=False):
        import pandas as pd

        self.validate_rows(records, allow_nulls=allow_nulls)
        if not records:
            return []
        started = time.monotonic_ns()
        if self.stage.kind == "filter":
            messages = self._filter_formatter.messages(
                [record.values for record in records], index=[record.source_index for record in records],
                allow_nulls=allow_nulls)
        else:
            frame = pd.DataFrame([record.values for record in records],
                                 index=[record.source_index for record in records])
            docs = self.instructions.df2multimodal_info(frame, self.columns)
            if type(self.method) is MapMethod:
                messages = [self.instructions.map_formatter(
                    self.lm, doc, self.instruction, system_prompt=self.method.system_prompt) for doc in docs]
            else:
                messages = [self.instructions.extract_formatter(
                    self.lm, doc, self.method.output_columns, self.method.extract_quotes) for doc in docs]
        result = []
        for record, message in zip(records, messages, strict=True):
            result.append(dict(
                stage_identity(query_id, record, self.stage.stage_id, model_role=self.stage.model_role),
                messages_json=json.dumps(message, ensure_ascii=False, allow_nan=False),
                prepare_worker_pid=os.getpid(), prepare_started_ns=started,
                prepare_finished_ns=time.monotonic_ns(),
            ))
        return result

    def interpret_results(self, results):
        from lotus.sem_ops.postprocessors import extract_postprocess, filter_postprocess, map_postprocess

        responses, invalid = [], []
        for result in results:
            try:
                responses.append(engine_response(result, self.lm))
                validate_engine_protocol(result)
            except ValueError as error:
                invalid.append(error)
        for response in responses:
            self.lm._update_stats(response, is_cached=False)
        if invalid:
            raise RuntimeError("record method has failed, unknown, or invalid engine calls") from invalid[0]
        texts = [self.lm._get_top_choice(response) for response in responses]
        if self.stage.kind == "filter":
            return filter_postprocess(texts, self.lm, self.method.default,
                                      output_tokens=self.method.output_tokens)
        if type(self.method) is MapMethod:
            return map_postprocess(texts, self.lm, False)
        return extract_postprocess(texts, self.lm, False)
