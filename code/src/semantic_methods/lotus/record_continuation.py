"""Opt-in record-local LOTUS programs for the existing Core method driver."""

from dataclasses import asdict
import json

from src.semantic_methods.continuation import Continue, Final, Request
from .record_chain import ChainRun, RecordOccurrence, _parent_seeds
from src.serving.backends.vllm_engine import IDENTITY_FIELDS


def encode(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode("utf-8")


class PreparedCallProgram:
    """One already-formatted stage; its original parser remains at the caller."""

    def __init__(self, stage, *, max_result_bytes):
        self.stage, self.max_result_bytes = stage, max_result_bytes

    @staticmethod
    def input(row, *, parent_call_ids=()):
        return encode(dict(row=row, parent_call_ids=list(parent_call_ids)))

    def start(self, value):
        value = json.loads(value)
        row = value["row"]
        if row["algorithm_stage"] != self.stage.stage_id or row["model_role"] != self.stage.model_role:
            raise ValueError("prepared call changed its declared stage")
        value["generation"] = asdict(self.stage.generation)
        return Continue(Request(self.stage.stage_id, encode(value), 1, self.max_result_bytes),
                        encode({name: row[name] for name in IDENTITY_FIELDS}))

    def resume(self, state, result):
        identity, value = json.loads(state), json.loads(result)
        if any(value.get(name) != identity[name] for name in IDENTITY_FIELDS):
            raise ValueError("prepared result changed its stage evaluation identity")
        return Final(result)


class LotusChainProgram:
    """One shared formatter/parser set; each row keeps its own bounded bytes.

    Extract and global algorithms remain on their original relation path. This
    program only generates already-permitted Filter/Map requests, not scheduling.
    """

    def __init__(self, chain, *, max_result_bytes, estimate_tokens=None):
        if any(stage.kind not in ("filter", "map") for stage in chain.stages):
            raise ValueError("FIFO continuations support only record-local Filter/Map")
        if len({stage.model_role for stage in chain.stages}) != 1:
            raise ValueError("a FIFO program requires one explicit model role")
        if len({stage.method.model_name for stage in chain.stages}) != 1:
            raise ValueError("a FIFO program requires one model deployment")
        if type(max_result_bytes) is not int or max_result_bytes <= 0:
            raise ValueError("a positive result byte limit is required")
        if estimate_tokens is not None and not callable(estimate_tokens):
            raise TypeError("token estimation must be an explicit callable")
        self.chain, self.max_result_bytes = chain, max_result_bytes
        self.estimate_tokens = estimate_tokens

    @staticmethod
    def input(record, *, parent_call_ids=(), columns=None, allow_nulls=False):
        if type(record) is not RecordOccurrence:
            raise TypeError("a RecordOccurrence is required")
        _parent_seeds([(record.row_id, record.source_position)],
                      {(record.row_id, record.source_position): parent_call_ids})
        return encode(dict(record=record.describe(), parent_call_ids=parent_call_ids,
                           columns=columns, allow_nulls=allow_nulls))

    def _step(self, run):
        if not run.prepare():
            result = run.result()
            result["records"] = [r.describe() for r in result["records"]]
            return Final(encode(result))
        (row,) = run.prepared
        key = (row["row_id"], row["source_position"])
        value = dict(row=row, generation=asdict(run.method.stage.generation),
                     parent_call_ids=run.parents.get(key, ()))
        units = 1
        if self.estimate_tokens is not None:
            value["token_cost"] = self.estimate_tokens(row, run.method.stage.generation)
            units = value["token_cost"]["reserved_tokens"]
        payload = encode(value)
        return Continue(Request(run.method.stage.stage_id, payload, units, self.max_result_bytes),
                        encode(run.snapshot()))

    def start(self, value):
        value = json.loads(value)
        record = RecordOccurrence.from_dict(value["record"])
        run = self.chain.start_batch([record],
            initial_parents={(record.row_id, record.source_position): tuple(value["parent_call_ids"])},
            columns=value["columns"], allow_nulls=value["allow_nulls"])
        return self._step(run)

    def resume(self, state, result):
        run = ChainRun.restore(self.chain, json.loads(state))
        try:
            run.complete([json.loads(result)])
            return self._step(run)
        except BaseException as error:
            run.failure(error)
            raise
