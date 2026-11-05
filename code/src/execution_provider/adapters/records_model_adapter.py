"""Bounded records model adaptation shared by synchronous and async owners.

Query orchestration depends on this adapter. The adapter owns no query loop,
capacity ledger or scheduling policy, and never imports the query host.
"""
from contextlib import nullcontext
import json

from src.semantic_methods.lotus.records import CallLedger, identity
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS

from .lotus_records_actor_owner import RecordsActorOwner, _ModelActor


def _measure(timings, name):
    return nullcontext() if timings is None else timings.measure(name)


def _encode(value, maximum):
    encoded = bytearray()
    encoder = json.JSONEncoder(ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    for piece in encoder.iterencode(value):
        piece = piece.encode("utf-8")
        if len(encoded) + len(piece) > maximum:
            raise ValueError("records bytes exceed the reserved item capacity")
        encoded.extend(piece)
    return bytes(encoded)


def _capacity_failure(value):
    """Keep identity and known usage when a full result cannot fit its reservation."""
    occurrence = {name:value[name] for name in IDENTITY_FIELDS}
    return dict(
        occurrence, status="unknown" if value["status"] == "unknown" else "failed",
        text=None, prompt_tokens=value.get("prompt_tokens"),
        completion_tokens=value.get("completion_tokens"), total_tokens=value.get("total_tokens"),
        submission_confirmed=value.get("submission_confirmed", value["status"] == "succeeded"),
        error_type="ResultCapacityExceeded", result_fields_omitted=True,
    )


def _check_result_capacity(occurrence, maximum):
    # Real token counts are lengths of finite Python sequences. Reserve their
    # widest supported representation before allowing this call to be sent.
    value = dict(occurrence, status="unknown", prompt_tokens=(1 << 64) - 1,
                 completion_tokens=(1 << 64) - 1, total_tokens=(1 << 65) - 2)
    _encode(_capacity_failure(value), maximum)


class _CoreModelActor(_ModelActor):
    def run_core(self, rows, deadline, maximum):
        result = self.run(rows, deadline)
        if type(result) is not list or len(result) != 1:
            raise RuntimeError("single-row model completion is unconfirmed")
        value = result[0]
        occurrence = identity(rows[0]["query_id"], rows[0]["row_id"],
                              rows[0]["source_position"], rows[0]["call_index"])
        audit = CallLedger([occurrence], 1)
        audit.record(value)
        # The driver and Core receive only bounded bytes. The small failure
        # representation was checked before dispatch and retains known billing.
        try:
            return _encode(value, maximum)
        except ValueError:
            return _encode(_capacity_failure(value), maximum)


class _ActorOwner(RecordsActorOwner):
    """Reuse only actor allocation and shutdown, with a bounded result method."""

    close_deadline = None

    def _model_actor_type(self):
        return _CoreModelActor
