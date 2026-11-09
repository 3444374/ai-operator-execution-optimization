"""CPU-only message-length reuse for the existing prepared-request organizer.

This changes size prediction, never the actual IPC encoder or model input. The
two frames are the organizer's projections of the same validated prepared row.
"""
import json

from src.execution_provider.adapters.lotus_chain_token_batches import PreparedTokenOrganizer
from src.serving.backends.async_engine_wire import _frame


def reused_message_frame_sizes(submit, rpc, limits):
    """Encode the shared message string once, then measure both small envelopes."""
    message = submit['row']['messages_json']
    extra = len(json.dumps(message, ensure_ascii=False, allow_nan=False,
                          separators=(',', ':')).encode())-2
    sizes = []
    for value, maximum in ((submit, limits.item_input_bytes), (rpc, limits.input_bytes)):
        envelope = dict(value, row=dict(value['row'], messages_json=''))
        size = len(_frame(envelope, maximum))-4+extra
        if size > maximum:
            raise ValueError('async engine IPC item exceeds its byte capacity')
        sizes.append(size)
    return tuple(sizes)


class MessageSizeReuseOrganizer(PreparedTokenOrganizer):
    """Explicit research construction; inherit every selection and token rule."""
    _request_frame_sizes = staticmethod(reused_message_frame_sizes)


def direct_json_frame_sizes(submit, rpc, limits):
    """Internal control: ordinary C JSON encoding, without shared measurement."""
    sizes = []
    for value, maximum in ((submit, limits.item_input_bytes), (rpc, limits.input_bytes)):
        size = len(json.dumps(value, ensure_ascii=False, allow_nan=False,
                              separators=(',', ':')).encode())
        if size > maximum:
            raise ValueError('async engine IPC item exceeds its byte capacity')
        sizes.append(size)
    return tuple(sizes)


class DirectJsonSizeOrganizer(PreparedTokenOrganizer):
    _request_frame_sizes = staticmethod(direct_json_frame_sizes)
