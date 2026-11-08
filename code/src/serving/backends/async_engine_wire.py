"""Shared async IPC encoding and compact terminal records, without transport."""
from __future__ import annotations

import json
import struct

_HEADER = struct.Struct("!I")
_OPEN_FRAME_BYTES = 64 * 1024


def _frame(value, maximum):
    data = bytearray()
    for piece in json.JSONEncoder(ensure_ascii=False, allow_nan=False,
                                  separators=(",", ":")).iterencode(value):
        piece = piece.encode()
        if len(data) + len(piece) > maximum:
            raise ValueError("async engine IPC item exceeds its byte capacity")
        data.extend(piece)
    return _HEADER.pack(len(data)) + data



def _compact_result(value):
    # Preserve the shared converter's identity, usage and confirmation values.
    # Only potentially large output representations are omitted on overflow.
    return dict(value, text=None, prompt_token_ids_json="null", output_token_ids_json="null",
                logprobs_json="null", status="unknown" if value["status"] == "unknown" else "failed",
                error_type="ResultCapacityExceeded", result_fields_omitted=True)
