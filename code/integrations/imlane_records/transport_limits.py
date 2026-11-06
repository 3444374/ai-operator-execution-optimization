"""Finite records-bridge declarations, independent of model request budgets.

Rows count Arrow wire occurrences. A packed chain batch is one such row. Byte
allowances protect the existing flat Arrow schemas and serial pipe protocol;
they are not model or hardware concurrency estimates.
"""

from dataclasses import asdict, dataclass
import itertools
import json

from integrations.imlane_records.resource_limits import owner_limits


# The pinned author allocates a 128 MiB Lane. Keep space for both Arrow streams.
LANE_BYTES = 128 * 1024**2
ARROW_IPC_OVERHEAD_BYTES = 65536
_BYTE_FIELDS = frozenset({"max_input_bytes", "max_output_bytes", "max_command_bytes", "max_response_bytes"})
_INPUT_STRINGS = ("call_id", "query_id", "stage_id", "record_json")


@dataclass(frozen=True)
class TransportLimits:
    max_pending_blocks: int
    max_block_rows: int
    max_query_rows: int
    max_queries: int
    owner_shm_bytes: int
    owner_rss_bytes: int
    max_input_bytes: int = 65536
    max_output_bytes: int = 1048576
    max_command_bytes: int = 131072
    max_response_bytes: int = 8 * 1048576

    def __post_init__(self):
        if any(type(value) is not int or not 0 < value < 2**63 for value in asdict(self).values()):
            raise ValueError("records transport declarations must be positive int64 values")
        lane = self.max_input_bytes + self.max_output_bytes + 2 * ARROW_IPC_OVERHEAD_BYTES
        if self.owner_shm_bytes < LANE_BYTES or lane > LANE_BYTES:
            raise ValueError("records input/output declarations exceed the author's 128 MiB Lane or owner SHM allowance")
        # This checks declared wire storage. The existing sampler still checks
        # actual full-process RSS and SHM, including author and model overhead.
        retained = (self.max_pending_blocks + 1) * (self.max_input_bytes + self.max_output_bytes)
        if retained + self.max_command_bytes + self.max_response_bytes > self.owner_rss_bytes:
            raise ValueError("records pending/frame declarations exceed the owner's RSS allowance")

    def describe(self):
        return asdict(self)

    def argv(self):
        return json.dumps(self.describe(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("the bridge requires its complete finite transport declaration")
        return cls(**value)

    def command(self, operation, **values):
        payload = (json.dumps(dict(op=operation, **values), ensure_ascii=False, allow_nan=False) + "\n").encode()
        if len(payload) > self.max_command_bytes:
            raise ValueError("records command exceeds its declared wire allowance before submission")
        return payload

    def check_input(self, rows):
        if not 0 < len(rows) <= self.max_block_rows:
            raise ValueError("records block exceeds its declared wire row allowance")
        if any(type(row) is not dict or set(row) != set(_INPUT_STRINGS) | {"row_id"}
               or any(type(row[name]) is not str for name in _INPUT_STRINGS) for row in rows):
            raise ValueError("records input requires its fixed string and int64 wire fields")
        if any(not row[name] or len(row[name].encode("utf-8")) > 4096
               for row in rows for name in ("call_id", "query_id", "stage_id")):
            raise ValueError("records identity is empty or exceeds the bridge identity byte boundary")
        # The C++ carrier builds four non-null utf8 arrays and one int64 array.
        size = 4 * 4 * (len(rows) + 1) + 8 * len(rows)
        size += sum(len(row[name].encode("utf-8")) for row in rows for name in _INPUT_STRINGS)
        if size > self.max_input_bytes:
            raise ValueError("records input exceeds its declared Arrow byte allowance before submission")
        return size

    def check_output(self, rows):
        size = 2 * 4 * (len(rows) + 1) + 8 * len(rows)
        size += sum(len(row[name].encode("utf-8")) for row in rows for name in ("call_id", "outcome_json"))
        if size > self.max_output_bytes:
            raise ValueError("records output exceeds its declared Arrow byte allowance")
        return size

    def check_submit(self, block_id, rows):
        self.check_input(rows)
        return self.command("submit", block_id=block_id, rows=rows)


def for_owner(args, resources, *, max_query_rows, max_queries=1, packed=False):
    """Resolve known wire work and the caller's already-declared owner budgets."""
    memory = owner_limits(getattr(args, "owner_resource_limits", None))
    supplied = getattr(args, "transport_limits", None)
    if supplied is None:
        supplied = {}
    if type(supplied) is not dict or supplied.keys() - _BYTE_FIELDS:
        raise ValueError("transport_limits accepts only finite input/output/command/response byte allowances")
    return TransportLimits(max_pending_blocks=resources.request_capacity(),
        max_block_rows=1 if packed else resources.prepare_batch_rows,
        max_query_rows=max_query_rows, max_queries=max_queries,
        owner_shm_bytes=memory["shm_bytes"], owner_rss_bytes=memory["rss_bytes"], **supplied)


def query_call_plan(value, max_model_calls):
    """Bind a resident owner's finite query IDs and permanent model claims."""
    if (type(value) is not dict or not value
            or any(type(query) is not str or not query or type(count) is not int or count <= 0
                   for query, count in value.items())
            or max_model_calls is None or sum(value.values()) > max_model_calls):
        raise ValueError("resident queries require explicit positive calls covered by the session model budget")
    return dict(value)


def preflight_blocks(limits, rows, *, block_rows, block_prefix, encode):
    """Validate finite source blocks before allocating an author scope."""
    source = iter(rows)
    count = 0
    for index in itertools.count():
        batch = list(itertools.islice(source, block_rows))
        if not batch:
            return count
        wire = encode(batch)
        count += len(wire)
        if count > limits.max_query_rows:
            raise ValueError("source exceeds the declared wire occurrence allowance")
        limits.check_submit(block_prefix + "-" + str(index), wire)


def close_block(query_id, stage_id, block_prefix):
    """The existing explicit worker close occurrence is outside model work."""
    return block_prefix + "-close", [dict(call_id="imlane-" + block_prefix + "-close:" + query_id,
        row_id=0, query_id=query_id, stage_id=stage_id, record_json=json.dumps({"operation": "close"}))]
