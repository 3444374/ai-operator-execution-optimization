"""One already prepared HTTP occurrence per author-owned IMLane invocation."""
from pathlib import Path
import asyncio
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import pyarrow as pa
from src.baselines.text.frameworks.native_calls import _call_from_wire, _outcome_wire, _request


def complete_http(table):
    table.validate(full=True)
    if table.column_names != ['call_id', 'row_id', 'native_call'] or table.num_rows != 1:
        raise ValueError('IMLane complete HTTP function requires one original occurrence')
    call = _call_from_wire(table['native_call'][0].as_py())
    if call.call_id != table['call_id'][0].as_py() or call.row_id != table['row_id'][0].as_py():
        raise ValueError('IMLane complete HTTP input changed occurrence identity')
    # IMLane invokes a synchronous Arrow UDF; asyncio is local to its HTTP kernel.
    outcome = asyncio.run(_request(call))
    return pa.table({'call_id': pa.array([outcome.call_id], type=pa.string()),
        'row_id': pa.array([outcome.row_id], type=pa.int64()),
        'native_outcome': pa.array([_outcome_wire(outcome)], type=pa.string())})
