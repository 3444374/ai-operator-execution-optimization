"""No-model VARCHAR kernels loaded only by the CPU qualification entry."""

import pyarrow as pa


def echo_strings(table):
    return pa.table({'value': table['c1'].combine_chunks()})


def echo_sliced(table):
    values = table['c1'].combine_chunks()
    padded = pa.concat_arrays([pa.array(['offset sentinel'], type=pa.string()), values])
    return pa.table({'value': padded.slice(1, len(values))})


def return_null(table):
    values = table['c1'].to_pylist()
    return pa.table({'value': pa.array([
        None if value == 'null sentinel' else value for value in values
    ], type=pa.string())})
