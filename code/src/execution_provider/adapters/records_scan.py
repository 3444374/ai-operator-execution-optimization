"""Prepare the Daft Native CSV scan shared by the two record owners."""

import os


def prepare_native_records_scan(source, scan, *, owner_name):
    """Return the projected frame and the caller-owned Ray module.

    The caller has already checked the source declaration and semantic method.
    """
    import daft
    import ray
    from daft.runners import _get_runner, get_or_create_runner
    from src.infrastructure.daft_runtime import configure_daft_runner
    if ray.__version__ != "2.56.1" or not ray.is_initialized():
        raise RuntimeError(f"{owner_name} requires caller-owned Ray 2.56.1")
    if daft.__version__ != "0.7.21":
        raise RuntimeError(f"{owner_name} requires Daft 0.7.21 Native")
    if _get_runner() is None:
        if os.environ.get("DAFT_RUNNER") not in (None, "native"):
            raise RuntimeError(f"{owner_name} requires an explicit Daft Native configuration")
        configure_daft_runner("native")
    if get_or_create_runner().name != "native":
        raise RuntimeError(f"{owner_name} requires Daft 0.7.21 Native")
    types = {"string": daft.DataType.string(), "int64": daft.DataType.int64()}
    frame = daft.read_csv(str(source.csv_path), infer_schema=False, schema={
        name: types[dtype] for name, dtype in scan.csv_schema
    })
    if scan.predicate is not None:
        column, value = scan.predicate
        frame = frame.filter(daft.col(column) == value)
    return frame.select("row_id", "source_position", *scan.payload_columns), ray
