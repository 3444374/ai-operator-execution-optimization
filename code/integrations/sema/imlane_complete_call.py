"""Preserve the author loop while reusing the delivered complete HTTP kernel."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from integrations.sema.imlane_http_udf import complete_http as _original_complete_http
from src.baselines.text.frameworks.native_calls import (
    NativeCallOutcome, _call_from_wire, _outcome_from_wire, _outcome_wire, native_error_text,
)


def complete_http(table):
    call = _call_from_wire(table["native_call"][0].as_py())
    try:
        original_loop = asyncio.get_event_loop()
    except RuntimeError:
        original_loop = None
    started = time.monotonic_ns()
    try:
        try:
            result = _original_complete_http(table)
        except Exception as error:
            # HTTP failures are already values from the shared kernel. Other UDF
            # failures retain their category while the author receives an Arrow result.
            import pyarrow as pa
            outcome = NativeCallOutcome(call.call_id, call.row_id, None,
                execution_error=native_error_text(error))
            result = pa.table({"call_id": pa.array([call.call_id], type=pa.string()),
                "row_id": pa.array([call.row_id], type=pa.int64()),
                "native_outcome": pa.array([_outcome_wire(outcome)], type=pa.string())})
    finally:
        asyncio.set_event_loop(original_loop)
    trace_root = os.environ.get("IMLANE_HTTP_TRACE_DIR")
    if trace_root:
        try:
            outcome = _outcome_from_wire(result["native_outcome"][0].as_py())
            row = dict(call_id=call.call_id, row_id=call.row_id, worker_pid=os.getpid(),
                       kernel_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       request_sha256=hashlib.sha256(call.payload).hexdigest(),
                       status=None if outcome.response is None else outcome.response.status_code,
                       transport_error_type=outcome.transport_error.split(":", 1)[0],
                       execution_error_type=outcome.execution_error.split(":", 1)[0],
                       local_duration_ns=time.monotonic_ns() - started)
            modules = {}
            for name in ('pyarrow', 'aiohttp', 'src.baselines.text.frameworks.native_calls'):
                module = sys.modules.get(name)
                source = getattr(module, '__file__', None)
                modules[name] = dict(version=getattr(module, '__version__', None),
                    source_path=source, source_sha256=(hashlib.sha256(Path(source).read_bytes()).hexdigest()
                                                    if source is not None else None))
            row['runtime'] = dict(python_version=list(sys.version_info[:3]), python_prefix=sys.prefix,
                                  python_executable=sys.executable, modules=modules)
            ray = sys.modules.get("ray")
            if ray is not None and ray.is_initialized():
                context = ray.get_runtime_context()
                row.update(ray_actor_id=str(context.get_actor_id()), ray_job_id=str(context.get_job_id()))
            root = Path(trace_root)
            root.mkdir(parents=True, exist_ok=True)
            with (root / f"{os.getpid()}.jsonl").open("a") as stream:
                stream.write(json.dumps(row) + "\n")
        except Exception:
            # Optional observation is not an original-method failure or a model response.
            pass
    return result
