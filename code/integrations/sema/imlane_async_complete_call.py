"""One complete HTTP occurrence awaited on the original author executor loop."""
import asyncio
import hashlib
import json
import marshal
import os
from pathlib import Path
import sys
import threading
import time
from types import CodeType
import uuid
import weakref

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import pyarrow as pa
from src.baselines.text.frameworks import native_calls as _native_calls
from src.baselines.text.frameworks.native_calls import (
    NativeCallOutcome, _call_from_wire, _outcome_wire, _request, native_error_text,
)

_KERNEL_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
_REQUEST_SOURCE_SHA256 = hashlib.sha256(Path(_request.__code__.co_filename).read_bytes()).hexdigest()
_REQUEST_CODE_SHA256 = hashlib.sha256(marshal.dumps(_request.__code__)).hexdigest()
_LOOP_GENERATIONS = weakref.WeakKeyDictionary()


def _check_loaded_source(module):
    """Check executing function bodies, including decorated helpers, against source."""
    path=Path(module.__file__).resolve()
    compiled=compile(path.read_bytes(),str(path),'exec',dont_inherit=True,optimize=sys.flags.optimize)
    def check(body,container):
        for code in body.co_consts:
            if not isinstance(code,CodeType):continue
            value=getattr(container,code.co_name,None)
            if isinstance(value,type):check(code,value)
            else:
                value=getattr(value,'__wrapped__',value)
                actual=getattr(value,'__code__',None)
                if actual is not None and (actual!=code or Path(actual.co_filename).resolve()!=path):
                    raise RuntimeError('loaded native HTTP code differs from its source: '+code.co_name)
    check(compiled,module)


_check_loaded_source(_native_calls)


def _trace(call, outcome, started):
    directory = os.environ.get('IMLANE_HTTP_TRACE_DIR')
    if not directory:
        return
    try:
        loop = asyncio.get_running_loop()
        generation = _LOOP_GENERATIONS.get(loop)
        if generation is None:
            generation = _LOOP_GENERATIONS[loop] = uuid.uuid4().hex
        row = dict(call_id=call.call_id, row_id=call.row_id, worker_pid=os.getpid(),
            native_thread_id=threading.get_native_id(), loop_generation=generation,
            loop_running=loop.is_running(), loop_closed=loop.is_closed(),
            kernel_sha256=_KERNEL_SHA256, native_calls_sha256=_REQUEST_SOURCE_SHA256,
            loaded_request_code_sha256=_REQUEST_CODE_SHA256,loaded_http_code_matches_source=True,
            request_sha256=hashlib.sha256(call.payload).hexdigest(),
            status=None if outcome.response is None else outcome.response.status_code,
            transport_error_type=outcome.transport_error.split(':', 1)[0],
            execution_error_type=outcome.execution_error.split(':', 1)[0],
            local_duration_ns=time.monotonic_ns() - started)
        ray = sys.modules.get('ray')
        if ray is not None and ray.is_initialized():
            context = ray.get_runtime_context()
            row.update(ray_actor_id=str(context.get_actor_id()), ray_job_id=str(context.get_job_id()))
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        with (root / f'{os.getpid()}.jsonl').open('a') as stream:
            stream.write(json.dumps(row) + '\n')
    except Exception:
        # Optional observations do not replace the original result or first error.
        pass


async def complete_http(table):
    call = _call_from_wire(table['native_call'][0].as_py())
    started = time.monotonic_ns()
    try:
        table.validate(full=True)
        if table.column_names != ['call_id', 'row_id', 'native_call'] or table.num_rows != 1:
            raise ValueError('IMLane complete HTTP function requires one original occurrence')
        if call.call_id != table['call_id'][0].as_py() or call.row_id != table['row_id'][0].as_py():
            raise ValueError('IMLane complete HTTP input changed occurrence identity')
        # The author awaits this function. The existing kernel owns and closes
        # its per-call session, including its request-local redirect cookie state.
        outcome = await _request(call)
    except Exception as error:
        outcome = NativeCallOutcome(call.call_id, call.row_id, None,
                                    execution_error=native_error_text(error))
    result = pa.table({'call_id': pa.array([outcome.call_id], type=pa.string()),
        'row_id': pa.array([outcome.row_id], type=pa.int64()),
        'native_outcome': pa.array([_outcome_wire(outcome)], type=pa.string())})
    _trace(call, outcome, started)
    return result
