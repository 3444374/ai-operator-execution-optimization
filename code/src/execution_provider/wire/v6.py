"""Multi-in-flight Map transport; Map semantics and evidence reuse the v5 codec.

Each task RPC returns an accepted-prefix count of zero or one, without waiting
for model completion. A poll RPC returns one completion in completion order.
Only one transport RPC may await its reply; model requests remain concurrent.
"""

from functools import partial

from . import v5

PROTOCOL_VERSION = 6
MAX_WINDOW_TASKS = 64
EXECUTION_ID = v5.ASYNC_EXECUTION_ID
MAX_FRAME_BYTES = v5.MAX_FRAME_BYTES
MAX_INPUT_BYTES = v5.MAX_INPUT_BYTES
MAX_OUTPUT_BYTES = v5.MAX_OUTPUT_BYTES
ERROR_CODES = v5.ERROR_CODES
provider_execution_digest = partial(
    v5.provider_execution_digest, protocol_version=6, provider_execution_id=EXECUTION_ID
)
build_open_message = partial(
    v5.build_open_message, protocol_version=6, provider_execution_id=EXECUTION_ID
)
build_task_message = partial(
    v5.build_task_message, protocol_version=6, provider_execution_id=EXECUTION_ID
)
validate_open = partial(v5.validate_open, protocol_version=6, provider_execution_id=EXECUTION_ID)
validate_task = partial(v5.validate_task, protocol_version=6)
build_completion_message = partial(v5.build_completion_message, protocol_version=6)
validate_completion = partial(v5.validate_completion, protocol_version=6)
build_error_message = partial(v5.build_error_message, protocol_version=6)
validate_error = partial(v5.validate_error, protocol_version=6)
