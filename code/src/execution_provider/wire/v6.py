"""Multi-in-flight Map transport; Map semantics and evidence share the Map codec.

Each task RPC returns an accepted-prefix count of zero or one, without waiting
for model completion. A poll RPC returns one completion in completion order.
Only one transport RPC may await its reply; model requests remain concurrent.
"""

from functools import partial

from ..limits import MAX_INCREMENTAL_TASKS
from . import map_codec

PROTOCOL_VERSION = 6
MAX_WINDOW_TASKS = MAX_INCREMENTAL_TASKS
EXECUTION_ID = map_codec.ASYNC_EXECUTION_ID
MAX_FRAME_BYTES = map_codec.MAX_FRAME_BYTES
MAX_INPUT_BYTES = map_codec.MAX_INPUT_BYTES
MAX_OUTPUT_BYTES = map_codec.MAX_OUTPUT_BYTES
ERROR_CODES = map_codec.ERROR_CODES
provider_execution_digest = partial(
    map_codec.provider_execution_digest, protocol_version=6, provider_execution_id=EXECUTION_ID
)
build_open_message = partial(
    map_codec.build_open_message, protocol_version=6, provider_execution_id=EXECUTION_ID
)
build_task_message = partial(
    map_codec.build_task_message, protocol_version=6, provider_execution_id=EXECUTION_ID
)
validate_open = partial(
    map_codec.validate_open, protocol_version=6, provider_execution_id=EXECUTION_ID
)
validate_task = partial(map_codec.validate_task, protocol_version=6)
build_completion_message = partial(map_codec.build_completion_message, protocol_version=6)
validate_completion = partial(map_codec.validate_completion, protocol_version=6)
build_error_message = partial(map_codec.build_error_message, protocol_version=6)
validate_error = partial(map_codec.validate_error, protocol_version=6)
