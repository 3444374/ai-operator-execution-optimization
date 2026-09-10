"""Task-local observation identity; never serialized into a model message."""
from contextvars import ContextVar

request_identity = ContextVar('observed_request_identity', default=None)
# Ray's native HTTP stage accepts a JSON payload. This private envelope field is
# removed by its session adapter before the actual request body is serialized.
RAY_IDENTITY_FIELD = '_semloom_query_observation'
