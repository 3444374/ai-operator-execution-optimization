"""Typed scheduling failures used for retry-safe control flow."""


class EndpointCapacityUnavailable(RuntimeError):
    """The selected healthy endpoint is temporarily out of admission credit."""
