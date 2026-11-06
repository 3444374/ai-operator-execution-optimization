"""Observe installed provider SDK sources for preserved historical replays.

This module reports text and image-message provenance; it does not prepare,
submit or replay model requests.
"""

from .source import SDK_FILES, source_report


IMAGE_TEXT_PROFILE = "lotus-image-text-v1"


def validate_source(*, message_profile="text"):
    """Return installed sources for the selected historical message profile."""
    if type(message_profile) is not str or message_profile not in ("text", IMAGE_TEXT_PROFILE):
        raise ValueError("unsupported LOTUS message profile")
    files = SDK_FILES
    if message_profile == IMAGE_TEXT_PROFILE:
        files = (*files, "utils.py", "dtype_extensions/image.py", "sem_ops/sem_filter.py")
    return source_report("lotus-sdk-" + message_profile, files, provider_sources=True)
