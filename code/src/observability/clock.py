"""Process-independent monotonic clock identity, without execution dependencies."""

import hashlib
import os
from pathlib import Path
import time


def monotonic_clock_domain():
    """Identify a shared Linux monotonic clock without exposing host identity."""
    try:
        value = (Path('/proc/sys/kernel/random/boot_id').read_bytes()
                 + os.readlink('/proc/self/ns/time').encode()
                 + time.get_clock_info('monotonic').implementation.encode())
    except OSError:
        return None
    return hashlib.sha256(value).hexdigest()
