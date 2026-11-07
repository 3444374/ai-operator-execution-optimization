"""Replace selected SDK modules without discarding unrelated lazy imports."""

from contextlib import contextmanager
import sys


@contextmanager
def module_entries(values):
    missing = object()
    previous = {name: sys.modules.get(name, missing) for name in values}
    sys.modules.update(values)
    try:
        yield
    finally:
        for name, module in previous.items():
            if module is missing:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module
