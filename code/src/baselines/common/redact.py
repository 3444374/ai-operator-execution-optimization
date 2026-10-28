"""Compatibility import for the shared redaction implementation.

Keep one module object for existing callers, constants and monkeypatches.
"""
import importlib
import sys

_implementation = importlib.import_module("src.infrastructure.redact")
sys.modules[__name__] = _implementation
