"""Compatibility import for the shared observation implementation.

Use one module object so existing diagnostic state and monkeypatches stay shared.
"""
import importlib
import sys

_implementation = importlib.import_module("src.observability.execution_timing")
sys.modules[__name__] = _implementation
