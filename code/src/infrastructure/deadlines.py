"""Caller deadlines, separate from finite startup, polling and cleanup waits."""

import time


def deadline_after(timeout_s):
    return None if timeout_s is None else time.monotonic() + timeout_s


def deadline_expired(deadline):
    return deadline is not None and time.monotonic() >= deadline


def earliest_deadline(*deadlines):
    return min((value for value in deadlines if value is not None), default=None)


def remaining_time(deadline, maximum=None):
    if deadline is None:
        return maximum
    value = max(0., deadline - time.monotonic())
    return value if maximum is None else min(value, maximum)
