"""A fixed-capacity research choice using declared consumers and measured rebuilds.

This is an engineering heuristic, not an optimal cache or a novelty claim. It
only suggests reclaimable keys. The representation owner applies the decision.
"""
from dataclasses import dataclass
from fractions import Fraction


@dataclass(frozen=True)
class RetentionCandidate:
    key: str
    payload_bytes: int
    future_consumers: int
    rebuild_ns: int
    active_consumers: int = 0
    last_access: int = 0

    @property
    def value(self):
        return self.future_consumers * self.rebuild_ns


def consumer_value_victims(incoming, retained, *, capacity_bytes, max_entries):
    """Choose idle victims only if incoming estimated value exceeds their sum.

    Future consumers are declarations, not completed uses. Costs come from past
    preparations. Incorrect predictions can hurt performance, never ownership.
    """
    used_bytes, used_entries = sum(value.payload_bytes for value in retained), len(retained)
    if (not incoming.future_consumers or incoming.payload_bytes > capacity_bytes
            or incoming.rebuild_ns == 0):
        return ()
    candidates = sorted((value for value in retained if value.active_consumers == 0),
                        key=lambda v: (Fraction(v.value, v.payload_bytes), v.key))
    victims = []
    lost_value = 0
    for value in candidates:
        if used_bytes + incoming.payload_bytes <= capacity_bytes and used_entries < max_entries:
            break
        victims.append(value.key)
        used_bytes -= value.payload_bytes
        used_entries -= 1
        lost_value += value.value
    if (used_bytes + incoming.payload_bytes > capacity_bytes or used_entries >= max_entries
            or incoming.value <= lost_value):
        return ()
    return tuple(victims)


def lru_victims(incoming, retained, *, capacity_bytes, max_entries):
    """Ordinary recency comparator with the same active-borrow protection."""
    used_bytes, used_entries = sum(value.payload_bytes for value in retained), len(retained)
    if not incoming.future_consumers or incoming.payload_bytes > capacity_bytes:
        return ()
    victims = []
    for value in sorted((v for v in retained if v.active_consumers == 0), key=lambda v: (v.last_access, v.key)):
        if used_bytes + incoming.payload_bytes <= capacity_bytes and used_entries < max_entries:
            break
        victims.append(value.key)
        used_bytes -= value.payload_bytes
        used_entries -= 1
    if used_bytes + incoming.payload_bytes > capacity_bytes or used_entries >= max_entries:
        return ()
    return tuple(victims)
