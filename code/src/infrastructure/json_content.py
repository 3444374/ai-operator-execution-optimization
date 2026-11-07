"""Compare finite JSON values without encoding or copying complete documents."""
import math


def json_content_equal(left, right):
    """Compare built-in JSON types exactly; invalid or cyclic values differ.

    Object key order is immaterial. Arrays keep order, numeric types and signed
    float zero remain distinct, and tuples require caller-side normalization.
    Traversal retains ancestor identities only, rather than a whole encoded copy.
    """
    left_ancestors, right_ancestors = set(), set()

    def equal(a, b):
        kind = type(a)
        if kind is not type(b):
            return False
        if kind is type(None):
            return True
        if kind in (bool, int, str):
            return a == b
        if kind is float:
            return math.isfinite(a) and math.isfinite(b) and a.hex() == b.hex()
        if kind not in (list, dict) or len(a) != len(b):
            return False
        if kind is dict and (any(type(key) is not str for key in a)
                             or any(type(key) is not str for key in b)):
            return False
        left_id, right_id = id(a), id(b)
        if left_id in left_ancestors or right_id in right_ancestors:
            return False
        left_ancestors.add(left_id)
        right_ancestors.add(right_id)
        try:
            if kind is list:
                return all(equal(x, y) for x, y in zip(a, b))
            return all(key in b and equal(value, b[key]) for key, value in a.items())
        finally:
            left_ancestors.remove(left_id)
            right_ancestors.remove(right_id)

    try:
        return equal(left, right)
    except RecursionError:
        return False
