"""Read an existing Ray connection identity without importing or starting Ray."""
import sys


def observe_ray_runtime(ray):
    try:
        if (ray is None or sys.modules.get('ray') is not ray
                or not callable(getattr(ray, 'is_initialized', None))
                or ray.is_initialized() is not True
                or not callable(getattr(ray, 'get_runtime_context', None))):
            return None
        context = ray.get_runtime_context()
        identity = dict(job_id=context.get_job_id(), node_id=context.get_node_id())
        if any(type(value) is not str or not value.strip() for value in identity.values()):
            return None
        return ray, identity
    except Exception:
        return None
