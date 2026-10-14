"""Only the explicitly enabled diagnostic records CPU-resident metadata."""
import os

if os.environ.get("SEMLOOM_CLIP_TRACE_DIR"):
    import clip_trace_hooks
    clip_trace_hooks.install_kernel_observers()
