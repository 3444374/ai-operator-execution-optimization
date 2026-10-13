"""One process-owned Daft runner shared by its actual calling adapters."""
from threading import RLock


_lock = RLock()
_configured = None


def configure_daft_runner(runner, *, num_threads=None):
    """Reuse a selected runner; reject changes before invoking vendor setup."""
    if runner not in ('native', 'ray'):
        raise ValueError('Daft runner must be native or ray')
    if num_threads is not None and (type(num_threads) is not int or num_threads < 1):
        raise ValueError('Daft native thread count must be a positive integer')
    if runner == 'ray' and num_threads is not None:
        raise ValueError('Daft Ray runner does not take native thread configuration')
    import daft

    global _configured
    with _lock:
        if _configured is not None:
            selected, threads = _configured
            if selected != runner:
                raise RuntimeError(f'Daft runner is already configured as {selected}; cannot switch to {runner} in the same process.')
            if num_threads is not None and threads != num_threads:
                raise RuntimeError('Daft native thread count differs from its existing process configuration')
            return
        if runner == 'ray':
            daft.set_runner_ray(noop_if_initialized=True)
        elif num_threads is None:
            daft.set_runner_native()
        else:
            daft.set_runner_native(num_threads=num_threads)
        _configured = runner, num_threads
