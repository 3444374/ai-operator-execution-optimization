"""Open process-local experiment handles from a transferable budget descriptor."""
from contextlib import contextmanager


@contextmanager
def open_request_budget(budget):
    """Own descriptor handles only; a supplied live budget remains caller-owned."""
    if callable(getattr(budget, 'reserve', None)):
        yield budget
        return
    # Keep the POSIX mapping dependency out of existing ordinary budget paths.
    from .mapped_request_budget import MappedUnitClient, MappedUnitDescriptor
    if type(budget) is not MappedUnitDescriptor:
        raise TypeError('request accounting requires a budget or mapped descriptor')
    client = MappedUnitClient(budget)
    try:
        yield client
    except BaseException as error:
        try:
            client.close()
        except BaseException as failure:
            error.add_note('Request budget handle cleanup also failed: ' + type(failure).__name__)
        raise
    else:
        client.close()
