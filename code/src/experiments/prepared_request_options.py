"""Explicit FIFO/candidate options for the existing finite-chain observer entry."""
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.scheduling.organization.prepared_order import PreparedRequestOrder


def owner_options(value, *, tokenizer=None, max_active_requests):
    if value is None:
        return {}
    if type(value) is not dict or set(value) != {'token_budget', 'batch_concurrency', 'order'}:
        raise ValueError('prepared_requests requires token_budget, batch_concurrency and explicit order')
    budget, batches = value['token_budget'], value['batch_concurrency']
    if (type(budget) is not int or budget <= 0 or type(batches) is not int
            or not 1 <= batches <= max_active_requests):
        raise ValueError('positive token target and batch concurrency within the request grant required')
    order = value['order']
    if order is not None:
        if type(order) is not dict:
            raise ValueError('order must be an explicit object or null for FIFO')
        order = PreparedRequestOrder(**order)
    return dict(prepared_batching=PreparedBatching('token_budget', None, budget),
                batch_concurrency=batches, batching_tokenizer=tokenizer, prepared_order=order)
