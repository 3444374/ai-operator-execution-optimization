"""Offline token-prefix opportunities, without a cache or GPU execution model."""

from collections import Counter
from statistics import median
import time


def distribution(values):
    values = sorted(values)
    if not values:
        return dict(count=0, min=None, median=None, p95=None, max=None)
    return dict(count=len(values), min=values[0], median=median(values),
                p95=values[(95 * len(values) + 99) // 100 - 1], max=values[-1])


def common_prefix(left, right):
    for index, (a, b) in enumerate(zip(left, right)):
        if a != b:
            return index
    return min(len(left), len(right))


def _positive(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(name + ' must be a positive integer')


def analyze(requests, *, block_sizes, window_rows):
    """Keep all occurrences and compare orders within fixed source windows.

    A trie edge contains a complete token block and its parent. Identical blocks
    after different prefixes are distinct. Counts describe shared input content,
    not APC hits, avoided prefill work, or actual GPU batch membership.
    """
    if not block_sizes or len(set(block_sizes)) != len(block_sizes):
        raise ValueError('declare distinct positive block sizes')
    for size in block_sizes:
        _positive(size, 'block size')
    _positive(window_rows, 'window rows')
    ids, tokens = [], []
    for request in requests:
        identity, values = request['call_id'], request['token_ids']
        if type(identity) is not str or not identity:
            raise ValueError('each request requires a call_id')
        if (type(values) not in (tuple, list) or not values
                or any(type(value) is not int or value < 0 for value in values)):
            raise ValueError('each request requires complete nonnegative token IDs')
        ids.append(identity)
        tokens.append(tuple(values))
    if len(set(ids)) != len(ids):
        raise ValueError('duplicate call_id; retain duplicate content as distinct occurrences')

    started = time.perf_counter_ns()
    profiles = []
    for size in block_sizes:
        lookup, nodes, counts = {}, [], Counter()
        full_blocks = 0
        for values in tokens:
            parent = -1
            for offset in range(0, len(values) - size + 1, size):
                block = values[offset:offset + size]
                key = parent, block
                node = lookup.get(key)
                if node is None:
                    node = len(nodes)
                    lookup[key] = node
                    nodes.append(dict(node=node, parent=parent, depth_tokens=offset + size,
                                      block_token_ids=list(block)))
                counts[node] += 1
                full_blocks += 1
                parent = node
        for node in nodes:
            node['occurrences'] = counts[node['node']]
        profiles.append(dict(block_tokens=size, full_block_occurrences=full_blocks,
            distinct_prefix_blocks=len(nodes), shared_prefix_blocks=sum(n > 1 for n in counts.values()),
            repeated_full_block_content_tokens=(full_blocks - len(nodes)) * size,
            partial_tail_tokens=sum(len(values) % size for values in tokens), nodes=nodes))
    trie_seconds = (time.perf_counter_ns() - started) / 1e9

    orders = {}
    for name in ('source', 'length', 'token_prefix'):
        started = time.perf_counter_ns()
        order = []
        for start in range(0, len(tokens), window_rows):
            window = list(range(start, min(start + window_rows, len(tokens))))
            if name == 'length':
                window.sort(key=lambda index: len(tokens[index]))
            elif name == 'token_prefix':
                window.sort(key=lambda index: tokens[index])
            order.extend(window)
        organization_seconds = (time.perf_counter_ns() - started) / 1e9
        adjacent = [common_prefix(tokens[a], tokens[b]) for a, b in zip(order, order[1:])]
        orders[name] = dict(call_ids=[ids[index] for index in order],
            organization_seconds=organization_seconds, adjacent_prefix_tokens=distribution(adjacent),
            adjacent_full_block_content_tokens={str(size): sum(n // size * size for n in adjacent)
                                               for size in block_sizes})
    return dict(schema='semloom.token_prefix_opportunity.v1', requests=len(tokens),
        total_prompt_tokens=sum(map(len, tokens)), prompt_tokens=distribution(list(map(len, tokens))),
        duplicate_token_sequence_counts=sorted(Counter(tokens).values(), reverse=True),
        window_rows=window_rows, block_profiles=profiles, trie_seconds=trie_seconds, orders=orders,
        scope='one captured model/tokenizer namespace; complete token prefix identity; '
              'source-window reordering only, no dependency or cache simulation',
        cache_hits=None, avoided_model_work=None, gpu_batch_membership=None,
        inference_calls=0, performance_claim=False)
