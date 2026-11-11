"""Inspect explicit LOTUS text layout candidates without invoking a model.

Layout changes require a separate semantic profile and real quality evaluation
before use in a query. This module only calls the original method's formatter.
"""
from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import inspect
import json
import time

from src.infrastructure.private_artifacts import content_digest
from src.modalities.text.tokenization import chat_token_ids
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS


@contextmanager
def text_layout(method, *, instruction_first, columns):
    """An isolated profiling process owns this temporary formatter replacement."""
    instructions = method.instructions
    original, original_columns = instructions.user_message_formatter, method.columns

    def format_user(data, instruction=None):
        if instruction is None:
            return original(data, instruction)
        text, images = instructions.context_formatter(data)
        if images:
            raise ValueError('this layout study only supports text requests')
        return dict(role='user', content=f'{instruction}\n\nContext:\n{text}')

    try:
        if instruction_first:
            instructions.user_message_formatter = format_user
        method.columns = tuple(columns)
        yield
    finally:
        method.columns = original_columns
        instructions.user_message_formatter = original


def prefix_tree_opportunity(token_rows, block_tokens):
    """Count exact prefix blocks, including subgroups below the common prefix.

    A prefix tree describes possible reuse; it is not an engine cache simulation
    or a shared-attention kernel cost model.
    """
    tree, total, common = {}, 0, None
    for tokens in token_rows:
        node = tree
        blocks = [tuple(tokens[i:i + block_tokens])
                  for i in range(0, len(tokens) - block_tokens + 1, block_tokens)]
        if common is None:
            common = blocks
        else:
            n = 0
            for left, right in zip(common, blocks):
                if left != right:
                    break
                n += 1
            common = common[:n]
        for block in blocks:
            node = node.setdefault(block, {})
            total += 1

    def count_nodes(node):
        # Iterative traversal handles long captured requests without recursion.
        pending, count = [node], 0
        while pending:
            children = pending.pop()
            count += len(children)
            pending.extend(children.values())
        return count

    unique = count_nodes(tree)
    return dict(requests=len(token_rows), full_block_tokens=total * block_tokens,
                unique_prefix_block_tokens=unique * block_tokens,
                shared_prefix_tree_tokens=(total - unique) * block_tokens,
                all_member_common_tokens=len(common or ()) * block_tokens)


def profile_layouts(method, records, tokenizer, *, query_id, block_tokens, window_rows):
    """Preserve records/fields and capture each complete, explicitly changed prompt."""
    columns = tuple(method.columns)
    # Same field values receive the same key; common fields move earlier.
    common_first = tuple(sorted(columns, key=lambda name:
        (len({content_digest(record.values[name]) for record in records}), columns.index(name))))
    variants = [('original', False, columns)]
    move_instruction = method.stage.kind in ('filter', 'map')
    if move_instruction:
        variants.append(('instruction_first', True, columns))
    if common_first != columns and method.stage.kind in ('map', 'extract'):
        name = 'instruction_first_common_fields' if move_instruction else 'common_fields'
        variants.append((name, move_instruction, common_first))
    generation = asdict(method.stage.generation)
    arms = []
    for name, first, order in variants:
        started = time.perf_counter_ns()
        with text_layout(method, instruction_first=first, columns=order):
            rows = method.prepare_rows(records, query_id=query_id)
        prepared_ns = time.perf_counter_ns() - started
        requests = []
        started = time.perf_counter_ns()
        for row in rows:
            tokens = chat_token_ids(tokenizer, json.loads(row['messages_json']))
            requests.append(dict(row=row, generation=generation, token_ids=tokens,
                                 prompt_tokens=len(tokens)))
        tokenize_ns = time.perf_counter_ns() - started
        digest = content_digest([dict(row={k:r['row'][k] for k in (*IDENTITY_FIELDS, 'messages_json')},
                                     generation=r['generation'], token_ids=r['token_ids']) for r in requests])
        windows = [prefix_tree_opportunity([r['token_ids'] for r in requests[i:i + window_rows]], block_tokens)
                   for i in range(0, len(requests), window_rows)]
        arms.append(dict(layout=name, columns=order, original_layout=name == 'original',
                         quality='not_evaluated', formatter_ns=prepared_ns, tokenizer_ns=tokenize_ns,
                         capture=dict(schema='semloom.semantic_request_capture.v1', requests=requests,
                                      request_digest=digest, complete_initial_operator=True), windows=windows))
    return dict(schema='semloom.request_layout_study.v1', source_type='original_method_CPU_formatting',
                formatter_sha256=hashlib.sha256(inspect.getsource(method.instructions.user_message_formatter).encode()).hexdigest(),
                field_values_digest=content_digest([r.values for r in records]),
                block_tokens_hypothesis=block_tokens, window_rows=window_rows, arms=arms,
                model_calls=0, gpu=False,
                scope='Different complete prompt layouts. Fields and occurrences retained; semantic quality and engine effects are unmeasured.')
