"""Deterministic context-isolated natural and token-stratified Map samples.

Profile all source messages before selection. Answers never affect selection;
out-of-context messages are reported and excluded without modifying text.
"""
import hashlib
import random

from .squad_map import INSTRUCTION, INPUT_TEMPLATE, SPLITS, WORKLOAD, map_messages, validate_manifest
from ..common.private_artifacts import content_digest
from ...modalities.text.tokenization import chat_token_count


def prepare_capacity_samples(examples, source_sha256, tokenizer, *, rows_per_split,
                             seed, context_limit, tokenizer_identity):
    if type(rows_per_split) is not int or rows_per_split < 3 or type(seed) is not int:
        raise ValueError('at least three rows and an integer seed required')
    if type(context_limit) is not int or context_limit <= 64:
        raise ValueError('invalid model context limit')
    ids = [r.source_example_id for r in examples]
    if not ids or len(ids) != len(set(ids)) or any(not isinstance(i, str) or not i for i in ids):
        raise ValueError('source IDs must be nonempty and unique')
    contexts = [hashlib.sha256(r.context.encode()).hexdigest() for r in examples]
    groups = sorted(set(contexts))
    random.Random(seed).shuffle(groups)
    partition = {key: SPLITS[i % 2] for i, key in enumerate(groups)}
    pools = {key: [] for key in SPLITS}
    profile = []
    for position, (example, context) in enumerate(zip(examples, contexts)):
        text = INPUT_TEMPLATE.format(context=example.context, question=example.question)
        tokens = chat_token_count(tokenizer, map_messages(text))
        eligible = tokens + 64 <= context_limit
        profile.append(dict(source_example_id=example.source_example_id, source_position=position,
                            context_sha256=context, split=partition[context], input_tokens=tokens,
                            fits_context=eligible))
        if eligible:
            pools[partition[context]].append(dict(
                source_example_id=example.source_example_id, source_position=position,
                context_sha256=context, input_text=text, input_bytes=len(text.encode()),
                input_tokens=tokens, messages_sha256=content_digest(map_messages(text)),
                reference_answers=list(example.reference_answers)))
    manifests = {}
    for sampling in ('natural', 'stratified'):
        splits = {}
        for split, pool in pools.items():
            if len(pool) < rows_per_split:
                raise ValueError('insufficient context-eligible partition rows')
            rng = random.Random(f'{seed}:{split}:{sampling}')
            if sampling == 'natural':
                selected = rng.sample(pool, rows_per_split)
            else:
                ordered = sorted(pool, key=lambda r: (r['input_tokens'], r['source_example_id']))
                bins = [ordered[len(ordered)*i//3:len(ordered)*(i+1)//3] for i in range(3)]
                counts = [rows_per_split // 3 + (i < rows_per_split % 3) for i in range(3)]
                selected = [row for group, count in zip(bins, counts) for row in rng.sample(group, count)]
                rng.shuffle(selected)
            splits[split] = selected
        manifest = dict(schema=WORKLOAD, source_sha256=source_sha256, source_rows=len(examples),
                        partition='seeded_context_groups_then_' + sampling, seed=seed,
                        instruction=INSTRUCTION, input_template=INPUT_TEMPLATE,
                        generation={'temperature': 0, 'max_tokens': 64},
                        tokenizer_identity=tokenizer_identity, context_limit=context_limit,
                        profile_sha256=content_digest(profile), splits=splits)
        manifest['sha256'] = content_digest(manifest)
        validate_manifest(manifest)
        manifests[sampling] = manifest
    return dict(schema='semloom.squad.capacity_samples.v1', profile=profile, manifests=manifests,
                excluded_context_rows=sum(not row['fits_context'] for row in profile),
                source_contexts=len(groups), eligible_rows={k: len(v) for k, v in pools.items()})
