"""Project Movie Q3 binding using Daft's native AI prompt expression."""

from ..sembench_movie_native import q3_decision, q3_messages


def execute_q3(rows, *, model, tokenizer, sampling_params, engine_kwargs=None,
               provider_options=None):
    """Run in a supervised process; Daft owns AI scheduling and model lifetime.

    The caller supplies the selected model's local tokenizer. Daft's native
    vLLM expression takes raw strings, so chat serialization is explicit here.
    This experimental provider has no public model-close/usage receipt; process
    exit and physical attempts must be observed by the outer experiment runner.
    """
    options = dict(provider_options or {})
    # The local native sink ignores distributed actor/resource options.
    allowed = {'do_prefix_routing', 'max_buffer_size', 'min_bucket_size',
               'prefix_match_threshold', 'batch_size'}
    if options.keys() - allowed:
        raise ValueError('unsupported native Daft AI options: ' + ', '.join(sorted(options.keys() - allowed)))
    eligible = sum(row['id'] == 'taken_3' for row in rows)
    metadata = dict(backend='daft-native-ai', implementation='project SemBench workload binding',
        lifecycle='isolated caller process; native AI owner',
        logical_inference_rows=eligible, physical_attempts=None,
        model=model, sampling_params=dict(sampling_params), engine_kwargs=dict(engine_kwargs or {}),
        provider_options=options, omitted_options='Daft/vLLM native defaults',
        resolved_engine_capacity=None, model_close_receipt=None,
        usage='native expression returns text only', provider_status='upstream experimental')
    if not rows or not eligible:
        return dict(predictions=[], positive_review_cnt=0, metadata=metadata)

    import daft
    from daft.functions.ai import prompt
    from vllm import SamplingParams

    metadata['daft_version'] = daft.__version__
    if daft.get_or_infer_runner_type() != 'native':
        raise ValueError('this binding requires the Daft native runner in its caller process')
    metadata['runner'] = 'native; caller configuration or SDK default'
    # Keep the complete input relation. The native filter precedes the AI node;
    # only eligible rows need the model-native chat serialization.
    inputs = [dict(row, _q3_prompt=tokenizer.apply_chat_template(
        q3_messages(row['reviewText']), tokenize=False, add_generation_prompt=True)
        if row['id'] == 'taken_3' else '') for row in rows]
    frame = daft.from_pylist(inputs).filter(daft.col('id') == 'taken_3')
    frame = frame.with_column('generated_text', prompt(daft.col('_q3_prompt'),
        provider='vllm-prefix-caching', model=model, engine_args=dict(engine_kwargs or {}),
        generate_args={'sampling_params': SamplingParams(**dict(sampling_params))}, **options))
    materialized = frame.with_column('decision', daft.col('generated_text').apply(
        q3_decision, return_dtype=daft.DataType.bool())).select(
            'occurrence_id', 'reviewId', 'id', 'generated_text', 'decision').collect()
    predictions = materialized.to_pylist()
    count = materialized.filter(daft.col('decision')).count_rows()
    return dict(predictions=predictions, positive_review_cnt=count, metadata=metadata)
