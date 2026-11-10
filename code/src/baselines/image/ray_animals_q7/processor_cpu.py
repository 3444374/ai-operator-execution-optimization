def main():
    """Actual Ray image/template/token UDFs and local HF processor, zero engine."""
    import asyncio
    from contextlib import contextmanager
    from copy import deepcopy
    import base64
    import gzip
    import hashlib
    from io import BytesIO
    import json
    import os
    from pathlib import Path
    from unittest.mock import patch
    assert os.environ.get('CUDA_VISIBLE_DEVICES') == ''
    import torch
    import ray
    from PIL import Image
    from transformers import AutoConfig, AutoProcessor
    from ray.llm._internal.batch.stages.prepare_image_stage import PrepareImageUDF
    from ray.llm._internal.batch.stages.chat_template_stage import ChatTemplateUDF
    from ray.llm._internal.batch.stages.tokenize_stage import TokenizeUDF
    from ray.data.llm import build_processor, vLLMEngineProcessorConfig
    from vllm.v1.engine.async_llm import AsyncLLM
    from vllm.model_executor.models import ModelRegistry
    ROOT = Path(os.environ['Q7_OUTPUT'])
    MODEL = os.environ['Q7_MODEL']
    import importlib.metadata
    assert importlib.metadata.version('ray') == '2.56.1'
    profile = json.loads(Path(os.environ['Q7_CONFIG']).read_text())
    assert profile['runtime_profile'] in ('official-ray-0.22', 'unified-candidate-0.25.1')
    expected_vllm = {'official-ray-0.22': '0.22.0', 'unified-candidate-0.25.1': '0.25.1'}[profile['runtime_profile']]
    assert importlib.metadata.version('vllm') == expected_vllm
    messages = json.loads(gzip.decompress((ROOT / 'MESSAGES-PRIVATE.json.gz').read_bytes()))
    occurrences = json.loads((ROOT / 'OCCURRENCES.json').read_text())
    assert len(messages) == len(occurrences) == 128
    attempts = []

    def forbidden(*args, **kwargs):
        attempts.append('model_or_Ray_runtime')
        raise AssertionError('CPU only')

    async def collect(iterator):
        return [value async for value in iterator]
    sha = lambda data: hashlib.sha256(data).hexdigest()

    @contextmanager
    def bounded_CPU_runtime():
        try:
            ray.init(address='local', num_cpus=1, num_gpus=0, object_store_memory=83886080, include_dashboard=False, _temp_dir=str(ROOT / 'ray-temp'))
            yield
        finally:
            ray.shutdown()
            (ROOT / 'RAY-CLOSE.json').write_text(json.dumps(dict(driver_disconnected=not ray.is_initialized(), model_constructor_attempts=len(attempts), GPU=0)) + '\n')
    with patch.object(AsyncLLM, '__init__', forbidden), bounded_CPU_runtime():
        image = PrepareImageUDF('data', ['messages'])
        chat = ChatTemplateUDF('data', ['messages'], MODEL)
        tokenize = TokenizeUDF('data', ['prompt'], MODEL)
        batch = [dict(messages=deepcopy(value), __idx_in_batch=i) for i, value in enumerate(messages)]
        prepared_images = asyncio.run(collect(image.udf(batch)))
        prompts = asyncio.run(collect(chat.udf(batch)))
        tokens = asyncio.run(collect(tokenize.udf(prompts)))
        assert [value['__idx_in_batch'] for value in prompts] == list(range(128))
        assert [value['__idx_in_batch'] for value in tokens] == list(range(128))
        config = AutoConfig.from_pretrained(MODEL, local_files_only=True, trust_remote_code=False)
        supported = ModelRegistry.get_supported_archs()
        assert all((architecture in supported for architecture in config.architectures))
        engine_kwargs = json.loads(Path(os.environ['Q7_CONFIG']).read_text())['engine_kwargs']
        engine_cfg = vLLMEngineProcessorConfig(model_source=MODEL, engine_kwargs=engine_kwargs, prepare_image_stage=True)
        processor = build_processor(engine_cfg, preprocess=lambda row: row, postprocess=lambda row: row)
        stages = list(processor.list_stage_names())
        assert any(('image' in str(name).lower() for name in stages))
        assert next((i for i, n in enumerate(stages) if 'image' in n.lower())) < next((i for i, n in enumerate(stages) if 'chat' in n.lower()))
        engine_stage = processor.get_stage_by_name('vLLMEngineStage')
        engine_stage.map_batches_kwargs.update(max_restarts=0, max_task_retries=0)
        assert engine_stage.map_batches_kwargs['max_restarts'] == engine_stage.map_batches_kwargs['max_task_retries'] == 0
        reference = AutoProcessor.from_pretrained(MODEL, local_files_only=True, trust_remote_code=False)
        details = []
        for index, (message, prompt, token, row) in enumerate(zip(messages, prompts, tokens, occurrences, strict=True)):
            text = reference.apply_chat_template(deepcopy(message), tokenize=False, add_generation_prompt=True, continue_final_message=False)
            assert prompt['prompt'] == text
            ids = reference.tokenizer(text)['input_ids']
            assert token['tokenized_prompt'] == ids
            assert prepared_images[index]['__idx_in_batch'] == index
            ray_images = prepared_images[index]['image']
            assert len(ray_images) == 1
            rgb = ray_images[0].convert('RGB')
            assert sha(rgb.tobytes()) == row['RGB_sha256']
            with Image.open(row['source_image']) as source:
                original = source.convert('RGB')
                first = reference(text=[text], images=[original], return_tensors='pt')
                second = reference(text=[text], images=[rgb], return_tensors='pt')
            assert set(first) == set(second)
            assert all((torch.equal(first[key], second[key]) for key in first))
            details.append(dict(row_id=row['row_id'], raw_template_token_ids=ids, expanded_processor_token_ids=first['input_ids'].tolist()[0], image_grid_thw=first['image_grid_thw'].tolist(), pixel_values_shape=list(first['pixel_values'].shape), pixel_values_dtype=str(first['pixel_values'].dtype), pixel_values_sha256=sha(first['pixel_values'].numpy().tobytes()), raw_token_ids_scope='native Ray TokenizeUDF before image-pad expansion;not equal-by-definition to expanded model inputs', expanded_image_pad_tokens=int((first['input_ids'] == reference.tokenizer.convert_tokens_to_ids('<|image_pad|>')).sum())))
        assert len(details) == 128 and (not attempts) and (not torch.cuda.is_initialized()) and ray.is_initialized()
        import sys, time
        sys.path.insert(0, os.environ['Q7_SOURCE'] + '/code')
        from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
        from src.experiments.cell_budget import CellBudgetLedger
        allowance = AttemptBudget('CPU-RAY-ANIMALS-Q7-boundary128', 128)
        ledger = CellBudgetLedger.create(ROOT / 'CPU-ONLY-shared-128.sqlite', allowance, deadline_utc=time.time() + 600)
        ledger.reserve_unit(allowance.budget_id, 128)
        unit = ledger.claim_shared_unit(allowance.budget_id)
        for index in range(128):
            unit.reserve(sha(str(index).encode()))
        try:
            unit.reserve(sha(b'129th'))
        except BudgetExhausted:
            pass
        else:
            raise AssertionError('129th software attempt accepted')
        assert unit.attempts == 128 and unit.remaining == 0
        image_config = reference.image_processor.to_dict()
        import importlib.metadata, inspect
        sdk_paths = {cls.__name__: Path(inspect.getfile(cls)) for cls in (PrepareImageUDF, ChatTemplateUDF, TokenizeUDF)}
        result = dict(status='passed', rows=128, original_LOTUS_message_template_tokens_match=128, actual_Ray_native_image_decode_matches_original_RGB=128, processor_grid_pixels_and_expanded_token_inputs_match=128, processor_class=type(reference).__name__, image_processor_class=type(reference.image_processor).__name__, original_image_processor_config=image_config, no_resize_or_template_override=True, native_stages=stages, engine_kwargs_declared=engine_kwargs, engine_parameters_not_runtime_measured=True, processor_config=engine_cfg.model_dump(), model_architectures=config.architectures, architectures_registered=True, architecture_registration_is_not_actual_GPU_compatibility_test=True, software_guard129_rejected=True, actual_shared_ledger_CPU_boundary128=True, actual_GPU_pre_generate_hook_not_executed=True, image_stage_precedes_chat_template=True, original_wire_messages_not_modified=True, HF_template_mutation_observed=sum((batch[i]['messages'] != messages[i] for i in range(128))), model_constructor_attempts=0, actual_model_calls=0, CUDA_initialized=False, Ray_CPU_runtime_started=True, Ray_GPUs=0, engine_restarts=0, engine_task_retries=0, versions={name: importlib.metadata.version(name) for name in ('ray', 'vllm', 'torch', 'transformers', 'Pillow')}, sdk_hashes={name: sha(path.read_bytes()) for name, path in sdk_paths.items()}, processor_metadata_sha256={name: sha((Path(MODEL) / name).read_bytes()) for name in ('config.json', 'preprocessor_config.json', 'chat_template.json')})
        (ROOT / 'TOKEN-PIXEL-DETAILS.json').write_text(json.dumps(details, indent=2) + '\n')
    assert not ray.is_initialized() and (not attempts)
    result['Ray_CPU_shutdown_confirmed'] = True
    (ROOT / 'CPU-RAY-PROCESSOR.json').write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n')
    print(json.dumps({key: result[key] for key in ('status', 'rows', 'model_constructor_attempts', 'actual_model_calls', 'CUDA_initialized', 'Ray_CPU_shutdown_confirmed')}))
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Zero-model Q7 qualification. Required environment and config schema: adjacent README.md and config.example.json')
    parser.parse_args()
    main()
