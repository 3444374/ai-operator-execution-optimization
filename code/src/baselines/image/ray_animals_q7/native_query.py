"""Official Ray multimodal Processor with the already qualified experiment profile."""
from copy import deepcopy
import json
import os
from pathlib import Path
CONFIG=json.loads(Path(os.environ['Q7_CONFIG']).read_text())

def preprocess(row):
    return dict(messages=deepcopy(row['messages']),sampling_params=deepcopy(CONFIG['sampling']))

def postprocess(row):
    return dict(call_id=row['call_id'],generated_text=row['generated_text'],
        native_usage={k:row[k] for k in ('request_id','batch_uuid','num_input_tokens','num_generated_tokens','time_taken_llm') if k in row})

def build(model):
    from ray.data.llm import build_processor,vLLMEngineProcessorConfig
    processor=build_processor(vLLMEngineProcessorConfig(model_source=model,engine_kwargs=deepcopy(CONFIG['engine_kwargs']),
        prepare_image_stage=True),preprocess=preprocess,postprocess=postprocess)
    for name in processor.list_stage_names():
        stage=processor.get_stage_by_name(name)
        if type(stage).__name__=='vLLMEngineStage':stage.map_batches_kwargs.update(max_restarts=0,max_task_retries=0)
    return processor

def execute(rows,model):
    import ray.data
    count=len(rows)
    assert count>0 and len({r['call_id'] for r in rows})==count
    predictions=build(model)(ray.data.from_items(rows)).materialize().take_all()
    assert len(predictions)==len({r['call_id'] for r in predictions})==count
    assert {r['call_id'] for r in predictions}=={r['call_id'] for r in rows}
    return predictions
