from copy import deepcopy
SAMPLING = dict(max_tokens=8192,temperature=0,top_p=1,seed=0,stop=[],presence_penalty=0,frequency_penalty=0,repetition_penalty=1,n=1,structured_outputs={"json_object": True})
def preprocess(row):
    return dict(messages=deepcopy(row["messages"]), sampling_params=deepcopy(SAMPLING))
def postprocess(row):
    return dict(call_id=row["call_id"], generated_text=row["generated_text"], native_usage={k:row[k] for k in ("request_id","batch_uuid","num_input_tokens","num_generated_tokens","time_taken_llm") if k in row})
def build(model):
    from ray.data.llm import build_processor, vLLMEngineProcessorConfig
    processor = build_processor(vLLMEngineProcessorConfig(model_source=model,engine_kwargs={}),preprocess=preprocess,postprocess=postprocess)
    for name in processor.list_stage_names():
        stage = processor.get_stage_by_name(name)
        if type(stage).__name__ == "vLLMEngineStage":
            stage.map_batches_kwargs.update(max_restarts=0,max_task_retries=0)
    return processor
def execute(rows, model):
    import ray.data
    predictions = build(model)(ray.data.from_items(rows)).materialize().take_all()
    expected = {row["call_id"] for row in rows}
    actual = [row["call_id"] for row in predictions]
    if len(actual) != len(expected) or set(actual) != expected:
        raise ValueError("Missing, duplicate or unrelated Medical occurrence")
    return predictions
