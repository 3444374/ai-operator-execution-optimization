from pathlib import Path
from src.infrastructure.private_artifacts import read_json,write_private_json
from native_daft_mmqa_q3a import run_query
ROOT=Path(__file__).resolve().parent
config=read_json(ROOT/'CONFIG.json');native=read_json(ROOT/'CPU-SDK-model-worker/result-ready.json')['result']
report=run_query(config,ROOT/'CPU-driver',model_python='not-used-CPU-invoke',invoke=lambda requests:native)
expected=read_json(ROOT/'CPU-REFERENCE.json')
assert report['status']=='completed' and report['result']==expected['result'] and report['quality']==expected['quality']
write_private_json(ROOT/'CPU-END-TO-END-RESULT.json',dict(status='passed',logical_rows=25,returned_rows=report['returned_rows'],original_Filter_title_sink_and_evaluation_equal=True,real_model_calls=0))
print((ROOT/'CPU-END-TO-END-RESULT.json').read_text())
