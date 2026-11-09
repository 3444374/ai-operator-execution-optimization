import os
"""Original MMQA title sink/evaluator after complete Ray native outputs."""
import json,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCE=Path(os.environ['CASE_SOURCE']);sys.path.insert(0,str(SOURCE/'code'))
def main():
 from transformers import AutoTokenizer
 from src.infrastructure.private_artifacts import json_metric
 import mmqa_semantics
 output=Path(sys.argv[1]);config=json.loads((ROOT/'CONFIG.json').read_text());started=time.monotonic_ns()
 tok=AutoTokenizer.from_pretrained(config['model'],local_files_only=True,trust_remote_code=False)
 query,method,prepared,requests=mmqa_semantics.prepare(config,tok)
 result,semantic,texts=mmqa_semantics.finish(query,method,prepared,json.loads((output/'predictions-PRIVATE.json').read_text()))
 sink=time.monotonic_ns();(output/'semantic-results-PRIVATE.json').write_text(json.dumps(dict(result=mmqa_semantics.frame_value(result),**semantic)))
 evaluated=time.monotonic_ns();quality=json_metric(query.evaluate(result));end=time.monotonic_ns()
 (output/'driver.json').write_text(json.dumps(dict(baseline_name='Ray Data LLM native / project MMQA Q4 binding',semantic_operator='Original text genres JSON Extract -> original genre/title groups',execution_backend='official Ray build_processor / native vLLM0.22 actor',result=mmqa_semantics.frame_value(result),quality=quality,timing=dict(finish_started_monotonic_ns=started,sink_ready_monotonic_ns=sink,evaluation_started_monotonic_ns=evaluated,evaluation_finished_monotonic_ns=end)),indent=2)+'\n')
if __name__=='__main__':main()
