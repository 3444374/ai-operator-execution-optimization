"""Descriptive application comparisons; retain every warmup and repetition."""
from collections import defaultdict
from pathlib import Path
from statistics import median,stdev
import json
root=Path(__file__).resolve().parent
records=json.loads((root/'records.json').read_text())
rows=[]
for role in ('pg-map','lotus-map','duckdb-ai','daft-prompt','sema-map','pg-q3','lotus-q3'):
    selected=[v for v in records if v['role']==role and v['stage'] in ('evaluation','q3-evaluation')]
    measured=sorted((v for v in selected if v['repeat']>0),key=lambda v:v['repeat'])
    warm=next(v for v in selected if v['repeat']==0)
    assert len(measured)==3
    times=[v['full_query_seconds'] for v in measured]
    row=dict(role=role,point=warm['point'],rows_per_query=warm['expected_rows'],
        warmup_seconds=warm['full_query_seconds'],measured_seconds=times,median_seconds=median(times),
        sample_cv=stdev(times)/(sum(times)/3),observed_peak_http=[v['observed_peak_http'] for v in measured],
        accuracy_percent=[100*(v['quality']['true_positive']+v['quality']['true_negative'])/v['expected_rows'] for v in measured],
        quality=[v['quality'] for v in measured],model_prompt_tokens=[v['model_counter_delta']['vllm:prompt_tokens_total'] for v in measured],
        model_output_tokens=[v['model_counter_delta']['vllm:generation_tokens_total'] for v in measured],
        monetary_cost=dict(status='unavailable',reason='local Qwen pricing is not provided'),
        scope='three repeated queries over the same fixed collection; descriptive finite-candidate result')
    if 'q3' in role:row['count_results']=[v['count_result'] for v in measured];row['original_quality']=[v['original_query_quality'] for v in measured]
    rows.append(row)
value=dict(schema='semloom.semantic_application_descriptive.v1',rows=rows,
    full_query_formula='(t_query_terminal_ns-query_preparation_started_ns)/1e9',
    accuracy_formula='100*(true_positive+true_negative)/expected_rows',
    warmup_policy='repeat0 recorded separately; median and sample CV only repeat1..3',
    no_equivalence_or_general_optimality_claim=True)
(root/'analysis.json').write_text(json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
print(json.dumps({r['role']:{k:r[k] for k in ('point','median_seconds','accuracy_percent','measured_seconds','model_prompt_tokens','model_output_tokens')} for r in rows},ensure_ascii=False))
