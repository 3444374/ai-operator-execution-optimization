"""Fixed native 7B tokenizer/planner/API validation, with all GPUs hidden."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0,str(Path(__file__).resolve().parent))
from native_server import server_argv


def main():
    p=argparse.ArgumentParser();p.add_argument('--config',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    cfg=json.loads(a.config.read_text());a.output.mkdir(mode=0o700)
    assert os.environ.get('CUDA_VISIBLE_DEVICES')==''
    import torch
    import vllm
    from vllm.kalypso.budget import KVMemoryManager
    from vllm.kalypso.controller import SemanticPlan
    from vllm.kalypso.execution.vllm_executor import VLLMExecutor
    from vllm.kalypso.data import data
    from vllm.entrypoints.cli.serve import ServeSubcommand
    from vllm.utils.argparse_utils import FlexibleArgumentParser
    parser=FlexibleArgumentParser();ServeSubcommand().subparser_init(parser.add_subparsers(dest='command'))
    parsed=parser.parse_args(server_argv(cfg))
    assert parsed.model_tag==cfg['model'] and parsed.max_num_seqs==64 and parsed.max_model_len==4096
    assert parsed.worker_extension_cls=='native_server.WorkerObservationExtension'
    source=Path(cfg['install_root'])/'source'
    assert subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip()==cfg['source_commit']
    diff=subprocess.run(['git','-C',str(source),'diff','HEAD','--exit-code','--',
        'vllm/kalypso','vllm/v1','vllm/entrypoints/openai/api_server.py'],capture_output=True)
    assert diff.returncode==0
    KVMemoryManager.init(cfg['model'],6*1024**3,torch.bfloat16)
    executor=VLLMExecutor(cfg['model']);manager=KVMemoryManager.get_instance()
    tokenizer_template=manager.tokenizer.chat_template
    template_sha=hashlib.sha256(tokenizer_template.encode()).hexdigest()
    assert template_sha==cfg['expected_template_sha256']
    plans=[]
    for profile in ('qualification','warmup','full'):
        for q in (1,2,3):
            item=cfg['reference_inputs'][profile][f'movie-q{q}']
            manifest_path=Path(item['manifest'])
            assert hashlib.sha256(manifest_path.read_bytes()).hexdigest()==item['manifest_sha256']
            manifest=json.loads(manifest_path.read_text())
            rows=list(csv.DictReader((manifest_path.parent/'records.csv').open()))
            eligible=[r for r in rows if q==1 or r['id']=='taken_3']
            assert len(eligible)==item['calls']
            path=a.output/f'{profile}-q{q}.csv'
            with path.open('x',newline='') as f:
                writer=csv.DictWriter(f,fieldnames=['data']);writer.writeheader()
                writer.writerows(dict(data=r['reviewText']) for r in eligible)
            manager.reset_stages()
            contexts=list(data._data_source(None,path,executor))
            planner=SemanticPlan(executor,virtual_pinning=True,blocking=False)
            ops=planner.parse_ops([dict(op='sem_filter',args=dict(prompt=cfg['native_instruction']))])
            plan=planner.build(contexts,ops)
            assert len(ops)==1 and type(ops[0]).__name__=='SemFilter'
            assert ops[0].max_tokens==8 and not ops[0].pin and not ops[0].unpin
            previews=[]
            for i,ctx in enumerate(contexts):
                _,prompt=ops[0]._build_prompts(ctx)
                chat=executor._build_chat_request(prompt,8,False,0)
                tokens=manager.token_length(manager.apply_chat_template(prompt))
                assert tokens+8<=4096
                assert chat.model==cfg['model'] and chat.seed==42 and chat.frequency_penalty==.5 and chat.repetition_penalty==1.3
                previews.append(dict(idx=i,row_id=eligible[i]['row_id'],source_position=int(eligible[i]['source_position']),
                    prompt_tokens=tokens,messages_sha256=hashlib.sha256(json.dumps(prompt,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),
                    messages=prompt if i<2 else None))
            (a.output/f'{profile}-q{q}-native-prompts.json').write_text(json.dumps(previews,ensure_ascii=False,indent=2)+'\n')
            plans.append(dict(profile=profile,task=f'movie-q{q}',source_rows=len(rows),eligible_rows=len(contexts),
                max_prompt_tokens=max(x['prompt_tokens'] for x in previews),plan_segments=len(plan),
                pin=False,unpin=False,model=chat.model,sampling=cfg['native_sampling']))
    prior=Path(cfg['prior_model_receipt_path'])
    assert hashlib.sha256(prior.read_bytes()).hexdigest()==cfg['prior_model_receipt_sha256']
    assert not torch.cuda.is_initialized()
    report=dict(status='passed_real_native_CPU_planning_no_model_execution',vllm=vllm.__version__,
        model_config=cfg['model'],template_sha256=template_sha,argv=server_argv(cfg),plans=plans,
        cuda_initialized=False,model_weights_loaded=False,model_calls=0,
        prior_7B_receipt_SHA256_verified=True,weights_content_rehashed=False,
        author_critical_source_unchanged=True,CPU_numeric_KV_budget_GiB=6)
    (a.output/'CPU-NATIVE-RESULT.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
