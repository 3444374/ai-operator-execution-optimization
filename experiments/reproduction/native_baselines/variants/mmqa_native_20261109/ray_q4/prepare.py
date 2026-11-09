import os
"""Fresh original MMQA input -> messages; never retemplate rendered strings."""
import hashlib,json,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCE=Path(os.environ['CASE_SOURCE'])
sys.path.insert(0,str(SOURCE/'code'))
def prepare_case(output):
 from transformers import AutoTokenizer
 import mmqa_semantics
 config=json.loads((ROOT/'CONFIG.json').read_text());started=time.monotonic_ns()
 tok=AutoTokenizer.from_pretrained(config['model'],local_files_only=True,trust_remote_code=False)
 query,method,prepared,requests=mmqa_semantics.prepare(config,tok)
 rows=[dict(call_id=r['call_id'],messages=json.loads(r['messages_json'])) for r in prepared]
 assert len(rows)==len({r['call_id'] for r in rows})==24
 content=json.dumps(rows,ensure_ascii=False,indent=2)+'\n'
 canonical=ROOT/'messages-PRIVATE.json'
 if output!=ROOT:assert hashlib.sha256(content.encode()).hexdigest()==hashlib.sha256(canonical.read_bytes()).hexdigest()
 (output/'messages-PRIVATE.json').write_text(content)
 (output/'PREPARATION.json').write_text(json.dumps(dict(source_rows=25,rows=24,scenario='mmqa',query='Q4',model_columns=query.operation.describe()['model_columns'],started_monotonic_ns=started,finished_monotonic_ns=time.monotonic_ns(),messages_sha256=hashlib.sha256(content.encode()).hexdigest(),query_description=query.describe()),indent=2)+'\n')
 return query,method,prepared,requests
if __name__=='__main__':prepare_case(Path(sys.argv[1]))
