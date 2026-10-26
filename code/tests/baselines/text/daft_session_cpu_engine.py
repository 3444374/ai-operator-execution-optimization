"""Synchronous private-pipe CPU fixture. Does not import vLLM or CUDA."""
import hashlib,json,os,sys,uuid

def main():
    with os.fdopen(int(sys.argv[1]),'wb',buffering=0) as output:
        def send(value):output.write((json.dumps(value)+'\n').encode())
        uid='CPU-pipe-'+uuid.uuid4().hex
        for raw in sys.stdin.buffer:
            value=json.loads(raw)
            if value['op']=='open':send(dict(kind='ready',engine_uid=uid,pid=os.getpid()))
            elif value['op']=='run':
                rows=[]
                for row in value['rows']:
                    result={k:row[k] for k in ('query_id','model_role','algorithm_stage','row_id','source_position','call_index','call_id')}
                    result.update(status='succeeded',text='True',submission_confirmed=True,engine_uid=uid,
                        engine_request_id='CPU-'+row['call_id'],prompt_tokens=3,completion_tokens=1,total_tokens=4,
                        finish_reason='stop',messages_sha256=hashlib.sha256(row['messages_json'].encode()).hexdigest())
                    rows.append(result)
                send(dict(kind='results',rows=rows))
            elif value['op']=='close':
                send(dict(kind='closed',report=dict(status='closed',engine_api_shutdown_confirmed=True,gpu_exit_confirmed=None)))
                return
            else:raise ValueError('unexpected CPU fixture operation')

if __name__=='__main__':main()
