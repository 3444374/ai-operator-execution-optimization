"""Declared CPU values for ordinary methods; no chain-specific execution logic."""

import json
import os
import sys
import uuid

from src.baselines.text.frameworks.lotus_chain_engine_process import ChainEngineProcess


def reply(kind, messages):
    text = json.dumps(messages,ensure_ascii=False)
    position = next((i for i in range(3) if "doc"+str(i) in text),None)
    if kind == "map":
        return "mapped-doc"+str(position) + (" KEEP" if "KEEP" in text else " DROP")
    if kind == "filter":
        return "True" if "KEEP" in text else "False"
    if kind == "extract":
        if position == 2:
            return "{}"
        return json.dumps(dict(field="KEEP-last" if position==1 or "KEEP-last" in text else "DROP-first",
            row_id="business-updated",call_id="business-call-updated",extra="actual-extra-key"))
    raise ValueError("undeclared fixture method")


def terminal(row, text, uid, *, failed=False):
    from src.serving.backends.vllm_engine import _prepared

    item = _prepared([row])[0][0]
    return dict(item,status="failed" if failed else "succeeded",text=None if failed else text,
        submission_confirmed=True,prompt_tokens=5,completion_tokens=2,total_tokens=7,
        prompt_token_ids_json="[1,2,3,4,5]",output_token_ids_json="[6,7]",logprobs_json="null",
        engine_uid=uid,engine_request_id=item["call_id"],finish_reason="abort" if failed else "stop",
        error_type="DeclaredCPUFailure" if failed else None,
        usage_source="declared CPU response; no model inference")


class CpuChainProcess(ChainEngineProcess):
    mode = "normal"

    def _command(self, engine, result_fd):
        return [engine.model_python,"-B","-m",__name__,str(result_fd),self.mode]


if __name__ == "__main__":
    from src.baselines.text.frameworks.lotus_chain_engine_process import serve
    from src.serving.backends import vllm_engine

    mode = sys.argv[2]

    class CpuEngine:
        def __init__(self, engine, generation):
            self.uid, self.calls, self.limit = "imlane-chain-cpu-"+uuid.uuid4().hex,0,engine.max_model_calls

        def ready(self):
            assert not any(name in sys.modules for name in ("lotus","ray","vllm"))
            return dict(engine_uid=self.uid,pid=os.getpid(),fixture=True,real_model_requests=0)

        def run_prepared(self, rows, *, generation, deadline_monotonic):
            self.calls += len(rows)
            assert self.calls <= self.limit
            return [terminal(row,reply(row["algorithm_stage"].split("-")[0],json.loads(row["messages_json"])),
                             self.uid) for row in rows]

        def close(self):
            return dict(status="closed",engine_api_shutdown_confirmed=True,reserved_model_calls=self.calls,
                        real_model_requests=0)

    vllm_engine.VllmEngine = CpuEngine
    serve(int(sys.argv[1]))
