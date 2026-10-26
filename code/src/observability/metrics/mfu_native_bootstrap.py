"""Original vLLM HTTP entry with opt-in native statistics observation only."""
import runpy
from .mfu_trace import install

if __name__=='__main__':
    install()
    runpy.run_module('vllm.entrypoints.openai.api_server',run_name='__main__',alter_sys=True)
