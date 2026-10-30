import os
from pathlib import Path
import sys
os.environ['PYTHONPATH']=os.environ['SEM_RECORD_OBSERVER_CODE']+os.pathsep+str(Path(__file__).parent)
sys.path.insert(0,os.environ['SEM_RECORD_OBSERVER_CODE'])
if __name__=='__main__':
    import asyncio
    from record_observer import install_model
    install_model()
    from src.serving.backends.async_engine_process import serve
    asyncio.run(serve(*map(int,sys.argv[1:])))
