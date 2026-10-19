"""Original chain carrier with only its model process replaced by a CPU fixture."""

from functools import partial

from integrations.imlane_records import chain_udf
from tests.integrations.imlane_chain_fixture import CpuChainProcess


chain_udf.ChainWorker = partial(chain_udf.ChainWorker, engine_type=CpuChainProcess)
infer_records = chain_udf.infer_records
