"""Native graph parameter ownership over the shared finite method worker."""
from dataclasses import replace

from src.serving.backends.lotus_chain_worker import FusedChainBatch
from .sync_reference_engine import ReferenceChainEngineProcess


class ReferenceFusedChainBatch(FusedChainBatch):
    _engine_type = ReferenceChainEngineProcess

    def _engine_specification(self, value):
        spec = super()._engine_specification(value)
        return (replace(spec, max_model_len=spec.legacy_max_model_len)
                if spec.max_model_len is None else spec)
