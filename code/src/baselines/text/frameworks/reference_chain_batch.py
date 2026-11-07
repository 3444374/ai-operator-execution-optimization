"""Native graph chat-process selection for the shared finite method worker."""
from src.serving.backends.lotus_chain_worker import FusedChainBatch
from .sync_reference_engine import ReferenceChainEngineProcess


class ReferenceFusedChainBatch(FusedChainBatch):
    _engine_type = ReferenceChainEngineProcess
