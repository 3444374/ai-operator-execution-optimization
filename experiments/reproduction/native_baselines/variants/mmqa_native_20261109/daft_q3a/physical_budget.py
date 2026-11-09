"""Finite experiment allowance immediately before the existing native generate."""
import functools
import os
from pathlib import Path

from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.shared_request_budget import SharedClaimedUnit
from src.infrastructure.private_artifacts import content_digest


def unit():
    return SharedClaimedUnit(Path(os.environ['DAFT_PHYSICAL_BUDGET_PATH']),
        AttemptBudget(os.environ['DAFT_PHYSICAL_BUDGET_ID'],25),os.environ['DAFT_PHYSICAL_UNIT'])


def budgeted_generate(delegate):
    @functools.wraps(delegate)
    def generate(*args,**kwargs):
        request_id=kwargs.get('request_id',args[2] if len(args)>2 else None)
        unit().reserve(content_digest(dict(request_id=request_id)))
        return delegate(*args,**kwargs)
    return generate


def install():
    from src.baselines.text.frameworks.daft_native_trace import NativeMapTrace
    original=NativeMapTrace.attach
    def attach(self,model):
        original(self,model)
        model.generate=budgeted_generate(model.generate)
    NativeMapTrace.attach=attach
