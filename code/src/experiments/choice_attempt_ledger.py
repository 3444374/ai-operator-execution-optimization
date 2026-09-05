"""Compatibility defaults for the historical choice smoke budget.

New experiments supply their own AttemptBudget to experiments.attempt_ledger.
"""
from .attempt_ledger import (
    AttemptBudget, AttemptLedger as _AttemptLedger, BudgetError, BudgetExhausted,
    MAX_LEDGER_BYTES, observe_http_posts,
)

MAX_ATTEMPTS = 100
CHOICE_BUDGET = AttemptBudget("semloom.choice.4c.v1", MAX_ATTEMPTS)
HEADER = CHOICE_BUDGET.header


class AttemptLedger(_AttemptLedger):
    def __init__(self, path, budget=CHOICE_BUDGET):
        super().__init__(path, budget)

    @classmethod
    def create(cls, path, budget=CHOICE_BUDGET):
        return super().create(path, budget)
