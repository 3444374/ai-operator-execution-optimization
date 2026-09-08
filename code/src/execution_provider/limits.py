"""Incremental handshake field range shared with the PostgreSQL int32 decoder."""

# Encoding limit only; deployments choose much smaller task and byte budgets.
MAX_INCREMENTAL_TASKS = (1 << 31) - 1
