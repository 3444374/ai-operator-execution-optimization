# SQuAD v1.1 dev 256-row capability gate (2026-08-05)

DuckDB-ai arm, stratified 256-row slice of squad_v11_dev_short_answer.
Result: 256/256 success, 0 truncation at cap=64, EM=79.69%, F1=90.39%, 0 missing.
Key finding: cap=64 gives ZERO truncation on SQuAD (answers short, unlike ShareGPT).
Operator-only boundary only (database-E2E runner not implemented). Not a formal ranking.
See squad_capability_256.json for full report.
