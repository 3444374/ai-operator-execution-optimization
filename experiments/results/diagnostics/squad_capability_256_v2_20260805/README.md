# SQuAD 256-row capability gate v2 (rewritten, 2026-08-05)

DuckDB-ai arm, deterministic stratified 256-row sample.
Result: 256/256 success, 0 error/NULL/max_tokens error, EM=75.39%, F1=86.56%, 0 missing.
Avg 5.57 generation tokens/row (vLLM counter delta, NOT a finish_reason proxy).
Operator-only JCT 4.63s (adapter wall 5.17s, setup 0.50s).
Evidence: report.json + per_row_evidence.csv (EM/F1 recomputable) + sample_manifest.json (deterministic sample hash).
Not a formal ranking; operator-only boundary only.
