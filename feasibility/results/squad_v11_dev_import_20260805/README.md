# SQuAD v1.1 dev import provenance (2026-08-05)

Source: official rajpurkar/SQuAD-explorer GitHub repo (turbo sparse-clone), dataset/dev-v1.1.json.
Canonical rajpurkar.github.io was ~5KB/s from AutoDL and infeasible; the GitHub-hosted file is identical (same SHA256).
Imported 10570/10570 questions into workload squad_v11_dev_short_answer; exactly-once, all unique source_example_id, all carry multi-answer reference_answers JSONB. See provenance.json for version/split/SHA256/URL/count/importer-commit/content-hash/prompt-template-hash. cap=64 fixed at manifest export (not here). Importer only imports; EM/F1 quality eval is a separate evaluator (step 4).
