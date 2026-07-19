# 2026-07-19 local vLLM data-organization and shared-service scheduling update

- Extended `code/src/organizers.py` with `length_align_fixed_rows`,
  `length_align_token_budget`, `prefix_aware_fixed_rows`, and
  `prefix_aware_token_budget`.
- Added organization metrics to experiment CSV rows:
  `organization_policy_family`, `batch_prompt_token_spread_mean`, and
  `prefix_group_ratio`.
- Extended `code/scripts/postgres_ai_operator_profile.py` with
  `--scheduling-policy static|queue_adaptive` and adaptive min/max inflight,
  queue, running, KV, and polling parameters.
- Extended `code/scripts/run_kmax_interference_experiment.py` into a formal
  shared-vLLM sweep over background `K_max={8,16,unbounded}` plus a first
  `queue_adaptive` scenario.
- Added formal CSVs:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_length_prefix_ablation_20260719.csv`,
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_sweep_small_20260719.csv`,
  and
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_sweep_bulk_20260719.csv`.
- Added tuned adaptive supplement CSVs:
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_adaptive_tuned_small_20260719.csv`
  and
  `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_adaptive_tuned_bulk_20260719.csv`.
- Added figures `b22`-`b25` under `figures/data/backup/` and updated
  `figures/scripts/generate_local_vllm_ray_baseline_charts.py`,
  `figures/README.md`, `figures/audit/local_vllm_ray_baseline_charts_audit_20260718.md`,
  `experiments/results/local_vllm_qwen15b_baseline/README.md`, and
  `PROJECT_INDEX.md`.
- Boundary: the shared-vLLM sweep supports `K_max=8` as an admission-control
  guardrail for a shared endpoint. The first adaptive thresholds were too
  permissive and never downshifted; tuned thresholds did downshift but still
  did not beat static `K_max=8`, so queue-adaptive remains implemented and
  tested but not yet proven effective. Prefix-aware only records local
  prefix-group ratio, not KV-cache/APC benefit.
