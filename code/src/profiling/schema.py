"""Stable schema contract for profiler summary rows."""

from __future__ import annotations


FORMAL_RESULT_FIELDS = tuple(
    """
    status experiment_id phase repeat_index scenario_id random_seed
    server_version pgvector_version gpu_metrics_status gpu_name
    gpu_utilization_pct gpu_memory_used_mib gpu_memory_total_mib gpu_power_w
    database_trigger job_id operator seed_workload executor strategy data_source
    source_workload_name source_order source_max_prompt_tokens organizer
    organizer_partition_mode organizer_partitions daft_runner
    organizer_partition_effective model_backend model_endpoint_url model_name
    model_request_timeout_s total_rows written_rows db_fetch_rows ray_batch_rows
    batching_policy token_budget token_budget_policy token_budget_candidates
    token_budget_target_service_ms token_budget_arrival_ewma_alpha
    embedding_dim embedding_vector_dim
    completion_max_tokens completion_return_token_ids completion_prompt_format
    completion_temperature output_cost_mode output_cost_source packing_cost_unit
    cost_model_id cost_tokenizer_id packing_algorithm packing_scope
    packing_budget_utilization_mean packing_budget_utilization_p95
    packing_oversized_rows packing_input_rows packing_batch_count
    batch_estimated_cost_units_p50 batch_estimated_cost_units_p95
    batch_estimated_cost_units_p99 batch_estimated_cost_units_max
    organization_batch_count organization_batch_rows_mean
    organization_batch_rows_max organization_batch_cost_units_mean
    organization_batch_cost_units_p95 organization_row_cap_hit_ratio
    service_quantum_tokens service_quantum_count service_quantum_rows_mean
    service_quantum_work_mean service_quantum_work_p95
    service_quantum_oversized_rows
    model_workers ray_version actor_workers_per_endpoint
    ray_actor_max_concurrency
    ray_worker_num_cpus ray_worker_num_gpus endpoint_count actor_worker_count
    actor_worker_submission_counts max_inflight_limit admission_scope
    per_endpoint_inflight_limit max_active_work_per_endpoint
    max_active_work_per_endpoint_seen shared_credit_coordinator_name
    shared_credit_request_limit shared_credit_work_limit shared_credit_quantum
    shared_credit_job_weight effective_global_inflight_limit
    endpoint_routing
    pool_routing endpoint_pool_ids endpoint_gpu_ids long_request_token_threshold
    scheduling_policy adaptive_min_inflight adaptive_max_inflight
    controller_min_window controller_max_window adaptive_sample_interval_s
    adaptive_downshifts adaptive_upshifts adaptive_limit_mean control_trace_path
    control_trace_events arrival_replay arrival_time_scale arrival_replay_preload
    submission_granularity
    flush_policy flush_timeout_ms flush_max_wait_ms flush_trace_output
    flush_trace_path flush_trace_events submission_trace_path
    submission_trace_events resource_trace_path resource_trace_events
    resource_sample_interval_s resource_metrics_status gpu_utilization_pct_mean
    gpu_utilization_pct_p50 gpu_utilization_pct_p95 gpu_utilization_pct_max
    gpu_utilization_below_10pct_ratio gpu_memory_used_mib_mean
    gpu_memory_used_mib_max gpu_memory_utilization_pct_mean
    gpu_memory_utilization_pct_max gpu_power_w_mean gpu_power_w_max gpu_energy_j
    energy_j_per_1k_observed_tokens vllm_running_mean vllm_running_p50
    vllm_running_p95 vllm_running_max vllm_waiting_mean vllm_waiting_p50
    vllm_waiting_p95 vllm_waiting_max vllm_kv_cache_usage_mean
    vllm_kv_cache_usage_p50 vllm_kv_cache_usage_p95 vllm_kv_cache_usage_max
    mfu_estimation_method mfu_time_basis model_flops_per_token gpu_peak_tflops
    mfu_precision mfu_status mfu_estimate request_trace_path request_trace_events
    request_e2e_s_p50 request_e2e_s_p95 request_e2e_s_p99
    request_slo_target_ms request_slo_violation_ratio request_slo_goodput_per_s
    request_actual_output_tokens_observed request_actual_output_tokens_p50
    request_actual_output_tokens_p95 request_actual_output_tokens_p99
    request_finish_reason_observed request_finish_reason_stop_ratio
    request_finish_reason_length_ratio latency_granularity writeback_mode
    write_batch_rows object_count operator_invocations max_inflight_seen
    token_count batch_rows_min batch_rows_max batch_rows_mean batch_tokens_min
    batch_tokens_max batch_tokens_mean batch_tokens_p50 batch_tokens_p95
    batch_service_s_p50 batch_service_s_p95 batch_service_s_p99
    vllm_metrics_status vllm_prompt_tokens_delta vllm_generation_tokens_delta
    vllm_request_success_delta vllm_estimated_flops_per_gpu_delta
    vllm_e2e_request_latency_mean_s vllm_request_queue_time_mean_s
    vllm_request_inference_time_mean_s vllm_request_prefill_time_mean_s
    vllm_request_decode_time_mean_s vllm_num_requests_running_after
    vllm_num_requests_waiting_after vllm_kv_cache_usage_perc_after db_fetch_s
    arrow_build_s source_fetch_s organizer_from_arrow_s organizer_plan_s
    organizer_collect_s organization_policy_family
    batch_prompt_token_spread_mean prefix_group_ratio organizer_warnings
    model_service_s model_request_wall_s operator_wall_s submit_s bounded_wait_s
    avg_bounded_wait_s fanin_s writeback_s e2e_s rows_per_s tokens_per_s
    """.split()
)

GPU_METADATA_DEFAULTS = {
    "gpu_metrics_status": "unavailable",
    "gpu_name": "",
    "gpu_utilization_pct": "",
    "gpu_memory_used_mib": "",
    "gpu_memory_total_mib": "",
    "gpu_power_w": "",
}


def validated_formal_result_row(row: dict) -> dict:
    """Reject summary rows whose ordered fields drift from the CSV contract."""

    actual_fields = tuple(row)
    if actual_fields != FORMAL_RESULT_FIELDS:
        raise RuntimeError(
            "formal result schema drift: "
            f"actual fields {actual_fields!r} != "
            f"expected fields {FORMAL_RESULT_FIELDS!r}"
        )
    return row
