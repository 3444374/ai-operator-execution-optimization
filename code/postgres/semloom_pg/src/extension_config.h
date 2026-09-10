/* Query-visible extension configuration; no planner or transport declarations. */
#ifndef SEMLOOM_EXTENSION_CONFIG_H
#define SEMLOOM_EXTENSION_CONFIG_H

typedef enum SemloomProviderExecutionProfile
{
	SEMLOOM_PROVIDER_PROFILE_GOLDEN = 0,
	SEMLOOM_PROVIDER_PROFILE_OPENAI_COMPATIBLE_FIXED = 1,
	SEMLOOM_PROVIDER_PROFILE_QUERY_JOB = 4,
	SEMLOOM_PROVIDER_PROFILE_ASYNC_MAP = 3,
} SemloomProviderExecutionProfile;

extern int semloom_provider_window_tasks(void);
extern int semloom_provider_window_bytes(void);
extern bool semloom_predicate_prefetch_enabled(void);
extern bool semloom_filter_count_enabled(void);
extern bool semloom_total_window_budget_enabled(void);
extern bool semloom_test_window_memory_enabled(void);
extern int semloom_provider_staging_bytes(void);
extern const char *semloom_gateway_socket_path(void);
extern const char *semloom_reference_calibration_path(void);
extern const char *semloom_test_map_binding_column(void);
extern const char *semloom_test_filter_binding_column(void);
extern SemloomProviderExecutionProfile semloom_provider_execution_profile(void);
extern const char *semloom_provider_execution_profile_name(void);

#endif
