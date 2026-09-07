/* Query-visible extension configuration; no planner or transport declarations. */
#ifndef SEMLOOM_EXTENSION_CONFIG_H
#define SEMLOOM_EXTENSION_CONFIG_H

typedef enum SemloomProviderExecutionProfile
{
	SEMLOOM_PROVIDER_PROFILE_GOLDEN = 0,
	SEMLOOM_PROVIDER_PROFILE_OPENAI_COMPATIBLE_FIXED = 1,
} SemloomProviderExecutionProfile;

extern const char *semloom_gateway_socket_path(void);
extern const char *semloom_reference_calibration_path(void);
extern SemloomProviderExecutionProfile semloom_provider_execution_profile(void);
extern const char *semloom_provider_execution_profile_name(void);

#endif
