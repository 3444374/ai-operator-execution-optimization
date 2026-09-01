/* Planner-only matched-reference calibration for exact SemFilter. */
#ifndef SEMLOOM_SEM_FILTER_CALIBRATION_H
#define SEMLOOM_SEM_FILTER_CALIBRATION_H

#include "postgres.h"

#include "sem_filter_cost.h"
#include "sem_plan_spec.h"

typedef enum SemloomFilterCalibrationStatus
{
	SEMLOOM_FILTER_CALIBRATION_UNAVAILABLE = 0,
	SEMLOOM_FILTER_CALIBRATION_MATCHED,
	SEMLOOM_FILTER_CALIBRATION_REJECTED,
} SemloomFilterCalibrationStatus;

typedef struct SemloomFilterCalibration
{
	SemloomFilterCalibrationStatus status;
	const char *reason;
	const char *artifact_id;
	const char *workload_signature;
	const char *service_signature;
	double output_selectivity;
	double model_calls_per_input_row;
	double prompt_tokens_per_call;
	double output_tokens_per_call;
	double service_fixed_milliseconds;
	double service_ms_per_model_call;
	double service_ms_per_prompt_token;
	double service_ms_per_output_token;
	double held_out_max_relative_error;
	double accepted_max_relative_error;
} SemloomFilterCalibration;

extern bool semloom_filter_calibration_load(
	const SemloomPlanSpec *plan_spec,
	const char *provider_execution_profile,
	SemloomFilterCalibration *calibration);
extern void semloom_filter_calibration_apply(
	const SemloomFilterCalibration *calibration,
	SemloomFilterCostEstimate *estimate);

#endif
