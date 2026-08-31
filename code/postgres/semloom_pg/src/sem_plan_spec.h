/* PostgreSQL-owned semantic and physical identity carried in a plan tree. */
#ifndef SEMLOOM_SEM_PLAN_SPEC_H
#define SEMLOOM_SEM_PLAN_SPEC_H

#include "postgres.h"

#include "access/attnum.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"

#define SEMLOOM_PLAN_SPEC_SCHEMA_VERSION 1

typedef enum SemloomPlanOperatorKind
{
	SEMLOOM_PLAN_OPERATOR_MAP = 1,
	SEMLOOM_PLAN_OPERATOR_FILTER = 2,
} SemloomPlanOperatorKind;

typedef enum SemloomPlanValueKind
{
	SEMLOOM_PLAN_VALUE_TEXT = 1,
	SEMLOOM_PLAN_VALUE_TRISTATE = 2,
} SemloomPlanValueKind;

typedef enum SemloomPlanNullPolicy
{
	SEMLOOM_PLAN_NULL_PROPAGATE = 1,
} SemloomPlanNullPolicy;

typedef enum SemloomPlanErrorPolicy
{
	SEMLOOM_PLAN_ERROR_FAIL_QUERY = 1,
} SemloomPlanErrorPolicy;

typedef enum SemloomPlanOrderPolicy
{
	SEMLOOM_PLAN_ORDER_INPUT = 1,
} SemloomPlanOrderPolicy;

typedef struct SemloomPlanSpec
{
	uint32 schema_version;
	SemloomPlanOperatorKind operator_kind;
	SemloomPlanValueKind input_value_kind;
	SemloomPlanValueKind output_value_kind;
	SemloomPlanNullPolicy null_policy;
	SemloomPlanErrorPolicy error_policy;
	SemloomPlanOrderPolicy order_policy;
	uint32 semantic_spec_version;
	const char *semantic_spec_id;
	uint32 semantic_spec_id_length;
	const char *instruction;
	uint32 instruction_length;
	const char *prompt_program_id;
	uint32 prompt_program_version;
	const char *prompt_program_digest;
	const char *result_parser_id;
	uint32 result_parser_version;
	const char *result_parser_digest;
	const char *model_id;
	uint32 model_id_length;
	uint32 temperature;
	uint32 top_p;
	uint32 max_tokens;
	uint32 n;
	bool stream;
	const char *stop;
	const char *physical_algorithm;
	uint32 physical_algorithm_length;
	const char *physical_role;
	const char *semantic_spec_digest;
	const char *physical_algorithm_digest;
} SemloomPlanSpec;

extern List *semloom_plan_spec_make_recording_private(
	SemloomPlanOperatorKind operator_kind,
	AttrNumber input_column);
extern List *semloom_plan_spec_make_exact_filter_private(
	const char *instruction,
	const char *model_id,
	AttrNumber input_column);
extern void semloom_plan_spec_decode(List *custom_private,
									 MemoryContext owner_context,
									 SemloomPlanSpec *plan_spec,
									 AttrNumber *input_column);

#endif
