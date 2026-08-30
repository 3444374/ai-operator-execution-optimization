/* PostgreSQL-owned semantic and physical identity carried in a plan tree. */
#ifndef SEMLOOM_SEM_PLAN_SPEC_H
#define SEMLOOM_SEM_PLAN_SPEC_H

#include "postgres.h"

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

typedef struct SemloomPlanSpec
{
	uint32 schema_version;
	SemloomPlanOperatorKind operator_kind;
	SemloomPlanValueKind input_value_kind;
	SemloomPlanValueKind output_value_kind;
	SemloomPlanNullPolicy null_policy;
	SemloomPlanErrorPolicy error_policy;
	uint32 semantic_spec_version;
	const char *semantic_spec_id;
	uint32 semantic_spec_id_length;
	const char *physical_algorithm;
	uint32 physical_algorithm_length;
	const char *physical_role;
} SemloomPlanSpec;

extern List *semloom_plan_spec_make_recording_private(
	SemloomPlanOperatorKind operator_kind,
	AttrNumber input_column);
extern void semloom_plan_spec_decode(List *custom_private,
									 MemoryContext owner_context,
									 SemloomPlanSpec *plan_spec,
									 AttrNumber *input_column);

#endif
