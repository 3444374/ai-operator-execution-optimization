/* Strict copyObject-safe encoding for the current PostgreSQL semantic plan. */
#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/value.h"

#include "recording_contract.h"
#include "sem_plan_spec.h"

#define SEMLOOM_PLAN_SPEC_FIELD_COUNT 10
#define SEMLOOM_PLAN_SPEC_ID_MAX_BYTES 128
#define SEMLOOM_PLAN_ALGORITHM_MAX_BYTES 64
#define SEMLOOM_PLAN_ROLE_MAX_BYTES 32

#define SEMLOOM_PLAN_FIELD_SCHEMA_VERSION "schema_version"
#define SEMLOOM_PLAN_FIELD_OPERATOR_KIND "operator_kind"
#define SEMLOOM_PLAN_FIELD_INPUT_VALUE_KIND "input_value_kind"
#define SEMLOOM_PLAN_FIELD_OUTPUT_VALUE_KIND "output_value_kind"
#define SEMLOOM_PLAN_FIELD_NULL_POLICY "null_policy"
#define SEMLOOM_PLAN_FIELD_ERROR_POLICY "error_policy"
#define SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_VERSION "semantic_spec_version"
#define SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_ID "semantic_spec_id"
#define SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM "physical_algorithm"
#define SEMLOOM_PLAN_FIELD_PHYSICAL_ROLE "physical_role"

typedef enum SemloomPlanFieldBit
{
	SEMLOOM_PLAN_SEEN_SCHEMA_VERSION = 1U << 0,
	SEMLOOM_PLAN_SEEN_OPERATOR_KIND = 1U << 1,
	SEMLOOM_PLAN_SEEN_INPUT_VALUE_KIND = 1U << 2,
	SEMLOOM_PLAN_SEEN_OUTPUT_VALUE_KIND = 1U << 3,
	SEMLOOM_PLAN_SEEN_NULL_POLICY = 1U << 4,
	SEMLOOM_PLAN_SEEN_ERROR_POLICY = 1U << 5,
	SEMLOOM_PLAN_SEEN_SEMANTIC_SPEC_VERSION = 1U << 6,
	SEMLOOM_PLAN_SEEN_SEMANTIC_SPEC_ID = 1U << 7,
	SEMLOOM_PLAN_SEEN_PHYSICAL_ALGORITHM = 1U << 8,
	SEMLOOM_PLAN_SEEN_PHYSICAL_ROLE = 1U << 9,
} SemloomPlanFieldBit;

#define SEMLOOM_PLAN_ALL_FIELDS ((1U << SEMLOOM_PLAN_SPEC_FIELD_COUNT) - 1U)

static List *semloom_plan_spec_integer_field(const char *name, int value);
static List *semloom_plan_spec_string_field(const char *name, const char *value);
static int semloom_plan_spec_read_integer(Node *value);
static const char *semloom_plan_spec_read_string(Node *value,
											  Size max_bytes,
											  MemoryContext owner_context,
											  uint32 *length_out);
static void semloom_plan_spec_mark_seen(uint32 *seen_fields, uint32 field_bit);
static void semloom_plan_spec_validate(const SemloomPlanSpec *plan_spec);
pg_noreturn static void semloom_plan_spec_invalid(const char *message);

List *
semloom_plan_spec_make_recording_private(SemloomPlanOperatorKind operator_kind,
										 AttrNumber input_column)
{
	const char *semantic_spec_id;
	SemloomPlanValueKind output_value_kind;
	List *fields = NIL;

	if (operator_kind == SEMLOOM_PLAN_OPERATOR_MAP)
	{
		semantic_spec_id = SEMLOOM_MAP_RECORDING_SPEC_ID;
		output_value_kind = SEMLOOM_PLAN_VALUE_TEXT;
	}
	else if (operator_kind == SEMLOOM_PLAN_OPERATOR_FILTER)
	{
		semantic_spec_id = SEMLOOM_FILTER_RECORDING_SPEC_ID;
		output_value_kind = SEMLOOM_PLAN_VALUE_TRISTATE;
	}
	else
		semloom_plan_spec_invalid("invalid semantic plan specification");
	if (input_column <= 0)
		semloom_plan_spec_invalid("invalid semantic executor binding");

	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_SCHEMA_VERSION,
		SEMLOOM_PLAN_SPEC_SCHEMA_VERSION));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_OPERATOR_KIND,
		operator_kind));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_INPUT_VALUE_KIND,
		SEMLOOM_PLAN_VALUE_TEXT));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_OUTPUT_VALUE_KIND,
		output_value_kind));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_NULL_POLICY,
		SEMLOOM_PLAN_NULL_PROPAGATE));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_ERROR_POLICY,
		SEMLOOM_PLAN_ERROR_FAIL_QUERY));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_VERSION,
		SEMLOOM_RECORDING_SPEC_VERSION));
	fields = lappend(fields, semloom_plan_spec_string_field(
		SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_ID,
		semantic_spec_id));
	fields = lappend(fields, semloom_plan_spec_string_field(
		SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM,
		SEMLOOM_RECORDING_ALGORITHM));
	fields = lappend(fields, semloom_plan_spec_string_field(
		SEMLOOM_PLAN_FIELD_PHYSICAL_ROLE,
		"reference"));

	return list_make2(fields, makeInteger(input_column));
}

void
semloom_plan_spec_decode(List *custom_private,
						 MemoryContext owner_context,
						 SemloomPlanSpec *plan_spec,
						 AttrNumber *input_column)
{
	Node *fields_node;
	Node *binding_node;
	List *fields;
	uint32 seen_fields = 0;
	ListCell *cell;

	Assert(owner_context != NULL);
	Assert(plan_spec != NULL);
	Assert(input_column != NULL);
	MemSet(plan_spec, 0, sizeof(*plan_spec));
	if (list_length(custom_private) != 2)
		semloom_plan_spec_invalid("invalid semantic plan specification");
	fields_node = (Node *) linitial(custom_private);
	binding_node = (Node *) lsecond(custom_private);
	if (!IsA(fields_node, List) || !IsA(binding_node, Integer))
		semloom_plan_spec_invalid("invalid semantic plan specification");
	fields = (List *) fields_node;
	*input_column = (AttrNumber) intVal(binding_node);
	if (*input_column <= 0)
		semloom_plan_spec_invalid("invalid semantic executor binding");

	foreach(cell, fields)
	{
		Node *field_node = (Node *) lfirst(cell);
		List *field;
		Node *name_node;
		Node *value_node;
		const char *name;

		if (!IsA(field_node, List))
			semloom_plan_spec_invalid("invalid semantic plan specification field");
		field = (List *) field_node;
		if (list_length(field) != 2)
			semloom_plan_spec_invalid("invalid semantic plan specification field");
		name_node = (Node *) linitial(field);
		value_node = (Node *) lsecond(field);
		if (!IsA(name_node, String))
			semloom_plan_spec_invalid("invalid semantic plan specification field");
		name = strVal(name_node);

		if (strcmp(name, SEMLOOM_PLAN_FIELD_SCHEMA_VERSION) == 0)
		{
			semloom_plan_spec_mark_seen(&seen_fields,
				SEMLOOM_PLAN_SEEN_SCHEMA_VERSION);
			plan_spec->schema_version = semloom_plan_spec_read_integer(value_node);
		}
		else if (strcmp(name, SEMLOOM_PLAN_FIELD_OPERATOR_KIND) == 0)
		{
			semloom_plan_spec_mark_seen(&seen_fields,
				SEMLOOM_PLAN_SEEN_OPERATOR_KIND);
			plan_spec->operator_kind = (SemloomPlanOperatorKind)
				semloom_plan_spec_read_integer(value_node);
		}
		else if (strcmp(name, SEMLOOM_PLAN_FIELD_INPUT_VALUE_KIND) == 0)
		{
			semloom_plan_spec_mark_seen(&seen_fields,
				SEMLOOM_PLAN_SEEN_INPUT_VALUE_KIND);
			plan_spec->input_value_kind = (SemloomPlanValueKind)
				semloom_plan_spec_read_integer(value_node);
		}
		else if (strcmp(name, SEMLOOM_PLAN_FIELD_OUTPUT_VALUE_KIND) == 0)
		{
			semloom_plan_spec_mark_seen(&seen_fields,
				SEMLOOM_PLAN_SEEN_OUTPUT_VALUE_KIND);
			plan_spec->output_value_kind = (SemloomPlanValueKind)
				semloom_plan_spec_read_integer(value_node);
		}
		else if (strcmp(name, SEMLOOM_PLAN_FIELD_NULL_POLICY) == 0)
		{
			semloom_plan_spec_mark_seen(&seen_fields,
				SEMLOOM_PLAN_SEEN_NULL_POLICY);
			plan_spec->null_policy = (SemloomPlanNullPolicy)
				semloom_plan_spec_read_integer(value_node);
		}
		else if (strcmp(name, SEMLOOM_PLAN_FIELD_ERROR_POLICY) == 0)
		{
			semloom_plan_spec_mark_seen(&seen_fields,
				SEMLOOM_PLAN_SEEN_ERROR_POLICY);
			plan_spec->error_policy = (SemloomPlanErrorPolicy)
				semloom_plan_spec_read_integer(value_node);
		}
		else if (strcmp(name, SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_VERSION) == 0)
		{
			semloom_plan_spec_mark_seen(&seen_fields,
				SEMLOOM_PLAN_SEEN_SEMANTIC_SPEC_VERSION);
			plan_spec->semantic_spec_version = semloom_plan_spec_read_integer(value_node);
		}
		else if (strcmp(name, SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_ID) == 0)
		{
			semloom_plan_spec_mark_seen(&seen_fields,
				SEMLOOM_PLAN_SEEN_SEMANTIC_SPEC_ID);
			plan_spec->semantic_spec_id = semloom_plan_spec_read_string(
				value_node,
				SEMLOOM_PLAN_SPEC_ID_MAX_BYTES,
				owner_context,
				&plan_spec->semantic_spec_id_length);
		}
		else if (strcmp(name, SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM) == 0)
		{
			semloom_plan_spec_mark_seen(&seen_fields,
				SEMLOOM_PLAN_SEEN_PHYSICAL_ALGORITHM);
			plan_spec->physical_algorithm = semloom_plan_spec_read_string(
				value_node,
				SEMLOOM_PLAN_ALGORITHM_MAX_BYTES,
				owner_context,
				&plan_spec->physical_algorithm_length);
		}
		else if (strcmp(name, SEMLOOM_PLAN_FIELD_PHYSICAL_ROLE) == 0)
		{
			uint32 ignored_length;

			semloom_plan_spec_mark_seen(&seen_fields,
				SEMLOOM_PLAN_SEEN_PHYSICAL_ROLE);
			plan_spec->physical_role = semloom_plan_spec_read_string(
				value_node,
				SEMLOOM_PLAN_ROLE_MAX_BYTES,
				owner_context,
				&ignored_length);
		}
		else
			semloom_plan_spec_invalid("unknown semantic plan specification field");
	}

	if (seen_fields != SEMLOOM_PLAN_ALL_FIELDS ||
		list_length(fields) != SEMLOOM_PLAN_SPEC_FIELD_COUNT)
		semloom_plan_spec_invalid("incomplete semantic plan specification");
	semloom_plan_spec_validate(plan_spec);
}

static List *
semloom_plan_spec_integer_field(const char *name, int value)
{
	return list_make2(makeString(pstrdup(name)), makeInteger(value));
}

static List *
semloom_plan_spec_string_field(const char *name, const char *value)
{
	return list_make2(makeString(pstrdup(name)), makeString(pstrdup(value)));
}

static int
semloom_plan_spec_read_integer(Node *value)
{
	if (!IsA(value, Integer) || intVal(value) <= 0)
		semloom_plan_spec_invalid("invalid semantic plan specification field");
	return intVal(value);
}

static const char *
semloom_plan_spec_read_string(Node *value,
							  Size max_bytes,
							  MemoryContext owner_context,
							  uint32 *length_out)
{
	const char *source;
	Size length;

	if (!IsA(value, String))
		semloom_plan_spec_invalid("invalid semantic plan specification field");
	source = strVal(value);
	length = strlen(source);
	if (length == 0 || length > max_bytes || length > PG_UINT32_MAX)
		semloom_plan_spec_invalid("invalid semantic plan specification field");
	*length_out = (uint32) length;
	return MemoryContextStrdup(owner_context, source);
}

static void
semloom_plan_spec_mark_seen(uint32 *seen_fields, uint32 field_bit)
{
	if ((*seen_fields & field_bit) != 0)
		semloom_plan_spec_invalid("duplicate semantic plan specification field");
	*seen_fields |= field_bit;
}

static void
semloom_plan_spec_validate(const SemloomPlanSpec *plan_spec)
{
	bool map_spec = plan_spec->operator_kind == SEMLOOM_PLAN_OPERATOR_MAP &&
		plan_spec->output_value_kind == SEMLOOM_PLAN_VALUE_TEXT &&
		strcmp(plan_spec->semantic_spec_id, SEMLOOM_MAP_RECORDING_SPEC_ID) == 0;
	bool filter_spec = plan_spec->operator_kind == SEMLOOM_PLAN_OPERATOR_FILTER &&
		plan_spec->output_value_kind == SEMLOOM_PLAN_VALUE_TRISTATE &&
		strcmp(plan_spec->semantic_spec_id, SEMLOOM_FILTER_RECORDING_SPEC_ID) == 0;

	if (plan_spec->schema_version != SEMLOOM_PLAN_SPEC_SCHEMA_VERSION ||
		(!map_spec && !filter_spec) ||
		plan_spec->input_value_kind != SEMLOOM_PLAN_VALUE_TEXT ||
		plan_spec->null_policy != SEMLOOM_PLAN_NULL_PROPAGATE ||
		plan_spec->error_policy != SEMLOOM_PLAN_ERROR_FAIL_QUERY ||
		plan_spec->semantic_spec_version != SEMLOOM_RECORDING_SPEC_VERSION ||
		strcmp(plan_spec->physical_algorithm, SEMLOOM_RECORDING_ALGORITHM) != 0 ||
		strcmp(plan_spec->physical_role, "reference") != 0)
		semloom_plan_spec_invalid("unsupported semantic plan specification");
}

static void
semloom_plan_spec_invalid(const char *message)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("%s", message)));
	pg_unreachable();
}
