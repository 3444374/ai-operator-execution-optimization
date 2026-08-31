/* Strict copyObject-safe encoding for PostgreSQL-owned semantic plans. */
#include "postgres.h"

#include "common/cryptohash.h"
#include "common/sha2.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"

#include "recording_contract.h"
#include "semantic_filter_contract.h"
#include "sem_plan_spec.h"

#define SEMLOOM_RECORDING_PLAN_FIELD_COUNT 10
#define SEMLOOM_EXACT_FILTER_PLAN_FIELD_COUNT 27
#define SEMLOOM_PLAN_SPEC_ID_MAX_BYTES 128
#define SEMLOOM_PLAN_ALGORITHM_MAX_BYTES 64
#define SEMLOOM_PLAN_ROLE_MAX_BYTES 32
#define SEMLOOM_PLAN_PROGRAM_ID_MAX_BYTES 128

#define SEMLOOM_PLAN_FIELD_SCHEMA_VERSION "schema_version"
#define SEMLOOM_PLAN_FIELD_OPERATOR_KIND "operator_kind"
#define SEMLOOM_PLAN_FIELD_INPUT_VALUE_KIND "input_value_kind"
#define SEMLOOM_PLAN_FIELD_OUTPUT_VALUE_KIND "output_value_kind"
#define SEMLOOM_PLAN_FIELD_NULL_POLICY "null_policy"
#define SEMLOOM_PLAN_FIELD_ERROR_POLICY "error_policy"
#define SEMLOOM_PLAN_FIELD_ORDER_POLICY "order_policy"
#define SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_VERSION "semantic_spec_version"
#define SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_ID "semantic_spec_id"
#define SEMLOOM_PLAN_FIELD_INSTRUCTION "instruction"
#define SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_ID "prompt_program_id"
#define SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_VERSION "prompt_program_version"
#define SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_DIGEST "prompt_program_digest"
#define SEMLOOM_PLAN_FIELD_RESULT_PARSER_ID "result_parser_id"
#define SEMLOOM_PLAN_FIELD_RESULT_PARSER_VERSION "result_parser_version"
#define SEMLOOM_PLAN_FIELD_RESULT_PARSER_DIGEST "result_parser_digest"
#define SEMLOOM_PLAN_FIELD_MODEL_ID "model_id"
#define SEMLOOM_PLAN_FIELD_TEMPERATURE "temperature"
#define SEMLOOM_PLAN_FIELD_TOP_P "top_p"
#define SEMLOOM_PLAN_FIELD_MAX_TOKENS "max_tokens"
#define SEMLOOM_PLAN_FIELD_N "n"
#define SEMLOOM_PLAN_FIELD_STREAM "stream"
#define SEMLOOM_PLAN_FIELD_STOP "stop"
#define SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM "physical_algorithm"
#define SEMLOOM_PLAN_FIELD_PHYSICAL_ROLE "physical_role"
#define SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_DIGEST "semantic_spec_digest"
#define SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM_DIGEST "physical_algorithm_digest"

#define SEMLOOM_SEMANTIC_SPEC_DIGEST_DOMAIN "semloom-semantic-spec-v2\0"
#define SEMLOOM_PHYSICAL_ALGORITHM_DIGEST_DOMAIN \
	"semloom-physical-algorithm-v2\0"

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
	SEMLOOM_PLAN_SEEN_ORDER_POLICY = 1U << 10,
	SEMLOOM_PLAN_SEEN_INSTRUCTION = 1U << 11,
	SEMLOOM_PLAN_SEEN_PROMPT_PROGRAM_ID = 1U << 12,
	SEMLOOM_PLAN_SEEN_PROMPT_PROGRAM_VERSION = 1U << 13,
	SEMLOOM_PLAN_SEEN_PROMPT_PROGRAM_DIGEST = 1U << 14,
	SEMLOOM_PLAN_SEEN_RESULT_PARSER_ID = 1U << 15,
	SEMLOOM_PLAN_SEEN_RESULT_PARSER_VERSION = 1U << 16,
	SEMLOOM_PLAN_SEEN_RESULT_PARSER_DIGEST = 1U << 17,
	SEMLOOM_PLAN_SEEN_MODEL_ID = 1U << 18,
	SEMLOOM_PLAN_SEEN_TEMPERATURE = 1U << 19,
	SEMLOOM_PLAN_SEEN_TOP_P = 1U << 20,
	SEMLOOM_PLAN_SEEN_MAX_TOKENS = 1U << 21,
	SEMLOOM_PLAN_SEEN_N = 1U << 22,
	SEMLOOM_PLAN_SEEN_STREAM = 1U << 23,
	SEMLOOM_PLAN_SEEN_STOP = 1U << 24,
	SEMLOOM_PLAN_SEEN_SEMANTIC_SPEC_DIGEST = 1U << 25,
	SEMLOOM_PLAN_SEEN_PHYSICAL_ALGORITHM_DIGEST = 1U << 26,
} SemloomPlanFieldBit;

#define SEMLOOM_RECORDING_PLAN_FIELDS ((1U << 10) - 1U)
#define SEMLOOM_EXACT_FILTER_PLAN_FIELDS ((1U << 27) - 1U)

static List *semloom_plan_spec_integer_field(const char *name, int value);
static List *semloom_plan_spec_string_field(const char *name, const char *value);
static int semloom_plan_spec_read_positive_integer(Node *value);
static int semloom_plan_spec_read_nonnegative_integer(Node *value);
static const char *semloom_plan_spec_read_string(Node *value,
											  Size max_bytes,
											  MemoryContext owner_context,
											  uint32 *length_out);
static void semloom_plan_spec_mark_seen(uint32 *seen_fields, uint32 field_bit);
static void semloom_plan_spec_validate(const SemloomPlanSpec *plan_spec);
static void semloom_exact_filter_semantic_digest(
	const char *instruction,
	const char *model_id,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
static void semloom_exact_filter_physical_digest(
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
static void semloom_hash_begin(pg_cryptohash_ctx **context);
static void semloom_hash_bytes(pg_cryptohash_ctx *context,
							   const void *data,
							   Size length);
static void semloom_hash_text(pg_cryptohash_ctx *context, const char *value);
static void semloom_hash_uint32(pg_cryptohash_ctx *context, uint32 value);
static void semloom_hash_finish(
	pg_cryptohash_ctx *context,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
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
		SEMLOOM_PLAN_FIELD_SCHEMA_VERSION, SEMLOOM_PLAN_SPEC_SCHEMA_VERSION));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_OPERATOR_KIND, operator_kind));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_INPUT_VALUE_KIND, SEMLOOM_PLAN_VALUE_TEXT));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_OUTPUT_VALUE_KIND, output_value_kind));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_NULL_POLICY, SEMLOOM_PLAN_NULL_PROPAGATE));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_ERROR_POLICY, SEMLOOM_PLAN_ERROR_FAIL_QUERY));
	fields = lappend(fields, semloom_plan_spec_integer_field(
		SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_VERSION, SEMLOOM_RECORDING_SPEC_VERSION));
	fields = lappend(fields, semloom_plan_spec_string_field(
		SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_ID, semantic_spec_id));
	fields = lappend(fields, semloom_plan_spec_string_field(
		SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM, SEMLOOM_RECORDING_ALGORITHM));
	fields = lappend(fields, semloom_plan_spec_string_field(
		SEMLOOM_PLAN_FIELD_PHYSICAL_ROLE, "reference"));
	return list_make2(fields, makeInteger(input_column));
}

List *
semloom_plan_spec_make_exact_filter_private(const char *instruction,
										const char *model_id,
										AttrNumber input_column)
{
	char semantic_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	char physical_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	List *fields = NIL;

	if (instruction == NULL || model_id == NULL || input_column <= 0)
		semloom_plan_spec_invalid("invalid exact SemFilter plan specification");
	semloom_exact_filter_semantic_digest(instruction, model_id, semantic_digest);
	semloom_exact_filter_physical_digest(physical_digest);

#define APPEND_INT(name, value) \
	do { fields = lappend(fields, semloom_plan_spec_integer_field((name), (value))); } while (0)
#define APPEND_STRING(name, value) \
	do { fields = lappend(fields, semloom_plan_spec_string_field((name), (value))); } while (0)
	APPEND_INT(SEMLOOM_PLAN_FIELD_SCHEMA_VERSION,
			   SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION);
	APPEND_INT(SEMLOOM_PLAN_FIELD_OPERATOR_KIND, SEMLOOM_PLAN_OPERATOR_FILTER);
	APPEND_INT(SEMLOOM_PLAN_FIELD_INPUT_VALUE_KIND, SEMLOOM_PLAN_VALUE_TEXT);
	APPEND_INT(SEMLOOM_PLAN_FIELD_OUTPUT_VALUE_KIND, SEMLOOM_PLAN_VALUE_TRISTATE);
	APPEND_INT(SEMLOOM_PLAN_FIELD_NULL_POLICY, SEMLOOM_PLAN_NULL_PROPAGATE);
	APPEND_INT(SEMLOOM_PLAN_FIELD_ERROR_POLICY, SEMLOOM_PLAN_ERROR_FAIL_QUERY);
	APPEND_INT(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_VERSION,
			   SEMLOOM_EXACT_FILTER_SPEC_VERSION);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_ID, SEMLOOM_EXACT_FILTER_SPEC_ID);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM,
				  SEMLOOM_EXACT_FILTER_ALGORITHM);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ROLE, SEMLOOM_EXACT_FILTER_ROLE);
	APPEND_INT(SEMLOOM_PLAN_FIELD_ORDER_POLICY, SEMLOOM_PLAN_ORDER_INPUT);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_INSTRUCTION, instruction);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_ID, SEMLOOM_PROMPT_PROGRAM_ID);
	APPEND_INT(SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_VERSION,
			   SEMLOOM_PROMPT_PROGRAM_VERSION);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_DIGEST,
				  SEMLOOM_PROMPT_PROGRAM_DIGEST);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_RESULT_PARSER_ID, SEMLOOM_RESULT_PARSER_ID);
	APPEND_INT(SEMLOOM_PLAN_FIELD_RESULT_PARSER_VERSION,
			   SEMLOOM_RESULT_PARSER_VERSION);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_RESULT_PARSER_DIGEST,
				  SEMLOOM_RESULT_PARSER_DIGEST);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_MODEL_ID, model_id);
	APPEND_INT(SEMLOOM_PLAN_FIELD_TEMPERATURE, SEMLOOM_FILTER_TEMPERATURE);
	APPEND_INT(SEMLOOM_PLAN_FIELD_TOP_P, SEMLOOM_FILTER_TOP_P);
	APPEND_INT(SEMLOOM_PLAN_FIELD_MAX_TOKENS, SEMLOOM_FILTER_MAX_TOKENS);
	APPEND_INT(SEMLOOM_PLAN_FIELD_N, SEMLOOM_FILTER_N);
	APPEND_INT(SEMLOOM_PLAN_FIELD_STREAM, SEMLOOM_FILTER_STREAM);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_STOP, SEMLOOM_FILTER_STOP);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_DIGEST, semantic_digest);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM_DIGEST, physical_digest);
#undef APPEND_STRING
#undef APPEND_INT

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
		uint32 ignored_length;

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

#define READ_POS(field_name, bit, target) \
		if (strcmp(name, (field_name)) == 0) \
		{ \
			semloom_plan_spec_mark_seen(&seen_fields, (bit)); \
			(target) = semloom_plan_spec_read_positive_integer(value_node); \
		}
#define READ_NONNEG(field_name, bit, target) \
		if (strcmp(name, (field_name)) == 0) \
		{ \
			semloom_plan_spec_mark_seen(&seen_fields, (bit)); \
			(target) = semloom_plan_spec_read_nonnegative_integer(value_node); \
		}
#define READ_STRING(field_name, bit, target, max_length, length_target) \
		if (strcmp(name, (field_name)) == 0) \
		{ \
			semloom_plan_spec_mark_seen(&seen_fields, (bit)); \
			(target) = semloom_plan_spec_read_string(value_node, (max_length), \
				owner_context, (length_target)); \
		}

		READ_POS(SEMLOOM_PLAN_FIELD_SCHEMA_VERSION,
				 SEMLOOM_PLAN_SEEN_SCHEMA_VERSION, plan_spec->schema_version)
		else READ_POS(SEMLOOM_PLAN_FIELD_OPERATOR_KIND,
					  SEMLOOM_PLAN_SEEN_OPERATOR_KIND, plan_spec->operator_kind)
		else READ_POS(SEMLOOM_PLAN_FIELD_INPUT_VALUE_KIND,
					  SEMLOOM_PLAN_SEEN_INPUT_VALUE_KIND, plan_spec->input_value_kind)
		else READ_POS(SEMLOOM_PLAN_FIELD_OUTPUT_VALUE_KIND,
					  SEMLOOM_PLAN_SEEN_OUTPUT_VALUE_KIND, plan_spec->output_value_kind)
		else READ_POS(SEMLOOM_PLAN_FIELD_NULL_POLICY,
					  SEMLOOM_PLAN_SEEN_NULL_POLICY, plan_spec->null_policy)
		else READ_POS(SEMLOOM_PLAN_FIELD_ERROR_POLICY,
					  SEMLOOM_PLAN_SEEN_ERROR_POLICY, plan_spec->error_policy)
		else READ_POS(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_VERSION,
					  SEMLOOM_PLAN_SEEN_SEMANTIC_SPEC_VERSION,
					  plan_spec->semantic_spec_version)
		else READ_STRING(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_ID,
						 SEMLOOM_PLAN_SEEN_SEMANTIC_SPEC_ID,
						 plan_spec->semantic_spec_id,
						 SEMLOOM_PLAN_SPEC_ID_MAX_BYTES,
						 &plan_spec->semantic_spec_id_length)
		else READ_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM,
						 SEMLOOM_PLAN_SEEN_PHYSICAL_ALGORITHM,
						 plan_spec->physical_algorithm,
						 SEMLOOM_PLAN_ALGORITHM_MAX_BYTES,
						 &plan_spec->physical_algorithm_length)
		else READ_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ROLE,
						 SEMLOOM_PLAN_SEEN_PHYSICAL_ROLE,
						 plan_spec->physical_role,
						 SEMLOOM_PLAN_ROLE_MAX_BYTES,
						 &ignored_length)
		else READ_POS(SEMLOOM_PLAN_FIELD_ORDER_POLICY,
					  SEMLOOM_PLAN_SEEN_ORDER_POLICY, plan_spec->order_policy)
		else READ_STRING(SEMLOOM_PLAN_FIELD_INSTRUCTION,
						 SEMLOOM_PLAN_SEEN_INSTRUCTION,
						 plan_spec->instruction,
						 SEMLOOM_FILTER_INSTRUCTION_MAX_BYTES,
						 &plan_spec->instruction_length)
		else READ_STRING(SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_ID,
						 SEMLOOM_PLAN_SEEN_PROMPT_PROGRAM_ID,
						 plan_spec->prompt_program_id,
						 SEMLOOM_PLAN_PROGRAM_ID_MAX_BYTES,
						 &ignored_length)
		else READ_POS(SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_VERSION,
					  SEMLOOM_PLAN_SEEN_PROMPT_PROGRAM_VERSION,
					  plan_spec->prompt_program_version)
		else READ_STRING(SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_DIGEST,
						 SEMLOOM_PLAN_SEEN_PROMPT_PROGRAM_DIGEST,
						 plan_spec->prompt_program_digest,
						 SEMLOOM_SHA256_HEX_LENGTH,
						 &ignored_length)
		else READ_STRING(SEMLOOM_PLAN_FIELD_RESULT_PARSER_ID,
						 SEMLOOM_PLAN_SEEN_RESULT_PARSER_ID,
						 plan_spec->result_parser_id,
						 SEMLOOM_PLAN_PROGRAM_ID_MAX_BYTES,
						 &ignored_length)
		else READ_POS(SEMLOOM_PLAN_FIELD_RESULT_PARSER_VERSION,
					  SEMLOOM_PLAN_SEEN_RESULT_PARSER_VERSION,
					  plan_spec->result_parser_version)
		else READ_STRING(SEMLOOM_PLAN_FIELD_RESULT_PARSER_DIGEST,
						 SEMLOOM_PLAN_SEEN_RESULT_PARSER_DIGEST,
						 plan_spec->result_parser_digest,
						 SEMLOOM_SHA256_HEX_LENGTH,
						 &ignored_length)
		else READ_STRING(SEMLOOM_PLAN_FIELD_MODEL_ID,
						 SEMLOOM_PLAN_SEEN_MODEL_ID,
						 plan_spec->model_id,
						 SEMLOOM_FILTER_MODEL_MAX_BYTES,
						 &plan_spec->model_id_length)
		else READ_NONNEG(SEMLOOM_PLAN_FIELD_TEMPERATURE,
						 SEMLOOM_PLAN_SEEN_TEMPERATURE, plan_spec->temperature)
		else READ_NONNEG(SEMLOOM_PLAN_FIELD_TOP_P,
						 SEMLOOM_PLAN_SEEN_TOP_P, plan_spec->top_p)
		else READ_NONNEG(SEMLOOM_PLAN_FIELD_MAX_TOKENS,
						 SEMLOOM_PLAN_SEEN_MAX_TOKENS, plan_spec->max_tokens)
		else READ_NONNEG(SEMLOOM_PLAN_FIELD_N,
						 SEMLOOM_PLAN_SEEN_N, plan_spec->n)
		else READ_NONNEG(SEMLOOM_PLAN_FIELD_STREAM,
						 SEMLOOM_PLAN_SEEN_STREAM, plan_spec->stream)
		else READ_STRING(SEMLOOM_PLAN_FIELD_STOP,
						 SEMLOOM_PLAN_SEEN_STOP,
						 plan_spec->stop, 8, &ignored_length)
		else READ_STRING(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_DIGEST,
						 SEMLOOM_PLAN_SEEN_SEMANTIC_SPEC_DIGEST,
						 plan_spec->semantic_spec_digest,
						 SEMLOOM_SHA256_HEX_LENGTH,
						 &ignored_length)
		else READ_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM_DIGEST,
						 SEMLOOM_PLAN_SEEN_PHYSICAL_ALGORITHM_DIGEST,
						 plan_spec->physical_algorithm_digest,
						 SEMLOOM_SHA256_HEX_LENGTH,
						 &ignored_length)
		else
			semloom_plan_spec_invalid("unknown semantic plan specification field");
#undef READ_STRING
#undef READ_NONNEG
#undef READ_POS
	}

	if (plan_spec->schema_version == SEMLOOM_PLAN_SPEC_SCHEMA_VERSION)
	{
		if (seen_fields != SEMLOOM_RECORDING_PLAN_FIELDS ||
			list_length(fields) != SEMLOOM_RECORDING_PLAN_FIELD_COUNT)
			semloom_plan_spec_invalid("incomplete semantic plan specification");
	}
	else if (plan_spec->schema_version == SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION)
	{
		if (seen_fields != SEMLOOM_EXACT_FILTER_PLAN_FIELDS ||
			list_length(fields) != SEMLOOM_EXACT_FILTER_PLAN_FIELD_COUNT)
			semloom_plan_spec_invalid("incomplete semantic plan specification");
	}
	else
		semloom_plan_spec_invalid("unsupported semantic plan specification");
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
semloom_plan_spec_read_positive_integer(Node *value)
{
	if (!IsA(value, Integer) || intVal(value) <= 0)
		semloom_plan_spec_invalid("invalid semantic plan specification field");
	return intVal(value);
}

static int
semloom_plan_spec_read_nonnegative_integer(Node *value)
{
	if (!IsA(value, Integer) || intVal(value) < 0)
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
	if (plan_spec->schema_version == SEMLOOM_PLAN_SPEC_SCHEMA_VERSION)
	{
		bool map_spec = plan_spec->operator_kind == SEMLOOM_PLAN_OPERATOR_MAP &&
			plan_spec->output_value_kind == SEMLOOM_PLAN_VALUE_TEXT &&
			strcmp(plan_spec->semantic_spec_id, SEMLOOM_MAP_RECORDING_SPEC_ID) == 0;
		bool filter_spec = plan_spec->operator_kind == SEMLOOM_PLAN_OPERATOR_FILTER &&
			plan_spec->output_value_kind == SEMLOOM_PLAN_VALUE_TRISTATE &&
			strcmp(plan_spec->semantic_spec_id, SEMLOOM_FILTER_RECORDING_SPEC_ID) == 0;

		if ((!map_spec && !filter_spec) ||
			plan_spec->input_value_kind != SEMLOOM_PLAN_VALUE_TEXT ||
			plan_spec->null_policy != SEMLOOM_PLAN_NULL_PROPAGATE ||
			plan_spec->error_policy != SEMLOOM_PLAN_ERROR_FAIL_QUERY ||
			plan_spec->semantic_spec_version != SEMLOOM_RECORDING_SPEC_VERSION ||
			strcmp(plan_spec->physical_algorithm, SEMLOOM_RECORDING_ALGORITHM) != 0 ||
			strcmp(plan_spec->physical_role, "reference") != 0)
			semloom_plan_spec_invalid("unsupported semantic plan specification");
		return;
	}
	else
	{
		char semantic_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
		char physical_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];

		semloom_exact_filter_semantic_digest(plan_spec->instruction,
										  plan_spec->model_id,
										  semantic_digest);
		semloom_exact_filter_physical_digest(physical_digest);
		if (plan_spec->operator_kind != SEMLOOM_PLAN_OPERATOR_FILTER ||
			plan_spec->input_value_kind != SEMLOOM_PLAN_VALUE_TEXT ||
			plan_spec->output_value_kind != SEMLOOM_PLAN_VALUE_TRISTATE ||
			plan_spec->null_policy != SEMLOOM_PLAN_NULL_PROPAGATE ||
			plan_spec->error_policy != SEMLOOM_PLAN_ERROR_FAIL_QUERY ||
			plan_spec->order_policy != SEMLOOM_PLAN_ORDER_INPUT ||
			plan_spec->semantic_spec_version != SEMLOOM_EXACT_FILTER_SPEC_VERSION ||
			strcmp(plan_spec->semantic_spec_id, SEMLOOM_EXACT_FILTER_SPEC_ID) != 0 ||
			strcmp(plan_spec->prompt_program_id, SEMLOOM_PROMPT_PROGRAM_ID) != 0 ||
			plan_spec->prompt_program_version != SEMLOOM_PROMPT_PROGRAM_VERSION ||
			strcmp(plan_spec->prompt_program_digest, SEMLOOM_PROMPT_PROGRAM_DIGEST) != 0 ||
			strcmp(plan_spec->result_parser_id, SEMLOOM_RESULT_PARSER_ID) != 0 ||
			plan_spec->result_parser_version != SEMLOOM_RESULT_PARSER_VERSION ||
			strcmp(plan_spec->result_parser_digest, SEMLOOM_RESULT_PARSER_DIGEST) != 0 ||
			plan_spec->temperature != SEMLOOM_FILTER_TEMPERATURE ||
			plan_spec->top_p != SEMLOOM_FILTER_TOP_P ||
			plan_spec->max_tokens != SEMLOOM_FILTER_MAX_TOKENS ||
			plan_spec->n != SEMLOOM_FILTER_N ||
			plan_spec->stream != (bool) SEMLOOM_FILTER_STREAM ||
			strcmp(plan_spec->stop, SEMLOOM_FILTER_STOP) != 0 ||
			strcmp(plan_spec->physical_algorithm,
				   SEMLOOM_EXACT_FILTER_ALGORITHM) != 0 ||
			strcmp(plan_spec->physical_role, SEMLOOM_EXACT_FILTER_ROLE) != 0 ||
			strcmp(plan_spec->semantic_spec_digest, semantic_digest) != 0 ||
			strcmp(plan_spec->physical_algorithm_digest, physical_digest) != 0)
			semloom_plan_spec_invalid("unsupported exact SemFilter plan specification");
	}
}

static void
semloom_exact_filter_semantic_digest(
	const char *instruction,
	const char *model_id,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;
	uint8 stream = SEMLOOM_FILTER_STREAM;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context, SEMLOOM_SEMANTIC_SPEC_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_SEMANTIC_SPEC_DIGEST_DOMAIN) - 1);
	semloom_hash_uint32(context, SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION);
	semloom_hash_text(context, SEMLOOM_EXACT_FILTER_SPEC_ID);
	semloom_hash_uint32(context, SEMLOOM_EXACT_FILTER_SPEC_VERSION);
	semloom_hash_text(context, "SEM_FILTER");
	semloom_hash_text(context, "text");
	semloom_hash_text(context, "tristate");
	semloom_hash_text(context, instruction);
	semloom_hash_text(context, SEMLOOM_PROMPT_PROGRAM_ID);
	semloom_hash_uint32(context, SEMLOOM_PROMPT_PROGRAM_VERSION);
	semloom_hash_text(context, SEMLOOM_PROMPT_PROGRAM_DIGEST);
	semloom_hash_text(context, SEMLOOM_RESULT_PARSER_ID);
	semloom_hash_uint32(context, SEMLOOM_RESULT_PARSER_VERSION);
	semloom_hash_text(context, SEMLOOM_RESULT_PARSER_DIGEST);
	semloom_hash_text(context, "PROPAGATE_NULL");
	semloom_hash_text(context, "FAIL_QUERY");
	semloom_hash_text(context, SEMLOOM_EXACT_FILTER_ORDER_POLICY);
	semloom_hash_text(context, model_id);
	semloom_hash_uint32(context, SEMLOOM_FILTER_TEMPERATURE);
	semloom_hash_uint32(context, SEMLOOM_FILTER_TOP_P);
	semloom_hash_uint32(context, SEMLOOM_FILTER_MAX_TOKENS);
	semloom_hash_uint32(context, SEMLOOM_FILTER_N);
	semloom_hash_bytes(context, &stream, sizeof(stream));
	semloom_hash_text(context, SEMLOOM_FILTER_STOP);
	semloom_hash_finish(context, output);
}

static void
semloom_exact_filter_physical_digest(
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context, SEMLOOM_PHYSICAL_ALGORITHM_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_PHYSICAL_ALGORITHM_DIGEST_DOMAIN) - 1);
	semloom_hash_text(context, SEMLOOM_EXACT_FILTER_ALGORITHM);
	semloom_hash_text(context, SEMLOOM_EXACT_FILTER_ROLE);
	semloom_hash_finish(context, output);
}

static void
semloom_hash_begin(pg_cryptohash_ctx **context)
{
	*context = pg_cryptohash_create(PG_SHA256);
	if (*context == NULL || pg_cryptohash_init(*context) < 0)
	{
		if (*context != NULL)
			pg_cryptohash_free(*context);
		elog(ERROR, "could not initialize SemLoom semantic plan digest");
	}
}

static void
semloom_hash_bytes(pg_cryptohash_ctx *context, const void *data, Size length)
{
	if (length > 0 && pg_cryptohash_update(context, data, length) < 0)
	{
		pg_cryptohash_free(context);
		elog(ERROR, "could not update SemLoom semantic plan digest");
	}
}

static void
semloom_hash_text(pg_cryptohash_ctx *context, const char *value)
{
	Size length = strlen(value);

	if (length > PG_UINT32_MAX)
		elog(ERROR, "SemLoom semantic plan value is too long");
	semloom_hash_uint32(context, (uint32) length);
	semloom_hash_bytes(context, value, length);
}

static void
semloom_hash_uint32(pg_cryptohash_ctx *context, uint32 value)
{
	uint8 bytes[4];

	bytes[0] = (uint8) (value >> 24);
	bytes[1] = (uint8) (value >> 16);
	bytes[2] = (uint8) (value >> 8);
	bytes[3] = (uint8) value;
	semloom_hash_bytes(context, bytes, sizeof(bytes));
}

static void
semloom_hash_finish(pg_cryptohash_ctx *context,
						char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	uint8 digest[PG_SHA256_DIGEST_LENGTH];
	static const char hex[] = "0123456789abcdef";
	int index;

	if (pg_cryptohash_final(context, digest, sizeof(digest)) < 0)
	{
		pg_cryptohash_free(context);
		elog(ERROR, "could not finish SemLoom semantic plan digest");
	}
	pg_cryptohash_free(context);
	for (index = 0; index < PG_SHA256_DIGEST_LENGTH; index++)
	{
		output[index * 2] = hex[digest[index] >> 4];
		output[index * 2 + 1] = hex[digest[index] & 0x0f];
	}
	output[SEMLOOM_SHA256_HEX_LENGTH] = '\0';
}

static void
semloom_plan_spec_invalid(const char *message)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("%s", message)));
	pg_unreachable();
}
