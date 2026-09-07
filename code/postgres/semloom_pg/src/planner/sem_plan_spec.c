/* Strict copyObject-safe encoding for PostgreSQL-owned semantic plans. */
#include "postgres.h"

#include "common/cryptohash.h"
#include "common/sha2.h"
#include "catalog/pg_type_d.h"
#include "commands/explain_format.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"

#include "semantics/recording_contract.h"
#include "semantics/generation_profile.h"
#include "semantics/semantic_filter_contract.h"
#include "semantics/semantic_map_contract.h"
#include "planner/sem_plan_spec.h"

#define SEMLOOM_RECORDING_PLAN_FIELD_COUNT 10
#define SEMLOOM_EXACT_FILTER_PLAN_FIELD_COUNT 27
#define SEMLOOM_CHOICE_FILTER_PLAN_FIELD_COUNT 28
#define SEMLOOM_GENERATE_MAP_PLAN_FIELD_COUNT 29
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
#define SEMLOOM_PLAN_FIELD_GENERATION_PROFILE "generation_profile"
#define SEMLOOM_PLAN_FIELD_HAS_STOP "has_stop"
#define SEMLOOM_PLAN_FIELD_MAX_INPUT_BYTES "max_input_bytes"
#define SEMLOOM_PLAN_FIELD_MAX_OUTPUT_BYTES "max_output_bytes"

#define SEMLOOM_SEMANTIC_SPEC_DIGEST_DOMAIN "semloom-semantic-spec-v2\0"
#define SEMLOOM_CHOICE_SEMANTIC_SPEC_DIGEST_DOMAIN "semloom-semantic-spec-v3\0"
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
	SEMLOOM_PLAN_SEEN_GENERATION_PROFILE = 1U << 27,
	SEMLOOM_PLAN_SEEN_HAS_STOP = 1U << 28,
	SEMLOOM_PLAN_SEEN_MAX_INPUT_BYTES = 1U << 29,
	SEMLOOM_PLAN_SEEN_MAX_OUTPUT_BYTES = 1U << 30,
} SemloomPlanFieldBit;

#define SEMLOOM_RECORDING_PLAN_FIELDS ((1U << 10) - 1U)
#define SEMLOOM_EXACT_FILTER_PLAN_FIELDS ((1U << 27) - 1U)
#define SEMLOOM_CHOICE_FILTER_PLAN_FIELDS ((1U << 28) - 1U)
#define SEMLOOM_GENERATE_MAP_PLAN_FIELDS \
	((SEMLOOM_EXACT_FILTER_PLAN_FIELDS & ~SEMLOOM_PLAN_SEEN_STOP) | \
	 SEMLOOM_PLAN_SEEN_HAS_STOP | SEMLOOM_PLAN_SEEN_MAX_INPUT_BYTES | \
	 SEMLOOM_PLAN_SEEN_MAX_OUTPUT_BYTES)

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
static List *semloom_make_filter_private(const char *instruction,
	const char *model_id, AttrNumber input_column, const AiGenerationProfile *profile);
static List *semloom_plan_profile_encode(const AiGenerationProfile *profile);
static void semloom_plan_profile_decode(Node *node, MemoryContext owner_context,
	SemloomPlanSpec *plan_spec);
static void semloom_plan_profile_digest(const AiGenerationProfile *profile,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
static void semloom_exact_filter_semantic_digest(
	const char *instruction,
	const char *model_id,
	const AiGenerationProfile *profile,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
static void semloom_model_reference_physical_digest(
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
static void semloom_generate_map_semantic_digest(const char *instruction,
	const char *model_id, uint32 max_tokens,
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
	return semloom_make_filter_private(instruction, model_id, input_column, NULL);
}

List *
semloom_plan_spec_make_choice_filter_private(const char *instruction,
										 const char *model_id,
										 AttrNumber input_column)
{
	return semloom_make_filter_private(instruction, model_id, input_column,
		semloom_generation_profile_tristate());
}

static List *
semloom_make_filter_private(const char *instruction, const char *model_id,
						   AttrNumber input_column, const AiGenerationProfile *profile)
{
	char semantic_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	char physical_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	List *fields = NIL;

	if (instruction == NULL || model_id == NULL || input_column <= 0)
		semloom_plan_spec_invalid("invalid exact SemFilter plan specification");
	semloom_exact_filter_semantic_digest(instruction, model_id, profile, semantic_digest);
	semloom_model_reference_physical_digest(physical_digest);

#define APPEND_INT(name, value) \
	do { fields = lappend(fields, semloom_plan_spec_integer_field((name), (value))); } while (0)
#define APPEND_STRING(name, value) \
	do { fields = lappend(fields, semloom_plan_spec_string_field((name), (value))); } while (0)
	APPEND_INT(SEMLOOM_PLAN_FIELD_SCHEMA_VERSION,
			   profile == NULL ? SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION :
			   SEMLOOM_CHOICE_FILTER_PLAN_SCHEMA_VERSION);
	APPEND_INT(SEMLOOM_PLAN_FIELD_OPERATOR_KIND, SEMLOOM_PLAN_OPERATOR_FILTER);
	APPEND_INT(SEMLOOM_PLAN_FIELD_INPUT_VALUE_KIND, SEMLOOM_PLAN_VALUE_TEXT);
	APPEND_INT(SEMLOOM_PLAN_FIELD_OUTPUT_VALUE_KIND, SEMLOOM_PLAN_VALUE_TRISTATE);
	APPEND_INT(SEMLOOM_PLAN_FIELD_NULL_POLICY, SEMLOOM_PLAN_NULL_PROPAGATE);
	APPEND_INT(SEMLOOM_PLAN_FIELD_ERROR_POLICY, SEMLOOM_PLAN_ERROR_FAIL_QUERY);
	APPEND_INT(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_VERSION,
			   SEMLOOM_EXACT_FILTER_SPEC_VERSION);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_ID, SEMLOOM_EXACT_FILTER_SPEC_ID);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM,
				  SEMLOOM_MODEL_REFERENCE_ALGORITHM);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ROLE, SEMLOOM_MODEL_REFERENCE_ROLE);
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
	if (profile != NULL)
		fields = lappend(fields, list_make2(
			makeString(pstrdup(SEMLOOM_PLAN_FIELD_GENERATION_PROFILE)),
			semloom_plan_profile_encode(profile)));
#undef APPEND_STRING
#undef APPEND_INT

	return list_make2(fields, makeInteger(input_column));
}

List *
semloom_plan_spec_make_generate_map_private(const char *instruction,
										  const char *model_id, uint32 max_tokens,
										  AttrNumber input_column, Oid marker_function_oid)
{
	char semantic_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	char physical_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	List *fields = NIL;
	Const *function_binding;

	if (input_column <= 0 || !OidIsValid(marker_function_oid))
		semloom_plan_spec_invalid("invalid semantic executor binding");
	semloom_generate_map_semantic_digest(instruction, model_id, max_tokens, semantic_digest);
	semloom_model_reference_physical_digest(physical_digest);
#define APPEND_INT(name, value) \
	do { fields = lappend(fields, semloom_plan_spec_integer_field((name), (value))); } while (0)
#define APPEND_STRING(name, value) \
	do { fields = lappend(fields, semloom_plan_spec_string_field((name), (value))); } while (0)
	APPEND_INT(SEMLOOM_PLAN_FIELD_SCHEMA_VERSION, SEMLOOM_MAP_PLAN_SCHEMA_VERSION);
	APPEND_INT(SEMLOOM_PLAN_FIELD_OPERATOR_KIND, SEMLOOM_PLAN_OPERATOR_MAP);
	APPEND_INT(SEMLOOM_PLAN_FIELD_INPUT_VALUE_KIND, SEMLOOM_PLAN_VALUE_TEXT);
	APPEND_INT(SEMLOOM_PLAN_FIELD_OUTPUT_VALUE_KIND, SEMLOOM_PLAN_VALUE_TEXT);
	APPEND_INT(SEMLOOM_PLAN_FIELD_NULL_POLICY, SEMLOOM_PLAN_NULL_PROPAGATE);
	APPEND_INT(SEMLOOM_PLAN_FIELD_ERROR_POLICY, SEMLOOM_PLAN_ERROR_FAIL_QUERY);
	APPEND_INT(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_VERSION, 1);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_ID, SEMLOOM_MAP_SPEC_ID);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM, SEMLOOM_MODEL_REFERENCE_ALGORITHM);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ROLE, SEMLOOM_MODEL_REFERENCE_ROLE);
	APPEND_INT(SEMLOOM_PLAN_FIELD_ORDER_POLICY, SEMLOOM_PLAN_ORDER_INPUT);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_INSTRUCTION, instruction);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_ID, SEMLOOM_MAP_PROMPT_PROGRAM_ID);
	APPEND_INT(SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_VERSION, 1);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PROMPT_PROGRAM_DIGEST, SEMLOOM_MAP_PROMPT_PROGRAM_DIGEST);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_RESULT_PARSER_ID, SEMLOOM_MAP_RESULT_PARSER_ID);
	APPEND_INT(SEMLOOM_PLAN_FIELD_RESULT_PARSER_VERSION, 1);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_RESULT_PARSER_DIGEST, SEMLOOM_MAP_RESULT_PARSER_DIGEST);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_MODEL_ID, model_id);
	APPEND_INT(SEMLOOM_PLAN_FIELD_TEMPERATURE, 0);
	APPEND_INT(SEMLOOM_PLAN_FIELD_TOP_P, 1);
	APPEND_INT(SEMLOOM_PLAN_FIELD_MAX_TOKENS, max_tokens);
	APPEND_INT(SEMLOOM_PLAN_FIELD_N, 1);
	APPEND_INT(SEMLOOM_PLAN_FIELD_STREAM, 0);
	APPEND_INT(SEMLOOM_PLAN_FIELD_HAS_STOP, 0);
	APPEND_INT(SEMLOOM_PLAN_FIELD_MAX_INPUT_BYTES, SEMLOOM_MAP_MAX_INPUT_BYTES);
	APPEND_INT(SEMLOOM_PLAN_FIELD_MAX_OUTPUT_BYTES, SEMLOOM_MAP_MAX_OUTPUT_BYTES);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_SEMANTIC_SPEC_DIGEST, semantic_digest);
	APPEND_STRING(SEMLOOM_PLAN_FIELD_PHYSICAL_ALGORITHM_DIGEST, physical_digest);
#undef APPEND_STRING
#undef APPEND_INT
	function_binding = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
		ObjectIdGetDatum(marker_function_oid), false, true);
	return list_make2(fields, list_make2(makeInteger(input_column), function_binding));
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
	if (fields_node == NULL || binding_node == NULL ||
		!IsA(fields_node, List))
		semloom_plan_spec_invalid("invalid semantic plan specification");
	fields = (List *) fields_node;
	if (IsA(binding_node, Integer))
	{
		if (intVal(binding_node) <= 0 || intVal(binding_node) > PG_INT16_MAX)
			semloom_plan_spec_invalid("invalid semantic executor binding");
		*input_column = (AttrNumber) intVal(binding_node);
	}

	foreach(cell, fields)
	{
		Node *field_node = (Node *) lfirst(cell);
		List *field;
		Node *name_node;
		Node *value_node;
		const char *name;
		uint32 ignored_length;

		if (field_node == NULL || !IsA(field_node, List))
			semloom_plan_spec_invalid("invalid semantic plan specification field");
		field = (List *) field_node;
		if (list_length(field) != 2)
			semloom_plan_spec_invalid("invalid semantic plan specification field");
		name_node = (Node *) linitial(field);
		value_node = (Node *) lsecond(field);
		if (name_node == NULL || !IsA(name_node, String))
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
		else READ_NONNEG(SEMLOOM_PLAN_FIELD_HAS_STOP,
			SEMLOOM_PLAN_SEEN_HAS_STOP, plan_spec->has_stop)
		else READ_POS(SEMLOOM_PLAN_FIELD_MAX_INPUT_BYTES,
			SEMLOOM_PLAN_SEEN_MAX_INPUT_BYTES, plan_spec->max_input_bytes)
		else READ_POS(SEMLOOM_PLAN_FIELD_MAX_OUTPUT_BYTES,
			SEMLOOM_PLAN_SEEN_MAX_OUTPUT_BYTES, plan_spec->max_output_bytes)
		else if (strcmp(name, SEMLOOM_PLAN_FIELD_GENERATION_PROFILE) == 0)
		{
			semloom_plan_spec_mark_seen(&seen_fields, SEMLOOM_PLAN_SEEN_GENERATION_PROFILE);
			semloom_plan_profile_decode(value_node, owner_context, plan_spec);
		}
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
	else if (plan_spec->schema_version == SEMLOOM_CHOICE_FILTER_PLAN_SCHEMA_VERSION)
	{
		if (seen_fields != SEMLOOM_CHOICE_FILTER_PLAN_FIELDS ||
			list_length(fields) != SEMLOOM_CHOICE_FILTER_PLAN_FIELD_COUNT)
			semloom_plan_spec_invalid("incomplete semantic plan specification");
	}
	else if (plan_spec->schema_version == SEMLOOM_MAP_PLAN_SCHEMA_VERSION)
	{
		if (seen_fields != SEMLOOM_GENERATE_MAP_PLAN_FIELDS ||
			list_length(fields) != SEMLOOM_GENERATE_MAP_PLAN_FIELD_COUNT)
			semloom_plan_spec_invalid("incomplete semantic plan specification");
	}
	else
		semloom_plan_spec_invalid("unsupported semantic plan specification");
	if (plan_spec->schema_version == SEMLOOM_MAP_PLAN_SCHEMA_VERSION)
	{
		List *binding;
		Const *function_binding;
		Node *column;

		if (!IsA(binding_node, List) || list_length((List *) binding_node) != 2)
			semloom_plan_spec_invalid("invalid semantic executor binding");
		binding = (List *) binding_node;
		column = linitial(binding);
		if (column == NULL || !IsA(column, Integer) || intVal(column) <= 0 ||
			intVal(column) > PG_INT16_MAX || lsecond(binding) == NULL ||
			!IsA(lsecond(binding), Const))
			semloom_plan_spec_invalid("invalid semantic executor binding");
		function_binding = lsecond_node(Const, binding);
		if (function_binding->consttype != OIDOID || function_binding->constisnull ||
			!function_binding->constbyval || function_binding->constlen != sizeof(Oid) ||
			function_binding->consttypmod != -1 || OidIsValid(function_binding->constcollid) ||
			!OidIsValid(DatumGetObjectId(function_binding->constvalue)))
			semloom_plan_spec_invalid("invalid semantic executor binding");
		*input_column = (AttrNumber) intVal(column);
		plan_spec->marker_function_oid = DatumGetObjectId(function_binding->constvalue);
	}
	else if (!IsA(binding_node, Integer))
		semloom_plan_spec_invalid("invalid semantic executor binding");
	semloom_plan_spec_validate(plan_spec);
}

/* Named PG nodes, including ordered choices: no pointer to a live registry. */
static List *
semloom_plan_profile_encode(const AiGenerationProfile *profile)
{
	List *fields = NIL;
	List *choices = NIL;
	char digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
	uint32 index;

	semloom_plan_profile_digest(profile, digest);
	fields = lappend(fields, semloom_plan_spec_string_field("profile_id",
		pnstrdup((const char *) profile->profile_id.data, profile->profile_id.length)));
	fields = lappend(fields, semloom_plan_spec_integer_field("profile_version",
		profile->profile_version));
	fields = lappend(fields, semloom_plan_spec_string_field("constraint_kind", "CHOICE"));
	for (index = 0; index < profile->choice_count; index++)
		choices = lappend(choices, makeString(pnstrdup(
			(const char *) profile->choices[index].data, profile->choices[index].length)));
	fields = lappend(fields, list_make2(makeString(pstrdup("choices")), choices));
	fields = lappend(fields, semloom_plan_spec_string_field("profile_digest", digest));
	return fields;
}

static void
semloom_plan_profile_decode(Node *node, MemoryContext owner_context,
							SemloomPlanSpec *plan_spec)
{
	AiGenerationProfile *profile = &plan_spec->generation_profile;
	ListCell *cell;
	uint32 seen = 0;
	char digest[SEMLOOM_SHA256_HEX_LENGTH + 1];

	if (node == NULL || !IsA(node, List) || list_length((List *) node) != 5)
		semloom_plan_spec_invalid("invalid generation profile fields");
	foreach(cell, (List *) node)
	{
		Node *field_node = lfirst(cell);
		List *field;
		Node *value;
		const char *name;
		uint32 length;

		if (field_node == NULL || !IsA(field_node, List) ||
			list_length((List *) field_node) != 2 || linitial((List *) field_node) == NULL ||
			!IsA(linitial((List *) field_node), String))
			semloom_plan_spec_invalid("invalid generation profile field");
		field = (List *) field_node;
		name = strVal(linitial(field));
		value = lsecond(field);
		if (strcmp(name, "profile_id") == 0)
		{
			semloom_plan_spec_mark_seen(&seen, 1U);
			profile->profile_id.data = (const uint8 *) semloom_plan_spec_read_string(
				value, 128, owner_context, &profile->profile_id.length);
		}
		else if (strcmp(name, "profile_version") == 0)
		{
			semloom_plan_spec_mark_seen(&seen, 2U);
			profile->profile_version = semloom_plan_spec_read_positive_integer(value);
		}
		else if (strcmp(name, "constraint_kind") == 0)
		{
			const char *kind;

			semloom_plan_spec_mark_seen(&seen, 4U);
			kind = semloom_plan_spec_read_string(value, 16, owner_context, &length);
			if (strcmp(kind, "CHOICE") != 0)
				semloom_plan_spec_invalid("unsupported generation profile constraint");
			profile->constraint_kind = AI_GENERATION_CONSTRAINT_CHOICE;
		}
		else if (strcmp(name, "choices") == 0)
		{
			ListCell *choice;
			uint32 index = 0;

			semloom_plan_spec_mark_seen(&seen, 8U);
			if (value == NULL || !IsA(value, List) ||
				list_length((List *) value) != AI_GENERATION_PROFILE_MAX_CHOICES)
				semloom_plan_spec_invalid("invalid generation profile choices");
			foreach(choice, (List *) value)
			{
				profile->choices[index].data = (const uint8 *) semloom_plan_spec_read_string(
					lfirst(choice), 7, owner_context, &profile->choices[index].length);
				index++;
			}
			profile->choice_count = index;
		}
		else if (strcmp(name, "profile_digest") == 0)
		{
			semloom_plan_spec_mark_seen(&seen, 16U);
			plan_spec->generation_profile_digest = semloom_plan_spec_read_string(
				value, SEMLOOM_SHA256_HEX_LENGTH, owner_context, &length);
		}
		else
			semloom_plan_spec_invalid("unknown generation profile field");
	}
	if (seen != 31U)
		semloom_plan_spec_invalid("incomplete generation profile");
	semloom_plan_profile_digest(profile, digest);
	if (strcmp(digest, plan_spec->generation_profile_digest) != 0)
		semloom_plan_spec_invalid("generation profile digest mismatch");
}

static void
semloom_plan_profile_digest(const AiGenerationProfile *profile,
						   char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	uint8 bytes[SEMLOOM_GENERATION_PROFILE_CANONICAL_BYTES];
	uint32 length;
	pg_cryptohash_ctx *context;

	if (!semloom_generation_profile_encode(profile, bytes, sizeof(bytes), &length))
		semloom_plan_spec_invalid("unsupported generation profile");
	semloom_hash_begin(&context);
	semloom_hash_bytes(context, bytes, length);
	semloom_hash_finish(context, output);
}

void
semloom_plan_spec_explain(const SemloomPlanSpec *plan_spec, ExplainState *explain_state)
{
	ExplainPropertyText("Physical Role", plan_spec->physical_role, explain_state);
	if (plan_spec->model_id != NULL)
	{
		ExplainPropertyText("Semantic Spec", plan_spec->semantic_spec_id, explain_state);
		ExplainPropertyText("Physical Algorithm", plan_spec->physical_algorithm, explain_state);
		ExplainPropertyText("Prompt Program", plan_spec->prompt_program_id, explain_state);
		ExplainPropertyText("Result Parser", plan_spec->result_parser_id, explain_state);
		ExplainPropertyText("Model", plan_spec->model_id, explain_state);
	}
	if (plan_spec->schema_version == SEMLOOM_MAP_PLAN_SCHEMA_VERSION)
	{
		ExplainPropertyInteger("Semantic Plan Schema", NULL, plan_spec->schema_version, explain_state);
		ExplainPropertyText("Semantic Spec Digest", plan_spec->semantic_spec_digest, explain_state);
		ExplainPropertyInteger("Max Tokens", NULL, plan_spec->max_tokens, explain_state);
		ExplainPropertyInteger("Max Input Bytes", NULL, plan_spec->max_input_bytes, explain_state);
		ExplainPropertyInteger("Max Output Bytes", NULL, plan_spec->max_output_bytes, explain_state);
	}
	if (plan_spec->generation_profile_digest != NULL)
	{
		List *choices = NIL;
		uint32 index;

		ExplainPropertyInteger("Semantic Plan Schema", NULL, plan_spec->schema_version, explain_state);
		ExplainPropertyText("Semantic Spec Digest", plan_spec->semantic_spec_digest, explain_state);
		ExplainPropertyText("Generation Profile",
			(const char *) plan_spec->generation_profile.profile_id.data, explain_state);
		ExplainPropertyInteger("Generation Profile Version", NULL,
			plan_spec->generation_profile.profile_version, explain_state);
		ExplainPropertyText("Generation Constraint", "CHOICE", explain_state);
		for (index = 0; index < plan_spec->generation_profile.choice_count; index++)
			choices = lappend(choices, (char *) plan_spec->generation_profile.choices[index].data);
		ExplainPropertyList("Generation Choices", choices, explain_state);
		ExplainPropertyText("Generation Profile Digest", plan_spec->generation_profile_digest, explain_state);
		ExplainPropertyText("Generation Quality", "unqualified", explain_state);
	}
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
	if (value == NULL || !IsA(value, Integer) || intVal(value) <= 0)
		semloom_plan_spec_invalid("invalid semantic plan specification field");
	return intVal(value);
}

static int
semloom_plan_spec_read_nonnegative_integer(Node *value)
{
	if (value == NULL || !IsA(value, Integer) || intVal(value) < 0)
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

	if (value == NULL || !IsA(value, String))
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
	else if (plan_spec->schema_version == SEMLOOM_MAP_PLAN_SCHEMA_VERSION)
	{
		char semantic_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
		char physical_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];

		semloom_generate_map_semantic_digest(plan_spec->instruction, plan_spec->model_id,
			plan_spec->max_tokens, semantic_digest);
		semloom_model_reference_physical_digest(physical_digest);
		if (plan_spec->operator_kind != SEMLOOM_PLAN_OPERATOR_MAP ||
			plan_spec->input_value_kind != SEMLOOM_PLAN_VALUE_TEXT ||
			plan_spec->output_value_kind != SEMLOOM_PLAN_VALUE_TEXT ||
			plan_spec->null_policy != SEMLOOM_PLAN_NULL_PROPAGATE ||
			plan_spec->error_policy != SEMLOOM_PLAN_ERROR_FAIL_QUERY ||
			plan_spec->order_policy != SEMLOOM_PLAN_ORDER_INPUT ||
			plan_spec->semantic_spec_version != 1 ||
			strcmp(plan_spec->semantic_spec_id, SEMLOOM_MAP_SPEC_ID) != 0 ||
			strcmp(plan_spec->prompt_program_id, SEMLOOM_MAP_PROMPT_PROGRAM_ID) != 0 ||
			plan_spec->prompt_program_version != 1 ||
			strcmp(plan_spec->prompt_program_digest, SEMLOOM_MAP_PROMPT_PROGRAM_DIGEST) != 0 ||
			strcmp(plan_spec->result_parser_id, SEMLOOM_MAP_RESULT_PARSER_ID) != 0 ||
			plan_spec->result_parser_version != 1 ||
			strcmp(plan_spec->result_parser_digest, SEMLOOM_MAP_RESULT_PARSER_DIGEST) != 0 ||
			plan_spec->temperature != 0 || plan_spec->top_p != 1 || plan_spec->n != 1 ||
			plan_spec->stream || plan_spec->has_stop || plan_spec->stop != NULL ||
			plan_spec->max_input_bytes != SEMLOOM_MAP_MAX_INPUT_BYTES ||
			plan_spec->max_output_bytes != SEMLOOM_MAP_MAX_OUTPUT_BYTES ||
			strcmp(plan_spec->physical_algorithm, SEMLOOM_MODEL_REFERENCE_ALGORITHM) != 0 ||
			strcmp(plan_spec->physical_role, SEMLOOM_MODEL_REFERENCE_ROLE) != 0 ||
			strcmp(plan_spec->semantic_spec_digest, semantic_digest) != 0 ||
			strcmp(plan_spec->physical_algorithm_digest, physical_digest) != 0)
			semloom_plan_spec_invalid("unsupported generative SemMap plan specification");
	}
	else
	{
		char semantic_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];
		char physical_digest[SEMLOOM_SHA256_HEX_LENGTH + 1];

		semloom_exact_filter_semantic_digest(plan_spec->instruction,
										  plan_spec->model_id,
										  plan_spec->generation_profile_digest == NULL ? NULL :
										  &plan_spec->generation_profile,
										  semantic_digest);
		semloom_model_reference_physical_digest(physical_digest);
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
				   SEMLOOM_MODEL_REFERENCE_ALGORITHM) != 0 ||
			strcmp(plan_spec->physical_role, SEMLOOM_MODEL_REFERENCE_ROLE) != 0 ||
			strcmp(plan_spec->semantic_spec_digest, semantic_digest) != 0 ||
			strcmp(plan_spec->physical_algorithm_digest, physical_digest) != 0)
			semloom_plan_spec_invalid("unsupported exact SemFilter plan specification");
	}
}

static void
semloom_generate_map_semantic_digest(const char *instruction, const char *model_id,
									 uint32 max_tokens,
									 char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	SemloomMapPlanValues values;
	pg_cryptohash_ctx *context;
	uint8 *bytes;
	size_t length;
	size_t written;

	if (instruction == NULL || model_id == NULL)
		semloom_plan_spec_invalid("invalid generative SemMap plan specification");
	values = (SemloomMapPlanValues) {
		.instruction = {(const uint8 *) instruction, strlen(instruction), false},
		.model_id = {(const uint8 *) model_id, strlen(model_id), false},
		.max_tokens = max_tokens,
	};
	if (!semloom_map_plan_encode(&values, NULL, 0, &length))
		semloom_plan_spec_invalid("invalid generative SemMap plan specification");
	bytes = palloc(length);
	if (!semloom_map_plan_encode(&values, bytes, length, &written) || written != length)
		semloom_plan_spec_invalid("invalid generative SemMap plan specification");
	semloom_hash_begin(&context);
	semloom_hash_bytes(context, bytes, length);
	semloom_hash_finish(context, output);
	pfree(bytes);
}

static void
semloom_exact_filter_semantic_digest(
	const char *instruction,
	const char *model_id,
	const AiGenerationProfile *profile,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;
	uint8 stream = SEMLOOM_FILTER_STREAM;
	uint8 profile_bytes[SEMLOOM_GENERATION_PROFILE_CANONICAL_BYTES];
	uint32 profile_length = 0;
	const char *domain = profile == NULL ? SEMLOOM_SEMANTIC_SPEC_DIGEST_DOMAIN :
		SEMLOOM_CHOICE_SEMANTIC_SPEC_DIGEST_DOMAIN;

	if (profile != NULL && !semloom_generation_profile_encode(profile,
			profile_bytes, sizeof(profile_bytes), &profile_length))
		semloom_plan_spec_invalid("unsupported generation profile");

	semloom_hash_begin(&context);
	semloom_hash_bytes(context, domain, strlen(domain) + 1);
	semloom_hash_uint32(context, profile == NULL ? SEMLOOM_EXACT_FILTER_PLAN_SCHEMA_VERSION :
		SEMLOOM_CHOICE_FILTER_PLAN_SCHEMA_VERSION);
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
	if (profile != NULL)
	{
		semloom_hash_uint32(context, profile_length);
		semloom_hash_bytes(context, profile_bytes, profile_length);
	}
	semloom_hash_finish(context, output);
}

static void
semloom_model_reference_physical_digest(
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context, SEMLOOM_PHYSICAL_ALGORITHM_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_PHYSICAL_ALGORITHM_DIGEST_DOMAIN) - 1);
	semloom_hash_text(context, SEMLOOM_MODEL_REFERENCE_ALGORITHM);
	semloom_hash_text(context, SEMLOOM_MODEL_REFERENCE_ROLE);
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
