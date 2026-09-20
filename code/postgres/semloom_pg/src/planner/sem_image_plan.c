/* Typed image options and copyObject-safe semantic fields owned by PostgreSQL. */
#include "postgres.h"
#include "catalog/pg_type_d.h"
#include "commands/explain_format.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/jsonb.h"
#include "planner/sem_image_plan.h"
#include "semantics/image_identity.h"
#include "semantics/semantic_image_contract.h"
#include "semantics/sem_text.h"

static JsonbValue *
option(Jsonb *options, const char *name)
{
	JsonbValue key;
	JsonbValue *value;

	key.type = jbvString;
	key.val.string.val = (char *) name;
	key.val.string.len = strlen(name);
	value = findJsonbValueFromContainer(&options->root, JB_FOBJECT, &key);
	if (value == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("image option is missing: %s", name)));
	return value;
}

static const char *
text_option(Jsonb *options, const char *name, bool revision)
{
	JsonbValue *value = option(options, name);
	int index;

	if (value->type != jbvString || value->val.string.len < 1 || value->val.string.len > 128 ||
		!semloom_text_is_utf8_no_nul((const uint8 *) value->val.string.val, value->val.string.len))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid image text option: %s", name)));
	if (revision)
	{
		if (value->val.string.len != 40)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("image revision must contain 40 lowercase hexadecimal characters")));
		for (index = 0; index < 40; index++)
			if (strchr("0123456789abcdef", value->val.string.val[index]) == NULL)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid image revision")));
	}
	return pnstrdup(value->val.string.val, value->val.string.len);
}

static int
integer_option(Jsonb *options, const char *name, int maximum)
{
	JsonbValue *value = option(options, name);
	Datum number;
	int result;

	if (value->type != jbvNumeric)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("image dimension and input size must be integers")));
	number = NumericGetDatum(value->val.numeric);
	if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, number,
		DirectFunctionCall1(int4_numeric, Int32GetDatum(1)))) < 0 ||
		DatumGetInt32(DirectFunctionCall2(numeric_cmp, number,
		DirectFunctionCall1(int4_numeric, Int32GetDatum(maximum)))) > 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("image dimension or input size is out of range")));
	result = DatumGetInt32(DirectFunctionCall1(numeric_int4, number));
	if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, number,
		DirectFunctionCall1(int4_numeric, Int32GetDatum(result)))) != 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("image dimension and input size must be integers")));
	return result;
}

static void
decode_options(Jsonb *options, bool staged, SemloomPlanSpec *plan)
{
	AiOpenSpec identity = {0};
	char semantic_digest[65];
	char physical_digest[65];

	if (!JB_ROOT_IS_OBJECT(options) || JB_ROOT_COUNT(options) != 7)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("image options require exactly seven typed fields")));
	MemSet(plan, 0, sizeof(*plan));
	plan->schema_version = SEMLOOM_IMAGE_PLAN_SCHEMA_VERSION;
	plan->operator_kind = SEMLOOM_PLAN_OPERATOR_MAP;
	plan->input_value_kind = SEMLOOM_PLAN_VALUE_ENCODED_IMAGE;
	plan->output_value_kind = SEMLOOM_PLAN_VALUE_FLOAT4_VECTOR;
	plan->null_policy = SEMLOOM_PLAN_NULL_PROPAGATE;
	plan->error_policy = SEMLOOM_PLAN_ERROR_FAIL_QUERY;
	plan->order_policy = SEMLOOM_PLAN_ORDER_INPUT;
	plan->semantic_spec_version = 1;
	plan->semantic_spec_id = SEMLOOM_IMAGE_SPEC_ID;
	plan->semantic_spec_id_length = strlen(plan->semantic_spec_id);
	plan->model_id = text_option(options, "model_id", false);
	plan->model_id_length = strlen(plan->model_id);
	plan->image_model_revision = text_option(options, "model_revision", true);
	plan->image_processor_id = text_option(options, "processor_id", false);
	plan->image_processor_revision = text_option(options, "processor_revision", true);
	plan->image_dtype = text_option(options, "dtype", false);
	if (strcmp(plan->image_dtype, "float32") != 0 && strcmp(plan->image_dtype, "float16") != 0 &&
		strcmp(plan->image_dtype, "bfloat16") != 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unsupported image model dtype")));
	plan->image_dimension = integer_option(options, "dimension", SEMLOOM_IMAGE_MAX_DIMENSION);
	plan->image_input_size = integer_option(options, "input_size", 1024);
	plan->image_staged = staged;
	plan->physical_algorithm = staged ? "IMAGE_STAGED_V1" : "IMAGE_REFERENCE_SYNC_V1";
	plan->physical_algorithm_length = strlen(plan->physical_algorithm);
	plan->physical_role = staged ? "staged" : "reference";
	plan->prompt_program_id = SEMLOOM_IMAGE_PREPARE_ID;
	plan->prompt_program_version = 1;
	plan->result_parser_id = SEMLOOM_IMAGE_PARSER_ID;
	plan->result_parser_version = 1;
	plan->max_input_bytes = SEMLOOM_IMAGE_MAX_INPUT_BYTES;
	plan->max_output_bytes = plan->image_dimension * 4;
#define SLICE(field, text) identity.field = (AiByteSlice) {(const uint8 *) (text), strlen(text)}
	SLICE(model_id, plan->model_id);
	SLICE(image_model_revision, plan->image_model_revision);
	SLICE(image_processor_id, plan->image_processor_id);
	SLICE(image_processor_revision, plan->image_processor_revision);
	SLICE(image_dtype, plan->image_dtype);
#undef SLICE
	identity.image_dimension = plan->image_dimension;
	identity.image_input_size = plan->image_input_size;
	semloom_image_spec_digest(&identity, semantic_digest);
	semloom_image_physical_digest(staged, physical_digest);
	plan->semantic_spec_digest = pstrdup(semantic_digest);
	plan->physical_algorithm_digest = pstrdup(physical_digest);
}

List *
semloom_image_plan_fields(FuncExpr *marker, bool staged)
{
	Const *options;
	Jsonb *value;
	SemloomPlanSpec plan;

	if (list_length(marker->args) != 2 || !IsA(lsecond(marker->args), Const))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("image options must be plan-time constants")));
	options = lsecond_node(Const, marker->args);
	if (options->consttype != JSONBOID || options->constisnull)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("image options must be non-NULL jsonb")));
	value = DatumGetJsonbP(options->constvalue);
	decode_options(value, staged, &plan);
	return list_make5(
		list_make2(makeString(pstrdup("schema_version")), makeInteger(SEMLOOM_IMAGE_PLAN_SCHEMA_VERSION)),
		list_make2(makeString(pstrdup("image_options")), makeString(JsonbToCString(NULL, &value->root, VARSIZE(value)))),
		list_make2(makeString(pstrdup("staged")), makeInteger(staged ? 1 : 0)),
		list_make2(makeString(pstrdup("semantic_spec_digest")), makeString(pstrdup(plan.semantic_spec_digest))),
		list_make2(makeString(pstrdup("physical_algorithm_digest")), makeString(pstrdup(plan.physical_algorithm_digest))));
}

bool
semloom_image_plan_is_fields(List *fields)
{
	List *first;

	if (fields == NIL || !IsA(fields, List) || list_length(fields) != 5 ||
		linitial(fields) == NULL || !IsA(linitial(fields), List))
		return false;
	first = linitial(fields);
	return list_length(first) == 2 && linitial(first) != NULL && lsecond(first) != NULL &&
		IsA(linitial(first), String) &&
		strcmp(strVal(linitial(first)), "schema_version") == 0 &&
		IsA(lsecond(first), Integer) && intVal(lsecond(first)) == SEMLOOM_IMAGE_PLAN_SCHEMA_VERSION;
}

void
semloom_image_plan_decode(List *fields, MemoryContext owner, SemloomPlanSpec *plan)
{
	const char *names[] = {"schema_version", "image_options", "staged", "semantic_spec_digest", "physical_algorithm_digest"};
	Node *values[5];
	ListCell *cell;
	int index = 0;
	MemoryContext previous;
	Jsonb *options;

	if (list_length(fields) != 5)
		elog(ERROR, "invalid image plan field count");
	foreach(cell, fields)
	{
		List *field = lfirst(cell);

		if (field == NIL || !IsA(field, List) || list_length(field) != 2 ||
			linitial(field) == NULL || lsecond(field) == NULL ||
			!IsA(linitial(field), String) || strcmp(strVal(linitial(field)), names[index]) != 0)
			elog(ERROR, "invalid image plan field");
		values[index++] = lsecond(field);
	}
	if (!IsA(values[0], Integer) || intVal(values[0]) != SEMLOOM_IMAGE_PLAN_SCHEMA_VERSION ||
		!IsA(values[1], String) || strlen(strVal(values[1])) > 4096 ||
		!IsA(values[2], Integer) || (intVal(values[2]) != 0 && intVal(values[2]) != 1) ||
		!IsA(values[3], String) || !IsA(values[4], String))
		elog(ERROR, "invalid image plan values");
	previous = MemoryContextSwitchTo(owner);
	PG_TRY();
	{
		options = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(strVal(values[1]))));
		decode_options(options, intVal(values[2]) == 1, plan);
		MemoryContextSwitchTo(previous);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(previous);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (strcmp(plan->semantic_spec_digest, strVal(values[3])) != 0 ||
		strcmp(plan->physical_algorithm_digest, strVal(values[4])) != 0)
		elog(ERROR, "image plan identity mismatch");
}

void
semloom_image_plan_explain(const SemloomPlanSpec *plan, ExplainState *state)
{
	ExplainPropertyText("Semantic Spec", plan->semantic_spec_id, state);
	ExplainPropertyInteger("Semantic Plan Schema", NULL, plan->schema_version, state);
	ExplainPropertyText("Semantic Spec Digest", plan->semantic_spec_digest, state);
	ExplainPropertyText("Physical Algorithm", plan->physical_algorithm, state);
	ExplainPropertyText("Physical Algorithm Digest", plan->physical_algorithm_digest, state);
	ExplainPropertyText("Model", plan->model_id, state);
	ExplainPropertyText("Model Revision", plan->image_model_revision, state);
	ExplainPropertyText("Processor", plan->image_processor_id, state);
	ExplainPropertyText("Processor Revision", plan->image_processor_revision, state);
	ExplainPropertyText("Model Dtype", plan->image_dtype, state);
	ExplainPropertyText("Image Input", "single-frame JPEG/PNG encoded bytea", state);
	ExplainPropertyInteger("Image Input Size", "pixels", plan->image_input_size, state);
	ExplainPropertyInteger("Image Output Dimension", NULL, plan->image_dimension, state);
	ExplainPropertyInteger("Max Input Bytes", NULL, plan->max_input_bytes, state);
	ExplainPropertyInteger("Max Output Bytes", NULL, plan->max_output_bytes, state);
	ExplainPropertyText("Image Output", "projected L2-normalized finite float4[]", state);
}
