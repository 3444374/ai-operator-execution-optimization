/* Analyze supported Filter occurrences before constructing physical paths. */
#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/jsonb.h"

#include "planner/sem_filter_call.h"
#include "planner/sem_path_common.h"
#include "semantics/semantic_filter_contract.h"

static void semloom_exact_filter_arguments(FuncExpr *marker, char **instruction,
										 char **model_id, bool *choice_profile);
static bool semloom_json_key_equals(const JsonbValue *key, const char *expected);
static bool semloom_numeric_text_is_zero(const char *value);
pg_noreturn static void semloom_invalid_exact_filter_argument(const char *message);

List *
semloom_filter_calls(PlannerInfo *root, RelOptInfo *rel,
								Oid recording_oid,
								Oid exact_oid)
{
	List *calls = NIL;
	ListCell *cell;

	foreach(cell, rel->baserestrictinfo)
	{
		RestrictInfo *restriction = lfirst_node(RestrictInfo, cell);
		int count = semloom_filter_marker_count((Node *) restriction->clause,
										 recording_oid,
										 exact_oid);

		if (count == 0)
			continue;
		if (count != 1 || !IsA(restriction->clause, FuncExpr) ||
			!semloom_is_filter_marker(((FuncExpr *) restriction->clause)->funcid,
									 recording_oid,
									 exact_oid))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ai_semantic.filter must be one top-level AND predicate")));
		{
			FuncExpr *marker = (FuncExpr *) restriction->clause;
			SemloomFilterCall *call = palloc0(sizeof(*call));

			call->restriction = restriction;
			call->is_exact = marker->funcid == exact_oid;
			if (marker->funcid == recording_oid &&
				(list_length(marker->args) != 1 ||
				 exprType(linitial(marker->args)) != TEXTOID ||
				 marker->funcresulttype != BOOLOID))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("ai_semantic.filter capability requires one text input and boolean output")));
			if (marker->funcid == exact_oid &&
				(list_length(marker->args) != 3 ||
				 exprType(linitial(marker->args)) != TEXTOID ||
				 exprType(lsecond(marker->args)) != TEXTOID ||
				 exprType(lthird(marker->args)) != JSONBOID ||
				 marker->funcresulttype != BOOLOID))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("exact ai_semantic.filter requires text, text, jsonb and boolean output")));
			if (call->is_exact)
				semloom_exact_filter_arguments(marker, &call->instruction,
											   &call->model_id, &call->choice_profile);
			call->semantic = semloom_call_create(root->query_level, SEMLOOM_CALL_BASE_FILTER,
				list_length(calls), foreach_current_index(cell) + 1, marker);
			calls = lappend(calls, call);
		}
	}
	if (calls == NIL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("ai_semantic.filter must be a base-relation predicate")));
	if (list_length(calls) > SEMLOOM_MAX_FILTER_CALLS)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("SemFilter supports at most two top-level AND predicates")));
	return calls;
}

int
semloom_filter_marker_count(Node *node, Oid recording_oid, Oid exact_oid)
{
	int count = 0;

	if (OidIsValid(recording_oid))
		count += semloom_marker_count(node, recording_oid);
	if (OidIsValid(exact_oid) && exact_oid != recording_oid)
		count += semloom_marker_count(node, exact_oid);
	return count;
}

bool
semloom_is_filter_marker(Oid function_oid, Oid recording_oid, Oid exact_oid)
{
	return (OidIsValid(recording_oid) && function_oid == recording_oid) ||
		(OidIsValid(exact_oid) && function_oid == exact_oid);
}

static void
semloom_exact_filter_arguments(FuncExpr *marker,
								 char **instruction,
								 char **model_id,
								 bool *choice_profile)
{
	Node *instruction_node = lsecond(marker->args);
	Node *options_node = lthird(marker->args);
	Const *instruction_const;
	Const *options_const;
	text *instruction_text;
	Size instruction_length;
	Jsonb *options;
	JsonbIterator *iterator;
	JsonbValue value;
	JsonbIteratorToken token;
	bool seen_model = false;
	bool seen_temperature = false;
	bool seen_max_tokens = false;
	JsonbValue profile_key;
	JsonbValue *profile_value;

	*choice_profile = false;

	if (!IsA(instruction_node, Const) || ((Const *) instruction_node)->constisnull)
		semloom_invalid_exact_filter_argument(
			"SemFilter instruction must be a non-NULL plan-time constant");
	if (!IsA(options_node, Const) || ((Const *) options_node)->constisnull)
		semloom_invalid_exact_filter_argument(
			"SemFilter options must be a non-NULL plan-time constant");
	instruction_const = (Const *) instruction_node;
	options_const = (Const *) options_node;
	if (instruction_const->consttype != TEXTOID || options_const->consttype != JSONBOID)
		semloom_invalid_exact_filter_argument(
			"SemFilter instruction and options have invalid types");

	instruction_text = DatumGetTextPP(instruction_const->constvalue);
	instruction_length = VARSIZE_ANY_EXHDR(instruction_text);
	if (instruction_length == 0 ||
		instruction_length > SEMLOOM_FILTER_INSTRUCTION_MAX_BYTES ||
		memchr(VARDATA_ANY(instruction_text), '\0', instruction_length) != NULL)
		semloom_invalid_exact_filter_argument(
			"SemFilter instruction must contain 1 to 4096 UTF8 bytes");
	*instruction = pnstrdup(VARDATA_ANY(instruction_text), instruction_length);

	options = DatumGetJsonbP(options_const->constvalue);
	profile_key.type = jbvString;
	profile_key.val.string.val = "generation_profile";
	profile_key.val.string.len = strlen(profile_key.val.string.val);
	profile_value = JB_ROOT_IS_OBJECT(options) ?
		findJsonbValueFromContainer(&options->root, JB_FOBJECT, &profile_key) : NULL;
	/* No selector: retain the old count/error boundary, including extra keys. */
	if (!JB_ROOT_IS_OBJECT(options) ||
		JB_ROOT_COUNT(options) != (profile_value == NULL ? 3 : 4))
		semloom_invalid_exact_filter_argument(
			"SemFilter options must contain exactly model, temperature, and max_tokens");
	if (profile_value != NULL)
	{
		if (!semloom_json_key_equals(profile_value, SEMLOOM_CHOICE_FILTER_PROFILE_SELECTOR))
			semloom_invalid_exact_filter_argument("unsupported SemFilter generation_profile");
		*choice_profile = true;
	}
	iterator = JsonbIteratorInit(&options->root);
	while ((token = JsonbIteratorNext(&iterator, &value, true)) != WJB_DONE)
	{
		JsonbValue option_value;

		if (token != WJB_KEY)
			continue;
		if (JsonbIteratorNext(&iterator, &option_value, true) != WJB_VALUE)
			semloom_invalid_exact_filter_argument("invalid SemFilter option value");
		if (semloom_json_key_equals(&value, "model"))
		{
			if (seen_model || option_value.type != jbvString ||
				option_value.val.string.len <= 0 ||
				option_value.val.string.len > SEMLOOM_FILTER_MODEL_MAX_BYTES ||
				memchr(option_value.val.string.val,
					   '\0',
					   option_value.val.string.len) != NULL)
				semloom_invalid_exact_filter_argument(
					"SemFilter model must contain 1 to 128 UTF8 bytes");
			*model_id = pnstrdup(option_value.val.string.val,
								 option_value.val.string.len);
			seen_model = true;
		}
		else if (semloom_json_key_equals(&value, "temperature"))
		{
			char *numeric_text;

			if (seen_temperature || option_value.type != jbvNumeric)
				semloom_invalid_exact_filter_argument(
					"SemFilter temperature must be numeric zero");
			numeric_text = DatumGetCString(DirectFunctionCall1(
				numeric_out,
				NumericGetDatum(option_value.val.numeric)));
			if (!semloom_numeric_text_is_zero(numeric_text))
				semloom_invalid_exact_filter_argument(
					"SemFilter temperature must be numeric zero");
			pfree(numeric_text);
			seen_temperature = true;
		}
		else if (semloom_json_key_equals(&value, "max_tokens"))
		{
			char *numeric_text;

			if (seen_max_tokens || option_value.type != jbvNumeric)
				semloom_invalid_exact_filter_argument(
					"SemFilter max_tokens must be integer 8");
			numeric_text = DatumGetCString(DirectFunctionCall1(
				numeric_out,
				NumericGetDatum(option_value.val.numeric)));
			if (strcmp(numeric_text, "8") != 0)
				semloom_invalid_exact_filter_argument(
					"SemFilter max_tokens must be integer 8");
			pfree(numeric_text);
			seen_max_tokens = true;
		}
		else if (semloom_json_key_equals(&value, "generation_profile"))
		{
			/* Its type and exact supported selector were checked above. */
			Assert(*choice_profile);
		}
		else
			semloom_invalid_exact_filter_argument(
				"SemFilter options must contain exactly model, temperature, and max_tokens");
	}
	if (!seen_model || !seen_temperature || !seen_max_tokens)
		semloom_invalid_exact_filter_argument(
			"SemFilter options must contain exactly model, temperature, and max_tokens");
}

static bool
semloom_json_key_equals(const JsonbValue *key, const char *expected)
{
	Size expected_length = strlen(expected);

	return key->type == jbvString &&
		key->val.string.len == expected_length &&
		memcmp(key->val.string.val, expected, expected_length) == 0;
}

static bool
semloom_numeric_text_is_zero(const char *value)
{
	const char *cursor = value;
	bool saw_zero = false;

	if (*cursor == '-')
		cursor++;
	for (; *cursor != '\0'; cursor++)
	{
		if (*cursor == '0')
			saw_zero = true;
		else if (*cursor != '.')
			return false;
	}
	return saw_zero;
}

static void
semloom_invalid_exact_filter_argument(const char *message)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("%s", message)));
	pg_unreachable();
}
