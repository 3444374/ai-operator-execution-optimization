/* Strict planner-side loader for matched-reference SemFilter artifacts. */
#include "postgres.h"

#include <math.h>
#include <sys/stat.h>

#include "common/cryptohash.h"
#include "common/file_perm.h"
#include "common/sha2.h"
#include "nodes/pg_list.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/float.h"
#include "utils/json.h"
#include "utils/jsonb.h"

#include "semantic_filter_contract.h"
#include "sem_filter_calibration.h"
#include "semloom_pg.h"

#define SEMLOOM_CALIBRATION_SCHEMA_VERSION 1
#define SEMLOOM_CALIBRATION_FIELD_COUNT 29
#define SEMLOOM_CALIBRATION_MAX_BYTES 65536
#define SEMLOOM_CALIBRATION_COST_MODEL_ID \
	"semloom.exact_filter.reference-calibrated.v1"
#define SEMLOOM_CALIBRATION_PROFILE "openai-compatible-fixed"
#define SEMLOOM_CALIBRATION_ID_DOMAIN \
	"semloom-semfilter-reference-calibration-v1\0"

#define FIELD_SCHEMA_VERSION "schema_version"
#define FIELD_ARTIFACT_ID "artifact_id"
#define FIELD_COST_MODEL_ID "cost_model_id"
#define FIELD_GENERATED_AT "generated_at"
#define FIELD_SEMANTIC_DIGEST "semantic_spec_digest"
#define FIELD_PHYSICAL_DIGEST "physical_algorithm_digest"
#define FIELD_PROVIDER_PROFILE "provider_execution_profile"
#define FIELD_MODEL_ID "model_id"
#define FIELD_MODEL_ROLE "model_role"
#define FIELD_WORKLOAD_SIGNATURE "workload_signature"
#define FIELD_SERVICE_SIGNATURE "service_signature"
#define FIELD_TRAINING_SAMPLES "training_sample_count"
#define FIELD_HELD_OUT_SAMPLES "held_out_sample_count"
#define FIELD_TRAINING_ROWS "training_semantic_input_rows"
#define FIELD_HELD_OUT_ROWS "held_out_semantic_input_rows"
#define FIELD_OUTPUT_SELECTIVITY "output_selectivity"
#define FIELD_CALLS_PER_INPUT "model_calls_per_input_row"
#define FIELD_PROMPT_PER_CALL "prompt_tokens_per_call"
#define FIELD_OUTPUT_PER_CALL "output_tokens_per_call"
#define FIELD_SERVICE_FIXED_MS "service_fixed_milliseconds"
#define FIELD_SERVICE_PER_CALL "service_ms_per_model_call"
#define FIELD_SERVICE_PER_PROMPT "service_ms_per_prompt_token"
#define FIELD_SERVICE_PER_OUTPUT "service_ms_per_output_token"
#define FIELD_HELD_OUT_MEAN_ERROR "held_out_mean_relative_error"
#define FIELD_HELD_OUT_MAX_ERROR "held_out_max_relative_error"
#define FIELD_HELD_OUT_ERROR_LOWER "held_out_signed_error_lower"
#define FIELD_HELD_OUT_ERROR_UPPER "held_out_signed_error_upper"
#define FIELD_ACCEPTED_MAX_ERROR "accepted_max_relative_error"
#define FIELD_EVIDENCE_DIGEST "evidence_digest"

typedef struct SemloomParsedCalibration
{
	int schema_version;
	char *artifact_id;
	char *cost_model_id;
	char *generated_at;
	char *semantic_digest;
	char *physical_digest;
	char *provider_profile;
	char *model_id;
	char *model_role;
	char *workload_signature;
	char *service_signature;
	int training_samples;
	int held_out_samples;
	char *training_rows_text;
	char *held_out_rows_text;
	char *output_selectivity_text;
	char *calls_per_input_text;
	char *prompt_per_call_text;
	char *output_per_call_text;
	char *service_fixed_ms_text;
	char *service_per_call_text;
	char *service_per_prompt_text;
	char *service_per_output_text;
	char *held_out_mean_error_text;
	char *held_out_max_error_text;
	char *held_out_error_lower_text;
	char *held_out_error_upper_text;
	char *accepted_max_error_text;
	char *evidence_digest;
} SemloomParsedCalibration;

static bool semloom_calibration_read_file(const char *path, char **contents);
static bool semloom_calibration_parse(const char *contents,
									 SemloomParsedCalibration *parsed);
static bool semloom_calibration_parse_jsonb(const char *contents,
										 Jsonb **jsonb);
static bool semloom_calibration_validate(
	const SemloomParsedCalibration *parsed,
	const SemloomPlanSpec *plan_spec,
	const char *provider_execution_profile,
	SemloomFilterCalibration *calibration);
static char *semloom_calibration_json_string(const JsonbValue *value);
static bool semloom_calibration_json_integer(const JsonbValue *value, int *result);
static bool semloom_calibration_decimal(const char *text,
									 double minimum,
									 double maximum,
									 bool positive,
									 double *result);
static bool semloom_calibration_sha256(const char *value);
static bool semloom_calibration_timestamp(const char *value);
static bool semloom_calibration_identity(
	const SemloomParsedCalibration *parsed,
	char output[PG_SHA256_DIGEST_LENGTH * 2 + 1]);
static bool semloom_calibration_hash_text(pg_cryptohash_ctx *context,
										 const char *value);
static const char *semloom_calibration_rejection_reason(
	const SemloomParsedCalibration *parsed,
	const SemloomPlanSpec *plan_spec,
	const char *provider_execution_profile);

bool
semloom_filter_calibration_load(
	const SemloomPlanSpec *plan_spec,
	const char *provider_execution_profile,
	SemloomFilterCalibration *calibration)
{
	const char *path = semloom_reference_calibration_path();
	SemloomParsedCalibration parsed;
	char *contents = NULL;

	Assert(plan_spec != NULL);
	Assert(provider_execution_profile != NULL);
	Assert(calibration != NULL);
	MemSet(calibration, 0, sizeof(*calibration));
	calibration->status = SEMLOOM_FILTER_CALIBRATION_UNAVAILABLE;
	calibration->reason = "not-configured";
	calibration->artifact_id = "";
	calibration->workload_signature = "";
	calibration->service_signature = "";
	if (path == NULL || path[0] == '\0')
		return false;

	calibration->status = SEMLOOM_FILTER_CALIBRATION_REJECTED;
	calibration->reason = "unreadable-artifact";
	if (!is_absolute_path(path) || !semloom_calibration_read_file(path, &contents))
		return false;
	calibration->reason = "invalid-artifact";
	if (!semloom_calibration_parse(contents, &parsed))
		return false;
	return semloom_calibration_validate(
		&parsed, plan_spec, provider_execution_profile, calibration);
}

void
semloom_filter_calibration_apply(
	const SemloomFilterCalibration *calibration,
	SemloomFilterCostEstimate *estimate)
{
	Assert(calibration != NULL);
	Assert(estimate != NULL);
	estimate->calibration_status =
		calibration->status == SEMLOOM_FILTER_CALIBRATION_MATCHED ?
		"matched" :
		(calibration->status == SEMLOOM_FILTER_CALIBRATION_REJECTED ?
		 "rejected" : "unavailable");
	estimate->calibration_reason = calibration->reason;
	estimate->calibration_id = calibration->artifact_id;
	estimate->workload_signature = calibration->workload_signature;
	estimate->service_signature = calibration->service_signature;
	if (calibration->status != SEMLOOM_FILTER_CALIBRATION_MATCHED)
		return;

	estimate->cost_model_id = SEMLOOM_CALIBRATION_COST_MODEL_ID;
	estimate->output_selectivity = calibration->output_selectivity;
	estimate->estimated_model_calls = estimate->semantic_input_rows *
		calibration->model_calls_per_input_row;
	estimate->estimated_prompt_tokens = estimate->estimated_model_calls *
		calibration->prompt_tokens_per_call;
	estimate->estimated_output_tokens = estimate->estimated_model_calls *
		calibration->output_tokens_per_call;
	estimate->estimated_service_milliseconds =
		estimate->estimated_model_calls > 0 ?
		calibration->service_fixed_milliseconds +
		estimate->estimated_model_calls *
			calibration->service_ms_per_model_call +
		estimate->estimated_prompt_tokens *
			calibration->service_ms_per_prompt_token +
		estimate->estimated_output_tokens *
			calibration->service_ms_per_output_token : 0;
	/* One planner cost unit equals one predicted service millisecond. */
	estimate->ai_work_cost = estimate->estimated_service_milliseconds;
	estimate->held_out_max_relative_error =
		calibration->held_out_max_relative_error;
	estimate->accepted_max_relative_error =
		calibration->accepted_max_relative_error;
}

static bool
semloom_calibration_read_file(const char *path, char **contents)
{
	FILE *file;
	struct stat metadata;
	Size bytes_read;

	file = AllocateFile(path, PG_BINARY_R);
	if (file == NULL)
		return false;
	if (fstat(fileno(file), &metadata) != 0 ||
		metadata.st_size <= 0 ||
		metadata.st_size > SEMLOOM_CALIBRATION_MAX_BYTES)
	{
		FreeFile(file);
		return false;
	}
	*contents = palloc((Size) metadata.st_size + 1);
	bytes_read = fread(*contents, 1, (Size) metadata.st_size, file);
	if (bytes_read != (Size) metadata.st_size || ferror(file))
	{
		FreeFile(file);
		return false;
	}
	(*contents)[bytes_read] = '\0';
	if (memchr(*contents, '\0', bytes_read) != NULL)
	{
		FreeFile(file);
		return false;
	}
	FreeFile(file);
	return true;
}

static bool
semloom_calibration_parse(const char *contents,
						  SemloomParsedCalibration *parsed)
{
	Jsonb *jsonb;
	JsonbIterator *iterator;
	JsonbValue key;
	JsonbIteratorToken token;
	uint64 seen = 0;
	int field_count = 0;

	MemSet(parsed, 0, sizeof(*parsed));
	if (!semloom_calibration_parse_jsonb(contents, &jsonb))
		return false;
	if (!JB_ROOT_IS_OBJECT(jsonb) ||
		JB_ROOT_COUNT(jsonb) != SEMLOOM_CALIBRATION_FIELD_COUNT)
		return false;
	iterator = JsonbIteratorInit(&jsonb->root);
	if (JsonbIteratorNext(&iterator, &key, true) != WJB_BEGIN_OBJECT)
		return false;
	while ((token = JsonbIteratorNext(&iterator, &key, true)) != WJB_END_OBJECT)
	{
		JsonbValue value;
		char *name;
		uint64 bit;

		if (token == WJB_DONE || token != WJB_KEY ||
			JsonbIteratorNext(&iterator, &value, true) != WJB_VALUE)
			return false;
		name = semloom_calibration_json_string(&key);
		if (name == NULL)
			return false;
#define STRING_FIELD(field_name, field_bit, target) \
		if (strcmp(name, (field_name)) == 0) \
		{ \
			bit = UINT64CONST(1) << (field_bit); \
			(target) = semloom_calibration_json_string(&value); \
			if ((target) == NULL) \
				return false; \
		}
		STRING_FIELD(FIELD_ARTIFACT_ID, 1, parsed->artifact_id)
		else STRING_FIELD(FIELD_COST_MODEL_ID, 2, parsed->cost_model_id)
		else STRING_FIELD(FIELD_GENERATED_AT, 3, parsed->generated_at)
		else STRING_FIELD(FIELD_SEMANTIC_DIGEST, 4, parsed->semantic_digest)
		else STRING_FIELD(FIELD_PHYSICAL_DIGEST, 5, parsed->physical_digest)
		else STRING_FIELD(FIELD_PROVIDER_PROFILE, 6, parsed->provider_profile)
		else STRING_FIELD(FIELD_MODEL_ID, 7, parsed->model_id)
		else STRING_FIELD(FIELD_MODEL_ROLE, 8, parsed->model_role)
		else STRING_FIELD(FIELD_WORKLOAD_SIGNATURE, 9, parsed->workload_signature)
		else STRING_FIELD(FIELD_SERVICE_SIGNATURE, 10, parsed->service_signature)
		else STRING_FIELD(FIELD_TRAINING_ROWS, 13, parsed->training_rows_text)
		else STRING_FIELD(FIELD_HELD_OUT_ROWS, 14, parsed->held_out_rows_text)
		else STRING_FIELD(FIELD_OUTPUT_SELECTIVITY, 15, parsed->output_selectivity_text)
		else STRING_FIELD(FIELD_CALLS_PER_INPUT, 16, parsed->calls_per_input_text)
		else STRING_FIELD(FIELD_PROMPT_PER_CALL, 17, parsed->prompt_per_call_text)
		else STRING_FIELD(FIELD_OUTPUT_PER_CALL, 18, parsed->output_per_call_text)
		else STRING_FIELD(FIELD_SERVICE_FIXED_MS, 19, parsed->service_fixed_ms_text)
		else STRING_FIELD(FIELD_SERVICE_PER_CALL, 20, parsed->service_per_call_text)
		else STRING_FIELD(FIELD_SERVICE_PER_PROMPT, 21, parsed->service_per_prompt_text)
		else STRING_FIELD(FIELD_SERVICE_PER_OUTPUT, 22, parsed->service_per_output_text)
		else STRING_FIELD(FIELD_HELD_OUT_MEAN_ERROR, 23, parsed->held_out_mean_error_text)
		else STRING_FIELD(FIELD_HELD_OUT_MAX_ERROR, 24, parsed->held_out_max_error_text)
		else STRING_FIELD(FIELD_HELD_OUT_ERROR_LOWER, 25, parsed->held_out_error_lower_text)
		else STRING_FIELD(FIELD_HELD_OUT_ERROR_UPPER, 26, parsed->held_out_error_upper_text)
		else STRING_FIELD(FIELD_ACCEPTED_MAX_ERROR, 27, parsed->accepted_max_error_text)
		else STRING_FIELD(FIELD_EVIDENCE_DIGEST, 28, parsed->evidence_digest)
#undef STRING_FIELD
		else if (strcmp(name, FIELD_SCHEMA_VERSION) == 0)
		{
			bit = UINT64CONST(1) << 0;
			if (!semloom_calibration_json_integer(&value, &parsed->schema_version))
				return false;
		}
		else if (strcmp(name, FIELD_TRAINING_SAMPLES) == 0)
		{
			bit = UINT64CONST(1) << 11;
			if (!semloom_calibration_json_integer(&value, &parsed->training_samples))
				return false;
		}
		else if (strcmp(name, FIELD_HELD_OUT_SAMPLES) == 0)
		{
			bit = UINT64CONST(1) << 12;
			if (!semloom_calibration_json_integer(&value, &parsed->held_out_samples))
				return false;
		}
		else
			return false;
		if ((seen & bit) != 0)
			return false;
		seen |= bit;
		field_count++;
	}
	return JsonbIteratorNext(&iterator, &key, true) == WJB_DONE &&
		seen == ((UINT64CONST(1) << SEMLOOM_CALIBRATION_FIELD_COUNT) - 1) &&
		field_count == SEMLOOM_CALIBRATION_FIELD_COUNT;
}

/*
 * Artifact bytes are external planner input.  Convert only the expected JSON
 * and encoding failures into a redacted invalid-artifact result; interrupts,
 * OOM, and PostgreSQL internal errors must retain their original control flow.
 */
static bool
semloom_calibration_parse_jsonb(const char *contents, Jsonb **jsonb)
{
	MemoryContext caller_context = CurrentMemoryContext;
	text *json_text = cstring_to_text(contents);
	bool valid = false;

	PG_TRY();
	{
		if (json_validate(json_text, true, false))
		{
			*jsonb = DatumGetJsonbP(DirectFunctionCall1(
				jsonb_in, CStringGetDatum(contents)));
			valid = true;
		}
	}
	PG_CATCH();
	{
		ErrorData *error_data;

		MemoryContextSwitchTo(caller_context);
		error_data = CopyErrorData();
		FlushErrorState();
		if (error_data->sqlerrcode == ERRCODE_INVALID_TEXT_REPRESENTATION ||
			error_data->sqlerrcode == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE ||
			error_data->sqlerrcode == ERRCODE_UNTRANSLATABLE_CHARACTER)
		{
			FreeErrorData(error_data);
			return false;
		}
		ReThrowError(error_data);
	}
	PG_END_TRY();

	return valid;
}

static bool
semloom_calibration_validate(
	const SemloomParsedCalibration *parsed,
	const SemloomPlanSpec *plan_spec,
	const char *provider_execution_profile,
	SemloomFilterCalibration *calibration)
{
	char identity[PG_SHA256_DIGEST_LENGTH * 2 + 1];
	double training_rows;
	double held_out_rows;
	double mean_error;
	double lower_error;
	double upper_error;
	const char *mismatch;

	if (parsed->schema_version != SEMLOOM_CALIBRATION_SCHEMA_VERSION ||
		parsed->training_samples <= 0 || parsed->held_out_samples <= 0 ||
		strcmp(parsed->cost_model_id, SEMLOOM_CALIBRATION_COST_MODEL_ID) != 0 ||
		strcmp(parsed->provider_profile, SEMLOOM_CALIBRATION_PROFILE) != 0 ||
		strcmp(parsed->model_role, SEMLOOM_MODEL_REFERENCE_ROLE) != 0 ||
		!semloom_calibration_timestamp(parsed->generated_at) ||
		!semloom_calibration_sha256(parsed->artifact_id) ||
		!semloom_calibration_sha256(parsed->semantic_digest) ||
		!semloom_calibration_sha256(parsed->physical_digest) ||
		!semloom_calibration_sha256(parsed->workload_signature) ||
		!semloom_calibration_sha256(parsed->service_signature) ||
		!semloom_calibration_sha256(parsed->evidence_digest) ||
		strlen(parsed->model_id) == 0 ||
		strlen(parsed->model_id) > SEMLOOM_FILTER_MODEL_MAX_BYTES ||
		!semloom_calibration_decimal(parsed->training_rows_text, 0, HUGE_VAL,
										true, &training_rows) ||
		!semloom_calibration_decimal(parsed->held_out_rows_text, 0, HUGE_VAL,
										true, &held_out_rows) ||
		!semloom_calibration_decimal(parsed->output_selectivity_text, 0, 1,
										false, &calibration->output_selectivity) ||
		!semloom_calibration_decimal(parsed->calls_per_input_text, 0, 1,
										false, &calibration->model_calls_per_input_row) ||
		!semloom_calibration_decimal(parsed->prompt_per_call_text, 0, HUGE_VAL,
										false, &calibration->prompt_tokens_per_call) ||
		!semloom_calibration_decimal(parsed->output_per_call_text, 0, HUGE_VAL,
										false, &calibration->output_tokens_per_call) ||
		!semloom_calibration_decimal(parsed->service_fixed_ms_text, 0, HUGE_VAL,
										false, &calibration->service_fixed_milliseconds) ||
		!semloom_calibration_decimal(parsed->service_per_call_text, 0, HUGE_VAL,
										false, &calibration->service_ms_per_model_call) ||
		!semloom_calibration_decimal(parsed->service_per_prompt_text, 0, HUGE_VAL,
										false, &calibration->service_ms_per_prompt_token) ||
		!semloom_calibration_decimal(parsed->service_per_output_text, 0, HUGE_VAL,
										false, &calibration->service_ms_per_output_token) ||
		calibration->service_ms_per_model_call +
			calibration->service_ms_per_prompt_token +
			calibration->service_ms_per_output_token <= 0 ||
		!semloom_calibration_decimal(parsed->held_out_mean_error_text, 0, HUGE_VAL,
										false, &mean_error) ||
		!semloom_calibration_decimal(parsed->held_out_max_error_text, 0, HUGE_VAL,
										false, &calibration->held_out_max_relative_error) ||
		!semloom_calibration_decimal(parsed->held_out_error_lower_text, -HUGE_VAL,
										HUGE_VAL, false, &lower_error) ||
		!semloom_calibration_decimal(parsed->held_out_error_upper_text, -HUGE_VAL,
										HUGE_VAL, false, &upper_error) ||
		!semloom_calibration_decimal(parsed->accepted_max_error_text, 0, 1,
										false, &calibration->accepted_max_relative_error) ||
		mean_error > calibration->held_out_max_relative_error ||
		lower_error > upper_error ||
		calibration->held_out_max_relative_error >
			calibration->accepted_max_relative_error ||
		!semloom_calibration_identity(parsed, identity) ||
		strcmp(parsed->artifact_id, identity) != 0)
		return false;

	mismatch = semloom_calibration_rejection_reason(
		parsed, plan_spec, provider_execution_profile);
	if (mismatch != NULL)
	{
		calibration->reason = mismatch;
		return false;
	}
	calibration->status = SEMLOOM_FILTER_CALIBRATION_MATCHED;
	calibration->reason = "matched";
	calibration->artifact_id = parsed->artifact_id;
	calibration->workload_signature = parsed->workload_signature;
	calibration->service_signature = parsed->service_signature;
	return true;
}

static char *
semloom_calibration_json_string(const JsonbValue *value)
{
	if (value->type != jbvString || value->val.string.len < 0)
		return NULL;
	return pnstrdup(value->val.string.val, value->val.string.len);
}

static bool
semloom_calibration_json_integer(const JsonbValue *value, int *result)
{
	char *text_value;
	char *end = NULL;
	long parsed;

	if (value->type != jbvNumeric)
		return false;
	text_value = DatumGetCString(DirectFunctionCall1(
		numeric_out, NumericGetDatum(value->val.numeric)));
	errno = 0;
	parsed = strtol(text_value, &end, 10);
	if (errno != 0 || end == text_value || *end != '\0' ||
		parsed <= 0 || parsed > PG_INT32_MAX)
		return false;
	*result = (int) parsed;
	return true;
}

static bool
semloom_calibration_decimal(const char *text,
							 double minimum,
							 double maximum,
							 bool positive,
							 double *result)
{
	char *end = NULL;
	char *canonical;
	double parsed;

	errno = 0;
	parsed = strtod(text, &end);
	if (errno != 0 || end == text || *end != '\0' || !isfinite(parsed) ||
		parsed < minimum || parsed > maximum || (positive && parsed <= 0))
		return false;
	canonical = float8out_internal(parsed);
	if (strcmp(text, canonical) != 0)
		return false;
	*result = parsed;
	return true;
}

static bool
semloom_calibration_sha256(const char *value)
{
	int index;

	if (value == NULL || strlen(value) != PG_SHA256_DIGEST_LENGTH * 2)
		return false;
	for (index = 0; index < PG_SHA256_DIGEST_LENGTH * 2; index++)
		if (!((value[index] >= '0' && value[index] <= '9') ||
			  (value[index] >= 'a' && value[index] <= 'f')))
			return false;
	return true;
}

static bool
semloom_calibration_timestamp(const char *value)
{
	int index;

	if (value == NULL || strlen(value) != 20 ||
		value[4] != '-' || value[7] != '-' || value[10] != 'T' ||
		value[13] != ':' || value[16] != ':' || value[19] != 'Z')
		return false;
	for (index = 0; index < 19; index++)
		if (index != 4 && index != 7 && index != 10 &&
			index != 13 && index != 16 &&
			(value[index] < '0' || value[index] > '9'))
			return false;
	return true;
}

static bool
semloom_calibration_identity(
	const SemloomParsedCalibration *parsed,
	char output[PG_SHA256_DIGEST_LENGTH * 2 + 1])
{
	pg_cryptohash_ctx *context = pg_cryptohash_create(PG_SHA256);
	uint8 digest[PG_SHA256_DIGEST_LENGTH];
	char schema_version[16];
	char training_samples[32];
	char held_out_samples[32];
	const char *values[28];
	int index;
	static const char hex[] = "0123456789abcdef";

	if (context == NULL || pg_cryptohash_init(context) < 0)
		goto failure;
	if (pg_cryptohash_update(context,
						 (const uint8 *) SEMLOOM_CALIBRATION_ID_DOMAIN,
							 sizeof(SEMLOOM_CALIBRATION_ID_DOMAIN) - 1) < 0)
		goto failure;
	snprintf(schema_version, sizeof(schema_version), "%d", parsed->schema_version);
	snprintf(training_samples, sizeof(training_samples), "%d", parsed->training_samples);
	snprintf(held_out_samples, sizeof(held_out_samples), "%d", parsed->held_out_samples);
	values[0] = schema_version;
	values[1] = parsed->cost_model_id;
	values[2] = parsed->generated_at;
	values[3] = parsed->semantic_digest;
	values[4] = parsed->physical_digest;
	values[5] = parsed->provider_profile;
	values[6] = parsed->model_id;
	values[7] = parsed->model_role;
	values[8] = parsed->workload_signature;
	values[9] = parsed->service_signature;
	values[10] = training_samples;
	values[11] = held_out_samples;
	values[12] = parsed->training_rows_text;
	values[13] = parsed->held_out_rows_text;
	values[14] = parsed->output_selectivity_text;
	values[15] = parsed->calls_per_input_text;
	values[16] = parsed->prompt_per_call_text;
	values[17] = parsed->output_per_call_text;
	values[18] = parsed->service_fixed_ms_text;
	values[19] = parsed->service_per_call_text;
	values[20] = parsed->service_per_prompt_text;
	values[21] = parsed->service_per_output_text;
	values[22] = parsed->held_out_mean_error_text;
	values[23] = parsed->held_out_max_error_text;
	values[24] = parsed->held_out_error_lower_text;
	values[25] = parsed->held_out_error_upper_text;
	values[26] = parsed->accepted_max_error_text;
	values[27] = parsed->evidence_digest;
	for (index = 0; index < lengthof(values); index++)
		if (!semloom_calibration_hash_text(context, values[index]))
			goto failure;
	if (pg_cryptohash_final(context, digest, sizeof(digest)) < 0)
		goto failure;
	pg_cryptohash_free(context);
	for (index = 0; index < PG_SHA256_DIGEST_LENGTH; index++)
	{
		output[index * 2] = hex[digest[index] >> 4];
		output[index * 2 + 1] = hex[digest[index] & 0x0f];
	}
	output[PG_SHA256_DIGEST_LENGTH * 2] = '\0';
	return true;

failure:
	if (context != NULL)
		pg_cryptohash_free(context);
	return false;
}

static bool
semloom_calibration_hash_text(pg_cryptohash_ctx *context, const char *value)
{
	Size length = strlen(value);
	uint8 length_bytes[4];

	if (length > PG_UINT32_MAX)
		return false;
	length_bytes[0] = (uint8) (length >> 24);
	length_bytes[1] = (uint8) (length >> 16);
	length_bytes[2] = (uint8) (length >> 8);
	length_bytes[3] = (uint8) length;
	return pg_cryptohash_update(context, length_bytes, sizeof(length_bytes)) >= 0 &&
		(length == 0 ||
		 pg_cryptohash_update(context, (const uint8 *) value, length) >= 0);
}

static const char *
semloom_calibration_rejection_reason(
	const SemloomParsedCalibration *parsed,
	const SemloomPlanSpec *plan_spec,
	const char *provider_execution_profile)
{
	if (strcmp(parsed->semantic_digest, plan_spec->semantic_spec_digest) != 0)
		return "semantic-spec-mismatch";
	if (strcmp(parsed->physical_digest, plan_spec->physical_algorithm_digest) != 0)
		return "physical-algorithm-mismatch";
	if (strcmp(parsed->model_id, plan_spec->model_id) != 0)
		return "model-mismatch";
	if (strcmp(parsed->model_role, plan_spec->physical_role) != 0)
		return "model-role-mismatch";
	if (strcmp(parsed->provider_profile, provider_execution_profile) != 0)
		return "provider-profile-mismatch";
	return NULL;
}
