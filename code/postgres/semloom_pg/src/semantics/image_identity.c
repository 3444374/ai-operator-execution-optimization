/* Canonical image identities, using PostgreSQL's existing SHA-256 primitive. */
#include "postgres.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "semantics/image_identity.h"
#include "semantics/semantic_image_contract.h"

static pg_cryptohash_ctx *
start_hash(const char *domain, Size length)
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);

	if (ctx == NULL || pg_cryptohash_init(ctx) < 0 ||
		pg_cryptohash_update(ctx, (const uint8 *) domain, length) < 0)
		elog(ERROR, "could not initialize image identity digest");
	return ctx;
}

static void
hash_bytes(pg_cryptohash_ctx *ctx, const void *value, Size length)
{
	if (length > 0 && pg_cryptohash_update(ctx, value, length) < 0)
		elog(ERROR, "could not update image identity digest");
}

static void
hash_number(pg_cryptohash_ctx *ctx, uint64 value, int length)
{
	uint8 bytes[8];
	int index;

	for (index = 0; index < length; index++)
		bytes[index] = (uint8) (value >> ((length - index - 1) * 8));
	hash_bytes(ctx, bytes, length);
}

static void
hash_slice(pg_cryptohash_ctx *ctx, AiByteSlice value)
{
	hash_number(ctx, value.length, 4);
	hash_bytes(ctx, value.data, value.length);
}

static void
hash_text(pg_cryptohash_ctx *ctx, const char *value)
{
	AiByteSlice slice = {(const uint8 *) value, (uint32) strlen(value)};

	hash_slice(ctx, slice);
}

static void
finish_hash(pg_cryptohash_ctx *ctx, char output[65])
{
	uint8 bytes[PG_SHA256_DIGEST_LENGTH];
	const char *hex = "0123456789abcdef";
	int index;

	if (pg_cryptohash_final(ctx, bytes, sizeof(bytes)) < 0)
		elog(ERROR, "could not finish image identity digest");
	pg_cryptohash_free(ctx);
	for (index = 0; index < PG_SHA256_DIGEST_LENGTH; index++)
	{
		output[index * 2] = hex[bytes[index] >> 4];
		output[index * 2 + 1] = hex[bytes[index] & 15];
	}
	output[64] = '\0';
}

const char *
semloom_image_execution_id(bool staged)
{
	return staged ? "semloom.provider.image-staged.uds.v7" : "semloom.provider.image-reference.uds.v7";
}

void
semloom_image_spec_digest(const AiOpenSpec *spec, char output[65])
{
	const char *fields[] = {SEMLOOM_IMAGE_SPEC_ID, "bytea", "real[]", SEMLOOM_IMAGE_PREPARE_ID,
		SEMLOOM_IMAGE_PARSER_ID, "PROPAGATE_NULL", "FAIL_QUERY", "INPUT_ORDER", "projected_l2"};
	pg_cryptohash_ctx *ctx = start_hash("semloom-image-spec-v1", sizeof("semloom-image-spec-v1"));
	unsigned int index;

	for (index = 0; index < sizeof(fields) / sizeof(fields[0]); index++)
		hash_text(ctx, fields[index]);
	hash_slice(ctx, spec->model_id);
	hash_slice(ctx, spec->image_model_revision);
	hash_slice(ctx, spec->image_processor_id);
	hash_slice(ctx, spec->image_processor_revision);
	hash_slice(ctx, spec->image_dtype);
	hash_number(ctx, spec->image_dimension, 4);
	hash_number(ctx, spec->image_input_size, 4);
	hash_number(ctx, SEMLOOM_IMAGE_MAX_INPUT_BYTES, 4);
	hash_number(ctx, SEMLOOM_IMAGE_MAX_PIXELS, 4);
	finish_hash(ctx, output);
}

void
semloom_image_physical_digest(bool staged, char output[65])
{
	pg_cryptohash_ctx *ctx = start_hash("semloom-image-physical-v1", sizeof("semloom-image-physical-v1"));

	hash_text(ctx, staged ? "staged" : "reference");
	finish_hash(ctx, output);
}

void
semloom_image_execution_digest(const AiOpenSpec *spec, char output[65])
{
	pg_cryptohash_ctx *ctx = start_hash("semloom-image-execution-v1", sizeof("semloom-image-execution-v1"));

	hash_text(ctx, semloom_image_execution_id(spec->image_staged));
	hash_bytes(ctx, spec->semantic_spec_digest.data, spec->semantic_spec_digest.length);
	finish_hash(ctx, output);
}

void
semloom_image_payload_digest(const AiOpenSpec *spec, AiByteSlice input, char output[65])
{
	pg_cryptohash_ctx *ctx = start_hash("semloom-image-payload-v1", sizeof("semloom-image-payload-v1"));

	hash_bytes(ctx, spec->semantic_spec_digest.data, spec->semantic_spec_digest.length);
	hash_number(ctx, input.length, 8);
	hash_bytes(ctx, input.data, input.length);
	finish_hash(ctx, output);
}

void
semloom_image_completion_digest(const AiOpenSpec *spec, AiByteSlice payload_digest,
	uint64 sequence, AiByteSlice result, char output[65])
{
	char execution[65];
	pg_cryptohash_ctx *ctx = start_hash("semloom-image-completion-v1", sizeof("semloom-image-completion-v1"));

	semloom_image_execution_digest(spec, execution);
	hash_bytes(ctx, spec->semantic_spec_digest.data, spec->semantic_spec_digest.length);
	hash_bytes(ctx, spec->physical_algorithm_digest.data, spec->physical_algorithm_digest.length);
	hash_bytes(ctx, execution, 64);
	hash_bytes(ctx, payload_digest.data, payload_digest.length);
	hash_number(ctx, sequence, 8);
	hash_bytes(ctx, result.data, result.length);
	finish_hash(ctx, output);
}
