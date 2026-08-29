/*
 * Select a query-fixed provider and implement shared neutral-error helpers.
 *
 * Input is PostgreSQL-owned configuration plus an AiOpenSpec; output is an
 * opaque adapter/config pair.  Passing requires this module to perform no I/O.
 * Plan: experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md.
 */
#include "postgres.h"

#include "utils/memutils.h"

#include "provider_private.h"
#include "semloom_pg.h"

static bool semloom_slice_equals(const AiByteSlice *slice, const char *expected);

void
semloom_provider_select(MemoryContext owner_context, AiProvider *provider)
{
	const char *socket_path = semloom_gateway_socket_path();

	Assert(owner_context != NULL);
	Assert(provider != NULL);
	if (socket_path[0] == '\0')
		semloom_recording_provider_select(provider);
	else
		semloom_uds_provider_select(owner_context, socket_path, provider);
}

bool
semloom_provider_spec_is_recording(const AiOpenSpec *spec)
{
	bool map_spec;
	bool filter_spec;

	if (spec == NULL)
		return false;
	map_spec = spec->operator_kind == AI_PROVIDER_OPERATOR_MAP &&
		spec->output_value_kind == AI_PROVIDER_VALUE_TEXT &&
		semloom_slice_equals(&spec->semantic_spec_id,
						 SEMLOOM_MAP_RECORDING_SPEC_ID);
	filter_spec = spec->operator_kind == AI_PROVIDER_OPERATOR_FILTER &&
		spec->output_value_kind == AI_PROVIDER_VALUE_TRISTATE &&
		semloom_slice_equals(&spec->semantic_spec_id,
						 SEMLOOM_FILTER_RECORDING_SPEC_ID);

	return (map_spec || filter_spec) &&
		spec->input_value_kind == AI_PROVIDER_VALUE_TEXT &&
		spec->null_policy == AI_PROVIDER_NULL_PROPAGATE &&
		spec->error_policy == AI_PROVIDER_ERROR_FAIL_QUERY &&
		spec->semantic_spec_version == SEMLOOM_RECORDING_SPEC_VERSION &&
		semloom_slice_equals(&spec->physical_algorithm, SEMLOOM_RECORDING_ALGORITHM);
}

void
semloom_provider_error_clear(AiProviderError *error)
{
	if (error != NULL)
		MemSet(error, 0, sizeof(*error));
}

void
semloom_provider_error_set(AiProviderError *error,
						   uint32 code,
						   uint32 operation,
						   int system_errno,
						   const char *detail)
{
	Size detail_length = 0;

	Assert(error != NULL);
	semloom_provider_error_clear(error);
	error->code = code;
	error->operation = operation;
	error->system_errno = system_errno;
	if (detail != NULL)
	{
		detail_length = strlen(detail);
		if (detail_length >= AI_PROVIDER_ERROR_DETAIL_CAPACITY)
			detail_length = AI_PROVIDER_ERROR_DETAIL_CAPACITY - 1;
		memcpy(error->detail, detail, detail_length);
	}
	error->detail[detail_length] = '\0';
	error->detail_length = (uint16) detail_length;
}

static bool
semloom_slice_equals(const AiByteSlice *slice, const char *expected)
{
	Size expected_length = strlen(expected);

	return slice != NULL && slice->length == expected_length &&
		(slice->length == 0 ||
		 (slice->data != NULL &&
		  memcmp(slice->data, expected, slice->length) == 0));
}
