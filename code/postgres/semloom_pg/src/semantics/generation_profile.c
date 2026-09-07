/* Canonical encoding for the single supported, explicitly selected profile. */
#include <string.h>

#include "semantics/generation_profile.h"

#define SLICE_LITERAL(value) {(const uint8_t *) (value), sizeof(value) - 1}

static const AiGenerationProfile tristate_profile = {
	SLICE_LITERAL(SEMLOOM_TRISTATE_PROFILE_ID),
	SEMLOOM_TRISTATE_PROFILE_VERSION,
	AI_GENERATION_CONSTRAINT_CHOICE,
	AI_GENERATION_PROFILE_MAX_CHOICES,
	{SLICE_LITERAL("TRUE"), SLICE_LITERAL("FALSE"), SLICE_LITERAL("UNKNOWN")}
};

static const uint8_t profile_domain[] = "semloom-generation-profile-v1";
static const AiByteSlice choice_kind = SLICE_LITERAL("CHOICE");

_Static_assert(sizeof(profile_domain) + 4 + sizeof(SEMLOOM_TRISTATE_PROFILE_ID) - 1
			   + 4 + 4 + sizeof("CHOICE") - 1 + 4
			   + 4 + sizeof("TRUE") - 1 + 4 + sizeof("FALSE") - 1
			   + 4 + sizeof("UNKNOWN") - 1 == SEMLOOM_GENERATION_PROFILE_CANONICAL_BYTES,
			   "profile canonical size must match the published vector");

const AiGenerationProfile *
semloom_generation_profile_tristate(void)
{
	return &tristate_profile;
}

static bool
slice_equal(AiByteSlice value, AiByteSlice expected)
{
	return value.length == expected.length && value.data != NULL &&
		memcmp(value.data, expected.data, expected.length) == 0;
}

static bool
profile_supported(const AiGenerationProfile *profile)
{
	uint32_t index;

	if (profile == NULL ||
		!slice_equal(profile->profile_id, tristate_profile.profile_id) ||
		profile->profile_version != SEMLOOM_TRISTATE_PROFILE_VERSION ||
		profile->constraint_kind != AI_GENERATION_CONSTRAINT_CHOICE ||
		profile->choice_count != AI_GENERATION_PROFILE_MAX_CHOICES)
		return false;
	for (index = 0; index < AI_GENERATION_PROFILE_MAX_CHOICES; index++)
	{
		if (!slice_equal(profile->choices[index], tristate_profile.choices[index]))
			return false;
	}
	return true;
}

static void
encode_uint32(uint8_t **cursor, uint32_t value)
{
	(*cursor)[0] = (uint8_t) (value >> 24);
	(*cursor)[1] = (uint8_t) (value >> 16);
	(*cursor)[2] = (uint8_t) (value >> 8);
	(*cursor)[3] = (uint8_t) value;
	*cursor += 4;
}

static void
encode_slice(uint8_t **cursor, AiByteSlice value)
{
	encode_uint32(cursor, value.length);
	memcpy(*cursor, value.data, value.length);
	*cursor += value.length;
}

bool
semloom_generation_profile_encode(const AiGenerationProfile *profile,
								  uint8_t *output,
								  uint32_t capacity,
								  uint32_t *written)
{
	uint8_t *cursor = output;
	uint32_t index;

	if (written == NULL)
		return false;
	*written = 0;
	if (output == NULL || capacity < SEMLOOM_GENERATION_PROFILE_CANONICAL_BYTES ||
		!profile_supported(profile))
		return false;

	/* The domain includes exactly one trailing NUL; slices exclude terminators. */
	memcpy(cursor, profile_domain, sizeof(profile_domain));
	cursor += sizeof(profile_domain);
	encode_slice(&cursor, profile->profile_id);
	encode_uint32(&cursor, profile->profile_version);
	encode_slice(&cursor, choice_kind);
	encode_uint32(&cursor, profile->choice_count);
	for (index = 0; index < profile->choice_count; index++)
		encode_slice(&cursor, profile->choices[index]);
	*written = (uint32_t) (cursor - output);
	return true;
}
