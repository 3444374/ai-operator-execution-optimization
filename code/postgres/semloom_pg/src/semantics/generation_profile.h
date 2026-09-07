/* Choice-profile values/canonical bytes; independent of PostgreSQL and wire I/O. */
#ifndef SEMLOOM_GENERATION_PROFILE_H
#define SEMLOOM_GENERATION_PROFILE_H

#include "provider/ai_provider_port.h"

#define SEMLOOM_TRISTATE_PROFILE_ID "semloom.generation.choice.tristate"
#define SEMLOOM_TRISTATE_PROFILE_VERSION 1
#define SEMLOOM_GENERATION_PROFILE_CANONICAL_BYTES 114

/* Process-lifetime immutable profile; callers must not modify its bytes. */
extern const AiGenerationProfile *semloom_generation_profile_tristate(void);

/* Encode all fields of the known profile, for hashing by the caller's SHA-256.
 * No allocation, I/O, PostgreSQL types, or cryptographic implementation here.
 * Input slices need only remain readable for this call and need no NUL suffix.
 * Output must not overlap input storage. On failure, output is unchanged and
 * *written is zero (when written is non-NULL). Never accepts partial output.
 */
extern bool semloom_generation_profile_encode(const AiGenerationProfile *profile,
											uint8_t *output,
											uint32_t capacity,
											uint32_t *written);

#endif
