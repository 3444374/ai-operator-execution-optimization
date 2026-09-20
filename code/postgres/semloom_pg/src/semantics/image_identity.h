#ifndef SEMLOOM_IMAGE_IDENTITY_H
#define SEMLOOM_IMAGE_IDENTITY_H

#include "provider/ai_provider_port.h"

extern void semloom_image_spec_digest(const AiOpenSpec *spec, char output[65]);
extern void semloom_image_physical_digest(bool staged, char output[65]);
extern void semloom_image_execution_digest(const AiOpenSpec *spec, char output[65]);
extern void semloom_image_payload_digest(const AiOpenSpec *spec, AiByteSlice input, char output[65]);
extern void semloom_image_completion_digest(const AiOpenSpec *spec, AiByteSlice payload_digest,
	uint64_t sequence, AiByteSlice result, char output[65]);
extern const char *semloom_image_execution_id(bool staged);

#endif
