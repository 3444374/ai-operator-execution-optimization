#ifndef SEMLOOM_WIRE_IMAGE_H
#define SEMLOOM_WIRE_IMAGE_H

#include "postgres.h"
#include "provider/ai_provider_port.h"

extern AiProviderStatus semloom_wire_image_open(pgsocket fd, const AiOpenSpec *spec,
	uint32 window, AiProviderError *error);
extern AiProviderStatus semloom_wire_image_task(pgsocket fd, const AiOpenSpec *spec,
	const AiPreparedTask *task, bool *accepted, AiProviderError *error);
extern AiProviderStatus semloom_wire_image_collect(pgsocket fd, const AiOpenSpec *spec,
	const AiPreparedTask *pending, uint32 count, AiCompletion *completion, AiProviderError *error);

#endif
