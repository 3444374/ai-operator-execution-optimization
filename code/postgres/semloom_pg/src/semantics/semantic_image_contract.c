#include "semantics/semantic_image_contract.h"

bool
semloom_image_vector_valid(const uint8_t *bytes, uint32_t length, uint32_t dimension)
{
	uint32_t index;

	if (bytes == 0 || dimension == 0 || dimension > SEMLOOM_IMAGE_MAX_DIMENSION ||
		length != dimension * 4)
		return false;
	for (index = 0; index < length; index += 4)
		if ((bytes[index] & 0x7f) == 0x7f && (bytes[index + 1] & 0x80) != 0)
			return false;
	return true;
}
