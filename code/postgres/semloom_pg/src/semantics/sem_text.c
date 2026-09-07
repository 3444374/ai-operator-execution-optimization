/* Validate Unicode scalar values without changing or terminating input bytes. */
#include <stddef.h>

#include "semantics/sem_text.h"

bool
semloom_text_is_utf8_no_nul(const uint8_t *data, uint32_t length)
{
	size_t index = 0;

	if (length > 0 && data == NULL)
		return false;
	while (index < length)
	{
		uint8_t byte = data[index++];
		size_t following;
		size_t offset;
		uint8_t minimum = 0x80;
		uint8_t maximum = 0xbf;

		if (byte == 0)
			return false;
		if (byte < 0x80)
			continue;
		if (byte >= 0xc2 && byte <= 0xdf)
			following = 1;
		else if (byte >= 0xe0 && byte <= 0xef)
		{
			following = 2;
			if (byte == 0xe0)
				minimum = 0xa0;
			if (byte == 0xed)
				maximum = 0x9f;
		}
		else if (byte >= 0xf0 && byte <= 0xf4)
		{
			following = 3;
			if (byte == 0xf0)
				minimum = 0x90;
			if (byte == 0xf4)
				maximum = 0x8f;
		}
		else
			return false;
		if (following > length - index || data[index] < minimum || data[index] > maximum)
			return false;
		for (offset = 1; offset < following; offset++)
		{
			if (data[index + offset] < 0x80 || data[index + offset] > 0xbf)
				return false;
		}
		index += following;
	}
	return true;
}
