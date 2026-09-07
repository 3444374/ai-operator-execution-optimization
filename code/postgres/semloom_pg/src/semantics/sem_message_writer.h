/* Internal two-message JSON encoding; callers own text semantics and storage. */
#ifndef SEMLOOM_SEM_MESSAGE_WRITER_H
#define SEMLOOM_SEM_MESSAGE_WRITER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct SemloomMessagePart
{
	const uint8_t *data;
	size_t length;
} SemloomMessagePart;

extern bool semloom_message_write(const SemloomMessagePart *system_parts,
								 size_t system_part_count,
								 SemloomMessagePart user,
								 uint8_t *destination,
								 size_t destination_length,
								 size_t *written_length);

#endif
