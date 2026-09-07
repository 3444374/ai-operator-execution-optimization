/* Encode borrowed system fragments and user bytes without allocation or I/O. */
#include <string.h>

#include "semantics/sem_message_writer.h"

typedef struct SemloomTaskWriter
{
	uint8_t *destination;
	size_t capacity;
	size_t length;
	bool valid;
} SemloomTaskWriter;

static void semloom_task_writer_bytes(SemloomTaskWriter *writer,
										  const void *data,
										  size_t length);
static void semloom_task_writer_json_string(SemloomTaskWriter *writer,
												 const uint8_t *data,
												 size_t length);

bool
semloom_message_write(const SemloomMessagePart *system_parts,
					  size_t system_part_count,
					  SemloomMessagePart user,
					  uint8_t *destination,
					  size_t destination_length,
					  size_t *written_length)
{
	static const char prefix[] = "[{\"role\":\"system\",\"content\":";
	static const char middle[] = "},{\"role\":\"user\",\"content\":";
	static const char suffix[] = "}]";
	SemloomTaskWriter writer = {
		.destination = destination,
		.capacity = destination_length,
		.length = 0,
		.valid = true,
	};
	size_t index;

	semloom_task_writer_bytes(&writer, prefix, sizeof(prefix) - 1);
	semloom_task_writer_bytes(&writer, "\"", 1);
	for (index = 0; index < system_part_count; index++)
		semloom_task_writer_json_string(&writer,
										 system_parts[index].data,
										 system_parts[index].length);
	semloom_task_writer_bytes(&writer, "\"", 1);
	semloom_task_writer_bytes(&writer, middle, sizeof(middle) - 1);
	semloom_task_writer_bytes(&writer, "\"", 1);
	semloom_task_writer_json_string(&writer, user.data, user.length);
	semloom_task_writer_bytes(&writer, "\"", 1);
	semloom_task_writer_bytes(&writer, suffix, sizeof(suffix) - 1);
	*written_length = writer.length;
	return writer.valid &&
		(destination == NULL || writer.length == destination_length);
}

static void
semloom_task_writer_bytes(SemloomTaskWriter *writer,
						  const void *data,
						  size_t length)
{
	if (writer->destination != NULL)
	{
		if (writer->length > writer->capacity ||
			length > writer->capacity - writer->length)
		{
			writer->valid = false;
			return;
		}
		memcpy(writer->destination + writer->length, data, length);
	}
	writer->length += length;
}

static void
semloom_task_writer_json_string(SemloomTaskWriter *writer,
								 const uint8_t *data,
								 size_t length)
{
	static const char hex[] = "0123456789abcdef";
	size_t index;

	for (index = 0; index < length; index++)
	{
		uint8_t byte = data[index];

		if (byte == '"' || byte == '\\')
		{
			uint8_t escaped[2] = {'\\', byte};

			semloom_task_writer_bytes(writer, escaped, sizeof(escaped));
		}
		else if (byte == '\b' || byte == '\f' || byte == '\n' ||
				 byte == '\r' || byte == '\t')
		{
			uint8_t escaped[2] = {'\\', 0};

			switch (byte)
			{
				case '\b': escaped[1] = 'b'; break;
				case '\f': escaped[1] = 'f'; break;
				case '\n': escaped[1] = 'n'; break;
				case '\r': escaped[1] = 'r'; break;
				default: escaped[1] = 't'; break;
			}
			semloom_task_writer_bytes(writer, escaped, sizeof(escaped));
		}
		else if (byte < 0x20)
		{
			uint8_t escaped[6] = {'\\', 'u', '0', '0',
				(uint8_t) hex[byte >> 4],
				(uint8_t) hex[byte & 0x0f]};

			semloom_task_writer_bytes(writer, escaped, sizeof(escaped));
		}
		else
			semloom_task_writer_bytes(writer, &byte, 1);
	}
}
