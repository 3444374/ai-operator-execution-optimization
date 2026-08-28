#include "postgres.h"

#include <errno.h>
#include <sys/socket.h>

#include "common/cryptohash.h"
#include "common/sha2.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/wait_classes.h"

#include "semloom_pg.h"

#define SEMLOOM_PLAN_DIGEST_DOMAIN "semloom-plan-v1\0"
#define SEMLOOM_PLAN_DIGEST_SUFFIX "SEM_MAP\0text\0text"
#define SEMLOOM_PAYLOAD_DIGEST_DOMAIN "semloom-payload-v1\0"
#define SEMLOOM_COMPLETION_DIGEST_DOMAIN "semloom-completion-v1\0"

static void semloom_hash_begin(pg_cryptohash_ctx **context);
static void semloom_hash_bytes(pg_cryptohash_ctx *context,
							   const void *data,
							   Size length);
static void semloom_hash_uint32(pg_cryptohash_ctx *context, uint32 value);
static void semloom_hash_uint64(pg_cryptohash_ctx *context, uint64 value);
static void semloom_hash_finish(pg_cryptohash_ctx *context,
								char output[SEMLOOM_SHA256_HEX_LENGTH + 1]);
static void semloom_socket_write_all(pgsocket socket_fd, const char *data, Size length);
static void semloom_socket_read_all(pgsocket socket_fd, char *data, Size length);
static void semloom_wait_for_socket(pgsocket socket_fd, int socket_event);

void
semloom_protocol_plan_digest(const SemloomSemanticPlanSpec *plan_spec,
							 char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context,
					   SEMLOOM_PLAN_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_PLAN_DIGEST_DOMAIN) - 1);
	semloom_hash_uint32(context, (uint32) plan_spec->mapped_column);
	semloom_hash_bytes(context,
					   SEMLOOM_PLAN_DIGEST_SUFFIX,
					   sizeof(SEMLOOM_PLAN_DIGEST_SUFFIX) - 1);
	semloom_hash_finish(context, output);
}

void
semloom_protocol_payload_digest(bool is_null,
								const char *payload,
								Size payload_length,
								char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;
	uint8 null_flag = is_null ? 1 : 0;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context,
					   SEMLOOM_PAYLOAD_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_PAYLOAD_DIGEST_DOMAIN) - 1);
	semloom_hash_bytes(context, &null_flag, sizeof(null_flag));
	semloom_hash_uint64(context, is_null ? 0 : payload_length);
	if (!is_null)
		semloom_hash_bytes(context, payload, payload_length);
	semloom_hash_finish(context, output);
}

void
semloom_protocol_completion_digest(
	const char plan_digest[SEMLOOM_SHA256_HEX_LENGTH + 1],
	const char payload_digest[SEMLOOM_SHA256_HEX_LENGTH + 1],
	uint64 sequence,
	bool is_null,
	const char *output_payload,
	Size output_length,
	char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	pg_cryptohash_ctx *context;
	uint8 null_flag = is_null ? 1 : 0;

	semloom_hash_begin(&context);
	semloom_hash_bytes(context,
					   SEMLOOM_COMPLETION_DIGEST_DOMAIN,
					   sizeof(SEMLOOM_COMPLETION_DIGEST_DOMAIN) - 1);
	semloom_hash_bytes(context, plan_digest, SEMLOOM_SHA256_HEX_LENGTH);
	semloom_hash_bytes(context, payload_digest, SEMLOOM_SHA256_HEX_LENGTH);
	semloom_hash_uint64(context, sequence);
	semloom_hash_bytes(context, &null_flag, sizeof(null_flag));
	semloom_hash_uint64(context, is_null ? 0 : output_length);
	if (!is_null)
		semloom_hash_bytes(context, output_payload, output_length);
	semloom_hash_finish(context, output);
}

void
semloom_protocol_send_frame(pgsocket socket_fd, const char *payload, Size payload_length)
{
	uint8 header[4];

	if (payload_length == 0 || payload_length > SEMLOOM_MAX_FRAME_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("SemLoom provider frame length is outside the protocol limit")));
	header[0] = (uint8) (payload_length >> 24);
	header[1] = (uint8) (payload_length >> 16);
	header[2] = (uint8) (payload_length >> 8);
	header[3] = (uint8) payload_length;
	semloom_socket_write_all(socket_fd, (const char *) header, sizeof(header));
	semloom_socket_write_all(socket_fd, payload, payload_length);
}

char *
semloom_protocol_receive_frame(pgsocket socket_fd)
{
	uint8 header[4];
	uint32 payload_length;
	char *payload;

	semloom_socket_read_all(socket_fd, (char *) header, sizeof(header));
	payload_length = ((uint32) header[0] << 24) |
		((uint32) header[1] << 16) |
		((uint32) header[2] << 8) |
		(uint32) header[3];
	if (payload_length == 0 || payload_length > SEMLOOM_MAX_FRAME_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("SemLoom provider returned an invalid frame length")));
	payload = palloc(payload_length + 1);
	semloom_socket_read_all(socket_fd, payload, payload_length);
	payload[payload_length] = '\0';
	return payload;
}

static void
semloom_hash_begin(pg_cryptohash_ctx **context)
{
	*context = pg_cryptohash_create(PG_SHA256);
	if (*context == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not initialize SemLoom SHA-256 digest")));
	if (pg_cryptohash_init(*context) < 0)
	{
		pg_cryptohash_free(*context);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not initialize SemLoom SHA-256 digest")));
	}
}

static void
semloom_hash_bytes(pg_cryptohash_ctx *context, const void *data, Size length)
{
	if (length > 0 && pg_cryptohash_update(context, data, length) < 0)
	{
		pg_cryptohash_free(context);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not update SemLoom SHA-256 digest")));
	}
}

static void
semloom_hash_uint32(pg_cryptohash_ctx *context, uint32 value)
{
	uint8 encoded[4];

	encoded[0] = (uint8) (value >> 24);
	encoded[1] = (uint8) (value >> 16);
	encoded[2] = (uint8) (value >> 8);
	encoded[3] = (uint8) value;
	semloom_hash_bytes(context, encoded, sizeof(encoded));
}

static void
semloom_hash_uint64(pg_cryptohash_ctx *context, uint64 value)
{
	uint8 encoded[8];
	int index;

	for (index = 0; index < lengthof(encoded); index++)
		encoded[index] = (uint8) (value >> (56 - index * 8));
	semloom_hash_bytes(context, encoded, sizeof(encoded));
}

static void
semloom_hash_finish(pg_cryptohash_ctx *context,
						char output[SEMLOOM_SHA256_HEX_LENGTH + 1])
{
	static const char hex_digits[] = "0123456789abcdef";
	uint8 digest[PG_SHA256_DIGEST_LENGTH];
	int index;

	if (pg_cryptohash_final(context, digest, sizeof(digest)) < 0)
	{
		pg_cryptohash_free(context);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not finish SemLoom SHA-256 digest")));
	}
	pg_cryptohash_free(context);
	for (index = 0; index < lengthof(digest); index++)
	{
		output[index * 2] = hex_digits[digest[index] >> 4];
		output[index * 2 + 1] = hex_digits[digest[index] & 0x0f];
	}
	output[SEMLOOM_SHA256_HEX_LENGTH] = '\0';
}

static void
semloom_socket_write_all(pgsocket socket_fd, const char *data, Size length)
{
	Size written = 0;

	while (written < length)
	{
		ssize_t result;
		int send_flags = 0;

		CHECK_FOR_INTERRUPTS();
#ifdef MSG_NOSIGNAL
		send_flags = MSG_NOSIGNAL;
#endif
		result = send(socket_fd, data + written, length - written, send_flags);
		if (result > 0)
		{
			written += result;
			continue;
		}
		if (result < 0 && errno == EINTR)
			continue;
		if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
		{
			semloom_wait_for_socket(socket_fd, WL_SOCKET_WRITEABLE);
			continue;
		}
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not write to SemLoom provider socket: %m")));
	}
}

static void
semloom_socket_read_all(pgsocket socket_fd, char *data, Size length)
{
	Size received = 0;

	while (received < length)
	{
		ssize_t result;

		CHECK_FOR_INTERRUPTS();
		result = recv(socket_fd, data + received, length - received, 0);
		if (result > 0)
		{
			received += result;
			continue;
		}
		if (result == 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("SemLoom provider disconnected before completing a frame")));
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
		{
			semloom_wait_for_socket(socket_fd, WL_SOCKET_READABLE);
			continue;
		}
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not read from SemLoom provider socket: %m")));
	}
}

static void
semloom_wait_for_socket(pgsocket socket_fd, int socket_event)
{
	for (;;)
	{
		int events;

		events = WaitLatchOrSocket(MyLatch,
								   WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | socket_event,
								   socket_fd,
								   0,
								   PG_WAIT_EXTENSION);
		if (events & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}
		if (events & socket_event)
			return;
	}
}
