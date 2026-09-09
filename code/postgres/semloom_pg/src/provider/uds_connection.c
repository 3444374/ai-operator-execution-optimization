/* Nonblocking PG-aware UDS connection ownership shared by control and task streams. */
#include "postgres.h"
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include "miscadmin.h"
#include "storage/fd.h"
#include "provider/provider_private.h"
#include "provider/uds_connection.h"
#include "provider/wire/wire_common.h"

AiProviderStatus
semloom_uds_connect_socket(const char *socket_path, pgsocket *socket_fd,
                          bool *external_fd_acquired, AiProviderError *error)
{
	struct sockaddr_un address;
	int socket_flags;
	int connect_result;

	if (strlen(socket_path) >= sizeof(address.sun_path))
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_INVALID_SPEC,
								   0,
								   0,
								   "SemLoom provider socket path is too long");
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (socket_path[0] != '/')
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_INVALID_SPEC,
								   0,
								   0,
								   "SemLoom provider socket path must be absolute");
		return AI_PROVIDER_STATUS_ERROR;
	}
	if (!AcquireExternalFD())
	{
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_RESOURCE_EXHAUSTED,
								   0,
								   0,
								   "could not reserve a file descriptor for the SemLoom provider");
		return AI_PROVIDER_STATUS_ERROR;
	}
	*external_fd_acquired = true;
	*socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (*socket_fd == PGINVALID_SOCKET)
	{
		int saved_errno = errno;

		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   saved_errno,
								   0,
								   "could not create SemLoom provider socket");
		return AI_PROVIDER_STATUS_ERROR;
	}

	socket_flags = fcntl(*socket_fd, F_GETFL, 0);
	if (socket_flags < 0 ||
		fcntl(*socket_fd, F_SETFL, socket_flags | O_NONBLOCK) < 0)
	{
		int saved_errno = errno;

		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   saved_errno,
								   0,
								   "could not make SemLoom provider socket nonblocking");
		return AI_PROVIDER_STATUS_ERROR;
	}

	MemSet(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	strlcpy(address.sun_path, socket_path, sizeof(address.sun_path));
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		connect_result = connect(*socket_fd,
								 (struct sockaddr *) &address,
								 sizeof(address));
		if (connect_result == 0 || errno == EISCONN)
			break;
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
		{
			semloom_wire_common_wait_connect_retry();
			continue;
		}
		if (errno == EINPROGRESS || errno == EALREADY)
		{
			AiProviderStatus status =
				semloom_wire_common_wait_connected(*socket_fd, error);

			if (status != AI_PROVIDER_STATUS_OK)
				return status;
			break;
		}
		semloom_provider_error_set(error,
								   AI_PROVIDER_ERROR_SYSTEM,
								   errno,
								   0,
								   "could not connect to SemLoom provider socket");
		return AI_PROVIDER_STATUS_ERROR;
	}

	return AI_PROVIDER_STATUS_OK;
}

void
semloom_uds_close_socket(pgsocket *socket_fd, bool *external_fd_acquired)
{
    if (*socket_fd != PGINVALID_SOCKET)
        closesocket(*socket_fd);
    *socket_fd = PGINVALID_SOCKET;
    if (*external_fd_acquired)
        ReleaseExternalFD();
    *external_fd_acquired = false;
}
