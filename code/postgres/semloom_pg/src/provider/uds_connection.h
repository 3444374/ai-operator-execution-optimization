#ifndef SEMLOOM_UDS_CONNECTION_H
#define SEMLOOM_UDS_CONNECTION_H
#include "provider/ai_provider_port.h"
extern AiProviderStatus semloom_uds_connect_socket(const char *, pgsocket *, bool *, AiProviderError *);
extern void semloom_uds_close_socket(pgsocket *, bool *);
#endif
