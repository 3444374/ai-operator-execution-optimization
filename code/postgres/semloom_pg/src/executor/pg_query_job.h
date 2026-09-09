/* Query ownership is separate from operator task/result buffers. */
#ifndef SEMLOOM_PG_QUERY_JOB_H
#define SEMLOOM_PG_QUERY_JOB_H
#include "utils/memutils.h"
#include "provider/ai_provider_port.h"
typedef struct PgQueryJobFlow PgQueryJobFlow;
extern PgQueryJobFlow *pg_query_job_register(MemoryContext, const char *);
extern AiProviderStatus pg_query_job_join(PgQueryJobFlow *, pgsocket, AiProviderError *);
extern void pg_query_job_node_end(MemoryContext);
#endif
