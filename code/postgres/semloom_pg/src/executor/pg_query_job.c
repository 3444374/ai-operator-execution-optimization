/* One lazy control connection per EState query context; no scheduling policy here. */
#include "postgres.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "executor/pg_query_job.h"
#include "provider/provider_private.h"
#include "provider/uds_connection.h"
#include "provider/wire/wire_common.h"

#define QUERY_BINDING_VERSION 1
#define QUERY_TOKEN_LENGTH 64

typedef struct PgQueryJobOwner
{
    MemoryContext context;
    MemoryContextCallback cleanup;
    ResourceOwner resources;
    bool remembered;
    pgsocket socket_fd;
    bool external_fd;
    bool ended;
    char *socket_path;
    char token[QUERY_TOKEN_LENGTH + 1];
    int flow_count;
    int ended_flows;
    struct PgQueryJobOwner *next;
} PgQueryJobOwner;

struct PgQueryJobFlow
{
    PgQueryJobOwner *owner;
    int ordinal;
    bool ended;
};

static PgQueryJobOwner *query_owners;
static void query_resource_release(Datum value);
static const ResourceOwnerDesc query_resource = {
    .name = "SemLoom query control",
    .release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
    .release_priority = RELEASE_PRIO_FIRST,
    .ReleaseResource = query_resource_release,
};

static void
query_close(PgQueryJobOwner *owner)
{
    owner->ended = true;
    semloom_uds_close_socket(&owner->socket_fd, &owner->external_fd);
    memset(owner->token, 0, sizeof(owner->token));
    if (owner->remembered)
    {
        ResourceOwnerForget(owner->resources, PointerGetDatum(owner), &query_resource);
        owner->remembered = false;
    }
}

static void
query_resource_release(Datum value)
{
    PgQueryJobOwner *owner = (PgQueryJobOwner *) DatumGetPointer(value);
    /* The resource owner already removed this item; cleanup must not throw or wait. */
    owner->remembered = false;
    query_close(owner);
}

static void
query_memory_release(void *value)
{
    PgQueryJobOwner *owner = value;
    PgQueryJobOwner **link = &query_owners;
    query_close(owner);
    while (*link != NULL && *link != owner)
        link = &(*link)->next;
    if (*link == owner)
        *link = owner->next;
}

PgQueryJobFlow *
pg_query_job_register(MemoryContext context, const char *socket_path)
{
    PgQueryJobOwner *owner;
    PgQueryJobFlow *flow;
    for (owner = query_owners; owner != NULL; owner = owner->next)
        if (owner->context == context)
            break;
    if (owner == NULL)
    {
        owner = MemoryContextAllocZero(context, sizeof(*owner));
        owner->context = context;
        owner->resources = CurrentResourceOwner;
        owner->socket_fd = PGINVALID_SOCKET;
        owner->socket_path = MemoryContextStrdup(context, socket_path);
        owner->cleanup.func = query_memory_release;
        owner->cleanup.arg = owner;
        MemoryContextRegisterResetCallback(context, &owner->cleanup);
        owner->next = query_owners;
        query_owners = owner;
    }
    if (owner->ended || owner->socket_fd != PGINVALID_SOCKET ||
        owner->flow_count == PG_INT32_MAX || strcmp(owner->socket_path, socket_path) != 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("SemLoom query flow registration is no longer available")));
    flow = MemoryContextAllocZero(context, sizeof(*flow));
    flow->owner = owner;
    flow->ordinal = owner->flow_count++;
    return flow;
}

static AiProviderStatus
query_reply(pgsocket fd, const char *request, const char *kind, bool with_token,
            PgQueryJobOwner *owner, AiProviderError *error)
{
    char *payload;
    Jsonb *message;
    JsonbValue *token;
    int32 version;
    bool matches;
    int i;
    AiProviderStatus status = semloom_wire_common_send_frame(fd, request, strlen(request), error);
    if (status != AI_PROVIDER_STATUS_OK)
        return status;
    status = semloom_wire_common_receive_frame(fd, &payload, error);
    if (status != AI_PROVIDER_STATUS_OK)
        return status;
    status = semloom_wire_common_parse_json_unique(payload, &message, error);
    pfree(payload);
    if (status != AI_PROVIDER_STATUS_OK)
        return status;
    if (JB_ROOT_IS_OBJECT(message) && JB_ROOT_COUNT(message) == 3 &&
        semloom_wire_common_json_int32(message, "binding_version", &version, error) &&
        version == QUERY_BINDING_VERSION &&
        semloom_wire_common_json_string_equals(message, "type", "query_error", &matches, error) && matches)
    {
        bool capacity = false;
        semloom_wire_common_json_string_equals(message, "code", "QUERY_CAPACITY", &capacity, error);
        pfree(message);
        semloom_provider_error_set(error, capacity ? AI_PROVIDER_ERROR_RESOURCE_EXHAUSTED : AI_PROVIDER_ERROR_PROTOCOL,
                                  0, 0, capacity ? "SemLoom query exceeds service registration capacity" :
                                  "SemLoom query membership was rejected");
        return AI_PROVIDER_STATUS_ERROR;
    }
    if (!JB_ROOT_IS_OBJECT(message) || JB_ROOT_COUNT(message) != (with_token ? 3 : 2) ||
        !semloom_wire_common_json_int32(message, "binding_version", &version, error) ||
        version != QUERY_BINDING_VERSION ||
        !semloom_wire_common_json_string_equals(message, "type", kind, &matches, error) || !matches)
        goto invalid;
    if (with_token)
    {
        if (!semloom_wire_common_json_value(message, "token", &token, error) ||
            token->type != jbvString || token->val.string.len != QUERY_TOKEN_LENGTH)
            goto invalid;
        for (i = 0; i < QUERY_TOKEN_LENGTH; i++)
            if (!((token->val.string.val[i] >= '0' && token->val.string.val[i] <= '9') ||
                  (token->val.string.val[i] >= 'a' && token->val.string.val[i] <= 'f')))
                goto invalid;
        memcpy(owner->token, token->val.string.val, QUERY_TOKEN_LENGTH);
        owner->token[QUERY_TOKEN_LENGTH] = '\0';
    }
    pfree(message);
    return AI_PROVIDER_STATUS_OK;
invalid:
    pfree(message);
    semloom_provider_error_set(error, AI_PROVIDER_ERROR_PROTOCOL, 0, 0,
                              "SemLoom query membership response is invalid");
    return AI_PROVIDER_STATUS_ERROR;
}

AiProviderStatus
pg_query_job_prepare(PgQueryJobFlow *flow, AiProviderError *error)
{
    PgQueryJobOwner *owner = flow->owner;
    char request[256];
    AiProviderStatus status;
    if (owner->ended || flow->ended)
    {
        semloom_provider_error_set(error, AI_PROVIDER_ERROR_PROTOCOL, 0, 0,
                                  "SemLoom query has ended");
        return AI_PROVIDER_STATUS_ERROR;
    }
    if (owner->socket_fd == PGINVALID_SOCKET)
    {
        /* Remember before I/O: PG errors or interrupts during connect must close the control. */
        ResourceOwnerEnlarge(owner->resources);
        ResourceOwnerRemember(owner->resources, PointerGetDatum(owner), &query_resource);
        owner->remembered = true;
        status = semloom_uds_connect_socket(owner->socket_path, &owner->socket_fd,
                                           &owner->external_fd, error);
        if (status != AI_PROVIDER_STATUS_OK)
            return status;
        snprintf(request, sizeof(request),
                 "{\"type\":\"query_open\",\"binding_version\":%d,\"flow_count\":%d}",
                 QUERY_BINDING_VERSION, owner->flow_count);
        status = query_reply(owner->socket_fd, request, "query_opened", true, owner, error);
        if (status != AI_PROVIDER_STATUS_OK)
            return status;
    }
    return AI_PROVIDER_STATUS_OK;
}

AiProviderStatus
pg_query_job_join(PgQueryJobFlow *flow, pgsocket stream, AiProviderError *error)
{
    PgQueryJobOwner *owner = flow->owner;
    char request[256];
    if (owner->ended || flow->ended || owner->token[0] == '\0')
    {
        semloom_provider_error_set(error, AI_PROVIDER_ERROR_PROTOCOL, 0, 0,
                                  "SemLoom query flow is not available");
        return AI_PROVIDER_STATUS_ERROR;
    }
    snprintf(request, sizeof(request),
             "{\"type\":\"stream_join\",\"binding_version\":%d,\"token\":\"%s\",\"flow\":%d}",
             QUERY_BINDING_VERSION, owner->token, flow->ordinal);
    return query_reply(stream, request, "stream_joined", false, owner, error);
}

void
pg_query_job_flow_end(PgQueryJobFlow *flow)
{
    PgQueryJobOwner *owner;
    if (flow == NULL || flow->ended)
        return;
    flow->ended = true;
    owner = flow->owner;
    if (!owner->ended && ++owner->ended_flows == owner->flow_count)
        query_close(owner);
}
