/*
 * resource_client_v3 — parameterized successor of the archived 4D
 * resource_client_v2 (experiments/results/postgresql/
 * semmap_real_model_resource_20260904/raw/semmap_4d_selected_evidence/
 * resource/resource_client_v2.c), which stays untouched as evidence.
 *
 * Identical single-row-mode semantics and wire behavior; the only change
 * is an optional rounds/rows argv so a --diagnostic run actually runs a
 * reduced workload instead of lying about it in the README (the v2
 * client hardcoded 3 rounds x 2000 rows, so the diagnostic runner's
 * expected-values-only override produced fabricated correctness
 * failures while the client still ran full scale).
 *
 * usage: client socket-dir port gateway release-file finish-file
 *                [rounds [rows-per-round]]
 * Defaults preserve the v2 formal identity: 3 rounds x 2000 rows.
 */
#define _POSIX_C_SOURCE 200809L

#include <libpq-fe.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define OUTPUT_BYTES 65536

static void
fail(PGconn *connection, const char *message)
{
    fprintf(stderr, "%s: %s\n", message,
            connection == NULL ? strerror(errno) : PQerrorMessage(connection));
    exit(1);
}

static void
wait_for_file(const char *path, int seconds)
{
    struct timespec delay = {0, 10000000};
    int attempts = seconds * 100;

    while (attempts-- > 0)
    {
        if (access(path, F_OK) == 0)
            return;
        nanosleep(&delay, NULL);
    }
    fprintf(stderr, "timed out waiting for control file\n");
    exit(1);
}

static void
exec_command(PGconn *connection, const char *command)
{
    PGresult *result = PQexec(connection, command);
    if (PQresultStatus(result) != PGRES_COMMAND_OK)
    {
        PQclear(result);
        fail(connection, "command failed");
    }
    PQclear(result);
}

static void
set_gateway(PGconn *connection, const char *gateway)
{
    const char *values[1] = {gateway};
    PGresult *result = PQexecParams(
        connection,
        "SELECT set_config('semloom_pg.gateway_socket',$1,false)",
        1, NULL, values, NULL, NULL, 0);
    if (PQresultStatus(result) != PGRES_TUPLES_OK)
    {
        PQclear(result);
        fail(connection, "could not set gateway socket");
    }
    PQclear(result);
}

static int
run_query(PGconn *connection, const char *where_clause, int expected_rows)
{
    char query[1024];
    int rows = 0;
    int final_seen = 0;
    PGresult *result;

    if (snprintf(
            query, sizeof(query),
            "SELECT ai_semantic.map(payload,'Generate fixed output.',"
            "'{\"model\":\"golden-map-resource-v1\",\"temperature\":0,"
            "\"max_tokens\":128}'::jsonb) FROM ONLY resource_rows %s",
            where_clause) >= (int) sizeof(query))
        fail(NULL, "query buffer overflow");
    if (PQsendQuery(connection, query) != 1)
        fail(connection, "PQsendQuery failed");
    if (PQsetSingleRowMode(connection) != 1)
        fail(connection, "PQsetSingleRowMode failed");

    while ((result = PQgetResult(connection)) != NULL)
    {
        ExecStatusType status = PQresultStatus(result);
        if (status == PGRES_SINGLE_TUPLE)
        {
            const char *value;
            int length;
            int index;

            if (PQntuples(result) != 1 || PQnfields(result) != 1 || PQgetisnull(result, 0, 0))
            {
                PQclear(result);
                fail(connection, "invalid single-row result shape");
            }
            value = PQgetvalue(result, 0, 0);
            length = PQgetlength(result, 0, 0);
            if (length != OUTPUT_BYTES)
            {
                PQclear(result);
                fail(connection, "unexpected output length");
            }
            for (index = 0; index < length; index++)
            {
                if (value[index] != 'y')
                {
                    PQclear(result);
                    fail(connection, "unexpected output byte");
                }
            }
            rows++;
        }
        else if (status == PGRES_TUPLES_OK)
            final_seen++;
        else
        {
            PQclear(result);
            fail(connection, "query returned an error");
        }
        PQclear(result);
    }
    if (rows != expected_rows || final_seen != 1)
        fail(connection, "unexpected row or final-result count");
    return rows;
}

static int
parse_positive(const char *text, const char *what)
{
    char *end = NULL;
    long value = strtol(text, &end, 10);

    if (end == text || *end != '\0' || value <= 0 || value > 1000000)
    {
        fprintf(stderr, "invalid %s: %s\n", what, text);
        exit(2);
    }
    return (int) value;
}

int
main(int argc, char **argv)
{
    char connection_info[1024];
    PGconn *connection;
    int rounds = 3;
    int rows_per_round = 2000;
    int total_rows;
    int round;

    if (argc < 6 || argc > 8)
    {
        fprintf(stderr,
                "usage: client socket-dir port gateway release-file finish-file"
                " [rounds [rows-per-round]]\n");
        return 2;
    }
    if (argc >= 7)
        rounds = parse_positive(argv[6], "rounds");
    if (argc >= 8)
        rows_per_round = parse_positive(argv[7], "rows-per-round");
    total_rows = rounds * rows_per_round;
    if (snprintf(connection_info, sizeof(connection_info),
                 "host=%s port=%s user=postgres dbname=postgres", argv[1], argv[2]) >=
        (int) sizeof(connection_info))
        return 2;
    setvbuf(stdout, NULL, _IOLBF, 0);
    connection = PQconnectdb(connection_info);
    if (PQstatus(connection) != CONNECTION_OK)
        fail(connection, "connection failed");
    exec_command(connection, "SET semloom_pg.provider_execution_profile='golden'");
    exec_command(connection, "SET statement_timeout='300s'");
    set_gateway(connection, argv[3]);

    run_query(connection, "WHERE id=1", 1);
    printf("{\"event\":\"warmup_complete\",\"backend_pid\":%d}\n", PQbackendPID(connection));
    wait_for_file(argv[4], 120);
    for (round = 1; round <= rounds; round++)
    {
        int rows = run_query(connection, "", rows_per_round);
        printf("{\"event\":\"round_complete\",\"round\":%d,\"rows\":%d}\n", round, rows);
    }
    printf("{\"event\":\"all_complete\",\"rows\":%d}\n", total_rows);
    wait_for_file(argv[5], 120);
    PQfinish(connection);
    return 0;
}
