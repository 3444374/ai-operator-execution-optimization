"""Execute the actual runtime close function against a small resource-ownership harness."""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class QueryJobCleanupTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("cc"), "C compiler is required")
    def test_repeated_close_ends_each_flow_once(self):
        root = Path(__file__).parents[2] / "postgres/semloom_pg/src/executor"

        def extract(name, symbol):
            source = (root / name).read_text()
            start = source.index("void\n" + symbol + "(")
            end = source.index("{", start) + 1
            depth = 1
            while depth:
                depth += (source[end] == "{") - (source[end] == "}")
                end += 1
            return source[start:end]

        function = extract("pg_query_job.c", "pg_query_job_flow_end") + extract(
            "pg_semantic_runtime.c", "pg_semantic_runtime_close"
        )
        harness = (
            r"""
#include <stdbool.h>
#include <stddef.h>
#include <assert.h>
#define PG_SEMANTIC_RUNTIME_CLOSED 5
typedef struct { bool ended; int ended_flows, flow_count; } PgQueryJobOwner;
typedef struct { PgQueryJobOwner *owner; int ordinal; bool ended; } PgQueryJobFlow;
typedef struct { int state; PgQueryJobFlow *query_flow; } PgSemanticRuntime;
static int sessions_closed, flows_ended;
static void pg_semantic_runtime_release_session(PgSemanticRuntime *r) { sessions_closed++; }
static void query_close(PgQueryJobOwner *owner) { owner->ended = true; flows_ended++; }
"""
            + function
            + r"""
int main(void) {
    PgQueryJobOwner owner = {false, 0, 2};
    PgQueryJobFlow a = {&owner, 0, false}, b = {&owner, 1, false};
    PgSemanticRuntime first = {1, &a}, second = {1, &b};
    pg_query_job_flow_end(&a);
    pg_query_job_flow_end(&a);
    assert(owner.ended_flows == 1 && !owner.ended);
    pg_semantic_runtime_close(&first);
    pg_semantic_runtime_close(&first);
    pg_semantic_runtime_close(NULL);
    assert(owner.ended_flows == 1 && sessions_closed == 1);
    pg_semantic_runtime_close(&second);
    pg_semantic_runtime_close(&second);
    pg_query_job_flow_end(&b);
    assert(owner.ended_flows == 2 && sessions_closed == 2 && flows_ended == 1);
    return 0;
}
"""
        )
        with tempfile.TemporaryDirectory() as directory:
            binary = str(Path(directory) / "close-check")
            result = subprocess.run(
                ["cc", "-x", "c", "-o", binary, "-"], input=harness, text=True, capture_output=True
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            completed = subprocess.run([binary], capture_output=True, text=True, cwd=directory)
            self.assertEqual(completed.returncode, 0, completed.stderr)
