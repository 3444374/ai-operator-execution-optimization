"""Execute the actual runtime close function against a small resource-ownership harness."""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class QueryJobCleanupTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("cc"), "C compiler is required")
    def test_repeated_close_ends_each_flow_once(self):
        source = (
            Path(__file__).parents[2] / "postgres/semloom_pg/src/executor/pg_semantic_runtime.c"
        ).read_text()
        start = source.index("void\npg_semantic_runtime_close(")
        body_start = source.index("{", start)
        depth = 1
        end = body_start + 1
        while depth:
            depth += (source[end] == "{") - (source[end] == "}")
            end += 1
        function = source[start:end]
        harness = (
            r"""
#include <stdbool.h>
#include <stddef.h>
#include <assert.h>
#define PG_SEMANTIC_RUNTIME_CLOSED 5
typedef struct { int state; bool query_job; void *owner_context; } PgSemanticRuntime;
static int sessions_closed, flows_ended;
static void pg_semantic_runtime_release_session(PgSemanticRuntime *r) { sessions_closed++; }
static void pg_query_job_node_end(void *owner) { flows_ended++; }
"""
            + function
            + r"""
int main(void) {
    PgSemanticRuntime first = {1, true, NULL}, second = {1, true, NULL};
    pg_semantic_runtime_close(&first);
    pg_semantic_runtime_close(&first);
    pg_semantic_runtime_close(NULL);
    assert(flows_ended == 1 && sessions_closed == 1);
    pg_semantic_runtime_close(&second);
    pg_semantic_runtime_close(&second);
    assert(flows_ended == 2 && sessions_closed == 2);
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
