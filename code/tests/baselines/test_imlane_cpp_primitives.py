"""Compile actual bridge primitives with existing Arrow; no author/JSON/HTTP execution.

Explicit paths avoid changing the host environment:
IMLANE_CPP_TEST_ARROW_INCLUDE and IMLANE_CPP_TEST_ARROW_LIB select an existing
Arrow header/library pair. Source extraction compiles the real functions; the
only author stand-in is the TableConverter template declaration. The whole
bridge and author CPython/Ray qualification remain separate.
"""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[2] / 'integrations/sema/imlane_http.cpp'
HEADER = r"""#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <unordered_set>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
namespace IMLane::DBEnd { template<class In, class Out> class TableConverter; }
"""

BODY = r"""Table make_table(const std::string& id, int64_t row, const std::string& wire, bool null_result=false) {
    arrow::StringBuilder ids, results; arrow::Int64Builder rows;
    if (!ids.Append(id).ok() || !rows.Append(row).ok()) throw std::runtime_error("fixture Arrow input failed");
    if (!(null_result ? results.AppendNull() : results.Append(wire)).ok()) throw std::runtime_error("fixture Arrow result failed");
    auto a=ids.Finish().ValueOrDie(), b=rows.Finish().ValueOrDie(), c=results.Finish().ValueOrDie();
    return arrow::Table::Make(arrow::schema({arrow::field("call_id", arrow::utf8()),
        arrow::field("row_id", arrow::int64()), arrow::field("native_outcome", arrow::utf8())}), {a,b,c});
}
template<class F> void rejected(F&& operation) {
    bool rejected=false;
    try { operation(); } catch (const std::exception&) { rejected=true; }
    if (!rejected) throw std::runtime_error("finite fixture did not reject invalid input");
}
std::string outcome(Table table) {
    return std::static_pointer_cast<arrow::StringScalar>(table->column(2)->GetScalar(0).ValueOrDie())->ToString();
}
int main(int argc,char** argv) {
    if(argc!=3 || chdir(argv[1])) return 2;
    std::string scope(argv[2]);
    if (scope == "copy") {
        IMLane::DBEnd::TableConverter<arrow::Table,Table> converter;
        TaskMetrics metrics; metrics.output_wire_limit=1024;
        Table saved;
        {
            TaskMetricsScope scope(metrics);
            for(int i=0;i<3;++i) {
                const std::string wire="original-output-"+std::to_string(i);
                auto lane=make_table("repeated-source",7,wire);
                Table copied;
                converter.ConvertFromExchange(lane,copied);
                auto raw=std::static_pointer_cast<arrow::StringArray>(lane->column(2)->chunk(0));
                auto data=raw->value_data()->mutable_data();
                if (!data) throw std::runtime_error("fixture Lane is not mutable");
                std::fill(data,data+wire.size(),'x');
                if(outcome(copied)!=wire) throw std::runtime_error("copied output retained Lane storage");
                if(i==0) saved=copied;
            }
            if(outcome(saved)!="original-output-0" || metrics.output_copies!=3)
                throw std::runtime_error("old output changed after repeated converter use");
            metrics.output_wire_limit=4;
            rejected([&]{Table out; converter.ConvertFromExchange(make_table("id",0,"oversize"),out);});
            metrics.output_wire_limit=1024;
            rejected([&]{Table out; converter.ConvertFromExchange(make_table(std::string(4097,'i'),0,"a"),out);});
            rejected([&]{Table out; converter.ConvertFromExchange(make_table("id",-1,"a"),out);});
            rejected([&]{Table out; converter.ConvertFromExchange(make_table("id",0,"",true),out);});
        }
        if(active_task_metrics) throw std::runtime_error("task metrics escaped their scope");
    }
    if (scope == "read") {
        File input(std::tmpfile(),std::fclose);
        std::fwrite("abc\nxyz",1,7,input.get()); std::rewind(input.get());
        std::string line;
        if(!read_bounded_line(input.get(),line,3) || line!="abc" ||
           !read_bounded_line(input.get(),line,3) || line!="xyz" ||
           read_bounded_line(input.get(),line,3)) throw std::runtime_error("bounded reading altered bytes or EOF");
        std::rewind(input.get());
        rejected([&]{read_bounded_line(input.get(),line,2);});
        if(line.size()!=2) throw std::runtime_error("reader allocated beyond its allowance");
    }
    if (scope == "file") {
        BridgeLimits limits;
        auto zero=std::string(32,'0'), one=std::string(32,'1'), two=std::string(32,'2'), three=std::string(32,'3');
        auto directory=[](const std::string& id){return "batch-"+id;};
        if(mkdir(directory(zero).c_str(),0700)) throw std::runtime_error("fixture directory creation failed");
        std::ofstream input(directory(zero)+"/input.jsonl"); input<<"ab\n"; input.close();
        if(!input) throw std::runtime_error("fixture input write failed");
        limits.max_input_wire_bytes=3;
        auto stream=batch_input(zero,directory(zero)+"/input.jsonl",limits); stream.reset();
        rejected([&]{batch_input(zero,"../input.jsonl",limits);});
        limits.max_input_wire_bytes=2;
        rejected([&]{batch_input(zero,directory(zero)+"/input.jsonl",limits);});
        limits.max_input_wire_bytes=3;
        if(symlink(directory(zero).c_str(),directory(one).c_str())) throw std::runtime_error("fixture symlink creation failed");
        rejected([&]{batch_input(one,directory(one)+"/input.jsonl",limits);});
        if(mkdir(directory(two).c_str(),0700)) throw std::runtime_error("fixture directory creation failed");
        if(symlink(("../"+directory(zero)+"/input.jsonl").c_str(),(directory(two)+"/input.jsonl").c_str()))
            throw std::runtime_error("fixture symlink creation failed");
        rejected([&]{batch_input(two,directory(two)+"/input.jsonl",limits);});
        if(mkdir(directory(three).c_str(),0700) || mkfifo((directory(three)+"/input.jsonl").c_str(),0600))
            throw std::runtime_error("fixture FIFO creation failed");
        rejected([&]{batch_input(three,directory(three)+"/input.jsonl",limits);});
    }
    std::cout << "PASS " << scope << std::endl;
}
"""


class IMLaneCppPrimitivesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        include = os.environ.get('IMLANE_CPP_TEST_ARROW_INCLUDE')
        library = os.environ.get('IMLANE_CPP_TEST_ARROW_LIB')
        compiler = shutil.which(os.environ.get('IMLANE_CPP_TEST_CXX', 'clang++'))
        if os.name != 'posix' or not include or not library or not compiler:
            raise unittest.SkipTest('explicit existing Arrow headers/library and a POSIX C++ compiler are required')
        if not (Path(include) / 'arrow/api.h').is_file() or not Path(library).is_dir():
            raise unittest.SkipTest('selected existing Arrow header/library paths are unavailable')
        cls.temporary = tempfile.TemporaryDirectory(prefix='imlane-cpp-primitives-')
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(cls.temporary.name)
        cls.binary = cls.root / 'primitives'
        source = SOURCE.read_text()
        converter = source[source.index('using Clock ='):source.index('namespace dbend =')]
        reader = source[source.index('bool read_bounded_line('):source.index('struct Call {')]
        files = source[source.index('using File ='):source.index('void write_frame(')]
        cpp = cls.root / 'primitives.cpp'
        cpp.write_text(HEADER + converter + reader
            + 'struct BridgeLimits { size_t max_input_wire_bytes=48*1024*1024; };\n' + files + BODY)
        result = subprocess.run([compiler, '-std=c++17', '-O0', '-I' + include,
            '-L' + library, '-Wl,-rpath,' + library, str(cpp), '-larrow', '-o', str(cls.binary)],
            capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise AssertionError('existing Arrow primitive compilation failed:\n' + result.stdout + result.stderr)

    def run_scope(self, scope):
        root = self.root / scope
        root.mkdir()
        result = subprocess.run([str(self.binary), str(root), scope],
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.strip(), 'PASS ' + scope)

    def test_real_arrow_copy_owns_output_after_lane_reuse(self):
        self.run_scope('copy')

    def test_bounded_reader_retains_bytes_and_rejects_overflow(self):
        self.run_scope('read')

    def test_fixed_batch_paths_reject_symlinks_and_nonregular_files(self):
        self.run_scope('file')


if __name__ == '__main__':
    unittest.main()
