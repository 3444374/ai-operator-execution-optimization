"""Execute the thin driver's CSV sizing/reader expression without an author DSO.

The local Python DuckDB checks the actual reader and raw CSV round trip. It does
not qualify the pinned Linux author library, prediction SQL, IPC or a model.
"""

import csv
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[3] / 'integrations/imlane_duckdb/sembench.cpp'


@unittest.skipUnless(shutil.which('c++') and importlib.util.find_spec('duckdb'),
                     'requires a local C++ compiler and Python DuckDB')
class PreparedCSVInputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import duckdb

        cls.duckdb = duckdb
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        root = Path(cls.temporary.name)
        source = SOURCE.read_text()
        start = source.index('std::string literal(')
        end = source.index('\nauto query(', start)
        harness = root / 'csv_input.cpp'
        harness.write_text('#include <cstdint>\n#include <fstream>\n#include <iostream>\n'
            '#include <limits>\n#include <stdexcept>\n#include <string>\n' + source[start:end] + '''
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    try {
        const auto bytes = csv_maximum_record_bytes(argv[1]);
        std::cout << bytes << "\\n" << prepared_source(argv[1], bytes + 1) << "\\n";
        return 0;
    } catch (const std::exception &error) {
        std::cerr << error.what() << "\\n";
        return 1;
    }
}
''')
        cls.binary = root / 'csv_input'
        compiler = os.environ.get('CXX') or shutil.which('c++')
        subprocess.run([compiler, '-std=c++17', '-O2', '-Wall', '-Wextra', '-Werror', str(harness),
                        '-o', str(cls.binary)], check=True, capture_output=True, text=True, timeout=30)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        # Exercise the production SQL literal quoting as well as CSV quoting.
        self.path = Path(temporary.name) / "input'quoted.csv"
        self.connection = self.duckdb.connect()
        self.addCleanup(self.connection.close)

    @staticmethod
    def csv_record(row):
        stream = io.StringIO(newline='')
        csv.writer(stream).writerow(row)
        return stream.getvalue().encode('utf-8')

    def write_csv(self, rows, *, trailing_newline=True):
        records = [self.csv_record(('call_id', 'messages_json'))]
        records.extend(self.csv_record(row) for row in rows)
        if not trailing_newline:
            records[-1] = records[-1].removesuffix(b'\r\n')
        self.path.write_bytes(b''.join(records))
        return max(map(len, records))

    def reader(self):
        result = subprocess.run([str(self.binary), str(self.path)], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        record_bytes, expression = result.stdout.splitlines()
        record_bytes = int(record_bytes)
        self.assertTrue(expression.endswith('maximum_line_size=' + str(record_bytes + 1) + ')'))
        return record_bytes, expression

    def test_image_message_over_default_limit_is_read_without_changing_any_bytes(self):
        messages = [{'role': 'user', 'content': [{'type': 'image_url',
                     'image_url': {'url': 'data:image/jpeg;base64,'}}]}]
        row = lambda: ('occ-image', json.dumps(messages, ensure_ascii=False))
        messages[0]['content'][0]['image_url']['url'] += 'A' * (2293907 - len(self.csv_record(row())))
        expected = [row()]
        maximum = self.write_csv(expected)
        self.assertEqual(maximum, 2293907)
        measured, expression = self.reader()
        self.assertEqual(measured, maximum)
        old_expression = re.sub(r', maximum_line_size=\d+\)$', ')', expression)
        with self.assertRaisesRegex(self.duckdb.Error, 'Maximum line size'):
            self.connection.execute('SELECT * FROM ' + old_expression).fetchall()
        self.assertEqual(self.connection.execute('SELECT * FROM ' + expression).fetchall(), expected)

    def test_utf8_quotes_and_quoted_record_newlines_use_bytes_not_characters(self):
        rows = [('id,"雪"\ncontinued', json.dumps([{'role': 'user', 'content': '雪,"\n\r\\'}],
                                                    ensure_ascii=False)), ('last', '[]')]
        maximum = self.write_csv(rows, trailing_newline=False)
        self.assertGreater(maximum, max(map(len, self.path.read_bytes().splitlines(keepends=True))))
        measured, expression = self.reader()
        self.assertEqual(measured, maximum)
        self.assertEqual(self.connection.execute('SELECT * FROM ' + expression).fetchall(), rows)

    def test_many_short_records_do_not_turn_file_size_into_a_row_or_buffer_limit(self):
        rows = [(str(i), json.dumps([{'role': 'user', 'content': 'x' * 16384}])) for i in range(300)]
        maximum = self.write_csv(rows)
        self.assertGreater(self.path.stat().st_size, maximum * 100)
        measured, expression = self.reader()
        self.assertEqual(measured, maximum)
        self.assertNotIn('buffer_size', expression)
        self.assertEqual(self.connection.execute('SELECT * FROM ' + expression).fetchall(), rows)

    def test_header_only_and_empty_file_keep_the_original_empty_relation(self):
        for raw in (self.csv_record(('call_id', 'messages_json')), b''):
            with self.subTest(raw=raw):
                self.path.write_bytes(raw)
                measured, expression = self.reader()
                self.assertEqual(measured, len(raw))
                self.assertEqual(self.connection.execute('SELECT * FROM ' + expression).fetchall(), [])

    def test_missing_or_unreadable_source_has_no_default_limit_fallback(self):
        for path in (self.path, self.path.parent):
            with self.subTest(path=path):
                result = subprocess.run([str(self.binary), str(path)], capture_output=True, text=True, timeout=5)
                # Some standard libraries expose a directory stream as EOF;
                # DuckDB still owns and rejects that invalid CSV source.
                if path.is_dir() and result.returncode == 0:
                    _, expression = result.stdout.splitlines()
                    with self.assertRaises(self.duckdb.Error):
                        self.connection.execute('SELECT * FROM ' + expression).fetchall()
                    continue
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, '')
                self.assertIn('prepared CSV for record sizing', result.stderr)

    def test_malformed_csv_is_still_rejected_by_the_reader(self):
        self.path.write_bytes(self.csv_record(('call_id', 'messages_json')) + b'occ,"unterminated\r\n')
        _, expression = self.reader()
        with self.assertRaises(self.duckdb.Error):
            self.connection.execute('SELECT * FROM ' + expression).fetchall()


if __name__ == '__main__':
    unittest.main()
