from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

_IMPORTER_PATH = CODE_ROOT / "scripts" / "data" / "import_squad_workload.py"
_spec = importlib.util.spec_from_file_location("import_squad_workload", _IMPORTER_PATH)
importer = importlib.util.module_from_spec(_spec)
sys.modules["import_squad_workload"] = importer  # register before exec so @dataclass resolves
_spec.loader.exec_module(importer)


def _dev(qas):
    return {
        "version": "1.1",
        "data": [{"title": "t", "id": "p", "paragraphs": [{"context": "CTX", "qas": qas}]}],
    }


class SquadImportParseTests(unittest.TestCase):
    def test_parse_preserves_multi_answer_array(self) -> None:
        parsed = _dev([{
            "id": "q1", "question": "Q1?",
            "answers": {"answer_start": [0, 0, 0], "text": ["a1", "a2", "a3"]},
        }])
        rows = importer.parse_squad_dev(parsed)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].reference_answers, ("a1", "a2", "a3"))
        self.assertEqual(rows[0].source_example_id, "q1")

    def test_parse_preserves_special_characters_and_newlines(self) -> None:
        context = "Café résumé 🎉 with\ntwo\nlines"
        question = "What is 'αβγ' here?"
        parsed = {
            "version": "1.1",
            "data": [{
                "title": "t", "id": "p",
                "paragraphs": [{
                    "context": context,
                    "qas": [{"id": "q9", "question": question,
                             "answers": {"answer_start": [0], "text": ["α"]}}],
                }],
            }],
        }
        rows = importer.parse_squad_dev(parsed)
        self.assertIn(context, rows[0].prompt)
        self.assertIn(question, rows[0].prompt)
        self.assertEqual(rows[0].reference_answers, ("α",))

    def test_parse_rejects_duplicate_qa_id(self) -> None:
        parsed = _dev([
            {"id": "dup", "question": "a?", "answers": {"answer_start": [0], "text": ["x"]}},
            {"id": "dup", "question": "b?", "answers": {"answer_start": [0], "text": ["y"]}},
        ])
        with self.assertRaisesRegex(ValueError, "duplicate SQuAD qa id"):
            importer.parse_squad_dev(parsed)

    def test_parse_rejects_missing_reference_answers(self) -> None:
        parsed = _dev([{"id": "q1", "question": "Q?", "answers": {"answer_start": [], "text": []}}])
        with self.assertRaisesRegex(ValueError, "no reference answers"):
            importer.parse_squad_dev(parsed)


class SquadImportHashTests(unittest.TestCase):
    def _row(self, qa_id: str, ref: str = "a1"):
        return importer.SquadRow(
            source_example_id=qa_id, context="CTX", question="Q?",
            reference_answers=(ref,), prompt=importer.build_prompt("CTX", "Q?"),
        )

    def test_content_hash_stable_and_order_invariant(self) -> None:
        rows_a = [self._row("q1"), self._row("q2")]
        rows_b = [self._row("q2"), self._row("q1")]  # different order
        self.assertEqual(importer.compute_content_hash(rows_a), importer.compute_content_hash(rows_b))

    def test_content_hash_changes_when_reference_changes(self) -> None:
        rows_a = [self._row("q1", "a1")]
        rows_b = [self._row("q1", "a2")]
        self.assertNotEqual(importer.compute_content_hash(rows_a), importer.compute_content_hash(rows_b))


class SquadImportCountGateTests(unittest.TestCase):
    def test_exact_count_passes(self) -> None:
        importer._validate_dev_count(10570)

    def test_too_few_raises(self) -> None:
        with self.assertRaisesRegex(SystemExit, "expected exactly 10570"):
            importer._validate_dev_count(10569)

    def test_too_many_raises(self) -> None:
        with self.assertRaisesRegex(SystemExit, "expected exactly 10570"):
            importer._validate_dev_count(10571)


class SquadImportTemplateTests(unittest.TestCase):
    def test_build_prompt_is_exact_locked_template(self) -> None:
        prompt = importer.build_prompt("CXT", "QST")
        self.assertEqual(
            prompt,
            "Answer the question using only the context.\n"
            "Return only the shortest answer span. Do not explain.\n"
            "\n"
            "Context:\n"
            "CXT\n"
            "\n"
            "Question:\n"
            "QST\n"
            "\n"
            "Answer:\n",
        )
        self.assertTrue(importer.prompt_template_hash())


if __name__ == "__main__":
    unittest.main()
