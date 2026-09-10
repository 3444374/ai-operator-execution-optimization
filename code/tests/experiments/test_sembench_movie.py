"""Supplemental audits cannot hide canceling errors, duplicates or bad outputs."""
import unittest

from src.baselines.text.sembench_movie import classification_audit


class MovieAuditTests(unittest.TestCase):
    def test_exact_count_does_not_hide_opposite_classification_errors(self):
        report=classification_audit({'a':'POSITIVE','b':'NEGATIVE'},[('a','NEGATIVE'),('b','POSITIVE')])
        self.assertEqual(report['count_error_from_classifications'],0)
        self.assertEqual(report['false_positive'],1)
        self.assertEqual(report['false_negative'],1)

    def test_invalid_positive_and_missing_results_are_visible(self):
        labels={'a':'POSITIVE','b':'NEGATIVE'}
        report=classification_audit(labels,[('a','maybe')],require_complete=False)
        self.assertEqual(report['invalid'],1)
        self.assertEqual(report['missing_rows'],1)
        self.assertEqual(report['count_error_from_classifications'],-1)
        with self.assertRaises(ValueError):
            classification_audit(labels,[('a','POSITIVE')])
        with self.assertRaises(ValueError):
            classification_audit(labels,[('a','POSITIVE'),('a','POSITIVE')])
