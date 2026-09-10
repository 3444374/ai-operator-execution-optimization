"""Preserve upstream duplicate review IDs and raw row occurrences separately."""
import csv
from pathlib import Path
import tempfile
import unittest

from src.experiments.postgresql.query_workloads import prepare,movie_csv_examples,read_prepared


class QueryWorkloadTests(unittest.TestCase):
    def test_duplicate_original_ids_keep_both_occurrences_and_labels(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'Reviews.csv'
            with source.open('w',newline='') as stream:
                writer=csv.writer(stream);writer.writerow(['reviewId','id','reviewText','scoreSentiment'])
                writer.writerows([['same','film','review','POSITIVE'],['same','film','review','POSITIVE']])
            prepare(root/'prepared','movie',movie_csv_examples(source),{},max_rows=2)
            manifest=root/'prepared/manifest.json'
            raw=list(read_prepared(manifest,'raw.jsonl'));refs=list(read_prepared(manifest,'references.jsonl'))
            self.assertEqual([r[4] for r in raw],['same','same'])
            self.assertEqual([r[1] for r in raw],['movie-row-0','movie-row-1'])
            self.assertEqual([r['review_id'] for r in refs],['same','same'])
            self.assertEqual(raw[0][2:],raw[1][2:])
