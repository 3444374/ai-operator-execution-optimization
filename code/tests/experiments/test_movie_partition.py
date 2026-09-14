"""Movie grouping and exclusion are independent of labels and source ordering."""
import csv
from pathlib import Path
import tempfile
import unittest
from src.experiments.postgresql.movie_partition import prepare_movie_partitions
from src.experiments.postgresql.query_workloads import read_prepared


def write(path,rows):
    with path.open('w',newline='') as stream:
        writer=csv.writer(stream);writer.writerow(('id','reviewId','reviewText','scoreSentiment'));writer.writerows(rows)


class MoviePartitionTests(unittest.TestCase):
    def test_previous_movies_ids_text_and_duplicates_are_excluded(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'source.csv';prior=root/'prior.csv'
            write(prior,[('old-movie','old-id','old text','POSITIVE')])
            candidates=[(f'movie-{i}',f'id-{i}',f'text {i}','POSITIVE' if i%2 else 'NEGATIVE') for i in range(80)]
            rows=[('old-movie','unused','new text','NEGATIVE'),('new','old-id','other text','NEGATIVE'),
                  ('new','unused2','old text','NEGATIVE'),candidates[0],*candidates]
            write(source,rows)
            report=prepare_movie_partitions(source,prior,root/'a',tuning_rows=8,evaluation_rows=8)
            self.assertEqual(report['counts']['prior_excluded'],3)
            self.assertEqual(report['counts']['duplicate'],1)
            selected={key:list(read_prepared(root/'a'/key/'manifest.json','raw.jsonl')) for key in ('tuning','evaluation')}
            self.assertFalse({r[2] for r in selected['tuning']} & {r[2] for r in selected['evaluation']})
            original={key:{r[4] for r in value} for key,value in selected.items()}
            write(source,[(m,i,t,'NEGATIVE' if l=='POSITIVE' else 'POSITIVE') for m,i,t,l in reversed(rows)])
            prepare_movie_partitions(source,prior,root/'b',tuning_rows=8,evaluation_rows=8)
            for key in original:
                self.assertEqual(original[key],{r[4] for r in read_prepared(root/'b'/key/'manifest.json','raw.jsonl')})
