"""PG18.3 ordinary-predicate statistics check; reads no model payload."""
import argparse
import hashlib
import json
import os
from pathlib import Path

import psycopg

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--manifest", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
manifest = json.loads(args.manifest.read_text())
rows = [(r["doc_id"], r["split"], r["cell"]) for r in manifest["rows"]]
assert len(rows) == 1216
query = "SELECT doc_id FROM calibration_inputs WHERE split='warmup' AND cell=0"
with psycopg.connect(os.environ["SEMLOOM_QUAL_DSN"], autocommit=True) as connection:
    assert connection.execute("SHOW server_version").fetchone()[0] == "18.3"
    connection.execute("CREATE TABLE calibration_inputs(doc_id bigint,split text,cell integer)")
    with connection.cursor().copy("COPY calibration_inputs FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
    connection.execute("ANALYZE calibration_inputs")
    counts = connection.execute("SELECT count(*),count(*) FILTER(WHERE split='warmup'),"
        "count(*) FILTER(WHERE cell=0),count(*) FILTER(WHERE split='warmup' AND cell=0) FROM calibration_inputs").fetchone()
    assert counts == (1216,64,160,64)
    before = connection.execute("EXPLAIN (ANALYZE,FORMAT JSON) " + query).fetchone()[0]
    connection.execute("CREATE STATISTICS calibration_split_cell (mcv,dependencies) ON split,cell FROM calibration_inputs")
    connection.execute("ANALYZE calibration_inputs")
    after = connection.execute("EXPLAIN (ANALYZE,FORMAT JSON) " + query).fetchone()[0]
    evidence = dict(postgresql_version="18.3", query=query, contains_ai_condition=False,
        manifest_sha256=hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        loaded_fields=["doc_id","split","cell"], held_out_payload_read=False,
        counts=dict(total=counts[0],warmup=counts[1],cell_zero=counts[2],joint=counts[3]),
        before=before,after=after,
        statistics_sql="CREATE STATISTICS calibration_split_cell (mcv, dependencies) ON split, cell FROM calibration_inputs; ANALYZE calibration_inputs;")
    with args.output.open("x") as handle:
        json.dump(evidence,handle,sort_keys=True,indent=2)
        handle.write("\n")
    assert before[0]["Plan"]["Plan Rows"] == 8
    assert before[0]["Plan"]["Actual Rows"] == after[0]["Plan"]["Actual Rows"] == 64
    assert after[0]["Plan"]["Plan Rows"] == 64
    print("PG18.3 ordinary predicate: estimated 8 -> 64; actual 64 -> 64; no AI condition")
