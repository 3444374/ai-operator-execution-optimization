"""Original SemBench Movie queries on one verified finite relation.

The upstream query owns filtering, LIMIT and COUNT. The existing LOTUS executors
only replace its complete uncached LM batch. Preflight verifies labels, while
quality evaluation follows consumer EOF; occurrence IDs never replace review IDs.
"""

from dataclasses import dataclass
import importlib.metadata
import json
from pathlib import Path

from src.baselines.common.private_artifacts import write_private_json
from src.baselines.text.sembench_movie import (
    SEMBENCH_COMMIT,
    classification_audit,
    evaluate_original,
    run_original_lotus_query,
    verify_checkout,
)
from .query_inputs import QueryInputs
from .query_workloads import file_identity, load_manifest, movie_csv_examples, read_prepared


MOVIE_MANIFEST_DIGEST = "e753baa59f487203ad3148653d15726311c992dc742ae97d6d7825ab5acca4e2"
MOVIE_CSV_DIGEST = "ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf"
LOTUS_MOVIE_ARMS = (
    "lotus-adapted-native",
    "lotus-method-native-daft",
    "lotus-method-native-ray-data",
    "lotus-method-semloom",
)
MOVIE_VERSIONS = {
    "lotus-ai": "1.2.4",
    "pandas": "2.3.3",
    "duckdb": "1.5.4",
    "scipy": "1.17.1",
    "litellm": "1.95.0",
}


@dataclass(frozen=True)
class MovieDataset:
    checkout: Path
    manifest: Path
    csv: Path | None = None
    expected_manifest_digest: str = MOVIE_MANIFEST_DIGEST
    role: str = "original_movie_sf2000"

    def __post_init__(self):
        for name in ("checkout", "manifest", "csv"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, Path(value).resolve())
        if self.role not in ("original_movie_sf2000", "software_fixture"):
            raise ValueError("Movie input must have an explicit benchmark or software-fixture role")
        digest = self.expected_manifest_digest
        if (
            type(digest) is not str
            or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)
        ):
            raise ValueError("Movie input requires its complete manifest content digest")
        if self.role == "original_movie_sf2000" and (
            digest != MOVIE_MANIFEST_DIGEST or self.csv is None
        ):
            raise ValueError("the original Movie task requires the selected CSV and manifest")

    def load_rows(self):
        """Only raw fields cross the query/method entry; no reference labels."""
        manifest = load_manifest(self.manifest)
        if (
            manifest["kind"] != "movie"
            or manifest["sha256"] != self.expected_manifest_digest
            or manifest["rows"] > 4096
        ):
            raise ValueError("Movie relation differs from its selected identity or row bound")
        spec = QueryInputs(
            "movie",
            "input_table",
            manifest["rows"],
            manifest["max_source_bytes"],
            manifest["max_input_bytes"],
        )
        values, seen = [], set()
        for position, raw in enumerate(read_prepared(self.manifest, "raw.jsonl")):
            spec.convert(raw)
            if (
                len(raw) != 5
                or raw[0] != position
                or raw[1] != "movie-row-" + str(position)
                or raw[1] in seen
                or not isinstance(raw[4], str)
            ):
                raise ValueError("Movie source position or independent occurrence identity differs")
            values.append(dict(row_id=raw[1], movie_id=raw[2], text=raw[3], review_id=raw[4]))
            seen.add(raw[1])
        return values

    def references(self, rows):
        """Validate label association using occurrences, including repeated reviewId."""
        references = list(read_prepared(self.manifest, "references.jsonl"))
        if len(references) != len(rows):
            raise ValueError("Movie labels do not cover the source relation")
        labels = {}
        for row, reference in zip(rows, references):
            if (
                set(reference) != {"row_id", "review_id", "movie_id", "reference"}
                or reference["row_id"] != row["row_id"]
                or reference["review_id"] != row["review_id"]
                or reference["movie_id"] != row["movie_id"]
                or reference["reference"] not in ("POSITIVE", "NEGATIVE")
            ):
                raise ValueError("Movie label occurrence or source fields differ")
            labels[row["row_id"]] = reference["reference"]
        return labels

    def preflight(self, *, require_libraries=True):
        """Read-only source/data check; callers run this before creating runtimes."""
        source = verify_checkout(self.checkout)
        versions = {}
        if require_libraries:
            for name, expected in MOVIE_VERSIONS.items():
                actual = importlib.metadata.version(name)
                if actual != expected:
                    raise ValueError(f"{name} version differs from the selected Movie profile")
                versions[name] = actual
        rows = self.load_rows()
        labels = self.references(rows)
        manifest = load_manifest(self.manifest)
        provenance = manifest["provenance"]
        if self.role == "original_movie_sf2000":
            if (
                manifest["rows"] != 2000
                or provenance.get("sembench_commit") != SEMBENCH_COMMIT
                or provenance.get("scale_factor") != 2000
                or provenance.get("csv", {}).get("sha256") != MOVIE_CSV_DIGEST
                or file_identity(self.csv) != provenance["csv"]
            ):
                raise ValueError("original Movie preparation provenance or CSV identity differs")
            examples = list(movie_csv_examples(self.csv))
            expected = [
                (r["row_id"], r["movie_id"], r["text"], labels[r["row_id"]], r["review_id"])
                for r in rows
            ]
            if examples != expected:
                raise ValueError(
                    "prepared Movie fields/labels differ from the selected upstream CSV"
                )
        elif provenance.get("source") != "synthetic SemBench software fixture":
            raise ValueError("fixture input cannot be presented as original benchmark data")
        relevant = [
            "src/scenario/movie/runner/lotus_runner/lotus_runner.py",
            "src/scenario/movie/evaluation/evaluate.py",
            "src/runner/generic_runner.py",
            "src/evaluator/generic_evaluator.py",
        ]
        relevant += [f"files/movie/query/gold_sql/Q{q}.sql" for q in (1, 2, 3)]
        taken = [r for r in rows if r["movie_id"] == "taken_3"]
        return dict(
            schema="semloom.sembench_movie_preflight.v1",
            status="passed",
            sembench_commit=SEMBENCH_COMMIT,
            upstream_lotus_version="1.1.3",
            selected_lotus_version="1.2.4",
            versions=versions,
            libraries_verified=require_libraries,
            role=self.role,
            manifest_content_digest=manifest["sha256"],
            manifest_file=file_identity(self.manifest),
            files=manifest["files"],
            csv=file_identity(self.csv) if self.csv is not None else None,
            source_files={name: file_identity(source / name) for name in relevant},
            relation_rows=len(rows),
            independent_row_ids=len(labels),
            unique_review_ids=len({r["review_id"] for r in rows}),
            duplicate_review_id_occurrences=len(rows) - len({r["review_id"] for r in rows}),
            movie_ids=len({r["movie_id"] for r in rows}),
            positive_rows=sum(v == "POSITIVE" for v in labels.values()),
            taken_3_rows=len(taken),
            taken_3_positive_rows=sum(labels[r["row_id"]] == "POSITIVE" for r in taken),
            cache=False,
            retries=0,
            max_tokens=8,
            stop=["\n"],
            source_scope="finite external prepared relation; no PostgreSQL execution claim",
        )


@dataclass(frozen=True)
class MovieQuery:
    dataset: MovieDataset
    query_id: int

    def __post_init__(self):
        if (
            type(self.dataset) is not MovieDataset
            or type(self.query_id) is not int
            or self.query_id not in (1, 2, 3)
        ):
            raise ValueError("only original Movie Q1/Q2/Q3 are supported")

    def model_rows(self, rows):
        return rows if self.query_id == 1 else [r for r in rows if r["movie_id"] == "taken_3"]

    def check_executor_shape(self, arm, rows):
        if arm not in LOTUS_MOVIE_ARMS:
            raise ValueError("original Movie query requires an explicitly supported LOTUS executor")
        count = len(self.model_rows(rows))
        return count


class MovieQueryResult:
    """Per-query original relation result and occurrence audit; no scheduler."""

    def __init__(self, query, rows, root):
        self.query, self.rows, self.root = query, rows, Path(root)
        self.model_rows = query.model_rows(rows)
        self.decisions, self.prompts, self.result_rows = [], [], []
        self.result = None

    def execute(self):
        import pandas as pd
        from src.baselines.text.frameworks.lotus_pg import observe_filter_rows

        frame = pd.DataFrame(
            [(r["review_id"], r["movie_id"], r["text"], r["row_id"]) for r in self.rows],
            columns=["reviewId", "id", "reviewText", "_execution_row_id"],
        )
        with observe_filter_rows(self.decisions.extend, self.prompts.extend):
            self.result = run_original_lotus_query(
                self.query.dataset.checkout, self.query.query_id, lambda: frame
            )
        expected_ids = [r["row_id"] for r in self.model_rows]
        if (
            [r[0] for r in self.decisions] != expected_ids
            or any(type(v) is not bool for _, v in self.decisions)
            or [p["row_id"] for p in self.prompts] != expected_ids
        ):
            raise ValueError(
                "original Filter decisions/prompts do not cover independent input occurrences"
            )
        kept = [identity for identity, value in self.decisions if value]
        if self.query.query_id == 3:
            if (
                list(self.result.columns) != ["positive_review_cnt"]
                or len(self.result) != 1
                or int(self.result.iloc[0, 0]) != len(kept)
            ):
                raise ValueError("original COUNT differs from parsed Filter decisions")
            self.result_rows = [("positive_review_cnt", str(int(self.result.iloc[0, 0])))]
        else:
            source_ids = {r["row_id"]: r["review_id"] for r in self.rows}
            returned = list(self.result.iloc[:, 0]) if len(self.result.columns) else []
            if returned != [source_ids[key] for key in kept[:5]]:
                raise ValueError(
                    "original LIMIT/projected reviewId differs from retained occurrences"
                )
            self.result_rows = [(key, source_ids[key]) for key in kept[:5]]
        write_private_json(
            self.root / "original-query-result.json",
            dict(
                query_id=self.query.query_id,
                dataframe=json.loads(self.result.to_json(orient="split")),
                row_occurrences=[r[0] for r in self.result_rows]
                if self.query.query_id != 3
                else None,
                result_scope="unchanged upstream projection or aggregate; recorder pairs are a separate audit",
            ),
        )
        write_private_json(self.root / "filter-decisions.json", self.decisions)
        write_private_json(self.root / "filter-prompts.json", self.prompts)
        yield from self.result_rows

    def quality(self):
        import pandas as pd
        from .query_evaluation import json_metric

        labels = self.query.dataset.references(self.rows)
        labeled = pd.DataFrame(
            [(r["review_id"], r["movie_id"], r["text"], labels[r["row_id"]]) for r in self.rows],
            columns=["reviewId", "id", "reviewText", "scoreSentiment"],
        )
        metric = evaluate_original(
            self.query.dataset.checkout, self.query.query_id, self.result, labeled
        )
        selected_labels = {r["row_id"]: labels[r["row_id"]] for r in self.model_rows}
        metric["classification_audit"] = classification_audit(
            selected_labels,
            [(key, "POSITIVE" if value else "NEGATIVE") for key, value in self.decisions],
        )
        metric.update(
            dataset_role=self.query.dataset.role,
            audit_scope="parsed native Filter decisions on every eligible occurrence; before LIMIT",
            quality_equivalence_proven=False,
        )
        return json_metric(metric)
