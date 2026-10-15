"""Repeated native queries in one owner; actual LOTUS/FAISS with a CPU fixture."""
import pytest

from src.semantic_methods.lotus import image_workflow as workflow
from tests.semantic_methods.test_lotus_image_native import frame, rm


def queries():
    image = frame().iloc[2]["image"]
    return [workflow.RetrievalQuery("blue", image, "blue"),
            workflow.RetrievalQuery("bird", "bird")]


def test_five_rounds_share_owner_and_index_and_keep_all_first_values(tmp_path, rm, monkeypatch):
    from lotus.vector_store import FaissVS

    loads = []
    original = FaissVS.load_index

    def load(self, *args, **kwargs):
        loads.append(id(self))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(FaissVS, "load_index", load)
    phases = []
    result = workflow.run_workflow(frame(), rm, queries(), index_dir=tmp_path / "index",
        embedding_identity={"fixture": True}, k=1, query_repeats=5,
        phase_observer=lambda phase: phases.append((phase, len(loads))))
    records = result["queries"]
    assert phases[0][0] == "after_index_or_register" and phases[1][0] == "after_queries"
    assert phases[0][1] == phases[1][1]
    assert len(records) == 10 and len(rm.observation.calls) == 11
    assert [r["query_id"] for r in records] == [f"{q}@{i}" for i in range(5) for q in ("blue", "bird")]
    assert [r["query_ordinal"] for r in records] == list(range(10))
    assert [r["repeat_index"] for r in records] == [i for i in range(5) for _ in range(2)]
    assert all(r["doc_ids"] == (["blue"] if r["source_query_id"] == "blue" else ["red"]) for r in records)
    assert all(0 <= r["native_call_return_elapsed_s"] <= r["elapsed_s"] for r in records)
    assert all(r["submitted_monotonic_ns"] <= r["returned_monotonic_ns"] for r in records)


def test_new_process_style_registration_keeps_native_first_load_in_query(tmp_path, rm, monkeypatch):
    from lotus.vector_store import FaissVS

    index = tmp_path / "index"
    workflow.run_workflow(frame(), rm, [], index_dir=index,
                         embedding_identity={}, mode="index")
    loads = []
    original = FaissVS.load_index

    def load(self, *args, **kwargs):
        loads.append(id(self))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(FaissVS, "load_index", load)
    phases = []
    result = workflow.run_workflow(frame(), rm, queries(), index_dir=index,
        embedding_identity={}, mode="search", k=1, query_repeats=5,
        phase_observer=lambda phase: phases.append((phase, len(loads))))
    assert phases == [("after_index_or_register", 0), ("after_queries", 1)]
    assert len(set(loads)) == 1 and len(result["queries"]) == 10


def test_native_return_clock_stops_before_output_audit(tmp_path, rm, monkeypatch):
    import pandas as pd

    now = [1_000_000_000]

    def clock():
        now[0] += 1_000
        return now[0]

    original = pd.DataFrame.sem_search

    class Result:
        def __init__(self, result):
            self.result = result

        @property
        def index(self):
            now[0] += 1_000_000_000  # Model a costly audit after native return.
            return self.result.index

        def __getitem__(self, name):
            return self.result[name]

    def search(self, *args, **kwargs):
        return Result(original(self)(*args, **kwargs))

    monkeypatch.setattr(workflow.time, "monotonic_ns", clock)
    monkeypatch.setattr(pd.DataFrame, "sem_search", search)
    result = workflow.run_workflow(frame(), rm, queries()[:1], index_dir=tmp_path / "index",
                                   embedding_identity={}, k=1, query_repeats=2)
    for record in result["queries"]:
        assert record["elapsed_s"] - record["native_call_return_elapsed_s"] >= 1


def test_mid_round_failure_preserves_completed_and_failed_queries_and_releases_owner(tmp_path, rm):
    import lotus

    before = lotus.settings.rm
    original = rm._embed
    calls = []

    def embed(docs):
        calls.append(len(docs))
        if len(calls) == 3:  # Gallery, first image query, failing second query.
            raise RuntimeError("second query fixture failed")
        return original(docs)

    rm._embed = embed
    report = {}
    with pytest.raises(RuntimeError, match="second query fixture failed"):
        workflow.run_workflow(frame(), rm, queries()[:1], index_dir=tmp_path / "index",
            embedding_identity={}, k=1, query_repeats=5, report=report)
    assert [r["query_id"] for r in report["queries"]] == ["blue@0", "blue@1"]
    assert [r["status"] for r in report["queries"]] == ["succeeded", "failed"]
    assert report["queries"][1]["native_call_return_elapsed_s"] is None
    assert report["queries"][1]["returned_monotonic_ns"] is None
    assert report["queries"][1]["submitted_monotonic_ns"] is not None
    assert lotus.settings.rm is before
    assert workflow._owner.acquire(blocking=False)
    workflow._owner.release()


@pytest.mark.parametrize("repeats", [True, 0, -1, 1.5])
def test_invalid_repeat_count_rejected_before_index_or_query(tmp_path, rm, repeats):
    with pytest.raises(ValueError, match="query repeats"):
        workflow.run_workflow(frame(), rm, queries(), index_dir=tmp_path / "index",
                             embedding_identity={}, query_repeats=repeats)
    assert rm.observation.calls == [] and not (tmp_path / "index").exists()
