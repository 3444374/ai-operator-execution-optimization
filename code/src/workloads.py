from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DocumentRow:
    doc_id: int
    tenant_id: int
    category: str
    text: str


SYNTHETIC_WORKLOAD = "synthetic"
AI_COMPLETE_CONTROLLED_WORKLOAD = "ai_complete_controlled"
WORKLOAD_NAMES = (SYNTHETIC_WORKLOAD, AI_COMPLETE_CONTROLLED_WORKLOAD)


def generate_document_rows(start: int, stop: int, workload: str) -> list[DocumentRow]:
    if start < 0 or stop < start:
        raise ValueError("invalid document row range")
    if workload == SYNTHETIC_WORKLOAD:
        return [_synthetic_row(i) for i in range(start, stop)]
    if workload == AI_COMPLETE_CONTROLLED_WORKLOAD:
        return [_ai_complete_controlled_row(i) for i in range(start, stop)]
    raise ValueError(f"unknown seed workload: {workload}")


def _synthetic_row(i: int) -> DocumentRow:
    return DocumentRow(
        doc_id=i,
        tenant_id=i % 16,
        category=f"cat_{i % 8}",
        text=f"document {i} tenant {i % 16} category {i % 8} " + ("token " * 32),
    )


def _ai_complete_controlled_row(i: int) -> DocumentRow:
    template = i % 6
    tenant_id = i % 16
    if template == 0:
        category = "short_support_summary"
        text = _support_summary_prompt(i)
    elif template == 1:
        category = "long_incident_summary"
        text = _incident_summary_prompt(i)
    elif template == 2:
        category = "medium_contract_extraction"
        text = _contract_extraction_prompt(i)
    elif template == 3:
        category = "short_product_review"
        text = _review_classification_prompt(i)
    elif template == 4:
        category = "medium_sql_explanation"
        text = _sql_explanation_prompt(i)
    else:
        category = "long_research_note"
        text = _research_note_prompt(i)
    return DocumentRow(doc_id=i, tenant_id=tenant_id, category=category, text=text)


def _repeat(text: str, count: int) -> str:
    return " ".join(text for _ in range(count))


def _support_summary_prompt(i: int) -> str:
    detail = _repeat(
        "The customer reports intermittent timeout errors after a data import. "
        "They retried the job, checked credentials, and attached logs with request ids.",
        1 + (i % 4),
    )
    return (
        "Summarize the following customer support ticket in three bullet points, "
        "then list the likely owner team.\n\n"
        f"Ticket id: SUP-{10000 + i}\n"
        f"Priority: {['low', 'medium', 'high', 'urgent'][i % 4]}\n"
        f"Body: {detail}"
    )


def _incident_summary_prompt(i: int) -> str:
    detail = _repeat(
        "At 09:14 UTC the batch inference queue started growing faster than completions drained. "
        "Dashboard snapshots show high request variance, mixed prompt lengths, and delayed retries.",
        2 + (i % 5),
    )
    return (
        "Write an incident summary for an engineering postmortem. Include impact, timeline, "
        "root cause hypothesis, and immediate mitigation.\n\n"
        f"Incident id: INC-{20000 + i}\n"
        f"Service: ai-complete-worker-{i % 3}\n"
        f"Notes: {detail}"
    )


def _contract_extraction_prompt(i: int) -> str:
    detail = _repeat(
        "The vendor shall process analytics documents only for the stated project. "
        "The agreement renews monthly unless either party gives written notice. "
        "Confidential records must be deleted within thirty days after termination.",
        1 + (i % 3),
    )
    return (
        "Extract contract fields as compact JSON with keys party, renewal, deletion_days, "
        "and risk_flags. Use null when a field is not present.\n\n"
        f"Clause set: CTR-{30000 + i}\n"
        f"Text: {detail}"
    )


def _review_classification_prompt(i: int) -> str:
    detail = _repeat(
        "The user likes the fast setup and clear dashboard, but complains that long reports "
        "stall during export and that error messages do not identify the failed source.",
        1 + (i % 4),
    )
    return (
        "Classify the review sentiment as positive, neutral, or negative. Then extract "
        "at most three product issues.\n\n"
        f"Review id: REV-{40000 + i}\n"
        f"Review: {detail}"
    )


def _sql_explanation_prompt(i: int) -> str:
    predicate = ["tenant_id = 7", "category = 'support_summary'", "updated_at > now() - interval '1 day'"][i % 3]
    return (
        "Explain this SQL query to a data engineer and identify one possible performance risk.\n\n"
        "SQL:\n"
        "SELECT tenant_id, category, count(*) AS n\n"
        "FROM documents\n"
        f"WHERE {predicate}\n"
        "GROUP BY tenant_id, category\n"
        "ORDER BY n DESC\n"
        "LIMIT 20;"
    )


def _research_note_prompt(i: int) -> str:
    detail = _repeat(
        "A database AI operator can expose a simple SQL surface while delegating model execution "
        "to an external serving layer. The open question is how upstream data organization changes "
        "request shape, queueing behavior, and service utilization.",
        2 + (i % 6),
    )
    return (
        "Rewrite the research note as a concise problem statement and list two measurable metrics.\n\n"
        f"Note id: RSH-{50000 + i}\n"
        f"Note: {detail}"
    )
