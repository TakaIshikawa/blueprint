import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_queue_dead_letter_requirements import (
    SourceQueueDeadLetterRequirement,
    SourceQueueDeadLetterRequirementsReport,
    build_source_queue_dead_letter_requirements,
    derive_source_queue_dead_letter_requirements,
    extract_source_queue_dead_letter_requirements,
    generate_source_queue_dead_letter_requirements,
    source_queue_dead_letter_requirements_to_dict,
    source_queue_dead_letter_requirements_to_dicts,
    source_queue_dead_letter_requirements_to_markdown,
    summarize_source_queue_dead_letter_requirements,
)


def test_extracts_dead_letter_categories_in_stable_order():
    result = build_source_queue_dead_letter_requirements(
        _source_brief(
            source_payload={
                "dead_letter": {
                    "routing": "Failed messages must route to a dead-letter queue with metadata.",
                    "poison": "Poison message classification must identify non-retryable payloads.",
                    "exhaustion": "Retry exhaustion after 5 retries must move messages to the DLQ.",
                    "replay": "Replay tooling must support manual redrive and bulk replay.",
                    "quarantine": "Quarantine review must allow operators to inspect failed messages.",
                    "alerting": "Alerting must monitor DLQ depth and message age dashboards.",
                    "retention": "Retention must keep failed messages for 14 days before purge.",
                    "ownership": "Ownership requires a runbook and service owner escalation path.",
                }
            }
        )
    )

    assert isinstance(result, SourceQueueDeadLetterRequirementsReport)
    assert all(isinstance(record, SourceQueueDeadLetterRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "dlq_routing",
        "poison_message",
        "retry_exhaustion",
        "replay_tooling",
        "quarantine_review",
        "alerting",
        "retention",
        "ownership",
    ]
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"
    assert result.records[0].suggested_owners == ("backend", "platform")


def test_models_objects_strings_and_missing_summary_are_supported():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Dead letter queue routing is required when max retries are exhausted.",
                "DLQ alerting must monitor failed message age.",
            ]
        )
    )
    source = SourceBrief.model_validate(_source_brief(summary="Poison message handling must classify malformed messages."))
    object_result = build_source_queue_dead_letter_requirements(
        SimpleNamespace(id="object-dlq", summary="DLQ ownership requires a support runbook.")
    )
    text_result = build_source_queue_dead_letter_requirements("Queue replay tooling must redrive dead-letter messages.")

    implementation_result = generate_source_queue_dead_letter_requirements(implementation)
    source_result = derive_source_queue_dead_letter_requirements(source)

    assert {"dlq_routing", "retry_exhaustion", "alerting"} <= {record.category for record in implementation_result.records}
    assert source_result.summary["missing_detail_flags"] == ["missing_replay", "missing_alerting", "missing_owner"]
    assert [record.category for record in object_result.records] == ["dlq_routing", "ownership"]
    assert [record.category for record in text_result.records] == ["dlq_routing", "replay_tooling"]


def test_serialization_aliases_and_markdown_are_deterministic():
    source = _source_brief(summary="DLQ routing must retain failed messages and alert the service owner.")
    model = SourceBrief.model_validate(source)

    result = build_source_queue_dead_letter_requirements(source)
    extracted = extract_source_queue_dead_letter_requirements(model)
    payload = source_queue_dead_letter_requirements_to_dict(result)
    markdown = source_queue_dead_letter_requirements_to_markdown(result)

    assert extracted == result.records
    assert summarize_source_queue_dead_letter_requirements(result) == result.summary
    assert source_queue_dead_letter_requirements_to_dicts(result) == payload["requirements"]
    assert source_queue_dead_letter_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert markdown.startswith("# Source Queue Dead Letter Requirements Report: source-dlq")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |" in markdown


def test_negated_and_unrelated_queue_mentions_return_empty_reports():
    negated = build_source_queue_dead_letter_requirements(
        _source_brief(summary="DLQ support is out of scope and no dead-letter changes are required.")
    )
    unrelated = build_source_queue_dead_letter_requirements(
        _source_brief(summary="Queue copy mentions a cover letter template and dead code cleanup.")
    )
    blank = build_source_queue_dead_letter_requirements("")
    invalid = build_source_queue_dead_letter_requirements(42)

    assert negated.records == ()
    assert unrelated.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary["requirement_count"] == 0
    assert unrelated.summary["status"] == "no_queue_dead_letter_language"
    assert "No source queue dead-letter requirements were inferred" in unrelated.to_markdown()


def _source_brief(*, source_id="source-dlq", summary="General queue requirements.", source_payload=None):
    return {
        "id": source_id,
        "title": "Queue dead letter requirements",
        "domain": "messaging",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None):
    return {
        "id": "implementation-dlq",
        "source_brief_id": "source-dlq",
        "title": "Queue dead letter implementation",
        "domain": "messaging",
        "target_user": "operator",
        "buyer": "platform",
        "workflow_context": "Operators need failed message recovery.",
        "problem_statement": "Failed queue messages need deterministic handling.",
        "mvp_goal": "Plan DLQ handling.",
        "product_surface": "queue",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run DLQ extractor tests.",
        "definition_of_done": [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
