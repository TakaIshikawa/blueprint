import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_scheduled_job_requirements import (
    SourceScheduledJobRequirement,
    SourceScheduledJobRequirementsReport,
    build_source_scheduled_job_requirements,
    derive_source_scheduled_job_requirements,
    extract_source_scheduled_job_requirements,
    generate_source_scheduled_job_requirements,
    source_scheduled_job_requirements_to_dict,
    source_scheduled_job_requirements_to_dicts,
    source_scheduled_job_requirements_to_markdown,
    summarize_source_scheduled_job_requirements,
)


def test_cron_expression_extracts_schedule_operational_concerns():
    result = build_source_scheduled_job_requirements(
        _source_brief(
            source_payload={
                "scheduled_jobs": {
                    "invoice_reconciliation": (
                        "Invoice reconciliation job must run on cron 0 2 * * * in UTC, retry 3 attempts "
                        "with exponential backoff, prevent overlapping runs with a single instance lock, "
                        "skip missed runs after downtime, allow manual rerun by ops, remain idempotent with "
                        "dedupe keys, and owner: finance ops."
                    )
                }
            }
        )
    )

    assert isinstance(result, SourceScheduledJobRequirementsReport)
    assert all(isinstance(record, SourceScheduledJobRequirement) for record in result.records)
    assert result.source_id == "source-scheduled-jobs"
    assert [record.concern for record in result.records] == [
        "cadence",
        "timezone",
        "retry",
        "missed_run",
        "concurrency",
        "rerun",
        "idempotency",
        "ownership",
    ]
    by_concern = {record.concern: record for record in result.records}
    assert by_concern["cadence"].cron_expression == "0 2 * * *"
    assert by_concern["cadence"].source_field == "source_payload.scheduled_jobs.invoice_reconciliation"
    assert by_concern["timezone"].timezone_behavior == "utc"
    assert by_concern["retry"].retry_policy == "3 attempts with exponential backoff"
    assert by_concern["missed_run"].missed_run_handling == "skip missed"
    assert "single instance lock" in by_concern["concurrency"].concurrency_limit
    assert "manual rerun" in by_concern["rerun"].matched_terms
    assert by_concern["idempotency"].idempotency_expectation == "with dedupe keys"
    assert by_concern["ownership"].owner == "finance ops"
    assert "cron" in by_concern["cadence"].matched_terms
    assert result.summary["requirement_count"] == 8
    assert result.summary["has_cron_expression"] is True
    assert result.summary["requires_retry_policy"] is True
    assert result.summary["requires_concurrency_limit"] is True
    assert result.summary["requires_rerun_control"] is True
    assert result.summary["requires_idempotency"] is True
    assert result.summary["requires_ownership"] is True
    assert result.summary["status"] == "ready_for_scheduled_job_planning"


def test_natural_language_cadence_and_implementation_brief_inputs_are_supported():
    text_result = build_source_scheduled_job_requirements(
        """
# Scheduled jobs

- Daily digest job must run every 6 hours between 01:00 and 05:00 UTC.
- Weekly cleanup job should catch up missed runs after downtime.
"""
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Nightly export job must run at 23:30 local time using the tenant timezone.",
                "The scheduled export job should retry twice and page the data ops on-call after terminal failure.",
            ],
            definition_of_done=[
                "Manual rerun control is available for admins and records audit evidence.",
                "Scheduled job execution must be idempotent and duplicate safe.",
            ],
        )
    )

    assert [record.concern for record in text_result.records] == ["cadence", "timezone", "missed_run"]
    assert text_result.records[0].cadence.startswith("every 6 hours")
    assert text_result.records[0].time_window == "01:00 and 05:00 utc"
    implementation_result = generate_source_scheduled_job_requirements(implementation)
    assert implementation_result.source_id == "implementation-scheduled-jobs"
    assert {record.concern for record in implementation_result.records} == {
        "cadence",
        "timezone",
        "retry",
        "rerun",
        "idempotency",
        "ownership",
    }
    assert any(record.timezone_behavior == "the tenant timezone" for record in implementation_result.records)
    assert any(record.owner and record.owner.startswith("data ops on-call") for record in implementation_result.records)


def test_nested_structured_fields_extract_evidence_paths_and_details():
    result = build_source_scheduled_job_requirements(
        _source_brief(
            source_payload={
                "operations": {
                    "cron": [
                        {
                            "cadence": "Every 15 minutes",
                            "timezone": "UTC",
                            "missed_runs": "catch up one missed execution",
                            "concurrency_limit": "max concurrent 1 with lock",
                            "owner": "platform ops",
                        }
                    ]
                }
            }
        )
    )

    assert [record.concern for record in result.records] == [
        "cadence",
        "timezone",
        "missed_run",
        "concurrency",
        "ownership",
    ]
    by_concern = {record.concern: record for record in result.records}
    assert by_concern["cadence"].cadence == "every 15 minutes"
    assert by_concern["timezone"].timezone_behavior == "utc"
    assert by_concern["missed_run"].missed_run_handling == "catch up one missed execution"
    assert by_concern["concurrency"].concurrency_limit == "max concurrent 1 with lock"
    assert by_concern["ownership"].owner == "platform ops"
    assert all(record.source_field == "source_payload.operations.cron[0]" for record in result.records)
    assert all(record.evidence and record.source_field in record.evidence[0] for record in result.records)


def test_duplicate_evidence_serialization_markdown_aliases_and_no_mutation_are_stable():
    source = _source_brief(
        source_id="scheduled-dedupe",
        source_payload={
            "requirements": [
                "Daily billing job must run on cron 30 1 * * * and retry 2 attempts | operator note.",
                "Daily billing job must run on cron 30 1 * * * and retry 2 attempts | operator note.",
                "Daily billing job must be idempotent and duplicate safe.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_scheduled_job_requirements(source)
    model_result = extract_source_scheduled_job_requirements(model)
    generated = generate_source_scheduled_job_requirements(model)
    derived = derive_source_scheduled_job_requirements(model)
    payload = source_scheduled_job_requirements_to_dict(model_result)
    markdown = source_scheduled_job_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_scheduled_job_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_scheduled_job_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_scheduled_job_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "concern",
        "requirement_text",
        "cadence",
        "cron_expression",
        "time_window",
        "timezone_behavior",
        "retry_policy",
        "missed_run_handling",
        "concurrency_limit",
        "idempotency_expectation",
        "rerun_control",
        "owner",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "planning_note",
    ]
    assert [record.concern for record in model_result.records] == ["cadence", "retry", "idempotency"]
    cadence = model_result.records[0]
    assert cadence.cron_expression == "30 1 * * *"
    assert cadence.evidence == (
        "source_payload.requirements[0]: Daily billing job must run on cron 30 1 * * * and retry 2 attempts | operator note.",
    )
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Concern | Requirement | Cadence | Cron |" in markdown
    assert "operator note" in markdown
    assert "retry 2 attempts \\| operator note" in markdown
    assert model_result.records[0].requirement_category == "cadence"
    assert model_result.records[0].planning_notes == (model_result.records[0].planning_note,)


def test_negated_scope_incidental_schedule_malformed_and_invalid_inputs_are_empty():
    class BriefLike:
        id = "object-no-scheduled-jobs"
        summary = "No scheduled jobs, cron, or background jobs are required for this release."

    object_result = build_source_scheduled_job_requirements(
        SimpleNamespace(
            id="object-scheduled-jobs",
            summary="Background job must run daily and retry with backoff.",
            operations={"owner": "platform ops owns the scheduler runbook."},
        )
    )
    negated = build_source_scheduled_job_requirements(BriefLike())
    no_scope = build_source_scheduled_job_requirements(
        _source_brief(summary="Cron and scheduled jobs are out of scope and no scheduler work is planned.")
    )
    incidental = build_source_scheduled_job_requirements(
        _source_brief(
            title="Project planning",
            summary="The project schedule should include release planning and design review milestones.",
            source_payload={"requirements": ["Schedule a stakeholder review before implementation."]},
        )
    )
    malformed = build_source_scheduled_job_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_scheduled_job_requirements(42)
    blank = build_source_scheduled_job_requirements("")

    assert [record.concern for record in object_result.records] == ["cadence", "retry", "ownership"]
    assert negated.records == ()
    assert no_scope.records == ()
    assert incidental.records == ()
    assert malformed.records == ()
    assert invalid.records == ()
    assert blank.records == ()
    assert incidental.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "concern_counts": {
            "cadence": 0,
            "timezone": 0,
            "retry": 0,
            "missed_run": 0,
            "concurrency": 0,
            "rerun": 0,
            "idempotency": 0,
            "ownership": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "concerns": [],
        "has_cron_expression": False,
        "requires_timezone_behavior": False,
        "requires_retry_policy": False,
        "requires_missed_run_handling": False,
        "requires_concurrency_limit": False,
        "requires_rerun_control": False,
        "requires_idempotency": False,
        "requires_ownership": False,
        "status": "no_scheduled_job_language",
    }
    assert invalid.to_dicts() == []
    assert "No scheduled job requirements were found" in incidental.to_markdown()


def _source_brief(
    *,
    source_id="source-scheduled-jobs",
    title="Scheduled job requirements",
    domain="operations",
    summary="General scheduled job requirements.",
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "implementation-scheduled-jobs",
        "source_brief_id": "source-scheduled-jobs",
        "title": "Scheduled job rollout",
        "domain": "operations",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need scheduled job requirements before task generation.",
        "problem_statement": "Schedule semantics need to be extracted early.",
        "mvp_goal": "Plan scheduled job execution behavior from source briefs.",
        "product_surface": "scheduler",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for scheduled job coverage.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
    }
