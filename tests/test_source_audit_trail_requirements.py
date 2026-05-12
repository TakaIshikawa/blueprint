import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_audit_trail_requirements import (
    SourceAuditTrailRequirement,
    SourceAuditTrailRequirementsReport,
    build_source_audit_trail_requirements,
    derive_source_audit_trail_requirements,
    extract_source_audit_trail_requirements,
    generate_source_audit_trail_requirements,
    source_audit_trail_requirements_to_dict,
    source_audit_trail_requirements_to_dicts,
    source_audit_trail_requirements_to_markdown,
    summarize_source_audit_trail_requirements,
)


def test_complete_structured_brief_extracts_all_audit_trail_requirements():
    result = build_source_audit_trail_requirements(
        _source_brief(
            source_payload={
                "audit_trail": {
                    "actor": "Audit trail must capture actor identity, user id, and service account principal.",
                    "timestamp": "Audit events must include UTC timestamp and occurred at time.",
                    "event_type": (
                        "Audit event type taxonomy must cover create update delete and permission "
                        "change events with an event payload schema."
                    ),
                    "retention": "Retain audit events for seven years with legal hold and purge policy.",
                    "immutability": "Event records must be append-only, immutable, and cannot be deleted by admins.",
                    "export": "Auditors need CSV export and API access to audit events with authorization checks.",
                }
            }
        )
    )

    assert isinstance(result, SourceAuditTrailRequirementsReport)
    assert result.source_id == "sb-audit-trail"
    assert all(isinstance(record, SourceAuditTrailRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "actor_attribution",
        "timestamp_capture",
        "event_type_taxonomy",
        "retention_policy",
        "immutable_event_records",
        "exportability",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["actor_attribution"].missing_details == ()
    assert by_type["timestamp_capture"].missing_details == ()
    assert by_type["event_type_taxonomy"].missing_details == ()
    assert by_type["retention_policy"].missing_details == ()
    assert by_type["immutable_event_records"].missing_details == ()
    assert by_type["exportability"].missing_details == ()
    assert by_type["exportability"].confidence == "high"
    assert any("source_payload.audit_trail.actor" in item for item in by_type["actor_attribution"].evidence)
    assert result.summary["requirement_count"] == 6
    assert result.summary["type_counts"]["exportability"] == 1


def test_partial_brief_returns_deterministic_missing_guidance():
    result = derive_source_audit_trail_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=[
                    "Audit trail should record who performed account changes.",
                    "Keep audit logs for 13 months.",
                    "Audit history must be downloadable.",
                ]
            )
        )
    )

    assert result.source_id == "impl-audit-trail"
    assert [record.requirement_type for record in result.records] == [
        "actor_attribution",
        "retention_policy",
        "exportability",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["actor_attribution"].missing_details == ("service account handling",)
    assert by_type["retention_policy"].missing_details == ("deletion or legal hold policy",)
    assert by_type["exportability"].missing_details == (
        "export authorization",
        "export delivery path",
        "export format",
    )
    assert result.summary["requirement_types"] == [
        "actor_attribution",
        "retention_policy",
        "exportability",
    ]


def test_empty_noisy_and_negated_inputs_return_empty_report():
    noisy = build_source_audit_trail_requirements(
        _source_brief(
            title="Dashboard copy",
            summary="Improve onboarding labels and empty state copy.",
            source_payload={"body": "No audit trail or event history changes are in scope."},
        )
    )
    malformed = build_source_audit_trail_requirements(object())

    assert noisy.source_id == "sb-audit-trail"
    assert noisy.requirements == ()
    assert noisy.records == ()
    assert noisy.to_dicts() == []
    assert noisy.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "type_counts": {
            "actor_attribution": 0,
            "timestamp_capture": 0,
            "event_type_taxonomy": 0,
            "retention_policy": 0,
            "immutable_event_records": 0,
            "exportability": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "requirement_types": [],
    }
    assert "No audit trail requirements were found" in noisy.to_markdown()
    assert malformed.source_id is None
    assert malformed.requirements == ()


def test_model_input_aliases_serialization_and_markdown_are_stable():
    source = _source_brief(
        source_id="audit-model",
        summary="Audit trail must include who performed each billing settings change.",
        source_payload={
            "requirements": [
                "Audit events must include UTC timestamp.",
                "Audit event type taxonomy should identify each permission change.",
                "Audit events must be immutable and exportable as JSON.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    build_result = build_source_audit_trail_requirements(source)
    extract_result = extract_source_audit_trail_requirements(model)
    generate_result = generate_source_audit_trail_requirements(model)
    summary = summarize_source_audit_trail_requirements(generate_result)
    dict_payload = source_audit_trail_requirements_to_dict(generate_result)
    dicts_payload = source_audit_trail_requirements_to_dicts(generate_result)
    markdown = source_audit_trail_requirements_to_markdown(generate_result)

    assert source == original
    assert build_result.to_dict() == extract_result.to_dict() == generate_result.to_dict()
    assert summary["requirement_count"] == 4
    assert dict_payload["source_id"] == "audit-model"
    assert len(dicts_payload) == 4
    assert "# Source Audit Trail Requirements Report: audit-model" in markdown
    assert "| Source Brief | Type | Confidence |" in markdown
    assert json.loads(json.dumps(dict_payload, sort_keys=True))["source_id"] == "audit-model"


def _source_brief(
    *,
    source_id: str = "sb-audit-trail",
    title: str = "Audit trail requirements",
    summary: str = "",
    source_payload: dict | None = None,
) -> dict:
    return {
        "id": source_id,
        "source_project": "requirements",
        "source_entity_type": "brief",
        "source_id": f"{source_id}-upstream",
        "title": title,
        "summary": summary or title,
        "source_payload": source_payload or {},
        "source_links": {},
    }


def _implementation_brief(scope: list[str]) -> dict:
    return {
        "id": "impl-audit-trail",
        "source_brief_id": "sb-audit-trail",
        "title": "Audit trail implementation",
        "problem_statement": "Plan audit trail implementation details.",
        "mvp_goal": "Implement audit trail requirements.",
        "scope": scope,
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Run focused audit trail requirement tests.",
        "definition_of_done": ["Audit trail requirements are detected."],
    }
