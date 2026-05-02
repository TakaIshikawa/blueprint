import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_data_retention_requirements import (
    SourceDataRetentionRequirement,
    SourceDataRetentionRequirements,
    build_source_data_retention_requirements,
    extract_source_data_retention_requirements,
    source_data_retention_requirements_to_dict,
    summarize_source_data_retention_requirements,
)


def test_structured_fields_report_lifecycle_requirements_with_evidence():
    result = build_source_data_retention_requirements(
        _source_brief(
            source_payload={
                "data_retention_requirements": [
                    {
                        "scope": "billing records",
                        "policy": (
                            "Billing records must be retained for 7 years for tax audit evidence."
                        ),
                    },
                    {
                        "scope": "customer PII",
                        "deletion": (
                            "Customer PII must be deleted within 30 days after account closure."
                        ),
                    },
                    {
                        "scope": "support case files",
                        "legal_hold": (
                            "Do not delete support case files while a legal hold is active."
                        ),
                    },
                ]
            }
        )
    )

    assert isinstance(result, SourceDataRetentionRequirements)
    assert result.source_id == "sb-retention"
    assert all(isinstance(record, SourceDataRetentionRequirement) for record in result.requirements)
    assert [
        (
            record.requirement_type,
            record.data_scope,
            record.retention_window,
            record.deletion_trigger,
            record.legal_hold_signal,
            record.confidence,
        )
        for record in result.requirements
    ] == [
        (
            "legal_hold",
            "support case files",
            None,
            None,
            "Do not delete",
            "high",
        ),
        (
            "retention",
            "billing records",
            "for 7 years",
            None,
            None,
            "high",
        ),
        (
            "deletion",
            "customer pii",
            "within 30 days",
            "after account closure",
            None,
            "high",
        ),
    ]
    assert any(
        "source_payload.data_retention_requirements[0].policy" in item
        for item in result.requirements[1].evidence
    )
    assert result.summary["legal_hold_count"] == 1


def test_free_text_markdown_extracts_retention_deletion_archival_and_purge():
    result = extract_source_data_retention_requirements(
        _source_brief(
            source_payload={
                "markdown": (
                    "## Data lifecycle\n"
                    "- Retain audit logs for 13 months.\n"
                    "- Archive user exports after 90 days.\n"
                    "- Purge session records upon user request.\n"
                )
            }
        )
    )

    assert [
        (
            record.requirement_type,
            record.data_scope,
            record.retention_window,
            record.deletion_trigger,
        )
        for record in result.requirements
    ] == [
        ("retention", "audit logs", "for 13 months", None),
        ("purge", "session records", None, "upon user request"),
        ("archival", "user exports", "after 90 days", None),
    ]


def test_brief_without_retention_or_deletion_signals_returns_no_findings():
    result = build_source_data_retention_requirements(
        _source_brief(
            summary="Improve onboarding copy and simplify the dashboard settings layout.",
            source_payload={"body": "No lifecycle policy changes are in scope."},
        )
    )

    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "requirement_count": 0,
        "type_counts": {
            "legal_hold": 0,
            "retention": 0,
            "deletion": 0,
            "purge": 0,
            "archival": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "legal_hold_count": 0,
    }


def test_deterministic_json_serialization_and_summary_helpers():
    result = build_source_data_retention_requirements(
        _source_brief(
            source_payload={
                "requirements": {
                    "second": "Delete backup data after contract termination.",
                    "first": (
                        "Legal hold requires audit logs be retained until legal hold is lifted."
                    ),
                }
            }
        )
    )
    payload = source_data_retention_requirements_to_dict(result)

    assert payload == result.to_dict()
    assert result.to_dicts() == payload["requirements"]
    assert result.records == result.requirements
    assert summarize_source_data_retention_requirements(result) == result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "summary"]
    assert list(payload["requirements"][0]) == [
        "requirement_type",
        "data_scope",
        "retention_window",
        "deletion_trigger",
        "legal_hold_signal",
        "confidence",
        "evidence",
    ]
    assert [record.requirement_type for record in result.requirements] == [
        "legal_hold",
        "retention",
        "deletion",
    ]


def test_source_brief_model_input_is_supported_without_mutation():
    source = SourceBrief.model_validate(
        _source_brief(
            summary="Profile data must be anonymized within 60 days after deletion request.",
            source_payload={
                "privacy": {
                    "retention": (
                        "Account data should be kept for one year unless legal hold applies."
                    )
                }
            },
        )
    )
    before = copy.deepcopy(source.model_dump(mode="python"))

    result = build_source_data_retention_requirements(source)

    assert source.model_dump(mode="python") == before
    assert [
        (record.requirement_type, record.retention_window, record.deletion_trigger)
        for record in result.requirements
    ] == [
        ("retention", "for one year", None),
        ("deletion", "within 60 days", "after deletion request"),
    ]
    mapping_result = build_source_data_retention_requirements(source.model_dump(mode="python"))
    assert result.to_dict() == mapping_result.to_dict()


def _source_brief(
    *,
    source_id="sb-retention",
    title="Retention requirements",
    domain="privacy",
    summary="General retention requirements.",
    source_payload=None,
    source_links=None,
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
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }
