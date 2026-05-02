import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_audit_logging_requirements import (
    SourceAuditLoggingRequirement,
    SourceAuditLoggingRequirementsReport,
    build_source_audit_logging_requirements,
    extract_source_audit_logging_requirements,
    generate_source_audit_logging_requirements,
    source_audit_logging_requirements_to_dict,
    source_audit_logging_requirements_to_dicts,
    source_audit_logging_requirements_to_markdown,
    summarize_source_audit_logging_requirements,
)


def test_structured_payload_fields_extract_audit_logging_requirements():
    result = build_source_audit_logging_requirements(
        _source_brief(
            source_payload={
                "audit_logging": {
                    "admin_actions": (
                        "Admin actions must be recorded with actor identity and UTC timestamp."
                    ),
                    "security_events": (
                        "Security events include failed logins, MFA changes, and access denied."
                    ),
                    "retention": "Audit logs must be retained for 13 months.",
                    "immutability": "Audit logs should be append-only and tamper evident.",
                    "exports": "Compliance export must download audit logs as CSV.",
                }
            }
        )
    )

    assert isinstance(result, SourceAuditLoggingRequirementsReport)
    assert result.source_id == "sb-audit"
    assert all(isinstance(record, SourceAuditLoggingRequirement) for record in result.records)
    by_type = {record.requirement_type: record for record in result.requirements}

    assert list(by_type) == [
        "audit_trail",
        "admin_actions",
        "security_events",
        "audit_log_retention",
        "actor_identity",
        "timestamping",
        "immutable_logs",
        "exportable_audit_history",
    ]
    assert by_type["admin_actions"].subject_scope == "admin actions"
    assert by_type["admin_actions"].confidence == "high"
    assert "actor identity" not in by_type["admin_actions"].missing_details
    assert "timestamp source" not in by_type["timestamping"].missing_details
    assert "retention period" not in by_type["audit_log_retention"].missing_details
    assert "immutability mechanism" not in by_type["immutable_logs"].missing_details
    assert "export format" not in by_type["exportable_audit_history"].missing_details
    assert any(
        "source_payload.audit_logging.admin_actions" in item
        for item in by_type["admin_actions"].evidence
    )
    assert result.summary["requirement_count"] == len(result.requirements)
    assert result.summary["type_counts"]["security_events"] == 1


def test_free_text_markdown_extracts_audit_trail_security_and_export_scope():
    result = extract_source_audit_logging_requirements(
        _source_brief(
            source_payload={
                "markdown": (
                    "## Compliance controls\n"
                    "- Keep an audit trail for role changes and permission changes.\n"
                    "- Record security events like password resets and access denied.\n"
                    "- Auditors need exportable audit history for workspace settings.\n"
                )
            }
        )
    )

    assert [(record.requirement_type, record.subject_scope) for record in result.requirements] == [
        ("audit_trail", "role changes"),
        ("admin_actions", "role changes"),
        ("security_events", "security events"),
        ("exportable_audit_history", "workspace settings"),
    ]
    assert result.requirements[0].confidence == "high"


def test_duplicate_evidence_is_merged_and_deduplicated():
    result = build_source_audit_logging_requirements(
        {
            "id": "dupe-audit",
            "summary": "Audit logs must record admin actions. Audit logs must record admin actions.",
            "source_payload": {
                "requirements": [
                    "Audit logs must record admin actions.",
                    "audit logs must record admin actions.",
                ],
                "metadata": {"audit_logs": "Audit logs must record admin actions."},
            },
        }
    )

    audit_trail = next(
        record for record in result.requirements if record.requirement_type == "audit_trail"
    )

    assert audit_trail.evidence == tuple(
        sorted(set(audit_trail.evidence), key=lambda item: item.casefold())
    )
    assert len(audit_trail.evidence) == len(set(audit_trail.evidence))
    assert result.summary["requirement_count"] == len(result.requirements)


def test_empty_and_malformed_inputs_return_empty_aggregate():
    empty = build_source_audit_logging_requirements(
        _source_brief(
            title="Dashboard copy",
            summary="Improve onboarding copy.",
            source_payload={"body": "No audit logging or security event changes are in scope."},
        )
    )
    malformed = build_source_audit_logging_requirements(object())

    assert empty.source_id == "sb-audit"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "type_counts": {
            "audit_trail": 0,
            "admin_actions": 0,
            "security_events": 0,
            "audit_log_retention": 0,
            "actor_identity": 0,
            "timestamping": 0,
            "immutable_logs": 0,
            "exportable_audit_history": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "requirement_types": [],
    }
    assert "No audit logging requirements were found" in empty.to_markdown()
    assert malformed.source_id is None
    assert malformed.requirements == ()


def test_source_brief_model_input_matches_mapping_and_is_not_mutated():
    source = _source_brief(
        source_id="audit-model",
        summary="Audit trail must include who performed each billing settings change.",
        source_payload={
            "requirements": [
                "Audit logs must include timestamp and service account actor identity.",
                "Retain audit logs for one year.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_audit_logging_requirements(source)
    model_result = generate_source_audit_logging_requirements(model)
    payload = source_audit_logging_requirements_to_dict(model_result)

    assert source == original
    assert payload == source_audit_logging_requirements_to_dict(mapping_result)
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert source_audit_logging_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_audit_logging_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "subject_scope",
        "missing_details",
        "confidence",
        "evidence",
    ]


def test_markdown_helper_and_multiple_briefs_are_stable():
    result = build_source_audit_logging_requirements(
        [
            _source_brief(
                source_id="brief-b",
                summary="Security events must include timestamped failed logins.",
            ),
            _source_brief(
                source_id="brief-a",
                summary="Audit logs must track admin actions and export audit history.",
            ),
        ]
    )
    markdown = source_audit_logging_requirements_to_markdown(result)

    assert [(record.source_brief_id, record.requirement_type) for record in result.records] == [
        ("brief-a", "audit_trail"),
        ("brief-a", "admin_actions"),
        ("brief-a", "exportable_audit_history"),
        ("brief-b", "security_events"),
        ("brief-b", "timestamping"),
    ]
    assert result.source_id is None
    assert result.summary["source_count"] == 2
    assert markdown.startswith("# Source Audit Logging Requirements Report")
    assert "| Source Brief | Type | Scope | Confidence | Missing Details | Evidence |" in markdown


def _source_brief(
    *,
    source_id="sb-audit",
    title="Audit logging requirements",
    domain="compliance",
    summary="General audit logging requirements.",
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
