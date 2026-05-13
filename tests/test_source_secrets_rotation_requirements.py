import json

from blueprint.domain.models import SourceBrief
from blueprint.source_secrets_rotation_requirements import (
    SourceSecretsRotationRequirement,
    SourceSecretsRotationRequirementsReport,
    build_source_secrets_rotation_requirements,
    derive_source_secrets_rotation_requirements,
    extract_source_secrets_rotation_requirements,
    generate_source_secrets_rotation_requirements,
    source_secrets_rotation_requirements_to_dict,
    source_secrets_rotation_requirements_to_dicts,
    source_secrets_rotation_requirements_to_markdown,
    summarize_source_secrets_rotation_requirements,
)


def test_structured_security_infrastructure_and_integration_sections_extract_records():
    report = build_source_secrets_rotation_requirements(
        _source(
            {
                "security": [
                    "API keys, tokens, and certificates must be scoped by service and production environment.",
                    "Rotation cadence must run every 90 days with scheduled trigger and 7 day overlap window.",
                    "Security owner requires approver and backup owner for rotation changes.",
                    "Emergency revocation must revoke compromised tokens immediately by security on-call responder.",
                ],
                "infrastructure": [
                    "Secrets must be stored in Vault with IAM access policy per production environment.",
                    "Audit evidence must include audit log, Jira ticket, actor, and timestamp.",
                ],
                "integrations": [
                    "Rollout coordination requires dual-write overlap window, consumer cutover, and rollback plan.",
                    "Consumers must list downstream services, Slack notification channel, and dependency owner team.",
                ],
            }
        )
    )

    assert isinstance(report, SourceSecretsRotationRequirementsReport)
    assert all(isinstance(record, SourceSecretsRotationRequirement) for record in report.records)
    assert [record.requirement_type for record in report.records] == [
        "secret_types",
        "rotation_cadence",
        "ownership",
        "storage_backend",
        "rollout_coordination",
        "emergency_revocation",
        "audit_evidence",
        "consumers",
    ]
    assert all(record.readiness == "ready" for record in report.records)
    assert report.summary["requirement_count"] == 8
    assert report.summary["category_counts"]["storage_backend"] == 1
    assert report.summary["readiness_counts"] == {"ready": 8, "needs_detail": 0}
    assert report.summary["missing_detail_count"] == 0


def test_partial_text_reports_missing_operational_details():
    report = build_source_secrets_rotation_requirements("Secrets rotation is required. Tokens should rotate. Vault storage is needed.")
    by_type = {record.requirement_type: record for record in report.records}

    assert [record.requirement_type for record in report.records] == ["secret_types", "rotation_cadence", "storage_backend"]
    assert by_type["rotation_cadence"].missing_details == ("cadence", "trigger", "grace_period")
    assert by_type["rotation_cadence"].readiness == "needs_detail"
    assert by_type["rotation_cadence"].missing_detail_guidance == "cadence; trigger; grace_period"
    assert report.summary["readiness_counts"]["needs_detail"] == 3
    assert report.summary["missing_field_counts"]["cadence"] == 1


def test_helpers_dict_markdown_list_records_findings_and_summary_are_stable():
    source = _source({"security": ["Tokens must rotate every 30 days with scheduled trigger and overlap window | note."]})
    model = SourceBrief.model_validate(source)
    report = build_source_secrets_rotation_requirements(model)
    payload = source_secrets_rotation_requirements_to_dict(report)

    assert extract_source_secrets_rotation_requirements(model).to_dict() == payload
    assert generate_source_secrets_rotation_requirements(model).to_dict() == payload
    assert derive_source_secrets_rotation_requirements(model).to_dict() == payload
    assert json.loads(json.dumps(payload)) == payload
    assert report.records == report.requirements
    assert report.findings == report.requirements
    assert source_secrets_rotation_requirements_to_dicts(report) == payload["requirements"]
    assert source_secrets_rotation_requirements_to_dicts(report.records) == payload["records"]
    assert summarize_source_secrets_rotation_requirements(report) == report.summary
    assert source_secrets_rotation_requirements_to_markdown(report) == report.to_markdown()
    assert "overlap window \\| note" in report.to_markdown()


def test_empty_negated_and_malformed_inputs_return_stable_empty_summary():
    empty = build_source_secrets_rotation_requirements(_source({"security": ["No secret, token, key, certificate, rotation, revocation, vault, or KMS work is required."]}))
    malformed = build_source_secrets_rotation_requirements({"id": "bad", "source_payload": {"notes": object()}})
    invalid = build_source_secrets_rotation_requirements(42)

    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary["requirement_count"] == 0
    assert empty.summary["readiness_counts"] == {"ready": 0, "needs_detail": 0}
    assert empty.summary["requirement_type_counts"]["rotation_cadence"] == 0
    assert "No secrets rotation requirements were inferred." in empty.to_markdown()
    assert malformed.records == ()
    assert invalid.records == ()


def _source(source_payload):
    return {
        "id": "sb-secrets",
        "title": "Secrets rotation requirements",
        "domain": "security",
        "summary": "Credential lifecycle planning.",
        "source_project": "project",
        "source_entity_type": "issue",
        "source_id": "issue-secrets",
        "source_payload": source_payload,
        "source_links": {},
    }
