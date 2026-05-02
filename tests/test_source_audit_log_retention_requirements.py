import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_audit_log_retention_requirements import (
    SourceAuditLogRetentionRequirement,
    SourceAuditLogRetentionRequirementsReport,
    build_source_audit_log_retention_requirements,
    extract_source_audit_log_retention_requirements,
    generate_source_audit_log_retention_requirements,
    source_audit_log_retention_requirements_to_dict,
    source_audit_log_retention_requirements_to_dicts,
    source_audit_log_retention_requirements_to_markdown,
    summarize_source_audit_log_retention_requirements,
)


def test_extracts_audit_capture_retention_storage_export_tamper_and_metadata_requirements():
    result = build_source_audit_log_retention_requirements(
        _source_brief(
            source_payload={
                "audit_logging": {
                    "events": (
                        "Audit events must capture admin export, delete, permission changes, actor, "
                        "action, resource, timestamp, IP address, and request id."
                    ),
                    "storage": (
                        "Audit logs must be immutable append-only records with tamper-evident hash chain "
                        "integrity and exportable SOC 2 evidence reports."
                    ),
                    "retention": "Retain audit logs for 7 years for compliance evidence review.",
                }
            }
        )
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert isinstance(result, SourceAuditLogRetentionRequirementsReport)
    assert all(isinstance(record, SourceAuditLogRetentionRequirement) for record in result.records)
    assert result.source_id == "source-audit-logs"
    assert {
        "audit_event_capture",
        "retention_window",
        "immutable_storage",
        "exportability",
        "tamper_evidence",
        "metadata_capture",
        "admin_access_logs",
        "compliance_evidence",
    } <= set(by_type)
    assert by_type["retention_window"].retention_window == "7 years"
    assert by_type["retention_window"].retention_days == 2555
    assert by_type["metadata_capture"].audit_surface == "data export"
    assert by_type["immutable_storage"].confidence == "high"
    assert result.summary["requires_immutable_storage"] is True
    assert result.summary["requires_export"] is True
    assert result.summary["requires_tamper_evidence"] is True
    assert result.summary["max_retention_days"] == 2555
    assert result.summary["status"] == "ready_for_audit_log_retention_planning"


def test_structured_and_free_text_inputs_normalize_retention_windows():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Admin access logs must record actor, action, resource, and timestamp.",
                "Audit log retention window is 1 year and records must be exportable for auditors.",
            ],
            definition_of_done=[
                "Event logs are retained for 90 days in immutable storage.",
            ],
        )
    )
    text_result = build_source_audit_log_retention_requirements(
        "Audit trail must capture data changes and retain audit logs for 90 days."
    )
    implementation_result = generate_source_audit_log_retention_requirements(implementation)

    retention = [
        (record.retention_window, record.retention_days)
        for record in implementation_result.records
        if record.requirement_type == "retention_window"
    ]

    assert text_result.source_id is None
    assert text_result.records[0].requirement_type == "audit_event_capture"
    assert ("90 days", 90) in retention
    assert ("1 year", 365) in retention
    assert any(record.requirement_type == "admin_access_logs" for record in implementation_result.records)
    assert any(record.requirement_type == "exportability" for record in implementation_result.records)


def test_model_aliases_serialization_markdown_and_no_mutation_are_stable():
    source = _source_brief(
        source_id="audit-model",
        source_payload={
            "acceptance_criteria": [
                "Audit logs must retain admin export events for 1 year with actor | resource metadata.",
                "Audit logs must retain admin export events for 1 year with actor | resource metadata.",
                "Compliance evidence export is required for auditor review.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_audit_log_retention_requirements(source)
    model_result = extract_source_audit_log_retention_requirements(model)
    payload = source_audit_log_retention_requirements_to_dict(model_result)
    markdown = source_audit_log_retention_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_audit_log_retention_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_audit_log_retention_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_audit_log_retention_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "source_field",
        "requirement_type",
        "audit_surface",
        "retention_window",
        "retention_days",
        "evidence",
        "confidence",
        "planning_notes",
    ]
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Source Field | Type | Surface | Retention Window |" in markdown
    assert "actor \\| resource metadata" in markdown
    assert any(record.retention_window == "1 year" for record in model_result.records)


def test_explicit_no_impact_suppression_unrelated_malformed_and_object_inputs_are_stable_empty():
    class BriefLike:
        id = "object-no-audit"
        summary = "No audit logging, audit log retention, or compliance evidence changes are required for this release."

    object_result = build_source_audit_log_retention_requirements(
        SimpleNamespace(
            id="object-audit",
            summary="Admin access logs must record support agent impersonation and retain audit logs for 90 days.",
        )
    )
    negated = build_source_audit_log_retention_requirements(BriefLike())
    no_scope = build_source_audit_log_retention_requirements(
        _source_brief(summary="Audit logging is out of scope and no audit log retention work is planned.")
    )
    unrelated = build_source_audit_log_retention_requirements(
        _source_brief(
            title="Dashboard polish",
            summary="Update report table labels and empty state copy.",
            source_payload={"requirements": ["CSV exports should include a generated at timestamp."]},
        )
    )
    malformed = build_source_audit_log_retention_requirements({"source_payload": {"notes": object()}})
    blank = build_source_audit_log_retention_requirements("")

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "requirement_type_counts": {
            "audit_event_capture": 0,
            "retention_window": 0,
            "immutable_storage": 0,
            "exportability": 0,
            "tamper_evidence": 0,
            "metadata_capture": 0,
            "admin_access_logs": 0,
            "compliance_evidence": 0,
        },
        "audit_surfaces": [],
        "retention_windows": [],
        "max_retention_days": 0,
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "requires_immutable_storage": False,
        "requires_export": False,
        "requires_tamper_evidence": False,
        "status": "no_audit_log_retention_language",
    }
    assert any(record.retention_window == "90 days" for record in object_result.records)
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source audit log retention requirements were inferred." in unrelated.to_markdown()


def test_deterministic_sorting_orders_by_type_surface_and_retention_window():
    result = build_source_audit_log_retention_requirements(
        [
            _source_brief(
                source_id="z-source",
                source_payload={"audit": "Audit logs must retain data export events for 7 years."},
            ),
            _source_brief(
                source_id="a-source",
                source_payload={
                    "audit": [
                        "Audit logs must retain admin access events for 90 days.",
                        "Audit logs must retain admin access events for 1 year.",
                    ]
                },
            ),
        ]
    )

    keys = [
        (record.source_brief_id, record.requirement_type, record.audit_surface, record.retention_days)
        for record in result.records
    ]

    assert result.source_id is None
    assert keys == sorted(
        keys,
        key=lambda item: (
            item[0] or "",
            [
                "audit_event_capture",
                "retention_window",
                "immutable_storage",
                "exportability",
                "tamper_evidence",
                "metadata_capture",
                "admin_access_logs",
                "compliance_evidence",
            ].index(item[1]),
            item[2].casefold(),
            item[3] if item[3] is not None else 10**9,
        ),
    )
    assert [
        record.retention_window
        for record in result.records
        if record.source_brief_id == "a-source" and record.requirement_type == "retention_window"
    ] == ["90 days", "1 year"]
    assert result.to_dict() == build_source_audit_log_retention_requirements(
        [
            _source_brief(
                source_id="z-source",
                source_payload={"audit": "Audit logs must retain data export events for 7 years."},
            ),
            _source_brief(
                source_id="a-source",
                source_payload={
                    "audit": [
                        "Audit logs must retain admin access events for 90 days.",
                        "Audit logs must retain admin access events for 1 year.",
                    ]
                },
            ),
        ]
    ).to_dict()


def _source_brief(
    *,
    source_id="source-audit-logs",
    title="Audit log retention requirements",
    domain="security",
    summary="General audit log retention requirements.",
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


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "implementation-audit-logs",
        "source_brief_id": "source-audit-logs",
        "title": "Audit log retention implementation",
        "domain": "security",
        "target_user": "admins",
        "buyer": "security",
        "workflow_context": "Security and compliance evidence.",
        "problem_statement": "Admins need reliable audit log retention.",
        "mvp_goal": "Plan audit logging controls.",
        "product_surface": "admin",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "architecture_notes": None,
        "data_requirements": None,
        "validation_plan": "Run audit log retention extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "integration_points": [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
