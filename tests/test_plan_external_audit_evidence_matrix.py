import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_external_audit_evidence_matrix import (
    PlanExternalAuditEvidenceMatrix,
    PlanExternalAuditEvidenceMatrixRow,
    build_plan_external_audit_evidence_matrix,
    derive_plan_external_audit_evidence_matrix,
    generate_plan_external_audit_evidence_matrix,
    plan_external_audit_evidence_matrix_to_dict,
    plan_external_audit_evidence_matrix_to_dicts,
    plan_external_audit_evidence_matrix_to_markdown,
    summarize_plan_external_audit_evidence_matrix,
)


def test_detects_audit_tasks_and_extracts_prose_signals():
    result = build_plan_external_audit_evidence_matrix(
        _plan(
            [
                _task(
                    "task-soc2",
                    title="Prepare SOC 2 customer evidence package",
                    description=(
                        "Build SOC 2 evidence package for external auditors. "
                        "Artifacts: screenshots of access reviews and CSV exports. "
                        "Control owner: Security Compliance. Retention: store in Vanta for 7 years. "
                        "Review cadence: quarterly before audit review. Redact PII and secrets before sharing."
                    ),
                ),
                _task(
                    "task-questionnaire",
                    title="Security questionnaire export",
                    description="Generate security questionnaire responses for vendor evidence.",
                    metadata={"audit": {"owner": "Vendor Risk"}},
                ),
                _task(
                    "task-copy",
                    title="Update onboarding copy",
                    description="Refresh button labels.",
                ),
            ]
        )
    )

    by_id = {row.task_id: row for row in result.rows}
    assert set(by_id) == {"task-soc2", "task-questionnaire"}
    assert isinstance(by_id["task-soc2"], PlanExternalAuditEvidenceMatrixRow)
    assert by_id["task-soc2"].audit_framework == "SOC 2"
    assert by_id["task-soc2"].evidence_artifact == "screenshots of access reviews and CSV exports"
    assert by_id["task-soc2"].collection_owner == "Security Compliance"
    assert by_id["task-soc2"].retention_or_storage_signal == "store in Vanta for 7 years"
    assert by_id["task-soc2"].review_cadence_or_deadline == "quarterly before audit review"
    assert by_id["task-soc2"].privacy_or_redaction_signal == "PII and secrets before sharing"
    assert by_id["task-soc2"].missing_fields == ()
    assert by_id["task-questionnaire"].audit_framework == "security questionnaire"
    assert by_id["task-questionnaire"].collection_owner == "Vendor Risk"
    assert by_id["task-questionnaire"].missing_fields == (
        "retention_or_storage_signal",
        "privacy_or_redaction_signal",
    )
    assert result.summary["total_task_count"] == 3
    assert result.summary["audit_task_count"] == 2
    assert result.summary["non_audit_task_count"] == 1
    assert result.summary["tasks_missing_retention"] == 1
    assert result.summary["tasks_missing_redaction"] == 1


def test_structured_metadata_builds_ready_row_with_stable_evidence():
    result = build_plan_external_audit_evidence_matrix(
        _plan(
            [
                _task(
                    "task-pci",
                    title="PCI evidence export",
                    metadata={
                        "external_audit": {
                            "framework": "PCI DSS",
                            "artifact": "encrypted change-management export",
                            "control_owner": "Payments Security",
                            "storage_location": "GRC evidence locker",
                            "review_deadline": "2026-06-30",
                            "redaction": "mask cardholder data and tokens",
                        }
                    },
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.audit_framework == "PCI DSS"
    assert row.evidence_artifact == "encrypted change-management export"
    assert row.collection_owner == "Payments Security"
    assert row.retention_or_storage_signal == "GRC evidence locker"
    assert row.review_cadence_or_deadline == "2026-06-30"
    assert row.privacy_or_redaction_signal == "mask cardholder data and tokens"
    assert row.recommendation.startswith("Ready:")
    assert "metadata.external_audit.artifact: encrypted change-management export" in row.evidence
    assert result.summary["missing_field_count"] == 0


def test_missing_artifact_owner_retention_and_redaction_are_actionable():
    result = build_plan_external_audit_evidence_matrix(
        _plan(
            [
                _task(
                    "task-hipaa",
                    title="HIPAA compliance review prep",
                    description="Prepare for HIPAA compliance review before customer audit.",
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.audit_framework == "HIPAA"
    assert row.missing_fields == (
        "evidence_artifact",
        "collection_owner",
        "retention_or_storage_signal",
        "privacy_or_redaction_signal",
    )
    assert "specify the evidence artifact" in row.recommendation
    assert "assign the control or evidence collection owner" in row.recommendation
    assert "state the retention period" in row.recommendation
    assert "define redaction or privacy handling" in row.recommendation
    assert result.summary["tasks_missing_artifact"] == 1
    assert result.summary["tasks_missing_owner"] == 1
    assert result.summary["tasks_missing_retention"] == 1
    assert result.summary["tasks_missing_redaction"] == 1


def test_markdown_escapes_table_cells_and_serializes_deterministically():
    result = build_plan_external_audit_evidence_matrix(
        _plan(
            [
                _task(
                    "task-md",
                    title="SOC 2 | ISO 27001 evidence export",
                    description=(
                        "Framework: SOC 2 | ISO 27001. Artifact: access review export | screenshots. "
                        "Owner: GRC | Security. Retention: archive in Drata for 3 years. "
                        "Monthly collection. Redaction: mask employee emails | tokens."
                    ),
                )
            ]
        )
    )

    markdown = plan_external_audit_evidence_matrix_to_markdown(result)
    payload = plan_external_audit_evidence_matrix_to_dict(result)

    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "audit_framework",
        "evidence_artifact",
        "collection_owner",
        "retention_or_storage_signal",
        "review_cadence_or_deadline",
        "privacy_or_redaction_signal",
        "missing_fields",
        "evidence",
        "recommendation",
    ]
    assert "SOC 2 \\| ISO 27001 evidence export" in markdown
    assert "SOC 2 \\| ISO 27001" in markdown
    assert "access review export \\| screenshots" in markdown
    assert markdown == result.to_markdown()


def test_non_audit_plan_returns_stable_empty_matrix_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-api",
                title="Add profile endpoint",
                description="Create backend route for profile reads.",
            )
        ],
        plan_id="plan-non-audit",
    )
    original = copy.deepcopy(plan)

    result = build_plan_external_audit_evidence_matrix(plan)

    assert plan == original
    assert isinstance(result, PlanExternalAuditEvidenceMatrix)
    assert result.plan_id == "plan-non-audit"
    assert result.rows == ()
    assert result.records == ()
    assert result.to_dict() == {
        "plan_id": "plan-non-audit",
        "summary": {
            "total_task_count": 1,
            "audit_task_count": 0,
            "non_audit_task_count": 1,
            "tasks_missing_owner": 0,
            "tasks_missing_artifact": 0,
            "tasks_missing_retention": 0,
            "tasks_missing_redaction": 0,
            "missing_field_count": 0,
            "missing_field_counts": {
                "audit_framework": 0,
                "evidence_artifact": 0,
                "collection_owner": 0,
                "retention_or_storage_signal": 0,
                "privacy_or_redaction_signal": 0,
            },
        },
        "rows": [],
    }
    assert result.to_markdown() == (
        "# Plan External Audit Evidence Matrix: plan-non-audit\n\n"
        "## Summary\n\n"
        "- Total tasks: 1\n"
        "- Audit-related tasks: 0\n"
        "- Tasks missing artifacts: 0\n"
        "- Tasks missing owners: 0\n"
        "- Tasks missing retention: 0\n"
        "- Tasks missing redaction: 0\n\n"
        "No external audit evidence rows were inferred."
    )
    assert generate_plan_external_audit_evidence_matrix({"tasks": "not a list"}) == ()
    assert build_plan_external_audit_evidence_matrix(None).summary["total_task_count"] == 0


def test_model_input_aliases_and_iterable_rows_match():
    plan = _plan(
        [
            _task(
                "task-model",
                title="External audit evidence",
                description=(
                    "External auditor package for ISO 27001. Artifact: audit logs. "
                    "Owner: GRC. Retention: evidence locker for 6 years. "
                    "Deadline: by 2026-07-15. Redaction: remove personal data."
                ),
            )
        ],
        plan_id="plan-model",
    )
    model = ExecutionPlan.model_validate(plan)

    result = build_plan_external_audit_evidence_matrix(model)
    derived = derive_plan_external_audit_evidence_matrix(result)
    summarized = summarize_plan_external_audit_evidence_matrix(plan)
    rows = generate_plan_external_audit_evidence_matrix(model)
    payload = plan_external_audit_evidence_matrix_to_dict(result)

    assert derived is result
    assert rows == result.rows
    assert summarized.to_dict() == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert plan_external_audit_evidence_matrix_to_dicts(result) == payload["rows"]
    assert plan_external_audit_evidence_matrix_to_dicts(rows) == payload["rows"]
    assert payload["rows"][0]["audit_framework"] == "ISO 27001"
    assert payload["rows"][0]["evidence_artifact"] == "audit logs"


def _plan(tasks, *, plan_id="plan-audit"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-audit",
        "milestones": [{"name": "Launch"}],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
