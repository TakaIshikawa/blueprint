import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_audit_evidence_readiness import (
    TaskAuditEvidenceReadinessPlan,
    TaskAuditEvidenceReadinessRecord,
    analyze_task_audit_evidence_readiness,
    build_task_audit_evidence_readiness_plan,
    derive_task_audit_evidence_readiness,
    extract_task_audit_evidence_readiness,
    generate_task_audit_evidence_readiness,
    recommend_task_audit_evidence_readiness,
    summarize_task_audit_evidence_readiness,
    task_audit_evidence_readiness_plan_to_dict,
    task_audit_evidence_readiness_plan_to_dicts,
    task_audit_evidence_readiness_plan_to_markdown,
    task_audit_evidence_readiness_to_dicts,
)


def test_detects_evidence_needs_from_task_text_paths_acceptance_and_metadata():
    result = build_task_audit_evidence_readiness_plan(
        _plan(
            [
                _task(
                    "task-compliance",
                    title="Prepare SOC2 compliance change",
                    description="Compliance review requires audit evidence for the updated retention control.",
                    files_or_modules=["docs/compliance/evidence_package.md"],
                    acceptance_criteria=[
                        "Attach compliance evidence and a validation report.",
                        "Owner approval is recorded before release.",
                    ],
                ),
                _task(
                    "task-migration",
                    title="Run customer data migration",
                    description="Backfill customer records and perform a schema migration.",
                    expected_file_paths=["db/migrations/20260502_customer_backfill.sql"],
                    validation_plan=[
                        "Attach migration dry-run output and row count reconciliation.",
                        "Include post-migration logs attached to the change record.",
                    ],
                ),
                _task(
                    "task-security",
                    title="Ship security review gate",
                    description="Security review is required for access control changes.",
                    metadata={
                        "security_review": {
                            "artifact": "Threat model and vulnerability scan are attached."
                        }
                    },
                ),
            ]
        )
    )

    assert isinstance(result, TaskAuditEvidenceReadinessPlan)
    assert all(isinstance(record, TaskAuditEvidenceReadinessRecord) for record in result.records)
    by_id = {record.task_id: record for record in result.records}

    assert by_id["task-compliance"].evidence_signals == (
        "audit_review",
        "compliance_review",
        "approval_gate",
    )
    assert by_id["task-compliance"].present_evidence_artifacts == (
        "compliance_evidence",
        "test_report",
        "approval_record",
    )
    assert by_id["task-compliance"].missing_recommended_artifacts == ("log_evidence",)
    assert by_id["task-migration"].present_evidence_artifacts == (
        "log_evidence",
        "migration_proof",
    )
    assert by_id["task-migration"].missing_recommended_artifacts == (
        "test_report",
        "approval_record",
    )
    assert by_id["task-security"].present_evidence_artifacts == (
        "security_review_artifact",
    )
    assert by_id["task-security"].missing_recommended_artifacts == (
        "test_report",
        "approval_record",
    )
    assert any("files_or_modules" in item for item in by_id["task-migration"].evidence)
    assert any("metadata.security_review" in item for item in by_id["task-security"].evidence)
    assert result.summary["signal_counts"]["compliance_review"] == 1
    assert result.summary["present_artifact_counts"]["migration_proof"] == 1
    assert result.summary["evidence_task_count"] == 3


def test_sensitive_task_without_evidence_requirements_is_high_risk():
    result = build_task_audit_evidence_readiness_plan(
        _plan(
            [
                _task(
                    "task-rollout",
                    title="Roll out regulated payment approval workflow",
                    description="Gradual rollout with release approval for PCI payment approval changes.",
                    acceptance_criteria=["Workflow is implemented."],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.risk_level == "high"
    assert record.present_evidence_artifacts == ()
    assert record.missing_recommended_artifacts == (
        "compliance_evidence",
        "log_evidence",
        "test_report",
        "approval_record",
        "rollout_proof",
    )
    assert record.recommended_evidence_artifacts == tuple(
        [
            "Attach compliance or control evidence that shows the acceptance criteria were met.",
            "Attach relevant logs or event excerpts proving the changed behavior ran in the target environment.",
            "Attach test reports or validation output for the required automated and manual checks.",
            "Record owner, compliance, security, or release approval with approver and timestamp.",
            "Attach rollout, canary, feature-flag ramp, deployment, or rollback verification proof.",
        ]
    )
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}


def test_present_artifacts_lower_risk_and_audit_log_behavior_alone_is_not_enough():
    result = build_task_audit_evidence_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Release compliance migration",
                    description="Compliance review covers a migration and canary rollout.",
                    acceptance_criteria=[
                        "Compliance evidence package is attached.",
                        "Test report and log excerpts are attached.",
                        "Approval record includes release owner sign-off.",
                        "Migration proof includes dry-run and row count reconciliation.",
                        "Rollout proof includes canary metrics.",
                    ],
                ),
                _task(
                    "task-audit-log",
                    title="Add audit log event capture",
                    description="Implement append-only audit logs with actor resource action fields.",
                    acceptance_criteria=["Audit log records are queryable."],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert result.evidence_task_ids == ("task-ready",)
    assert by_id["task-ready"].risk_level == "low"
    assert by_id["task-ready"].missing_recommended_artifacts == ()
    assert "task-audit-log" not in by_id
    assert result.not_applicable_task_ids == ("task-audit-log",)


def test_empty_no_impact_and_invalid_inputs_have_stable_empty_behavior():
    result = build_task_audit_evidence_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Change labels with no audit, compliance, security, migration, rollout, approval, or evidence required.",
                    files_or_modules=["src/ui/settings.tsx"],
                )
            ]
        )
    )
    invalid = build_task_audit_evidence_readiness_plan(42)

    assert result.records == ()
    assert result.recommendations == ()
    assert result.findings == ()
    assert result.evidence_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "evidence_task_count": 0,
        "not_applicable_task_ids": ["task-copy"],
        "missing_recommended_artifact_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "audit_review": 0,
            "compliance_review": 0,
            "security_review": 0,
            "migration_change": 0,
            "rollout_change": 0,
            "approval_gate": 0,
        },
        "present_artifact_counts": {
            "compliance_evidence": 0,
            "screenshot_evidence": 0,
            "log_evidence": 0,
            "test_report": 0,
            "approval_record": 0,
            "migration_proof": 0,
            "rollout_proof": 0,
            "security_review_artifact": 0,
        },
        "missing_artifact_counts": {
            "compliance_evidence": 0,
            "screenshot_evidence": 0,
            "log_evidence": 0,
            "test_report": 0,
            "approval_record": 0,
            "migration_proof": 0,
            "rollout_proof": 0,
            "security_review_artifact": 0,
        },
        "evidence_task_ids": [],
    }
    assert "No audit evidence readiness records" in result.to_markdown()
    assert "Not applicable tasks: task-copy" in result.to_markdown()
    assert invalid.records == ()
    assert invalid.to_markdown().startswith("# Task Audit Evidence Readiness")


def test_serialization_markdown_alias_order_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Security review | evidence",
                description="Security review requires threat model and vulnerability scan.",
            ),
            _task(
                "task-a",
                title="Rollout evidence",
                description="Feature flag rollout includes deployment evidence and pytest output.",
            ),
            _task("task-copy", title="Update copy", description="Change button text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = recommend_task_audit_evidence_readiness(plan)
    payload = task_audit_evidence_readiness_plan_to_dict(result)
    markdown = task_audit_evidence_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_audit_evidence_readiness_plan_to_dicts(result) == payload["records"]
    assert task_audit_evidence_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_audit_evidence_readiness_to_dicts(result.records) == payload["records"]
    assert analyze_task_audit_evidence_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_audit_evidence_readiness(plan).to_dict() == result.to_dict()
    assert extract_task_audit_evidence_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_audit_evidence_readiness(plan).to_dict() == result.to_dict()
    assert summarize_task_audit_evidence_readiness(plan).to_dict() == result.to_dict()
    assert result.recommendations == result.records
    assert result.findings == result.records
    assert result.evidence_task_ids == ("task-z", "task-a")
    assert result.not_applicable_task_ids == ("task-copy",)
    assert list(payload) == [
        "plan_id",
        "records",
        "evidence_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "evidence_signals",
        "risk_level",
        "present_evidence_artifacts",
        "missing_recommended_artifacts",
        "recommended_evidence_artifacts",
        "evidence",
    ]
    assert markdown.startswith("# Task Audit Evidence Readiness: plan-audit-evidence")
    assert "Security review \\| evidence" in markdown
    assert "| Task | Title | Risk | Evidence Signals | Present Artifacts | Missing Artifacts | Recommended Evidence | Evidence |" in markdown


def test_execution_plan_execution_task_mapping_iterable_and_object_inputs_are_supported():
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Security review model",
            description="Security review includes security checklist evidence.",
            acceptance_criteria=["Attach test report."],
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-plan",
                    title="Migration plan",
                    description="Schema migration requires migration verification.",
                )
            ],
            plan_id="plan-model",
        )
    )
    iterable_result = build_task_audit_evidence_readiness_plan(
        [
            _task(
                "task-iter",
                title="Approval iter",
                description="Owner approval and sign-off are required.",
            )
        ]
    )
    object_result = build_task_audit_evidence_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Compliance object",
            description="GDPR compliance review requires screenshots and compliance evidence.",
            acceptance_criteria=["Approval record is attached."],
        )
    )

    task_result = build_task_audit_evidence_readiness_plan(task_model)
    plan_result = build_task_audit_evidence_readiness_plan(plan_model)

    assert task_result.plan_id is None
    assert task_result.records[0].task_id == "task-model"
    assert task_result.records[0].present_evidence_artifacts == (
        "test_report",
        "security_review_artifact",
    )
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-plan"
    assert plan_result.records[0].evidence_signals == ("migration_change",)
    assert iterable_result.evidence_task_ids == ("task-iter",)
    assert object_result.records[0].present_evidence_artifacts == (
        "compliance_evidence",
        "screenshot_evidence",
        "approval_record",
    )


def _plan(tasks, plan_id="plan-audit-evidence"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-audit-evidence",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "web",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    expected_file_paths=None,
    acceptance_criteria=None,
    validation_plan=None,
    validation_commands=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-audit-evidence",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if expected_file_paths is not None:
        payload["expected_file_paths"] = expected_file_paths
    if validation_plan is not None:
        payload["validation_plan"] = validation_plan
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
