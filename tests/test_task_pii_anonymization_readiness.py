import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_pii_anonymization_readiness import (
    TaskPiiAnonymizationReadinessPlan,
    TaskPiiAnonymizationReadinessRecord,
    analyze_task_pii_anonymization_readiness,
    build_task_pii_anonymization_readiness_plan,
    extract_task_pii_anonymization_readiness,
    generate_task_pii_anonymization_readiness,
    recommend_task_pii_anonymization_readiness,
    summarize_task_pii_anonymization_readiness,
    task_pii_anonymization_readiness_plan_to_dict,
    task_pii_anonymization_readiness_plan_to_markdown,
)


def test_strong_pii_anonymization_readiness_is_ready():
    result = build_task_pii_anonymization_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Anonymize customer PII exports",
                    description="Hash email addresses and tokenize customer identifiers before analytics export.",
                    acceptance_criteria=[
                        "Reversibility policy states hashes are one-way and tokens require token vault approval.",
                        "Re-identification risk check covers linkage attacks and uniqueness.",
                        "Test fixture coverage includes email, phone, and name examples.",
                        "Downstream consumer validation confirms reporting and data contracts still work.",
                        "Retention policy aligns source PII, token maps, and deletion schedules.",
                        "Audit evidence records privacy review and verification decisions.",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskPiiAnonymizationReadinessPlan)
    assert result.pii_task_ids == ("task-ready",)
    record = result.records[0]
    assert isinstance(record, TaskPiiAnonymizationReadinessRecord)
    assert record.techniques == ("anonymization", "tokenization", "hashing")
    assert record.present_controls == (
        "reversibility_policy",
        "reidentification_risk_check",
        "test_fixture_coverage",
        "downstream_consumer_validation",
        "retention_alignment",
        "audit_evidence",
    )
    assert record.missing_controls == ()
    assert record.readiness_level == "ready"
    assert result.summary["task_count"] == 1
    assert result.summary["affected_task_count"] == 1
    assert result.summary["pii_task_count"] == 1
    assert result.summary["readiness_counts"] == {"missing": 0, "partial": 0, "ready": 1}
    assert result.summary["missing_control_count"] == 0


def test_partial_readiness_detects_tags_metadata_and_missing_controls():
    result = analyze_task_pii_anonymization_readiness(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Pseudonymize support data",
                    description="Replace customer IDs with surrogate keys for support analytics.",
                    tags=["pii", "pseudonymization"],
                    acceptance_criteria=[
                        "Reversibility policy documents the token map and detokenization owners.",
                        "Audit logging captures privacy review evidence.",
                    ],
                    metadata={"retention_alignment": "Token maps follow the data retention policy."},
                )
            ]
        )
    )

    record = result.records[0]
    assert record.techniques == ("pseudonymization",)
    assert record.present_controls == ("reversibility_policy", "retention_alignment", "audit_evidence")
    assert record.missing_controls == (
        "reidentification_risk_check",
        "test_fixture_coverage",
        "downstream_consumer_validation",
    )
    assert record.readiness_level == "partial"
    assert any("tags[1]" in item for item in record.evidence)
    assert any("metadata.retention_alignment" in item for item in record.evidence)


def test_missing_readiness_reports_all_controls_with_file_path_hints():
    result = build_task_pii_anonymization_readiness_plan(
        _plan(
            [
                _task(
                    "task-path",
                    title="Wire batch export",
                    description="Generate privacy-safe records.",
                    files_or_modules=[
                        "src/privacy/pii_redaction.py",
                        "src/anonymization/synthetic_data_builder.py",
                    ],
                    acceptance_criteria=["Export job completes."],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.techniques == ("anonymization", "synthetic_data", "redaction")
    assert record.present_controls == ()
    assert record.missing_controls == (
        "reversibility_policy",
        "reidentification_risk_check",
        "test_fixture_coverage",
        "downstream_consumer_validation",
        "retention_alignment",
        "audit_evidence",
    )
    assert record.readiness_level == "missing"
    assert "files_or_modules: src/privacy/pii_redaction.py" in record.evidence
    assert result.summary["missing_control_counts"]["audit_evidence"] == 1
    assert result.summary["technique_counts"]["redaction"] == 1


def test_acceptance_criteria_detects_masking_deidentification_and_controls():
    result = build_task_pii_anonymization_readiness_plan(
        _plan(
            [
                _task(
                    "task-ac",
                    title="Prepare data warehouse changes",
                    description="Update customer mart.",
                    acceptance_criteria=[
                        "Mask phone numbers and de-identify birth dates before publishing.",
                        "Fixture coverage includes masked and unmasked samples.",
                        "Consumer validation confirms downstream warehouse exports still parse.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.techniques == ("masking", "deidentification")
    assert record.present_controls == ("test_fixture_coverage", "downstream_consumer_validation")
    assert record.readiness_level == "partial"
    assert any("acceptance_criteria[0]" in item for item in record.evidence)


def test_non_pii_tasks_are_ignored_and_summary_is_stable():
    result = build_task_pii_anonymization_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust settings labels and loading states.",
                    files_or_modules=["src/ui/settings_panel.tsx"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.pii_task_ids == ()
    assert result.ignored_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "affected_task_count": 0,
        "pii_task_count": 0,
        "ignored_task_ids": ["task-copy"],
        "missing_control_count": 0,
        "readiness_counts": {"missing": 0, "partial": 0, "ready": 0},
        "missing_control_counts": {
            "reversibility_policy": 0,
            "reidentification_risk_check": 0,
            "test_fixture_coverage": 0,
            "downstream_consumer_validation": 0,
            "retention_alignment": 0,
            "audit_evidence": 0,
        },
        "technique_counts": {},
    }
    assert "No PII anonymization readiness records" in result.to_markdown()
    assert "Ignored tasks: task-copy" in result.to_markdown()


def test_deterministic_serialization_markdown_aliases_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Redact PII | exports",
                description="Redact customer names without controls yet.",
            ),
            _task(
                "task-a",
                title="Tokenize customer identifiers",
                description="Tokenization includes audit evidence and re-identification risk check.",
            ),
            _task("task-copy", title="Update empty state", description="Change UI copy."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_pii_anonymization_readiness(plan)
    payload = task_pii_anonymization_readiness_plan_to_dict(result)
    markdown = task_pii_anonymization_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert extract_task_pii_anonymization_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_pii_anonymization_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_pii_anonymization_readiness(plan).to_dict() == result.to_dict()
    assert result.pii_task_ids == ("task-z", "task-a")
    assert result.ignored_task_ids == ("task-copy",)
    assert list(payload) == [
        "plan_id",
        "records",
        "pii_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "techniques",
        "present_controls",
        "missing_controls",
        "readiness_level",
        "evidence",
        "recommended_readiness_steps",
    ]
    assert [record.readiness_level for record in result.records] == ["missing", "partial"]
    assert markdown.startswith("# Task PII Anonymization Readiness: plan-pii")
    assert "Redact PII \\| exports" in markdown
    assert "| Task | Title | Readiness | Techniques | Missing Controls | Evidence |" in markdown


def test_execution_plan_and_iterable_inputs_are_supported():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Mask user phone numbers",
                    description="Mask PII with fixture coverage and downstream consumer validation.",
                    acceptance_criteria=[
                        "Reversibility policy states masking is irreversible.",
                        "Re-identification risk check is complete.",
                        "Retention alignment follows deletion policy.",
                        "Audit evidence is attached.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )
    iterable_result = build_task_pii_anonymization_readiness_plan(
        [
            _task(
                "task-one",
                title="De-identify research dataset",
                description="Use de-identification with audit evidence.",
            )
        ]
    )

    result = build_task_pii_anonymization_readiness_plan(model)

    assert result.plan_id == "plan-model"
    assert result.records[0].task_id == "task-model"
    assert result.records[0].readiness_level == "ready"
    assert iterable_result.plan_id is None
    assert iterable_result.pii_task_ids == ("task-one",)


def _plan(tasks, plan_id="plan-pii"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-pii",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
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
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-pii",
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
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
