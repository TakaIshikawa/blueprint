import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_data_anonymization_readiness import (
    TaskDataAnonymizationReadinessFinding,
    TaskDataAnonymizationReadinessPlan,
    analyze_task_data_anonymization_readiness,
    build_task_data_anonymization_readiness_plan,
    extract_task_data_anonymization_readiness,
    generate_task_data_anonymization_readiness,
    recommend_task_data_anonymization_readiness,
    summarize_task_data_anonymization_readiness,
    task_data_anonymization_readiness_plan_to_dict,
    task_data_anonymization_readiness_plan_to_dicts,
    task_data_anonymization_readiness_plan_to_markdown,
)


def test_detects_anonymization_pseudonymization_hashing_tokenization_and_deidentification():
    result = build_task_data_anonymization_readiness_plan(
        _plan(
            [
                _task(
                    "task-export",
                    title="Build anonymized analytics export",
                    description=(
                        "Export an anonymized analytics dataset for partner sharing. "
                        "Pseudonymize customer identifiers, hash email addresses, tokenize account ids, "
                        "and de-identify profile fields before writing CSV reports."
                    ),
                    files_or_modules=[
                        "src/exports/anonymized_customer_export.py",
                        "src/privacy/tokenized_identifiers.py",
                    ],
                    acceptance_criteria=[
                        "Validation fixture proves PII fields are transformed.",
                        "Document downstream contract for partner use.",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskDataAnonymizationReadinessPlan)
    assert result.anonymization_task_ids == ("task-export",)
    finding = result.findings[0]
    assert isinstance(finding, TaskDataAnonymizationReadinessFinding)
    assert {
        "anonymization",
        "pseudonymization",
        "hashing",
        "tokenization",
        "de_identification",
        "analytics_dataset",
        "anonymized_export",
    } <= set(finding.transform_types)
    assert {
        "analytics dataset",
        "exports",
        "reports",
        "identifiers",
        "customer data",
        "personal data",
    } <= set(finding.data_surfaces)
    assert "reidentification_risk_review" in finding.missing_safeguards
    assert "salt_key_management" in finding.missing_safeguards
    assert "validation_fixture" in finding.present_safeguards
    assert "downstream_contract" in finding.present_safeguards
    assert finding.risk_level == "high"
    assert any(
        "description:" in item and "Pseudonymize customer identifiers" in item
        for item in finding.evidence
    )
    assert "files_or_modules: src/privacy/tokenized_identifiers.py" in finding.evidence
    assert result.summary["transform_counts"]["tokenization"] == 1
    assert result.summary["surface_counts"]["analytics dataset"] == 1


def test_metadata_and_acceptance_criteria_detect_present_safeguards():
    result = analyze_task_data_anonymization_readiness(
        _plan(
            [
                _task(
                    "task-logs",
                    title="Create privacy-safe logs",
                    description="Scrub logs and redact user email, IP address, and device identifiers from telemetry events.",
                    metadata={
                        "privacy_safe_logs": {
                            "field_inventory": "Schema inventory classifies PII and quasi-identifiers.",
                            "risk": "Re-identification risk review covers rare cohorts and joins.",
                            "transform": "Use irreversible one-way hashes with HMAC secret key management.",
                            "retention": "Raw log inputs expire after 7 days.",
                        }
                    },
                    acceptance_criteria=[
                        "Sampling policy enforces a minimum cohort threshold for analytics reports.",
                        "Downstream contract documents permitted use for log consumers.",
                        "Validation fixtures assert no PII remains in sanitized logs.",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert {"privacy_safe_logs", "redaction", "hashing"} <= set(finding.transform_types)
    assert {"logs", "personal data", "identifiers", "reports"} <= set(finding.data_surfaces)
    assert finding.present_safeguards == (
        "reidentification_risk_review",
        "irreversible_transform",
        "salt_key_management",
        "field_inventory",
        "sampling_policy",
        "downstream_contract",
        "retention_window",
        "validation_fixture",
    )
    assert finding.missing_safeguards == ()
    assert finding.risk_level == "low"
    assert any("metadata.privacy_safe_logs.field_inventory" in item for item in finding.evidence)


def test_redaction_requires_dataset_or_personal_data_context():
    result = build_task_data_anonymization_readiness_plan(
        _plan(
            [
                _task(
                    "task-ui",
                    title="Redact UI copy",
                    description="Redact a beta label from the dashboard header component.",
                ),
                _task(
                    "task-data",
                    title="Redact customer export",
                    description="Redact PII from customer export rows before sharing the CSV dataset.",
                ),
            ]
        )
    )

    assert result.anonymization_task_ids == ("task-data",)
    assert result.ignored_task_ids == ("task-ui",)
    finding = result.findings[0]
    assert finding.transform_types == ("redaction",)
    assert {"customer data", "exports", "personal data"} <= set(finding.data_surfaces)
    assert finding.missing_safeguards


def test_execution_plan_execution_task_single_task_and_empty_input():
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Generate de-identified test data",
            description="Generate synthetic test data from personal data fixtures with de-identification.",
        )
    )
    single_task = build_task_data_anonymization_readiness_plan(model_task)
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-hash",
                    title="Hash analytics identifiers",
                    description="Hash user identifiers for the warehouse analytics dataset.",
                ),
                _task("task-copy", title="Update copy", description="Adjust empty state wording."),
            ]
        )
    )
    plan_result = generate_task_data_anonymization_readiness(plan)
    empty = build_task_data_anonymization_readiness_plan([])
    noop = build_task_data_anonymization_readiness_plan(
        _plan([_task("task-copy", title="Update copy", description="Static text.")])
    )

    assert single_task.plan_id is None
    assert single_task.anonymization_task_ids == ("task-model",)
    assert plan_result.plan_id == "plan-anonymization"
    assert plan_result.anonymization_task_ids == ("task-hash",)
    assert plan_result.ignored_task_ids == ("task-copy",)
    assert empty.findings == ()
    assert empty.ignored_task_ids == ()
    assert noop.findings == ()
    assert noop.summary == {
        "task_count": 1,
        "anonymization_task_count": 0,
        "ignored_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "transform_counts": {
            "anonymization": 0,
            "pseudonymization": 0,
            "hashing": 0,
            "tokenization": 0,
            "de_identification": 0,
            "redaction": 0,
            "test_data_generation": 0,
            "privacy_safe_logs": 0,
            "analytics_dataset": 0,
            "anonymized_export": 0,
        },
        "surface_counts": {},
        "present_safeguard_counts": {
            "reidentification_risk_review": 0,
            "irreversible_transform": 0,
            "salt_key_management": 0,
            "field_inventory": 0,
            "sampling_policy": 0,
            "downstream_contract": 0,
            "retention_window": 0,
            "validation_fixture": 0,
        },
        "anonymization_task_ids": [],
    }
    assert "No task data anonymization readiness findings" in noop.to_markdown()
    assert "Ignored tasks: task-copy" in noop.to_markdown()


def test_serialization_markdown_aliases_order_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Tokenized partner export | privacy",
                description="Tokenize customer ids for a partner sharing dataset and anonymized export.",
            ),
            _task(
                "task-a",
                title="Synthetic data fixtures",
                description="Generate synthetic test data from PII records with validation fixture checks.",
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_data_anonymization_readiness(plan)
    payload = task_data_anonymization_readiness_plan_to_dict(result)
    markdown = task_data_anonymization_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.recommendations == result.findings
    assert result.to_dicts() == payload["findings"]
    assert task_data_anonymization_readiness_plan_to_dicts(result) == payload["findings"]
    assert task_data_anonymization_readiness_plan_to_dicts(result.findings) == payload["findings"]
    assert extract_task_data_anonymization_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_data_anonymization_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "findings",
        "records",
        "recommendations",
        "anonymization_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "data_surfaces",
        "transform_types",
        "required_safeguards",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "recommended_validation_checks",
        "evidence",
    ]
    assert result.anonymization_task_ids == ("task-z", "task-a")
    assert result.ignored_task_ids == ("task-copy",)
    assert markdown.startswith("# Task Data Anonymization Readiness Plan: plan-anonymization")
    assert "Tokenized partner export \\| privacy" in markdown
    assert (
        "| Task | Title | Risk | Data Surfaces | Transform Types | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |"
        in markdown
    )


def _plan(tasks, plan_id="plan-anonymization"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-anonymization",
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
        "execution_plan_id": "plan-anonymization",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
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
