import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_privacy_consent_readiness import (
    PrivacyConsentReadinessTask,
    TaskPrivacyConsentReadinessPlan,
    TaskPrivacyConsentReadinessRecord,
    analyze_task_privacy_consent_readiness,
    build_task_privacy_consent_readiness_plan,
    derive_task_privacy_consent_readiness,
    extract_task_privacy_consent_readiness,
    generate_task_privacy_consent_readiness,
    recommend_task_privacy_consent_readiness,
    summarize_task_privacy_consent_readiness,
    task_privacy_consent_readiness_plan_to_dict,
    task_privacy_consent_readiness_plan_to_dicts,
    task_privacy_consent_readiness_plan_to_markdown,
    task_privacy_consent_readiness_to_dicts,
)


def test_explicit_consent_workflow_generates_actionable_readiness_tasks():
    result = build_task_privacy_consent_readiness_plan(
        _plan(
            [
                _task(
                    "task-consent-workflow",
                    title="Implement marketing consent workflow",
                    description=(
                        "Capture opt-in consent in the signup form, store the consent policy version "
                        "and effective date, expose privacy preferences in the preference center, "
                        "use GDPR and CCPA jurisdiction-specific copy, audit consent events, and "
                        "propagate opt-out changes to downstream CRM and email providers."
                    ),
                    files_or_modules=[
                        "src/privacy/consent_capture.py",
                        "src/privacy/downstream_consent_sync.py",
                    ],
                    acceptance_criteria=[
                        "Users can withdraw consent from the preference center.",
                        "Audit history records previous value, new value, actor, and timestamp.",
                    ],
                    metadata={
                        "validation_commands": {
                            "test": ["poetry run pytest tests/test_consent_workflow.py"]
                        }
                    },
                )
            ]
        )
    )

    assert isinstance(result, TaskPrivacyConsentReadinessPlan)
    assert result.consent_task_ids == ("task-consent-workflow",)
    assert len(result.records) == 1
    record = result.records[0]
    assert isinstance(record, TaskPrivacyConsentReadinessRecord)
    assert record.readiness == "ready"
    assert record.detected_signals == (
        "consent",
        "consent_capture",
        "consent_withdrawal",
        "consent_versioning",
        "preference_center",
        "jurisdiction_copy",
        "auditability",
        "downstream_propagation",
    )
    assert _categories(record) == (
        "consent_capture",
        "consent_withdrawal",
        "consent_versioning",
        "preference_center",
        "jurisdiction_copy",
        "auditability",
        "downstream_propagation",
    )
    assert all(isinstance(task, PrivacyConsentReadinessTask) for task in record.generated_tasks)
    assert all(len(task.acceptance_criteria) == 3 for task in record.generated_tasks)
    assert record.missing_recommended_categories == ()
    assert record.suggested_validation_commands == (
        "poetry run pytest",
        "poetry run pytest tests/test_consent_workflow.py",
    )
    assert any("description: Capture opt-in consent" in item for item in record.evidence)
    assert result.summary["generated_task_category_counts"]["downstream_propagation"] == 1
    assert result.summary["signal_counts"]["jurisdiction_copy"] == 1


def test_withdrawal_only_requirement_stays_partial_without_inventing_capture_or_downstream():
    task = ExecutionTask.model_validate(
        _task(
            "task-withdrawal",
            title="Allow users to revoke tracking consent",
            description="Users must withdraw consent and opt out from tracking in privacy settings.",
            acceptance_criteria=[
                "Revoked consent blocks future tracking.",
                "Consent history records the withdrawal timestamp.",
            ],
            files_or_modules=["src/privacy/consent_withdrawal.py"],
        )
    )

    result = analyze_task_privacy_consent_readiness(task)

    record = result.records[0]
    assert record.detected_signals == (
        "consent",
        "consent_withdrawal",
        "preference_center",
        "auditability",
    )
    assert _categories(record) == (
        "consent_withdrawal",
        "preference_center",
        "auditability",
    )
    assert "consent_capture" not in _categories(record)
    assert "downstream_propagation" not in _categories(record)
    assert record.missing_recommended_categories == ()
    assert record.readiness == "needs_planning"


def test_missing_auditability_and_downstream_are_reported_as_planning_gaps():
    result = build_task_privacy_consent_readiness_plan(
        _plan(
            [
                _task(
                    "task-capture-only",
                    title="Capture cookie consent in onboarding",
                    description=(
                        "Onboarding must collect opt-in cookie consent through a consent banner "
                        "and show current choices in the preference center."
                    ),
                    acceptance_criteria=["Consent records include the policy version shown to the user."],
                )
            ],
            metadata={"validation_command": "poetry run pytest tests/test_cookie_consent.py"},
        )
    )

    record = result.records[0]
    assert record.readiness == "partial"
    assert _categories(record) == (
        "consent_capture",
        "consent_withdrawal",
        "consent_versioning",
        "preference_center",
        "auditability",
    )
    assert record.missing_recommended_categories == ("downstream_propagation",)
    assert result.summary["missing_recommended_category_counts"]["downstream_propagation"] == 1
    assert record.suggested_validation_commands == (
        "poetry run pytest",
        "poetry run pytest tests/test_cookie_consent.py",
    )


def test_unrelated_privacy_copy_and_negated_scope_are_no_impact():
    plan = _plan(
        [
            _task(
                "task-privacy-copy",
                title="Update privacy policy copy",
                description="Refresh static privacy notice wording for the help center.",
            ),
            _task(
                "task-no-consent",
                title="Polish profile settings copy",
                description="No consent workflow changes are required for this copy update.",
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_privacy_consent_readiness_plan(plan)
    empty = build_task_privacy_consent_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_privacy_consent_readiness_plan(13)

    assert plan == original
    assert result.records == ()
    assert result.consent_task_ids == ()
    assert result.no_impact_task_ids == ("task-privacy-copy", "task-no-consent")
    assert result.summary["consent_task_count"] == 0
    assert "No task privacy consent readiness records were inferred." in result.to_markdown()
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert invalid.records == ()


def test_text_object_model_serialization_markdown_and_aliases_are_stable():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Version consent records",
                description="Consent versioning stores the policy version and audit trail.",
            )
        ],
        plan_id="plan-model",
    )
    object_task = SimpleNamespace(
        id="task-object",
        title="Propagate consent withdrawal",
        description="Consent opt-out must propagate to downstream analytics processors.",
        files_or_modules=["src/privacy/consent_propagation.py"],
        acceptance_criteria=["Propagation is idempotent."],
        status="pending",
    )
    model = ExecutionPlan.model_validate(plan)

    result = build_task_privacy_consent_readiness_plan(plan)
    generated = generate_task_privacy_consent_readiness(model)
    extracted = extract_task_privacy_consent_readiness([object_task])
    text_result = recommend_task_privacy_consent_readiness(
        "Preference center must let users revoke consent and audit consent history."
    )
    payload = task_privacy_consent_readiness_plan_to_dict(result)
    markdown = task_privacy_consent_readiness_plan_to_markdown(result)

    assert generated.to_dict() == result.to_dict()
    assert summarize_task_privacy_consent_readiness(result) is result
    assert derive_task_privacy_consent_readiness(plan).to_dict() == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_privacy_consent_readiness_plan_to_dicts(result) == payload["records"]
    assert task_privacy_consent_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_privacy_consent_readiness_to_dicts(result.records) == payload["records"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Privacy Consent Readiness: plan-model")
    assert extracted.records[0].task_id == "task-object"
    assert "downstream_propagation" in _categories(extracted.records[0])
    assert text_result.records[0].task_id == "requirement-text"
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "consent_task_ids",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "generated_tasks",
        "missing_recommended_categories",
        "readiness",
        "suggested_validation_commands",
        "evidence",
    ]


def _categories(record):
    return tuple(task.category for task in record.generated_tasks)


def _plan(tasks, *, plan_id="plan-consent", metadata=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-consent",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "backend",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
        "metadata": metadata or {},
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-consent",
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
