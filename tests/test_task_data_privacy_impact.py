import copy
import json
from dataclasses import dataclass

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_data_privacy_impact import (
    TaskDataPrivacyImpactPlan,
    build_task_data_privacy_impact_plan,
    recommend_task_data_privacy_impacts,
    task_data_privacy_impact_plan_to_dict,
    task_data_privacy_impact_plan_to_markdown,
)


def test_privacy_sensitive_task_classifies_categories_safeguards_and_questions():
    result = build_task_data_privacy_impact_plan(
        _plan(
            [
                _task(
                    "task-privacy",
                    title="Export user profile PII to vendor webhook",
                    description=(
                        "Add CSV export for email addresses, full name, and user profile "
                        "fields shared with a third-party analytics processor."
                    ),
                    files_or_modules=[
                        "src/users/profiles/exporter.py",
                        "src/integrations/vendors/profile_webhook.py",
                    ],
                    acceptance_criteria=["Privacy review signs off outbound fields."],
                )
            ]
        )
    )

    impact = result.task_impacts[0]

    assert impact.task_id == "task-privacy"
    assert impact.impact_level == "high"
    assert impact.data_categories == (
        "pii",
        "analytics",
        "user_profiles",
        "exports",
        "third_party_sharing",
    )
    assert "Classify PII fields and apply data minimization before implementation." in (
        impact.required_safeguards
    )
    assert "Confirm the third party is approved for the data categories being shared." in (
        impact.required_safeguards
    )
    assert "Which exact personal data fields are collected, stored, displayed, or transformed?" in (
        impact.follow_up_questions
    )
    assert "What third party receives the data and under which processing agreement?" in (
        impact.follow_up_questions
    )
    assert result.privacy_task_ids == ("task-privacy",)
    assert result.no_impact_task_ids == ()
    assert result.summary["category_counts"]["pii"] == 1


def test_low_risk_task_returns_explicit_no_impact_result():
    result = build_task_data_privacy_impact_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard empty state",
                    description="Clarify copy and spacing for a static admin dashboard panel.",
                    files_or_modules=["src/ui/empty_state.py"],
                )
            ]
        )
    )

    impact = result.task_impacts[0]

    assert impact.task_id == "task-copy"
    assert impact.impact_level == "low"
    assert impact.data_categories == ()
    assert impact.required_safeguards == ()
    assert impact.follow_up_questions == ()
    assert impact.rationale == "No privacy-sensitive data handling signals detected."
    assert impact.evidence == ()
    assert result.privacy_task_ids == ()
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary == {
        "task_count": 1,
        "privacy_task_count": 0,
        "no_impact_task_count": 1,
        "high_impact_count": 0,
        "medium_impact_count": 0,
        "low_impact_count": 1,
        "category_counts": {
            "pii": 0,
            "phi": 0,
            "credentials": 0,
            "logs": 0,
            "analytics": 0,
            "user_profiles": 0,
            "exports": 0,
            "retention": 0,
            "deletion": 0,
            "consent": 0,
            "third_party_sharing": 0,
        },
    }


def test_mixed_task_shapes_are_supported_without_database_access():
    dict_task = _task(
        "task-retention",
        title="Add retention purge for account deletion",
        description="Purge user data after account deletion and apply TTL retention rules.",
        files_or_modules=["src/privacy/deletion/account_purge.py"],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-secret",
            title="Rotate OAuth refresh token storage",
            description="Move credentials into secret storage and prevent token logging.",
            files_or_modules=["src/auth/oauth_tokens.py"],
        )
    )
    object_task = TaskLike(
        id="task-consent",
        title="Add analytics consent gate",
        description="Check opt-in consent before emitting product analytics events.",
        files_or_modules=["src/analytics/consent_events.py"],
        acceptance_criteria=["Consent state is respected."],
    )

    result = recommend_task_data_privacy_impacts([dict_task, model_task, object_task])

    assert isinstance(result, TaskDataPrivacyImpactPlan)
    assert result.plan_id is None
    assert result.privacy_task_ids == ("task-retention", "task-secret", "task-consent")
    assert [impact.task_id for impact in result.task_impacts] == [
        "task-retention",
        "task-secret",
        "task-consent",
    ]
    assert _impact(result, "task-retention").data_categories == ("pii", "retention", "deletion")
    assert _impact(result, "task-secret").impact_level == "high"
    assert _impact(result, "task-secret").data_categories == ("credentials", "logs")
    assert _impact(result, "task-consent").data_categories == (
        "analytics",
        "consent",
    )


def test_execution_plan_input_serializes_without_mutation():
    plan = _plan(
        [
            _task(
                "task-health",
                title="Add patient PHI audit logs",
                description="Record audit events when medical records are viewed.",
                files_or_modules=["src/health/patients/audit_log.py"],
                metadata={"data": {"classification": "PHI"}},
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_data_privacy_impact_plan(ExecutionPlan.model_validate(plan))
    payload = task_data_privacy_impact_plan_to_dict(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["task_impacts"]
    assert list(payload) == [
        "plan_id",
        "task_impacts",
        "privacy_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["task_impacts"][0]) == [
        "task_id",
        "title",
        "impact_level",
        "data_categories",
        "required_safeguards",
        "follow_up_questions",
        "rationale",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_category_safeguard_question_and_evidence_ordering_is_stable():
    result = build_task_data_privacy_impact_plan(
        _plan(
            [
                _task(
                    "task-order",
                    title="Share credentials, consent, PII, deletion, and retention data",
                    description=(
                        "Delete personal data, retain audit logs, send api key credentials "
                        "to an external API, and honor opt-out consent."
                    ),
                    files_or_modules=[
                        "src/vendors/processor.py",
                        "src/privacy/retention.py",
                        "src/auth/secrets.py",
                    ],
                    metadata={
                        "z": {"signals": {"third": "third-party sharing"}},
                        "a": {"signals": {"analytics": "analytics tracking"}},
                    },
                )
            ]
        )
    )

    impact = result.task_impacts[0]

    assert impact.data_categories == (
        "pii",
        "credentials",
        "logs",
        "analytics",
        "retention",
        "deletion",
        "consent",
        "third_party_sharing",
    )
    assert impact.required_safeguards == tuple(dict.fromkeys(impact.required_safeguards))
    assert impact.follow_up_questions == tuple(dict.fromkeys(impact.follow_up_questions))
    assert impact.evidence == tuple(dict.fromkeys(impact.evidence))
    assert impact.evidence[0] == "title: Share credentials, consent, PII, deletion, and retention data"
    assert result.summary["high_impact_count"] == 1


def test_markdown_and_invalid_empty_inputs_are_deterministic():
    result = build_task_data_privacy_impact_plan(
        _plan(
            [
                _task(
                    "task-export",
                    title="Export customer email addresses",
                    description="Add a report download with PII fields.",
                    files_or_modules=["src/reports/customer_export.py"],
                )
            ]
        )
    )
    empty = build_task_data_privacy_impact_plan({"id": "plan-empty", "tasks": []})
    invalid = build_task_data_privacy_impact_plan({"id": "plan-invalid", "tasks": "nope"})
    none_source = build_task_data_privacy_impact_plan(None)  # type: ignore[arg-type]

    markdown = task_data_privacy_impact_plan_to_markdown(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Data Privacy Impact Plan: plan-privacy")
    assert "| `task-export` | high | pii, exports |" in markdown
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Data Privacy Impact Plan: plan-empty",
            "",
            "No tasks were available for privacy impact assessment.",
        ]
    )
    assert invalid.summary["task_count"] == 0
    assert invalid.task_impacts == ()
    assert none_source.summary["task_count"] == 0
    assert none_source.task_impacts == ()


@dataclass(frozen=True)
class TaskLike:
    id: str
    title: str
    description: str
    files_or_modules: list[str]
    acceptance_criteria: list[str]


def _impact(result, task_id):
    return next(impact for impact in result.task_impacts if impact.task_id == task_id)


def _plan(tasks):
    return {
        "id": "plan-privacy",
        "implementation_brief_id": "brief-privacy",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
