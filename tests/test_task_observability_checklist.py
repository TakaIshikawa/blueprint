import copy
import json

from blueprint.domain.models import ExecutionTask
from blueprint.task_observability_checklist import (
    TaskObservabilityChecklist,
    TaskObservabilityRecommendation,
    build_task_observability_checklist,
    task_observability_checklist_to_dict,
)


def test_backend_api_task_gets_logs_metrics_traces_and_inspection():
    result = build_task_observability_checklist(
        _plan(
            [
                _task(
                    "task-api",
                    title="Add profile API endpoint",
                    description="Create a backend route that returns the user profile response.",
                    files_or_modules=["src/blueprint/api/profile.py"],
                    acceptance_criteria=["API latency is validated in the request test."],
                )
            ]
        )
    )

    checklist = result.checklists[0]

    assert isinstance(checklist, TaskObservabilityChecklist)
    assert checklist.task_id == "task-api"
    assert checklist.severity == "medium"
    assert [recommendation.area for recommendation in checklist.recommendations] == [
        "logs",
        "metrics",
        "traces",
        "post_release_inspection",
    ]
    assert _recommendation(checklist, "traces") == TaskObservabilityRecommendation(
        area="traces",
        severity="medium",
        reason="Backend/API work benefits from request or dependency trace correlation.",
        checklist_items=(
            "Propagate trace context across the changed API or external dependency boundary.",
        ),
        evidence=(
            "files_or_modules: src/blueprint/api/profile.py",
            "title: Add profile API endpoint",
            "description: Create a backend route that returns the user profile response.",
            "acceptance_criteria[0]: API latency is validated in the request test.",
        ),
    )


def test_low_signal_task_gets_minimal_validation_outcome():
    result = build_task_observability_checklist(
        [
            _task(
                "task-copy",
                title="Tighten settings copy",
                description="Adjust labels in the settings page.",
                files_or_modules=["docs/settings-copy.md"],
                acceptance_criteria=["Copy matches the approved wording."],
            )
        ]
    )

    checklist = result.checklists[0]

    assert checklist.severity == "none"
    assert checklist.reason == "No runtime observability signal detected; focused validation is sufficient."
    assert checklist.checklist_items == (
        "Run the focused validation for this task; no extra observability is required unless runtime behavior changes.",
    )
    assert checklist.recommendations[0].area == "validation"
    assert checklist.recommendations[0].severity == "none"


def test_high_risk_auth_integration_job_and_migration_receive_stronger_items():
    result = build_task_observability_checklist(
        _plan(
            [
                _task(
                    "task-risky",
                    title="Replace billing webhook auth worker",
                    description=(
                        "Process external Stripe callbacks in an async queue and backfill "
                        "permission audit rows."
                    ),
                    files_or_modules=[
                        "src/blueprint/integrations/stripe_webhook.py",
                        "src/blueprint/workers/billing.py",
                        "migrations/versions/add_permission_audit.sql",
                    ],
                    acceptance_criteria=[
                        "Alert when retry failures exceed the threshold.",
                        "Audit events include actor, action, and permission decision.",
                    ],
                    risk_level="high",
                    metadata={"observability_notes": "Dashboard should show queue failures."},
                )
            ]
        )
    )

    checklist = result.checklists[0]

    assert checklist.severity == "high"
    assert checklist.risk_level == "high"
    assert [recommendation.area for recommendation in checklist.recommendations] == [
        "logs",
        "metrics",
        "traces",
        "audit_events",
        "dashboards",
        "alerts",
        "post_release_inspection",
    ]
    assert _recommendation(checklist, "audit_events").severity == "high"
    assert "Avoid secrets in telemetry while preserving actor and permission decision context." in (
        _recommendation(checklist, "audit_events").checklist_items
    )
    assert "Capture external provider, operation, status code, timeout, and retry outcome." in (
        _recommendation(checklist, "alerts").checklist_items
    )
    assert "Track affected row counts, batch progress, duration, and rollback inspection points." in (
        _recommendation(checklist, "post_release_inspection").checklist_items
    )


def test_frontend_flow_gets_client_metrics_and_release_inspection():
    result = build_task_observability_checklist(
        _plan(
            [
                _task(
                    "task-frontend",
                    title="Update checkout flow",
                    description="Change the client-side checkout form and browser validation.",
                    files_or_modules=["src/components/CheckoutFlow.tsx"],
                    acceptance_criteria=["Users can complete checkout from the updated screen."],
                )
            ]
        )
    )

    checklist = result.checklists[0]

    assert checklist.severity == "low"
    assert [recommendation.area for recommendation in checklist.recommendations] == [
        "metrics",
        "post_release_inspection",
    ]
    assert "Inspect client-side errors or funnel completion for the changed user flow." in (
        _recommendation(checklist, "metrics").checklist_items
    )


def test_model_and_dict_inputs_are_supported_without_mutation_and_serialize_stably():
    task_dict = _task(
        "task-dict",
        title="Sync vendor records",
        description="Call an external API client and record integration failures.",
        metadata={"providers": ["salesforce"]},
    )
    original = copy.deepcopy(task_dict)
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Run account cleanup job",
            description="Scheduled worker retries failed account cleanup jobs.",
            files_or_modules=["src/blueprint/jobs/account_cleanup.py"],
        )
    )

    result = build_task_observability_checklist(_plan([task_dict, task_model]))
    payload = task_observability_checklist_to_dict(result)

    assert task_dict == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["checklists"]
    assert result.severity_counts == {"none": 0, "low": 0, "medium": 2, "high": 0}
    assert list(payload) == ["plan_id", "checklists", "severity_counts"]
    assert list(payload["checklists"][0]) == [
        "task_id",
        "title",
        "risk_level",
        "severity",
        "reason",
        "checklist_items",
        "recommendations",
    ]
    assert json.loads(json.dumps(payload)) == payload
    markdown = result.to_markdown()
    assert markdown.startswith("# Task Observability Checklist: plan-observability")
    assert "### task-dict: Sync vendor records" in markdown


def _recommendation(checklist, area):
    return next(recommendation for recommendation in checklist.recommendations if recommendation.area == area)


def _plan(tasks):
    return {
        "id": "plan-observability",
        "implementation_brief_id": "brief-observability",
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
    risk_level=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
