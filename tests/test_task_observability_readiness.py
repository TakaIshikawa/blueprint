import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_observability_readiness import (
    ObservabilityReadinessTask,
    TaskObservabilityReadinessPlan,
    TaskObservabilityReadinessRecord,
    analyze_task_observability_readiness,
    build_task_observability_readiness_plan,
    derive_task_observability_readiness,
    extract_task_observability_readiness,
    generate_task_observability_readiness,
    recommend_task_observability_readiness,
    summarize_task_observability_readiness,
    task_observability_readiness_plan_to_dict,
    task_observability_readiness_plan_to_dicts,
    task_observability_readiness_plan_to_markdown,
    task_observability_readiness_to_dicts,
)


def test_api_observability_generates_execution_ready_tasks_with_verification():
    result = build_task_observability_readiness_plan(
        _plan(
            [
                _task(
                    "task-api-observability",
                    title="Instrument checkout API endpoint",
                    description=(
                        "Add observability for the checkout API endpoint with structured logs, metrics, "
                        "distributed tracing, dashboard panels, alert thresholds, and runbook links."
                    ),
                    files_or_modules=["src/api/checkout_endpoint.py"],
                    metadata={"verification": "Post-deploy validation must prove telemetry appears."},
                )
            ]
        )
    )

    assert isinstance(result, TaskObservabilityReadinessPlan)
    assert len(result.records) == 1
    record = result.records[0]
    assert isinstance(record, TaskObservabilityReadinessRecord)
    assert record.task_id == "task-api-observability"
    assert record.context == "api"
    assert record.detected_signals == (
        "observability",
        "structured_logging",
        "metrics",
        "tracing",
        "dashboard",
        "alerts",
        "runbook",
        "verification",
        "api_context",
    )
    assert _categories(record) == (
        "structured_logging",
        "metrics",
        "distributed_tracing",
        "dashboard",
        "alerting",
        "runbook",
        "verification",
    )
    assert all(isinstance(task, ObservabilityReadinessTask) for task in record.generated_tasks)
    assert all(task.acceptance_criteria for task in record.generated_tasks)
    assert all(task.verification_steps for task in record.generated_tasks)
    assert any("API endpoint" in task.description for task in record.generated_tasks)
    assert any("request_id" in item for item in record.acceptance_criteria)
    assert any("description: Add observability for the checkout API endpoint" in item for item in record.evidence)
    assert result.observability_task_ids == ("task-api-observability",)
    assert result.impacted_task_ids == result.observability_task_ids
    assert result.summary["generated_task_category_counts"]["distributed_tracing"] == 1


def test_background_job_observability_adapts_task_wording_and_preserves_serialization():
    plan = _plan(
        [
            _task(
                "task-worker-observability",
                title="Monitor invoice retry worker | v2",
                description=(
                    "The background job consumes invoice queue messages. Add JSON logs with job id, "
                    "metrics for queue depth and duration, traces for downstream calls, and a dashboard."
                ),
                acceptance_criteria=[
                    "Alerting covers dead letter queue growth.",
                    "Verification runs a worker smoke test and confirms metrics.",
                ],
                validation_commands={"observability": ["poetry run pytest tests/jobs/test_invoice_worker_obs.py"]},
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = analyze_task_observability_readiness(plan)
    payload = task_observability_readiness_plan_to_dict(result)
    markdown = task_observability_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_observability_readiness_plan_to_dicts(result) == payload["records"]
    assert task_observability_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_observability_readiness_to_dicts(result.records) == payload["records"]
    assert extract_task_observability_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_observability_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_observability_readiness(plan).to_dict() == result.to_dict()
    assert summarize_task_observability_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_observability_readiness(plan).to_dict() == result.to_dict()
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.records[0].context == "background_job"
    assert any("background job" in task.description for task in result.records[0].generated_tasks)
    assert any("job_id" in item for item in result.records[0].acceptance_criteria)
    assert any("validation_commands: poetry run pytest tests/jobs/test_invoice_worker_obs.py" in item for item in result.records[0].evidence)
    assert "Monitor invoice retry worker \\| v2" in markdown
    assert "| Task | Title | Readiness | Context | Signals | Generated Tasks | Evidence |" in markdown
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "observability_task_ids",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "context",
        "generated_tasks",
        "readiness",
        "evidence",
    ]


def test_alerting_only_brief_generates_threshold_runbook_and_verification_tasks():
    result = build_task_observability_readiness_plan(
        _plan(
            [
                _task(
                    "task-alerts",
                    title="Add import failure alerts",
                    description=(
                        "Alert thresholds are required for elevated importer error rate and lack of "
                        "successful executions. Notifications must include dashboard and runbook links."
                    ),
                    metadata={"owner": "PagerDuty route belongs to the data platform on-call."},
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == ("observability", "dashboard", "alerts", "runbook")
    assert _categories(record) == ("dashboard", "alerting", "runbook", "verification")
    by_category = {task.category: task for task in record.generated_tasks}
    assert "alerting" in by_category
    assert any("evaluation window" in item for item in by_category["alerting"].acceptance_criteria)
    assert any("runbook links" in item for item in by_category["alerting"].acceptance_criteria)
    assert any("alert rules or monitors" in item for item in by_category["alerting"].verification_steps)
    assert any("referenced link works" in item for item in by_category["runbook"].verification_steps)
    assert result.summary["generated_task_category_counts"]["verification"] == 1


def test_no_observability_requirements_and_object_model_inputs_are_supported():
    no_match = build_task_observability_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Update onboarding copy", description="Static text only."),
                _task(
                    "task-explicit-none",
                    title="Cache refactor",
                    description="No observability requirements are in scope for this cache-only cleanup.",
                ),
            ]
        )
    )
    empty = build_task_observability_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_observability_readiness_plan(13)
    object_task = SimpleNamespace(
        id="task-object",
        title="Add API telemetry",
        description="API route needs logs and metrics with trace correlation.",
        files_or_modules=["src/api/users.py"],
        acceptance_criteria=["Done."],
        status="pending",
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Worker alerting",
            description="Background job needs alerting on retry exhaustion and a runbook.",
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([model_task.model_dump(mode="python")], plan_id="plan-model"))

    object_result = build_task_observability_readiness_plan([object_task])
    model_result = build_task_observability_readiness_plan(plan_model)

    assert no_match.records == ()
    assert no_match.no_impact_task_ids == ("task-copy", "task-explicit-none")
    assert "No task observability readiness records were inferred." in no_match.to_markdown()
    assert "No-impact tasks: task-copy, task-explicit-none" in no_match.to_markdown()
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert invalid.records == ()
    assert object_result.records[0].context == "api"
    assert {"structured_logging", "metrics", "distributed_tracing"} <= set(_categories(object_result.records[0]))
    assert model_result.plan_id == "plan-model"
    assert model_result.records[0].context == "background_job"


def _categories(record):
    return tuple(task.category for task in record.generated_tasks)


def _plan(tasks, plan_id="plan-observability"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-observability",
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
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    validation_commands=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-observability",
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
        "status": "pending",
    }
    if metadata is not None:
        payload["metadata"] = metadata
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
