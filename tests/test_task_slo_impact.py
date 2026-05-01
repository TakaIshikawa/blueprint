import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_slo_impact import (
    TaskSloImpactFinding,
    TaskSloImpactPlan,
    build_task_slo_impact_plan,
    recommend_task_slo_impacts,
    task_slo_impact_plan_to_dict,
    task_slo_impact_plan_to_markdown,
)


def test_backend_api_latency_and_error_rate_tasks_get_slo_guidance_with_evidence():
    result = build_task_slo_impact_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update onboarding copy",
                    description="Clarify setup text in the README.",
                    files_or_modules=["docs/onboarding.md"],
                ),
                _task(
                    "task-api",
                    title="Reduce checkout API p99 latency",
                    description=(
                        "Tune the customer-facing backend endpoint so p99 response "
                        "time stays within the SLO."
                    ),
                    files_or_modules=["src/api/checkout.py"],
                    acceptance_criteria=[
                        "Compare p95 and p99 latency before and after deployment.",
                    ],
                    risk_level="high",
                ),
                _task(
                    "task-errors",
                    title="Lower payment error rate",
                    description=(
                        "Handle 5xx errors from the public API without increasing "
                        "the failed requests SLO."
                    ),
                    files_or_modules=["src/backend/payments/retries.py"],
                    tags=["customer-facing"],
                ),
            ]
        )
    )

    assert result.plan_id == "plan-slo"
    assert [impact.task_id for impact in result.task_impacts] == [
        "task-api",
        "task-errors",
        "task-docs",
    ]
    assert result.task_impacts[0] == TaskSloImpactFinding(
        task_id="task-api",
        title="Reduce checkout API p99 latency",
        severity="high",
        signals=("latency", "availability", "customer_reliability"),
        recommended_slo_checks=(
            "Compare p95 and p99 latency before and after the task on affected endpoints or jobs.",
            "Verify uptime, health-check, and successful-request SLOs for the affected service path.",
            "Run customer-journey or synthetic checks for the affected runtime-critical path.",
        ),
        rationale=(
            "Task touches runtime-critical SLO signals requiring explicit guardrails: "
            "latency, availability, customer reliability."
        ),
        evidence=(
            "title: Reduce checkout API p99 latency",
            "description: Tune the customer-facing backend endpoint so p99 response time stays within the SLO.",
            "acceptance_criteria[0]: Compare p95 and p99 latency before and after deployment.",
            "files_or_modules: src/api/checkout.py",
        ),
    )
    assert result.task_impacts[1].severity == "high"
    assert result.task_impacts[1].signals == (
        "availability",
        "error_rate",
        "customer_reliability",
    )
    assert any("failed requests SLO" in item for item in result.task_impacts[1].evidence)
    assert result.summary["severity_counts"] == {"high": 2, "medium": 0, "low": 1}
    assert result.summary["signal_counts"]["latency"] == 1
    assert result.summary["signal_counts"]["error_rate"] == 1


def test_background_jobs_produce_freshness_and_throughput_checks():
    result = build_task_slo_impact_plan(
        _plan(
            [
                _task(
                    "task-worker",
                    title="Improve ingestion worker freshness",
                    description=(
                        "Reduce queue backlog and sync lag for nightly ETL jobs."
                    ),
                    files_or_modules=[
                        "src/jobs/ingestion_worker.py",
                        "src/sync/freshness_watermark.py",
                    ],
                    acceptance_criteria=[
                        "Watermark delay and worker throughput are reported.",
                    ],
                )
            ]
        )
    )

    impact = result.task_impacts[0]

    assert impact.severity == "medium"
    assert impact.signals == ("throughput", "data_freshness")
    assert impact.recommended_slo_checks == (
        "Measure throughput, queue depth, backlog age, and worker saturation under expected load.",
        "Check data age, replication lag, watermark delay, and last successful run freshness.",
    )
    assert result.summary["signal_counts"]["throughput"] == 1
    assert result.summary["signal_counts"]["data_freshness"] == 1


def test_low_signal_tasks_return_deterministic_low_impact_records_without_mutation():
    plan = _plan(
        [
            _task(
                "task-copy",
                title="Update dashboard label",
                description="Rename a status label in static content.",
                files_or_modules=["src/ui/labels.ts"],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_slo_impact_plan(plan)

    assert plan == original
    assert result.task_impacts == (
        TaskSloImpactFinding(
            task_id="task-copy",
            title="Update dashboard label",
            severity="low",
            recommended_slo_checks=(
                "Confirm no SLO dashboards, alerts, or runtime-critical paths are affected.",
            ),
        ),
    )
    assert result.slo_impacted_task_ids == ()
    assert result.low_impact_task_ids == ("task-copy",)
    assert result.summary == {
        "task_count": 1,
        "slo_impacted_task_count": 0,
        "low_impact_task_count": 1,
        "severity_counts": {"high": 0, "medium": 0, "low": 1},
        "signal_counts": {
            "latency": 0,
            "availability": 0,
            "error_rate": 0,
            "throughput": 0,
            "data_freshness": 0,
            "customer_reliability": 0,
        },
    }


def test_model_iterable_single_task_and_serialization_are_stable():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-low",
                    title="Refine help text",
                    description="Update tooltip text.",
                    files_or_modules=["src/ui/help.ts"],
                ),
                _task(
                    "task-api",
                    title="Add public API health check",
                    description="Add readiness checks for production traffic.",
                    files_or_modules=["src/api/health.py"],
                ),
            ]
        )
    )

    result = recommend_task_slo_impacts(model)
    payload = task_slo_impact_plan_to_dict(result)
    iterable = build_task_slo_impact_plan([model.tasks[1], model.tasks[0]])
    single = build_task_slo_impact_plan(model.tasks[1])

    assert isinstance(result, TaskSloImpactPlan)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["task_impacts"]
    assert list(payload) == [
        "plan_id",
        "task_impacts",
        "slo_impacted_task_ids",
        "low_impact_task_ids",
        "summary",
    ]
    assert list(payload["task_impacts"][0]) == [
        "task_id",
        "title",
        "severity",
        "signals",
        "recommended_slo_checks",
        "rationale",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert [impact.task_id for impact in result.task_impacts] == ["task-api", "task-low"]
    assert [impact.task_id for impact in iterable.task_impacts] == ["task-api", "task-low"]
    assert single.plan_id is None
    assert single.task_impacts[0].task_id == "task-api"


def test_markdown_and_empty_plan_are_deterministic():
    result = build_task_slo_impact_plan(
        _plan(
            [
                _task(
                    "task-worker",
                    title="Tune worker throughput",
                    description="Increase worker capacity and queue throughput.",
                    files_or_modules=["src/workers/batch.py"],
                )
            ]
        )
    )
    empty = build_task_slo_impact_plan({"id": "plan-empty", "tasks": []})

    assert task_slo_impact_plan_to_markdown(result) == "\n".join(
        [
            "# Task SLO Impact Plan: plan-slo",
            "",
            "| Task | Severity | Signals | Recommended SLO Checks | Evidence |",
            "| --- | --- | --- | --- | --- |",
            "| `task-worker` | medium | throughput | Measure throughput, queue depth, "
            "backlog age, and worker saturation under expected load. | "
            "files_or_modules: src/workers/batch.py; title: Tune worker throughput; "
            "description: Increase worker capacity and queue throughput. |",
        ]
    )
    assert empty.to_markdown() == "\n".join(
        [
            "# Task SLO Impact Plan: plan-empty",
            "",
            "No tasks were available for SLO impact assessment.",
        ]
    )


def _plan(tasks):
    return {
        "id": "plan-slo",
        "implementation_brief_id": "brief-slo",
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
    tags=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": (
            acceptance_criteria if acceptance_criteria is not None else ["Done"]
        ),
        "risk_level": risk_level,
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        task["tags"] = tags
    return task
