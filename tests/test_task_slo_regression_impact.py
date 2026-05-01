import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_slo_regression_impact import (
    TaskSloRegressionImpactPlan,
    TaskSloRegressionImpactRecord,
    build_task_slo_regression_impact_plan,
    derive_task_slo_regression_impact_plan,
    summarize_task_slo_regression_impact,
    summarize_task_slo_regression_impacts,
    task_slo_regression_impact_plan_to_dict,
    task_slo_regression_impact_plan_to_markdown,
)


def test_latency_api_detection_recommends_guardrails_and_metrics():
    result = build_task_slo_regression_impact_plan(
        _plan(
            [
                _task(
                    "task-api",
                    title="Optimize checkout API endpoint",
                    description="Change checkout API handler on a critical path with p95 latency and timeout risk.",
                    files_or_modules=["src/api/routes/checkout.py"],
                    acceptance_criteria=["Endpoint returns the expected payment response."],
                )
            ]
        )
    )

    assert isinstance(result, TaskSloRegressionImpactPlan)
    assert result.plan_id == "plan-slo"
    assert result.impacted_task_ids == ("task-api",)
    record = result.records[0]
    assert isinstance(record, TaskSloRegressionImpactRecord)
    assert record.impacted_slo_dimensions == ("availability", "latency", "error_rate")
    assert record.risk_level == "high"
    assert "Capture current baseline SLO metrics before implementation." in record.missing_checks
    assert "Add latency and error-rate guardrails for the affected paths." in record.missing_checks
    assert "p95 latency" in record.suggested_metrics
    assert "5xx rate" in record.suggested_metrics
    assert "files_or_modules: src/api/routes/checkout.py" in record.evidence
    assert "title: Optimize checkout API endpoint" in record.evidence


def test_queue_background_job_detection_includes_throughput_and_freshness():
    result = build_task_slo_regression_impact_plan(
        _plan(
            [
                _task(
                    "task-worker",
                    title="Tune background job queue consumers",
                    description="Increase worker concurrency for queued invoice jobs and reduce queue lag.",
                    files_or_modules=["src/workers/invoice_consumer.py"],
                    tags=["slo", "background-job"],
                    metadata={"service": "billing queue"},
                )
            ]
        )
    )

    record = result.records[0]
    assert record.impacted_slo_dimensions == ("throughput", "freshness")
    assert any("Validate capacity" in item for item in record.missing_checks)
    assert "queue depth" in record.suggested_metrics
    assert "consumer throughput" in record.suggested_metrics
    assert "data freshness lag" in record.suggested_metrics
    assert "tags[1]: background-job" in record.evidence


def test_acceptance_criteria_coverage_reduces_missing_checks_and_risk():
    missing = build_task_slo_regression_impact_plan(
        _task(
            "task-cache",
            title="Change Redis cache TTL",
            description="Update cache invalidation for stale product reads and API error handling.",
            files_or_modules=["src/cache/product_cache.py"],
            acceptance_criteria=["Cache returns product data."],
        )
    ).records[0]
    covered = build_task_slo_regression_impact_plan(
        _task(
            "task-cache",
            title="Change Redis cache TTL",
            description="Update cache invalidation for stale product reads and API error handling.",
            files_or_modules=["src/cache/product_cache.py"],
            acceptance_criteria=[
                "Capture current baseline SLO metrics before implementation.",
                "Latency, p95, p99, 5xx, and error rate guardrails are validated.",
                "Capacity load test covers throughput, peak load, queue depth, and saturation.",
                "Review alert thresholds, paging route, burn rate monitor, and dashboard.",
                "Rollback criteria and kill switch are documented.",
                "Post-deploy launch watch monitors after deployment.",
            ],
        )
    ).records[0]

    assert missing.risk_level == "high"
    assert len(missing.missing_checks) == 6
    assert covered.missing_checks == ()
    assert covered.risk_level == "low"


def test_low_risk_classification_when_guardrails_and_monitoring_are_present():
    result = build_task_slo_regression_impact_plan(
        _task(
            "task-dependency",
            title="Update provider integration timeout",
            description="Change external provider API dependency timeout behavior.",
            acceptance_criteria=[
                "Baseline metrics are captured before and after the change.",
                "p95 latency and 5xx error rate guardrails fail rollout on regression.",
                "Alert threshold review covers SLO monitor, paging, and burn rate dashboard.",
                "Rollback criteria define when to revert.",
                "Post-deploy monitoring runs through the release watch window.",
            ],
            metadata={"slo_dimensions": ["availability", "latency", "error_rate"]},
        )
    )

    record = result.records[0]
    assert record.risk_level == "low"
    assert record.missing_checks == ()
    assert record.impacted_slo_dimensions == ("availability", "latency", "error_rate")


def test_non_relevant_task_is_suppressed_and_markdown_is_stable():
    result = build_task_slo_regression_impact_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Adjust documentation and labels for the profile page.",
                    files_or_modules=["docs/profile.md"],
                )
            ],
            plan_id="plan-empty",
        )
    )
    markdown = task_slo_regression_impact_plan_to_markdown(result)

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary == {
        "task_count": 1,
        "record_count": 0,
        "impacted_task_count": 0,
        "no_impact_task_count": 1,
        "missing_check_count": 0,
        "dimension_counts": {
            "availability": 0,
            "latency": 0,
            "error_rate": 0,
            "throughput": 0,
            "freshness": 0,
        },
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "impacted_task_ids": [],
        "no_impact_task_ids": ["task-copy"],
    }
    assert markdown.startswith("# Task SLO Regression Impact Plan: plan-empty")
    assert "No SLO regression impact records were inferred." in markdown


def test_pydantic_model_input_json_serialization_and_helper_aliases_without_mutation():
    task = _task(
        "task-model",
        title="Public API | dependency fallback",
        description="Add fallback for public API dependency failures and timeout handling.",
        files_or_modules=["src/integrations/vendor_client.py"],
        metadata={"validation": "Alert threshold review and rollback criteria are ready."},
    )
    original = copy.deepcopy(task)
    plan = ExecutionPlan.model_validate(_plan([task], plan_id="plan-model"))
    task_model = ExecutionTask.model_validate(task)

    result = build_task_slo_regression_impact_plan(plan)
    payload = task_slo_regression_impact_plan_to_dict(result)
    direct_result = summarize_task_slo_regression_impact(task_model)
    plural_result = summarize_task_slo_regression_impacts(task_model)
    derived_result = derive_task_slo_regression_impact_plan(result)

    assert task == original
    assert derived_result is result
    assert direct_result.records[0].task_id == "task-model"
    assert plural_result.records[0].task_id == "task-model"
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert result.findings == result.records
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "impacted_slo_dimensions",
        "missing_checks",
        "suggested_metrics",
        "risk_level",
        "evidence",
    ]
    assert "Public API \\| dependency fallback" in result.to_markdown()


def _plan(tasks, *, plan_id="plan-slo"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-slo",
        "milestones": [{"name": "Launch"}],
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
        "milestone": "Launch",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": None,
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        task["tags"] = tags
    return task
