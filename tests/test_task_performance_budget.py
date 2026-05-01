import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_performance_budget import (
    TaskPerformanceBudgetPlan,
    TaskPerformanceBudgetRecord,
    build_task_performance_budget_plan,
    derive_task_performance_budget_plan,
    task_performance_budget_plan_to_dict,
    task_performance_budget_plan_to_markdown,
)


def test_frontend_api_database_worker_and_infra_signals_map_to_distinct_budgets():
    result = build_task_performance_budget_plan(
        _plan(
            [
                _task(
                    "task-ui",
                    title="Split checkout bundle and improve page load",
                    description="Reduce Vite chunk size and keep LCP within the route budget.",
                    files_or_modules=["src/components/Checkout.tsx", "vite.config.ts"],
                ),
                _task(
                    "task-api",
                    title="Optimize API latency for checkout endpoint",
                    description="Keep p95 response time under the SLO for the REST endpoint.",
                    files_or_modules=["src/api/checkout.py"],
                ),
                _task(
                    "task-db",
                    title="Add database index for order query",
                    description="Compare EXPLAIN plan output for the SQL query.",
                    files_or_modules=["migrations/20260502_orders.sql"],
                ),
                _task(
                    "task-worker",
                    title="Tune worker queue throughput",
                    description="Increase consumer rate and reduce queue lag for Kafka backlog.",
                    files_or_modules=["src/workers/orders.py"],
                ),
                _task(
                    "task-infra",
                    title="Adjust CPU and memory limits",
                    description="Update HPA capacity for load test results.",
                    files_or_modules=["k8s/api/deployment.yaml"],
                ),
            ]
        )
    )

    categories_by_task = {
        record.task_id: record.budget_category for record in result.records
    }

    assert _categories(result, "task-ui") == ("frontend_bundle", "page_load")
    assert categories_by_task["task-api"] == "api_latency"
    assert categories_by_task["task-db"] == "database_query"
    assert categories_by_task["task-worker"] == "queue_throughput"
    assert categories_by_task["task-infra"] == "resource_utilization"


def test_batch_job_budget_includes_target_evidence_guard_and_followups():
    result = build_task_performance_budget_plan(
        _plan(
            [
                _task(
                    "task-backfill",
                    title="Implement nightly batch backfill",
                    description="Run an ETL import job for large customer history.",
                    files_or_modules=["jobs/backfills/customer_history.py"],
                    acceptance_criteria=[
                        "Benchmark representative input size before rollout.",
                    ],
                )
            ],
            test_strategy="Run performance validation before release.",
        )
    )

    record = result.records[0]

    assert record.budget_category == "batch_job"
    assert record.suggested_measurement_target == (
        "Keep job runtime within the current operational window plus 10%."
    )
    assert record.benchmark_evidence == (
        "Record representative input size, runtime, retry count, and resource usage before and after the change."
    )
    assert record.regression_guard == (
        "Add runtime and retry-count alerts for the job plus a representative benchmark fixture."
    )
    assert record.follow_up_actions == (
        "Benchmark against representative input sizes before scheduling the rollout.",
        "Define pause, resume, retry, and rollback behavior for long-running work.",
    )
    assert record.evidence_requirements == (
        "files_or_modules: jobs/backfills/customer_history.py",
        "title: Implement nightly batch backfill",
        "description: Run an ETL import job for large customer history.",
        "acceptance_criteria[0]: Benchmark representative input size before rollout.",
        "test_strategy: Run performance validation before release.",
        "Record representative input size, runtime, retry count, and resource usage before and after the change.",
    )


def test_tasks_without_performance_signals_are_absent_from_budget_plan():
    result = build_task_performance_budget_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Polish empty state copy",
                    description="Update label text in the settings form.",
                    files_or_modules=["src/components/SettingsCopy.tsx"],
                    acceptance_criteria=["Copy matches product guidance."],
                )
            ]
        )
    )

    assert result.plan_id == "plan-performance-budget"
    assert result.records == ()
    assert result.to_dict() == {"plan_id": "plan-performance-budget", "records": []}
    assert result.to_markdown() == "\n".join(
        [
            "# Task Performance Budget Plan: plan-performance-budget",
            "",
            "No task performance budget signals detected.",
        ]
    )


def test_sorting_serialization_aliases_model_inputs_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Reduce API p95 latency",
                description="Benchmark endpoint response time.",
                files_or_modules=["src/api/orders.py"],
            ),
            _task(
                "task-a",
                title="Improve dashboard bundle and page load",
                description="Track bundle size, LCP, and INP.",
                files_or_modules=["src/pages/Dashboard.tsx"],
                metadata={"web_vitals": "LCP under 2.5s"},
            ),
        ],
        plan_id="plan-model",
        test_strategy=None,
    )

    result = build_task_performance_budget_plan(ExecutionPlan.model_validate(plan))
    alias_result = derive_task_performance_budget_plan(plan)
    single = build_task_performance_budget_plan(ExecutionTask.model_validate(plan["tasks"][0]))
    payload = task_performance_budget_plan_to_dict(result)

    assert isinstance(result, TaskPerformanceBudgetPlan)
    assert isinstance(TaskPerformanceBudgetRecord, type)
    assert [record.task_id for record in result.records] == [
        "task-a",
        "task-a",
        "task-z",
    ]
    assert [record.budget_category for record in result.records] == [
        "frontend_bundle",
        "page_load",
        "api_latency",
    ]
    assert payload == result.to_dict()
    assert alias_result.to_dict() == result.to_dict()
    assert single.plan_id is None
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "budget_category",
        "suggested_measurement_target",
        "benchmark_evidence",
        "regression_guard",
        "evidence_requirements",
        "follow_up_actions",
        "detected_signals",
    ]
    assert task_performance_budget_plan_to_markdown(result) == "\n".join(
        [
            "# Task Performance Budget Plan: plan-model",
            "",
            "| Task | Category | Target | Benchmark Evidence | Regression Guard | Follow-up Actions |",
            "| --- | --- | --- | --- | --- | --- |",
            (
                "| task-a | frontend_bundle | Keep added compressed JavaScript/CSS under 25 KB or document an approved "
                "bundle budget exception. | Attach before/after bundle analyzer output with compressed and parsed "
                "size deltas. | Add a bundle-size check or CI artifact diff that fails when the approved budget "
                "is exceeded. | Confirm the affected bundles and routes before implementation.; Review dependency "
                "additions for lazy loading, splitting, or removal. |"
            ),
            (
                "| task-a | page_load | Keep p95 LCP under 2.5s and p95 INP under 200 ms on the target page. | Capture "
                "Lighthouse, WebPageTest, or RUM baseline and after-change p75/p95 Web Vitals. | Add synthetic "
                "or RUM alert thresholds for Web Vitals regressions on the affected route. | Identify target "
                "devices, network profile, and route-level baseline.; Confirm images, hydration, and "
                "render-blocking assets stay within budget. |"
            ),
            (
                "| task-z | api_latency | Keep p95 API latency under 300 ms and p99 under 1 s at expected peak traffic. | "
                "Record before/after load-test or trace samples for p50, p95, p99, error rate, and request volume. | "
                "Add latency SLO assertions, trace sampling, or performance tests for the changed endpoint. | "
                "Define expected request volume and percentile SLO before merging.; Confirm tracing covers the "
                "endpoint and downstream dependencies. |"
            ),
        ]
    )


def _categories(result, task_id):
    return tuple(
        record.budget_category for record in result.records if record.task_id == task_id
    )


def _plan(tasks, *, plan_id="plan-performance-budget", test_strategy="Run perf checks."):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-performance-budget",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": test_strategy,
        "handoff_prompt": "Implement the plan",
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
):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": None,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
