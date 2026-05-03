import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_rate_limit_backoff_readiness import (
    TaskRateLimitBackoffReadinessPlan,
    TaskRateLimitBackoffReadinessRecord,
    analyze_task_rate_limit_backoff_readiness,
    build_task_rate_limit_backoff_readiness_plan,
    extract_task_rate_limit_backoff_readiness,
    recommend_task_rate_limit_backoff_readiness,
    task_rate_limit_backoff_readiness_plan_to_dict,
    task_rate_limit_backoff_readiness_plan_to_dicts,
    task_rate_limit_backoff_readiness_plan_to_markdown,
)


def test_detects_rate_limit_backoff_categories_from_task_fields_and_metadata():
    result = build_task_rate_limit_backoff_readiness_plan(
        _plan(
            [
                _task(
                    "task-client",
                    title="Handle 429 client throttling",
                    description="API client handles Too Many Requests and honors Retry-After headers.",
                    files_or_modules=["src/clients/provider_retry_after.py"],
                    acceptance_criteria=[
                        "429 responses are retried only after the Retry-After header window."
                    ],
                    validation_command="poetry run pytest tests/clients/test_retry_after_429.py",
                ),
                _task(
                    "task-worker",
                    title="Worker retry backoff and DLQ",
                    description=(
                        "Queue worker retries transient failures with exponential backoff, jitter, "
                        "and dead-lettering after retry budget exhaustion."
                    ),
                    metadata={
                        "rate_limit_backoff_categories": ["worker_retry", "dead_lettering"],
                        "test_evidence": "Worker tests simulate retry budget exhaustion and DLQ routing.",
                    },
                ),
                _task(
                    "task-batch",
                    title="Batch retry user messaging",
                    description="Bulk import uses batch retries and shows user-facing retry messaging.",
                    acceptance_criteria=[
                        "Batch retries resume partial failures without duplicating completed chunks."
                    ],
                ),
                _task("task-copy", title="Profile copy", description="Adjust profile labels."),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert isinstance(result, TaskRateLimitBackoffReadinessPlan)
    assert all(isinstance(record, TaskRateLimitBackoffReadinessRecord) for record in result.records)
    assert by_id["task-client"].categories == ("rate_limit_response", "retry_after_header")
    assert by_id["task-client"].covered_acceptance_criteria == (
        "rate_limit_response",
        "retry_after_header",
    )
    assert by_id["task-client"].missing_acceptance_criteria == ()
    assert by_id["task-client"].suggested_test_evidence == ()
    assert by_id["task-worker"].categories == (
        "exponential_backoff",
        "jitter",
        "worker_retry",
        "dead_lettering",
    )
    assert by_id["task-worker"].covered_acceptance_criteria == ()
    assert (
        "Acceptance criteria should cover worker retry budgets, idempotency, and stop conditions."
        in by_id["task-worker"].missing_acceptance_criteria
    )
    assert (
        "Add worker tests for retry budget, idempotency, and stop conditions after transient failures."
        not in by_id["task-worker"].suggested_test_evidence
    )
    assert any("metadata.test_evidence" in item for item in by_id["task-worker"].evidence)
    assert by_id["task-batch"].categories == ("batch_retry", "user_retry_message")
    assert by_id["task-batch"].covered_acceptance_criteria == ("batch_retry",)
    assert by_id["task-batch"].missing_acceptance_criteria == (
        "Acceptance criteria should cover user-facing retry timing and quota messaging.",
    )
    assert result.impacted_task_ids == ("task-batch", "task-worker", "task-client")
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary["category_counts"]["retry_after_header"] == 1
    assert result.summary["category_counts"]["dead_lettering"] == 1


def test_recommendations_distinguish_covered_acceptance_from_missing_validation_evidence():
    result = analyze_task_rate_limit_backoff_readiness(
        _plan(
            [
                _task(
                    "task-covered-no-tests",
                    title="Retry-After and backoff contract",
                    description="Handle 429 rate-limit responses with Retry-After and exponential backoff.",
                    acceptance_criteria=[
                        "429 responses honor Retry-After.",
                        "Exponential backoff caps retries after the configured budget.",
                    ],
                ),
                _task(
                    "task-covered-with-tests",
                    title="Jittered backoff validation",
                    description="Add jitter to retry backoff.",
                    acceptance_criteria=[
                        "Tests validate jitter and exponential backoff timing stays within retry caps."
                    ],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert by_id["task-covered-no-tests"].covered_acceptance_criteria == (
        "rate_limit_response",
        "retry_after_header",
        "exponential_backoff",
    )
    assert by_id["task-covered-no-tests"].missing_acceptance_criteria == ()
    assert by_id["task-covered-no-tests"].suggested_test_evidence == (
        "Add tests that simulate 429 or throttled responses and assert controlled retry behavior.",
        "Add tests that assert Retry-After or reset-window headers drive the next retry time.",
        "Add tests for exponential backoff intervals, retry caps, and exhaustion behavior.",
    )
    assert by_id["task-covered-no-tests"].severity == "high"
    assert by_id["task-covered-with-tests"].missing_acceptance_criteria == ()
    assert by_id["task-covered-with-tests"].suggested_test_evidence == ()
    assert by_id["task-covered-with-tests"].severity == "low"


def test_no_match_tasks_aliases_deterministic_severity_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-medium",
                title="Jittered retries",
                description="Add jitter to client retry delays.",
                acceptance_criteria=["Jitter is applied to retry delays."],
            ),
            _task(
                "task-high",
                title="Worker 429 retries",
                description="Worker retries 429 throttling responses without documented criteria.",
            ),
            _task(
                "task-low",
                title="Fully covered jitter",
                description="Jittered exponential backoff for client retries.",
                acceptance_criteria=[
                    "Tests validate exponential backoff and jitter for retry caps."
                ],
            ),
            _task(
                "task-none",
                title="Settings UI",
                description="No rate-limit or backoff changes are in scope.",
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = recommend_task_rate_limit_backoff_readiness(plan)

    assert plan == original
    assert extract_task_rate_limit_backoff_readiness(plan).to_dict() == result.to_dict()
    assert result.recommendations == result.records
    assert result.findings == result.records
    assert [(record.task_id, record.severity) for record in result.records] == [
        ("task-high", "high"),
        ("task-medium", "medium"),
        ("task-low", "low"),
    ]
    assert result.no_impact_task_ids == ("task-none",)
    assert result.summary["severity_counts"] == {"high": 1, "medium": 1, "low": 1}


def test_execution_plan_execution_task_iterable_and_object_like_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Dead letter queue retries",
        description="Queue retries exhausted jobs into a dead-letter queue.",
        acceptance_criteria=["DLQ behavior is covered for exhausted worker retries."],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Retry-After model task",
            description="Client handles 429 and Retry-After with exponential backoff.",
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")], plan_id="plan-model"))

    iterable_result = build_task_rate_limit_backoff_readiness_plan([object_task])
    task_result = build_task_rate_limit_backoff_readiness_plan(task_model)
    plan_result = build_task_rate_limit_backoff_readiness_plan(plan_model)

    assert iterable_result.records[0].task_id == "task-object"
    assert iterable_result.records[0].categories == ("worker_retry", "dead_lettering")
    assert task_result.records[0].task_id == "task-model"
    assert task_result.records[0].categories == (
        "rate_limit_response",
        "retry_after_header",
        "exponential_backoff",
    )
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-model"


def test_serialization_empty_invalid_and_markdown_are_stable():
    result = build_task_rate_limit_backoff_readiness_plan(
        {
            "id": "task-single",
            "title": "Rate limit message | retry later",
            "description": "Return a Too Many Requests message and retry timing to users.",
            "acceptance_criteria": [
                "Tests assert user-facing retry message includes retry timing."
            ],
            "status": "pending",
        }
    )
    payload = task_rate_limit_backoff_readiness_plan_to_dict(result)
    markdown = task_rate_limit_backoff_readiness_plan_to_markdown(result)
    invalid = build_task_rate_limit_backoff_readiness_plan(42)
    empty = build_task_rate_limit_backoff_readiness_plan({"id": "empty-plan", "tasks": []})

    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_rate_limit_backoff_readiness_plan_to_dicts(result) == payload["records"]
    assert task_rate_limit_backoff_readiness_plan_to_dicts(result.records) == payload["records"]
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
        "severity",
        "categories",
        "covered_acceptance_criteria",
        "missing_acceptance_criteria",
        "suggested_test_evidence",
        "evidence",
    ]
    assert markdown.startswith("# Task Rate-Limit Backoff Readiness")
    assert "Rate limit message \\| retry later" in markdown
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert invalid.records == ()
    assert "No task rate-limit backoff readiness records were inferred." in invalid.to_markdown()


def _plan(tasks, plan_id="plan-rate-limit-backoff"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-rate-limit-backoff",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    depends_on=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    validation_command=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if validation_command is not None:
        task["validation_command"] = validation_command
    return task
