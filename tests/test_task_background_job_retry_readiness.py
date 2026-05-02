import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_background_job_retry_readiness import (
    TaskBackgroundJobRetryReadinessPlan,
    TaskBackgroundJobRetryReadinessRecord,
    analyze_task_background_job_retry_readiness,
    build_task_background_job_retry_readiness_plan,
    derive_task_background_job_retry_readiness,
    extract_task_background_job_retry_readiness,
    generate_task_background_job_retry_readiness,
    recommend_task_background_job_retry_readiness,
    summarize_task_background_job_retry_readiness,
    task_background_job_retry_readiness_plan_to_dict,
    task_background_job_retry_readiness_plan_to_dicts,
    task_background_job_retry_readiness_plan_to_markdown,
)


def test_detects_background_queue_retry_dlq_duplicate_timeout_and_safeguards():
    result = build_task_background_job_retry_readiness_plan(
        _plan(
            [
                _task(
                    "task-worker",
                    title="Harden async queue worker retries",
                    description=(
                        "Background job worker processes SQS queue messages with retry policy, "
                        "duplicate delivery handling, DLQ recovery, and timeout budgets."
                    ),
                    files_or_modules=[
                        "src/workers/email_retry_worker.py",
                        "src/queues/email_dead_letter_queue.py",
                    ],
                    acceptance_criteria=[
                        "Idempotent job handler uses an idempotency key.",
                        "Maximum retry attempts are capped with exponential backoff and jitter.",
                        "DLQ depth monitoring alerts the owner.",
                        "Per-attempt timeout budget and overall job timeout are documented.",
                        "Duplicate suppression prevents sending email twice.",
                    ],
                    validation_commands={
                        "test": ["poetry run pytest tests/workers/test_email_retry_timeout.py"]
                    },
                )
            ]
        )
    )

    assert isinstance(result, TaskBackgroundJobRetryReadinessPlan)
    assert result.impacted_task_ids == ("task-worker",)
    record = result.records[0]
    assert isinstance(record, TaskBackgroundJobRetryReadinessRecord)
    assert record.matched_signals == (
        "queue",
        "worker",
        "background_job",
        "retry",
        "dead_letter_queue",
        "duplicate_execution",
        "timeout",
    )
    assert record.categories == (
        "async_job",
        "queue_worker",
        "retry_policy",
        "dead_letter_queue",
        "duplicate_execution",
        "timeout",
    )
    assert record.present_safeguards == (
        "idempotent_job_handler",
        "retry_limit",
        "exponential_backoff",
        "dead_letter_monitoring",
        "timeout_budget",
        "duplicate_suppression",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert record.recommendations == ()
    assert record.recommended_checks == record.recommendations
    assert any("description:" in item and "Background job worker" in item for item in record.evidence)
    assert "files_or_modules: src/queues/email_dead_letter_queue.py" in record.evidence
    assert any("validation_commands:" in item and "retry_timeout" in item for item in record.evidence)
    assert result.summary["category_counts"]["retry_policy"] == 1
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_high_risk_for_retry_or_duplicate_execution_without_idempotency_or_retry_limits():
    result = analyze_task_background_job_retry_readiness(
        _plan(
            [
                _task(
                    "task-retry",
                    title="Add retry loop to invoice worker",
                    description="Queue worker retries failed invoice jobs after timeout failures.",
                ),
                _task(
                    "task-duplicate",
                    title="Handle duplicate scheduled job execution",
                    description="Cron scheduler may run the background job twice during deploys.",
                    acceptance_criteria=["Exponential backoff is used for retry storms."],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    retry = by_id["task-retry"]
    duplicate = by_id["task-duplicate"]

    assert retry.risk_level == "high"
    assert {"retry_policy", "timeout"} <= set(retry.categories)
    assert "idempotent_job_handler" in retry.missing_safeguards
    assert "retry_limit" in retry.missing_safeguards
    assert retry.recommendations[0].startswith("Verify the job handler is idempotent")
    assert duplicate.risk_level == "high"
    assert "duplicate_execution" in duplicate.categories
    assert "idempotent_job_handler" in duplicate.missing_safeguards
    assert "duplicate_suppression" in duplicate.missing_safeguards
    assert result.summary["risk_counts"] == {"high": 2, "medium": 0, "low": 0}


def test_metadata_acceptance_criteria_and_validation_plan_detect_present_safeguards():
    result = build_task_background_job_retry_readiness_plan(
        _plan(
            [
                _task(
                    "task-cron",
                    title="Add nightly cron retry processor",
                    description="Scheduled job retries failed exports.",
                    metadata={
                        "retry_policy": {
                            "retry_limit": "Maximum retry attempts stop at 4.",
                            "idempotent_job_handler": "Idempotent export handler uses job id.",
                            "duplicate_suppression": "Unique job key suppresses duplicate execution.",
                            "dead_letter_monitoring": "DLQ alerts include depth and owner.",
                            "timeout_budget": "Overall timeout budget is 15 minutes.",
                        }
                    },
                    acceptance_criteria=["Retry backoff uses exponential backoff with jitter."],
                    validation_plan="Run retry limit and DLQ monitoring tests.",
                )
            ]
        )
    )

    record = result.records[0]
    assert {"cron", "retry", "dead_letter_queue"} <= set(record.matched_signals)
    assert record.present_safeguards == (
        "idempotent_job_handler",
        "retry_limit",
        "exponential_backoff",
        "dead_letter_monitoring",
        "timeout_budget",
        "duplicate_suppression",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert any("metadata.retry_policy.retry_limit" in item for item in record.evidence)
    assert any("validation_plan:" in item for item in record.evidence)


def test_execution_plan_task_iterable_invalid_empty_and_no_impact_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Create worker timeout retry guard",
        description="Worker retries queue jobs with a retry limit and idempotent handler.",
        files_or_modules=["src/jobs/timeout_worker.py"],
        acceptance_criteria=["Duplicate suppression is covered by a unique job key."],
        metadata={"validation_commands": {"test": ["pytest tests/jobs/test_timeout_worker.py"]}},
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Queue retry processor",
            description="Build queue retry processing for account events.",
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")], plan_id="plan-model"))

    iterable_result = build_task_background_job_retry_readiness_plan([object_task])
    task_result = build_task_background_job_retry_readiness_plan(task_model)
    plan_result = generate_task_background_job_retry_readiness(plan_model)
    empty = build_task_background_job_retry_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_background_job_retry_readiness_plan(13)
    no_signal = build_task_background_job_retry_readiness_plan(
        _plan([_task("task-copy", title="Update helper copy", description="Static text only.")])
    )

    assert iterable_result.records[0].task_id == "task-object"
    assert iterable_result.records[0].risk_level == "medium"
    assert task_result.records[0].task_id == "task-model"
    assert task_result.records[0].risk_level == "high"
    assert plan_result.plan_id == "plan-model"
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert invalid.records == ()
    assert no_signal.records == ()
    assert no_signal.impacted_task_ids == ()
    assert no_signal.no_impact_task_ids == ("task-copy",)
    assert "No task background job retry-readiness records were inferred." in no_signal.to_markdown()
    assert "No-impact tasks: task-copy" in no_signal.to_markdown()


def test_serialization_markdown_aliases_sorting_and_no_mutation_are_stable():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Nightly cron ready | low",
                description="Scheduled job retries exports with timeout handling.",
                acceptance_criteria=[
                    "Idempotent job handler uses an idempotency key.",
                    "Maximum retry attempts are capped.",
                    "Retry backoff uses exponential backoff with jitter.",
                    "DLQ monitoring alerts on failed messages.",
                    "Overall timeout budget is documented.",
                    "Duplicate suppression uses a unique job key.",
                ],
            ),
            _task(
                "task-a",
                title="Retry worker loop",
                description="Add retry loop handling to the task queue worker.",
                acceptance_criteria=["Maximum retry attempts are capped."],
            ),
            _task(
                "task-m",
                title="Worker timeout budget",
                description="Add worker timeout budget for background jobs.",
                acceptance_criteria=["Overall timeout budget is documented."],
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_background_job_retry_readiness(plan)
    payload = task_background_job_retry_readiness_plan_to_dict(result)
    markdown = task_background_job_retry_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_background_job_retry_readiness_plan_to_dicts(result) == payload["records"]
    assert task_background_job_retry_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_background_job_retry_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_background_job_retry_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_background_job_retry_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_signals",
        "categories",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "recommendations",
        "evidence",
    ]
    assert result.impacted_task_ids == ("task-a", "task-m", "task-z")
    assert [record.risk_level for record in result.records] == ["high", "medium", "low"]
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary["risk_counts"] == {"high": 1, "medium": 1, "low": 1}
    assert markdown.startswith("# Task Background Job Retry Readiness: plan-retry")
    assert "Nightly cron ready \\| low" in markdown
    assert (
        "| Task | Title | Risk | Signals | Categories | Present Safeguards | Missing Safeguards | Recommendations | Evidence |"
        in markdown
    )


def _plan(tasks, plan_id="plan-retry"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-retry",
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
    validation_plan=None,
    validation_commands=None,
    metadata=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-retry",
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
    if validation_plan is not None:
        payload["validation_plan"] = validation_plan
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
