import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_flaky_validation_risk import (
    TaskFlakyValidationEvidence,
    score_task_flaky_validation_risk,
    task_flaky_validation_risk_to_dict,
)


def test_scores_network_and_external_api_validation_as_high_risk():
    report = score_task_flaky_validation_risk(
        _plan(
            [
                _task(
                    "task-webhook",
                    description="Validate retry behavior against the Stripe webhook integration.",
                    test_command="pytest tests/integrations/test_stripe_webhook.py --allow-net",
                    acceptance_criteria=[
                        "Webhook handling mocks external API failures and network timeouts."
                    ],
                    files=["src/integrations/stripe_webhook.py"],
                    metadata={"external_api": "stripe", "live_http": True},
                )
            ]
        )
    )

    task = report.tasks[0]
    categories = {item.category for item in task.evidence}

    assert task.risk_level == "high"
    assert task.score >= 65
    assert {"network", "external_api", "timing"}.issubset(categories)
    assert "test_command" in {item.source for item in task.evidence}
    assert "acceptance_criteria" in {item.source for item in task.evidence}
    assert "metadata" in {item.source for item in task.evidence}
    assert any("Mock network calls" in mitigation for mitigation in task.mitigations)
    assert any("Mock external APIs" in mitigation for mitigation in task.mitigations)


def test_scores_browser_snapshot_validation_and_recommends_stabilization():
    report = score_task_flaky_validation_risk(
        _plan(
            [
                _task(
                    "task-ui",
                    test_command="pnpm playwright test tests/e2e/dashboard.spec.ts --update-snapshots",
                    acceptance_criteria=[
                        "Dashboard browser visual regression screenshot matches the snapshot."
                    ],
                    files=["tests/e2e/dashboard.spec.ts", "tests/__snapshots__/dashboard.snap"],
                    metadata={"browser": "chromium"},
                )
            ]
        )
    )

    task = report.tasks[0]

    assert task.risk_level == "high"
    assert {"browser", "snapshot"}.issubset({item.category for item in task.evidence})
    assert any(item.source == "files_or_modules" for item in task.evidence)
    assert any("Pin browser" in mitigation for mitigation in task.mitigations)
    assert any("Stabilize snapshots" in mitigation for mitigation in task.mitigations)


def test_scores_timing_concurrency_random_and_current_time_as_high_risk():
    report = score_task_flaky_validation_risk(
        _plan(
            [
                _task(
                    "task-worker",
                    description="Worker uses async retries and generated UUID payloads.",
                    test_command="pytest tests/test_worker.py -n auto --timeout=5 --seed=123",
                    acceptance_criteria=[
                        "Race handling is deterministic when jobs wait for current time boundaries."
                    ],
                    metadata={"timezone": "UTC", "seed": 123},
                )
            ]
        )
    )

    task = report.tasks[0]

    assert task.risk_level == "high"
    assert {
        "timing",
        "concurrency",
        "randomness",
        "current_time",
    }.issubset({item.category for item in task.evidence})
    assert any("Freeze time" in mitigation for mitigation in task.mitigations)
    assert any("Seed random data" in mitigation for mitigation in task.mitigations)
    assert any("deterministic synchronization" in mitigation for mitigation in task.mitigations)


def test_scores_broad_commands_as_medium_risk_and_recommends_narrowing():
    report = score_task_flaky_validation_risk(
        _plan(
            [
                _task(
                    "task-cli",
                    test_command="poetry run pytest",
                    acceptance_criteria=["CLI parser handles quoted arguments."],
                    files=["src/cli.py", "tests/test_cli.py"],
                )
            ]
        )
    )

    task = report.tasks[0]

    assert task.risk_level == "medium"
    assert task.evidence == (
        TaskFlakyValidationEvidence(
            category="broad_command",
            source="test_command",
            detail="test command is broad: poetry run pytest",
            weight=30,
        ),
    )
    assert any("Narrow validation" in mitigation for mitigation in task.mitigations)


def test_stable_unit_test_task_is_low_risk_and_serializes():
    plan = _plan(
        [
            _task(
                "task-parser",
                test_command="poetry run pytest tests/test_parser.py -o addopts=''",
                acceptance_criteria=["Parser returns a deterministic error for malformed input."],
                files=["src/parser.py", "tests/test_parser.py"],
            )
        ]
    )

    report = score_task_flaky_validation_risk(ExecutionPlan.model_validate(plan))
    payload = task_flaky_validation_risk_to_dict(report)

    assert report.tasks[0].risk_level == "low"
    assert report.tasks[0].score == 5
    assert report.tasks[0].evidence == ()
    assert report.tasks[0].mitigations == ("Keep validation targeted and deterministic.",)
    assert payload["summary"] == {
        "task_count": 1,
        "risk_counts": {"low": 1, "medium": 0, "high": 0},
        "category_counts": {},
    }
    assert json.loads(json.dumps(payload)) == payload


def test_report_preserves_plan_order_and_counts_categories_once_per_task():
    report = score_task_flaky_validation_risk(
        _plan(
            [
                _task("task-unit", test_command="pytest tests/test_unit.py"),
                _task(
                    "task-cache",
                    test_command="pytest tests/test_cache.py",
                    acceptance_criteria=["Cache and database state are isolated between runs."],
                    files=["src/cache.py", "tests/test_cache.py"],
                    metadata={"isolation": "redis"},
                ),
            ],
            plan_id="plan-risk",
        )
    )

    assert report.plan_id == "plan-risk"
    assert [task.task_id for task in report.tasks] == ["task-unit", "task-cache"]
    assert report.tasks[0].risk_level == "low"
    assert report.tasks[1].risk_level == "medium"
    assert report.summary["category_counts"]["shared_state"] == 1
    assert any("Reset database" in mitigation for mitigation in report.tasks[1].mitigations)


def _plan(tasks, *, plan_id="plan-test"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Build", "description": "Build the feature"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    description=None,
    files=None,
    acceptance_criteria=None,
    test_command=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": description or f"Implement {task_id}",
        "milestone": "Build",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files or ["src/app.py"],
        "acceptance_criteria": acceptance_criteria or [f"{task_id} works"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
