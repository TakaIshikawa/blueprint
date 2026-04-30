import json

from blueprint.test_impact_summary import (
    IMPACT_CLASSIFICATIONS,
    TaskTestImpact,
    TestImpactSummary,
    build_test_impact_summary,
    test_impact_summary_to_dict,
)


def test_mixed_backend_frontend_docs_plan_gets_deterministic_classifications_and_markdown():
    summary = build_test_impact_summary(
        _plan(
            [
                _task(
                    "task-api",
                    "Add API route",
                    files=["src/blueprint/api/routes.py"],
                    acceptance=["Endpoint returns checklist data"],
                    test_command="poetry run pytest tests/test_api.py",
                ),
                _task(
                    "task-ui",
                    "Render checklist UI",
                    files=["frontend/components/Checklist.tsx"],
                    acceptance=["Screen renders the checklist"],
                    test_command="pnpm test -- Checklist.test.tsx",
                ),
                _task(
                    "task-docs",
                    "Document rollout flow",
                    files=["docs/rollout.md"],
                    acceptance=["README links to the runbook"],
                    test_command="poetry run pytest tests/test_docs_links.py",
                ),
                _task(
                    "task-migration",
                    "Add account audit migration",
                    files=["migrations/20260501_add_account_audit.sql"],
                    acceptance=["Migration applies cleanly"],
                    test_command="poetry run pytest tests/test_migrations.py",
                ),
            ]
        )
    )

    by_id = {task.task_id: task for task in summary.tasks}

    assert isinstance(summary, TestImpactSummary)
    assert isinstance(by_id["task-api"], TaskTestImpact)
    assert by_id["task-api"].classifications == ("unit", "integration")
    assert by_id["task-ui"].classifications == ("unit", "UI")
    assert by_id["task-docs"].classifications == ("docs-only",)
    assert by_id["task-migration"].classifications == ("unit", "migration")
    assert set(summary.impact_counts()) == set(IMPACT_CLASSIFICATIONS)

    markdown = summary.to_markdown()

    assert markdown == summary.to_markdown()
    assert markdown.startswith("# Test Impact Summary for plan-impact\n\n## Aggregates\n")
    assert "- Impact counts:" in markdown
    assert "  - integration: 1" in markdown
    assert "  - UI: 1" in markdown
    assert "\n## Task Details\n" in markdown
    assert "- `task-api` Add API route: unit, integration" in markdown
    assert "  - commands: `poetry run pytest tests/test_api.py`" in markdown


def test_duplicate_commands_are_deduped_while_preserving_source_order():
    plan = _plan(
        [
            _task(
                "task-api",
                "Add API support",
                files=["src/blueprint/api.py"],
                acceptance=["API works"],
                test_command="poetry run pytest tests/test_api.py",
                metadata={"test_commands": ["poetry run pytest tests/test_api.py"]},
            ),
            _task(
                "task-ui",
                "Render UI",
                files=["src/blueprint/ui.py"],
                acceptance=["UI works"],
                test_command="poetry run pytest tests/test_ui.py",
            ),
        ],
        metadata={
            "validation_commands": {
                "test": [
                    "poetry run pytest tests/test_api.py",
                    "poetry run pytest tests/test_smoke.py",
                ],
                "lint": ["poetry run ruff check"],
            }
        },
    )

    summary = build_test_impact_summary(plan)

    assert list(summary.recommended_commands) == [
        "poetry run pytest tests/test_api.py",
        "poetry run pytest tests/test_smoke.py",
        "poetry run ruff check",
        "poetry run pytest tests/test_ui.py",
    ]
    assert summary.tasks[0].recommended_commands == (
        "poetry run pytest tests/test_api.py",
        "poetry run pytest tests/test_smoke.py",
        "poetry run ruff check",
    )


def test_tasks_without_task_or_plan_validation_are_reported_as_gaps():
    summary = build_test_impact_summary(
        _plan(
            [
                _task(
                    "task-known",
                    "Refactor helper",
                    files=["src/blueprint/helpers.py"],
                    acceptance=["Helper handles edge cases"],
                    test_command="poetry run pytest tests/test_helpers.py",
                ),
                _task(
                    "task-gap",
                    "Investigate ambiguous behavior",
                    files=[],
                    acceptance=[],
                    test_command=None,
                ),
            ]
        )
    )

    by_id = {task.task_id: task for task in summary.tasks}

    assert by_id["task-gap"].classifications == ("unknown",)
    assert by_id["task-gap"].recommended_commands == ()
    assert by_id["task-gap"].has_validation_gap is True
    assert summary.validation_gaps == ("task-gap",)
    assert "Validation gaps: `task-gap`" in summary.to_markdown()


def test_plan_metadata_validation_commands_cover_tasks_without_task_commands():
    summary = build_test_impact_summary(
        _plan(
            [
                _task(
                    "task-service",
                    "Update service module",
                    files=["src/blueprint/service.py"],
                    acceptance=["Service accepts the new input"],
                    test_command=None,
                )
            ],
            metadata={
                "validation_commands": {
                    "test": ["poetry run pytest tests/test_service.py"],
                    "typecheck": ["poetry run mypy ."],
                }
            },
        )
    )

    assert summary.validation_gaps == ()
    assert summary.tasks[0].recommended_commands == (
        "poetry run pytest tests/test_service.py",
        "poetry run mypy .",
    )
    assert summary.recommended_commands == (
        "poetry run pytest tests/test_service.py",
        "poetry run mypy .",
    )


def test_serialization_is_json_compatible_and_stable():
    summary = build_test_impact_summary(
        _plan(
            [
                _task(
                    "task-api",
                    "Add API support",
                    files=["src/blueprint/api.py"],
                    acceptance=["API works"],
                    test_command="poetry run pytest tests/test_api.py",
                )
            ]
        )
    )

    payload = test_impact_summary_to_dict(summary)

    assert payload == summary.to_dict()
    assert list(payload) == ["plan_id", "tasks", "recommended_commands", "validation_gaps"]
    assert list(payload["tasks"][0]) == [
        "task_id",
        "title",
        "classifications",
        "recommended_commands",
        "has_validation_gap",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks, *, metadata=None):
    return {
        "id": "plan-impact",
        "implementation_brief_id": "brief-impact",
        "target_repo": "example/repo",
        "test_strategy": "Run focused validation",
        "metadata": metadata or {},
        "tasks": tasks,
    }


def _task(task_id, title, *, files, acceptance, test_command, metadata=None):
    task = {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "files_or_modules": files,
        "acceptance_criteria": acceptance,
        "risk_level": "low",
        "test_command": test_command,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
