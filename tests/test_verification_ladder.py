import json

from blueprint.domain.models import ExecutionPlan
from blueprint.verification_ladder import (
    VerificationCoverageGap,
    VerificationStep,
    build_verification_ladder,
    verification_ladder_to_dict,
)


def test_duplicate_commands_preserve_all_tasks_covered():
    ladder = build_verification_ladder(
        _plan(
            [
                _task("task-api", "Update API", test_command="pytest tests/test_api.py"),
                _task("task-worker", "Update worker", test_command="pytest tests/test_api.py"),
            ]
        )
    )

    assert ladder.gaps == ()
    assert ladder.steps == (
        VerificationStep(
            command="pytest tests/test_api.py",
            scope="targeted_test",
            reason="Focused task-level test command should run before broader checks.",
            tasks_covered=("task-api", "task-worker"),
            blocking=True,
        ),
    )
    assert ladder.steps[0].to_dict() == {
        "command": "pytest tests/test_api.py",
        "scope": "targeted_test",
        "reason": "Focused task-level test command should run before broader checks.",
        "tasks_covered": ["task-api", "task-worker"],
        "blocking": True,
    }


def test_mixed_pytest_lint_typecheck_and_build_commands_are_ordered_by_scope():
    ladder = build_verification_ladder(
        _plan(
            [
                _task("task-build", "Check package build", test_command="npm run build"),
                _task("task-lint", "Check lint", test_command="poetry run ruff check"),
                _task(
                    "task-test",
                    "Check focused regression",
                    test_command="poetry run pytest tests/test_queue.py::test_priority_order",
                ),
                _task("task-types", "Check types", test_command="npm run typecheck"),
            ]
        )
    )

    assert [(step.command, step.scope, step.tasks_covered) for step in ladder.steps] == [
        (
            "poetry run pytest tests/test_queue.py::test_priority_order",
            "targeted_test",
            ("task-test",),
        ),
        ("poetry run ruff check", "lint", ("task-lint",)),
        ("npm run typecheck", "typecheck", ("task-types",)),
        ("npm run build", "build", ("task-build",)),
    ]


def test_broad_package_level_fallback_commands_run_after_narrow_checks():
    ladder = build_verification_ladder(
        _plan(
            [
                _task("task-suite", "Run broad suite", test_command="poetry run pytest"),
                _task("task-focused", "Run focused test", test_command="pytest tests/test_ui.py"),
                _task("task-node-suite", "Run node suite", test_command="npm test"),
            ]
        )
    )

    assert [(step.command, step.scope) for step in ladder.steps] == [
        ("pytest tests/test_ui.py", "targeted_test"),
        ("poetry run pytest", "package_test"),
        ("npm test", "package_test"),
    ]
    assert [step.tasks_covered for step in ladder.steps] == [
        ("task-focused",),
        ("task-suite",),
        ("task-node-suite",),
    ]


def test_tasks_with_missing_validation_commands_are_structured_gaps():
    ladder = build_verification_ladder(
        _plan(
            [
                _task("task-ok", "Has command", test_command="pytest tests/test_ok.py"),
                _task("task-none", "No command", test_command=None),
                _task("task-empty", "Empty command", test_command="   "),
            ]
        )
    )

    assert ladder.gaps == (
        VerificationCoverageGap(
            task_id="task-none",
            title="No command",
            reason="missing task test_command",
            blocking=True,
        ),
        VerificationCoverageGap(
            task_id="task-empty",
            title="Empty command",
            reason="missing task test_command",
            blocking=True,
        ),
    )
    assert [gap.to_dict() for gap in ladder.gaps] == [
        {
            "task_id": "task-none",
            "title": "No command",
            "reason": "missing task test_command",
            "blocking": True,
        },
        {
            "task_id": "task-empty",
            "title": "Empty command",
            "reason": "missing task test_command",
            "blocking": True,
        },
    ]


def test_integration_and_custom_commands_classify_stably_for_model_inputs():
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-e2e",
                    "Check browser flow",
                    test_command="npx playwright test tests/e2e/login.spec.ts",
                ),
                _task(
                    "task-custom",
                    "Check custom script",
                    test_command="./scripts/verify-fixtures.sh",
                ),
            ]
        )
    )

    ladder = build_verification_ladder(plan_model)
    payload = verification_ladder_to_dict(ladder)

    assert ladder.plan_id == "plan-verification"
    assert [(step.command, step.scope) for step in ladder.steps] == [
        ("npx playwright test tests/e2e/login.spec.ts", "integration_test"),
        ("./scripts/verify-fixtures.sh", "custom_validation"),
    ]
    assert payload == ladder.to_dict()
    assert list(payload) == ["plan_id", "steps", "gaps"]
    assert list(payload["steps"][0]) == [
        "command",
        "scope",
        "reason",
        "tasks_covered",
        "blocking",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-verification",
        "implementation_brief_id": "brief-verification",
        "milestones": [],
        "tasks": tasks,
    }


def _task(task_id, title, *, test_command):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": [f"{title} is complete."],
        "test_command": test_command,
    }
