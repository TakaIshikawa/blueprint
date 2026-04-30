import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_validation_sequence import (
    TaskValidationSequence,
    ValidationCommandStep,
    ValidationFinding,
    ValidationPhase,
    build_task_validation_sequence,
    task_validation_sequence_to_dict,
)


def test_duplicate_commands_are_represented_once_with_all_originating_tasks():
    sequence = build_task_validation_sequence(
        _plan(
            [
                _task("task-api", "Update API", test_command="pytest tests/test_api.py"),
                _task("task-worker", "Update worker", test_command="pytest tests/test_api.py"),
            ]
        )
    )

    assert sequence.findings == ()
    assert sequence.phases[1] == ValidationPhase(
        name="targeted",
        commands=(
            ValidationCommandStep(
                command="pytest tests/test_api.py",
                phase="targeted",
                classification="targeted_test",
                task_ids=("task-api", "task-worker"),
                reused=True,
                reason="Focused task-level validation should run before broader checks.",
                escalation_reasons=(),
            ),
        ),
    )
    assert _phase_commands(sequence) == {
        "quick": [],
        "targeted": ["pytest tests/test_api.py"],
        "integration": [],
        "broad": [],
    }


def test_commands_are_classified_into_validation_phases():
    sequence = build_task_validation_sequence(
        _plan(
            [
                _task(
                    "task-format",
                    "Check format",
                    test_command="poetry run ruff format --check",
                ),
                _task("task-lint", "Check lint", test_command="poetry run ruff check"),
                _task("task-types", "Check types", test_command="npm run typecheck"),
                _task("task-unit", "Run unit", test_command="pytest tests/test_units.py"),
                _task(
                    "task-e2e",
                    "Run e2e",
                    test_command="npx playwright test tests/e2e/app.spec.ts",
                ),
                _task("task-build", "Build app", test_command="npm run build"),
                _task("task-suite", "Run suite", test_command="poetry run pytest"),
            ]
        )
    )

    assert [
        (step.command, step.classification)
        for phase in sequence.phases
        for step in phase.commands
    ] == [
        ("poetry run ruff format --check", "format"),
        ("poetry run ruff check", "lint"),
        ("npm run typecheck", "typecheck"),
        ("pytest tests/test_units.py", "targeted_test"),
        ("npx playwright test tests/e2e/app.spec.ts", "integration_test"),
        ("npm run build", "build"),
        ("poetry run pytest", "broad_test"),
    ]
    assert _phase_commands(sequence) == {
        "quick": [
            "poetry run ruff format --check",
            "poetry run ruff check",
            "npm run typecheck",
        ],
        "targeted": ["pytest tests/test_units.py"],
        "integration": [
            "npx playwright test tests/e2e/app.spec.ts",
            "npm run build",
        ],
        "broad": ["poetry run pytest"],
    }


def test_missing_task_validation_commands_are_structured_findings():
    sequence = build_task_validation_sequence(
        _plan(
            [
                _task("task-ok", "Has command", test_command="pytest tests/test_ok.py"),
                _task("task-none", "No command", test_command=None),
                _task("task-empty", "Empty command", test_command="   "),
            ]
        )
    )

    assert sequence.findings == (
        ValidationFinding(
            code="missing_task_validation_command",
            severity="warning",
            task_id="task-none",
            title="No command",
            message="Task has no task-level validation command.",
        ),
        ValidationFinding(
            code="missing_task_validation_command",
            severity="warning",
            task_id="task-empty",
            title="Empty command",
            message="Task has no task-level validation command.",
        ),
    )
    assert [finding.to_dict() for finding in sequence.findings] == [
        {
            "code": "missing_task_validation_command",
            "severity": "warning",
            "task_id": "task-none",
            "title": "No command",
            "message": "Task has no task-level validation command.",
        },
        {
            "code": "missing_task_validation_command",
            "severity": "warning",
            "task_id": "task-empty",
            "title": "Empty command",
            "message": "Task has no task-level validation command.",
        },
    ]


def test_risky_expected_files_escalate_related_commands_to_broad_phase():
    sequence = build_task_validation_sequence(
        _plan(
            [
                _task(
                    "task-config",
                    "Update config",
                    files_or_modules=["src/features/flags.py"],
                    expectedFiles=[" config/app.yml "],
                    test_command="pytest tests/test_flags.py",
                ),
                _task(
                    "task-db",
                    "Update migration",
                    files_or_modules=["src/features/users.py"],
                    metadata={"expected_files": ["migrations/202604301200_add_users.sql"]},
                    test_command="pytest tests/test_users.py",
                ),
            ]
        )
    )

    assert _phase_commands(sequence)["targeted"] == []
    broad_commands = [
        (step.command, step.phase, step.classification)
        for step in sequence.phases[3].commands
    ]
    assert broad_commands == [
        ("pytest tests/test_flags.py", "broad", "targeted_test"),
        ("pytest tests/test_users.py", "broad", "targeted_test"),
    ]
    assert sequence.phases[3].commands[0].escalation_reasons == (
        "config path requires broad validation: config/app.yml",
    )
    assert sequence.phases[3].commands[1].escalation_reasons == (
        "database path requires broad validation: migrations/202604301200_add_users.sql",
    )


def test_model_inputs_metadata_commands_and_output_order_are_deterministic():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-meta",
                    "Use metadata commands",
                    test_command=None,
                    metadata={
                        "validation_commands": {
                            "lint": ["poetry run ruff check"],
                            "test": ["pytest tests/test_meta.py"],
                        }
                    },
                ),
                _task(
                    "task-direct",
                    "Use direct command",
                    test_command="pytest tests/test_direct.py",
                ),
            ]
        )
    )

    first = build_task_validation_sequence(model)
    second = build_task_validation_sequence(model)
    payload = task_validation_sequence_to_dict(first)

    assert isinstance(first, TaskValidationSequence)
    assert payload == first.to_dict()
    assert payload == task_validation_sequence_to_dict(second)
    assert list(payload) == ["plan_id", "phases", "findings"]
    assert [phase["name"] for phase in payload["phases"]] == [
        "quick",
        "targeted",
        "integration",
        "broad",
    ]
    assert list(payload["phases"][0]["commands"][0]) == [
        "command",
        "phase",
        "classification",
        "task_ids",
        "reused",
        "reason",
        "escalation_reasons",
    ]
    assert _phase_commands(first) == {
        "quick": ["poetry run ruff check"],
        "targeted": ["pytest tests/test_meta.py", "pytest tests/test_direct.py"],
        "integration": [],
        "broad": [],
    }
    assert json.loads(json.dumps(payload)) == payload


def _phase_commands(sequence):
    return {
        phase.name: [command.command for command in phase.commands]
        for phase in sequence.phases
    }


def _plan(tasks):
    return {
        "id": "plan-validation-sequence",
        "implementation_brief_id": "brief-validation-sequence",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [{"name": "Implementation"}],
        "test_strategy": "Run focused validation",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "metadata": {},
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    test_command,
    files_or_modules=None,
    expectedFiles=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "execution_plan_id": "plan-validation-sequence",
        "title": title,
        "description": f"Implement {title}",
        "milestone": "Implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or ["src/features/example.py"],
        "acceptance_criteria": ["Behavior is covered"],
        "estimated_complexity": "medium",
        "risk_level": "low",
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
    }
    if expectedFiles is not None:
        task["expectedFiles"] = expectedFiles
    return task
