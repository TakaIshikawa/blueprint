from blueprint.task_evidence import (
    TaskEvidenceRequirement,
    build_task_evidence_requirements,
)


def test_build_task_evidence_requirements_returns_records_in_task_order():
    requirements = build_task_evidence_requirements(
        {
            "id": "plan-evidence",
            "tasks": [
                _task("task-backend", "Backend API", files=["src/api.py"]),
                _task("task-docs", "Docs", files=["README.md"]),
            ],
        }
    )

    assert [requirement.task_id for requirement in requirements] == [
        "task-backend",
        "task-docs",
    ]
    assert all(isinstance(requirement, TaskEvidenceRequirement) for requirement in requirements)


def test_backend_task_requires_test_output_without_ui_or_migration_artifacts():
    requirement = build_task_evidence_requirements(
        {
            "id": "plan-evidence",
            "tasks": [
                _task(
                    "task-api",
                    "Add backend endpoint",
                    files=["src/blueprint/api.py", "tests/test_api.py"],
                    acceptance=["API returns serialized task evidence"],
                    test_command="poetry run pytest tests/test_api.py",
                )
            ],
        }
    )[0]

    assert requirement.required_artifacts == ["implementation summary", "test output"]
    assert requirement.suggested_commands == ["poetry run pytest tests/test_api.py"]
    assert "screenshots" not in requirement.required_artifacts
    assert "migration or data operation logs" not in requirement.required_artifacts


def test_frontend_task_requires_screenshots():
    requirement = build_task_evidence_requirements(
        {
            "id": "plan-evidence",
            "tasks": [
                _task(
                    "task-ui",
                    "Update dashboard UI",
                    description="Adjust the frontend component responsive layout.",
                    files=["src/components/Dashboard.tsx", "src/styles/dashboard.css"],
                    acceptance=["Dashboard renders the updated empty state"],
                )
            ],
        }
    )[0]

    assert "screenshots" in requirement.required_artifacts
    assert "test output" not in requirement.required_artifacts
    assert any("screenshots" in item for item in requirement.completion_checklist)


def test_migration_task_requires_logs_and_test_output():
    requirement = build_task_evidence_requirements(
        {
            "id": "plan-evidence",
            "tasks": [
                _task(
                    "task-migration",
                    "Add user audit migration",
                    files=["migrations/20260501_add_user_audit.sql"],
                    acceptance=["Migration creates the audit table"],
                    test_command="poetry run pytest tests/test_db_migrations.py",
                )
            ],
        }
    )[0]

    assert requirement.required_artifacts == [
        "implementation summary",
        "test output",
        "migration or data operation logs",
    ]
    assert any("logs" in note for note in requirement.risk_notes)


def test_documentation_task_gets_minimal_completion_evidence():
    requirement = build_task_evidence_requirements(
        {
            "id": "plan-evidence",
            "tasks": [
                _task(
                    "task-docs",
                    "Document setup",
                    files=["docs/setup.md"],
                    acceptance=["Setup guide describes the Poetry workflow"],
                )
            ],
        }
    )[0]

    assert requirement.required_artifacts == ["implementation summary"]
    assert requirement.suggested_commands == []
    assert "screenshots" not in requirement.required_artifacts
    assert "migration or data operation logs" not in requirement.required_artifacts


def test_high_risk_cross_cutting_task_requires_reviewer_notes():
    requirement = build_task_evidence_requirements(
        {
            "id": "plan-evidence",
            "metadata": {
                "validation_commands": {
                    "test": ["poetry run pytest tests/test_task_queue.py"],
                    "lint": ["poetry run ruff check src tests"],
                }
            },
            "tasks": [
                _task(
                    "task-shared",
                    "Refactor shared task queue behavior",
                    description="Cross-cutting refactor across shared queue and exporter behavior.",
                    files=[
                        "src/blueprint/cli.py",
                        "src/blueprint/exporters/relay.py",
                        "src/blueprint/store/db.py",
                        "tests/test_task_queue.py",
                        "tests/test_relay_yaml_exporter.py",
                    ],
                    acceptance=["Existing queue and relay exports keep working"],
                    risk_level="high",
                    metadata={"risks": ["Queue behavior affects multiple exporters"]},
                )
            ],
        }
    )[0]

    assert requirement.required_artifacts == [
        "implementation summary",
        "test output",
        "reviewer notes",
    ]
    assert requirement.suggested_commands == [
        "poetry run pytest tests/test_task_queue.py",
        "poetry run ruff check src tests",
    ]
    assert any("high" in note for note in requirement.risk_notes)
    assert any("cross-cutting" in note for note in requirement.risk_notes)


def test_sparse_task_metadata_still_produces_useful_minimal_checklist():
    requirement = build_task_evidence_requirements(
        {"id": "plan-evidence", "tasks": [{"id": "task-sparse", "title": "Sparse task"}]}
    )[0]

    assert requirement.required_artifacts == ["implementation summary"]
    assert requirement.completion_checklist == [
        "Summarize the implementation and any intentional scope limits.",
        "Identify the files or modules changed while completing the task.",
        "Record the observable behavior that proves the task is complete.",
        "Run the smallest relevant validation available and note the result.",
    ]
    assert requirement.risk_notes == [
        "No task files were listed; evidence should name the files actually changed.",
        "No acceptance criteria were listed; evidence should define the observed "
        "completion signal.",
    ]


def _task(
    task_id,
    title,
    *,
    description=None,
    files=None,
    acceptance=None,
    test_command=None,
    risk_level=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {title}.",
        "files_or_modules": files or [],
        "acceptance_criteria": acceptance or [f"{title} is complete"],
    }
    if test_command is not None:
        task["test_command"] = test_command
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
