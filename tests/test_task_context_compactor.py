import json

import pytest

from blueprint.task_context_compactor import compact_task_context


def test_compacted_context_contains_selected_task_and_direct_dependencies_only_by_default():
    payload = compact_task_context(_plan(), _brief(), "task-api")

    assert payload["brief"] == {
        "architecture_notes": "Use the existing store and CLI command patterns.",
        "definition_of_done": ["Tests pass", "CLI output is documented"],
        "domain": "developer_tools",
        "id": "brief-context",
        "mvp_goal": "Let agents work from a compact task context.",
        "problem_statement": "Full plans are too large for autonomous handoffs.",
        "risks": ["Context packing could omit validation details"],
        "scope": ["Task compaction"],
        "status": "approved",
        "title": "Compact task context",
        "validation_plan": "Run focused compactor tests",
    }
    assert payload["plan"] == {
        "handoff_prompt": "Implement from the compact payload.",
        "id": "plan-context",
        "implementation_brief_id": "brief-context",
        "project_type": "python_package",
        "status": "ready",
        "target_engine": "codex",
        "target_repo": "example/blueprint",
        "task_count": 4,
        "test_strategy": "Run pytest",
    }
    assert payload["task"]["id"] == "task-api"
    assert payload["task"]["acceptance_criteria"] == [
        "Compacted context includes the selected task",
        "Validation context includes task acceptance criteria",
    ]
    assert payload["task"]["files_or_modules"] == [
        "src/blueprint/task_context_compactor.py",
        "tests/test_task_context_compactor.py",
    ]
    assert payload["task"]["test_command"] == (
        "poetry run pytest tests/test_task_context_compactor.py -o addopts=''"
    )
    assert payload["task"]["risk_level"] == "low"
    assert [task["id"] for task in payload["dependency_tasks"]] == ["task-setup"]
    assert "dependent_tasks" not in payload
    assert "tasks" not in payload["plan"]
    assert "task-ui" not in json.dumps(payload, sort_keys=True)
    assert "large unrelated details" not in json.dumps(payload, sort_keys=True)


def test_include_dependents_adds_direct_downstream_tasks_without_duplicates():
    plan = _plan()
    plan["tasks"].append(
        _task(
            "task-cycle",
            "Cycle",
            depends_on=["task-api"],
            metadata={"risk": "Appears once even with duplicate dependency IDs"},
        )
    )
    plan["tasks"][0]["depends_on"] = ["task-api", "task-api"]

    payload = compact_task_context(plan, _brief(), "task-api", include_dependents=True)

    assert [task["id"] for task in payload["dependency_tasks"]] == ["task-setup"]
    assert [task["id"] for task in payload["dependent_tasks"]] == [
        "task-cycle",
        "task-docs",
        "task-ui",
    ]
    assert len({task["id"] for task in payload["dependent_tasks"]}) == 3
    assert "task-setup" not in {task["id"] for task in payload["dependent_tasks"]}


def test_unknown_task_ids_raise_clear_value_error():
    with pytest.raises(ValueError, match="Unknown task ID: task-missing"):
        compact_task_context(_plan(), _brief(), "task-missing")


def test_returned_payload_is_json_serializable_and_deterministic():
    payload = compact_task_context(_plan(), _brief(), "task-api", include_dependents=True)
    repeated = compact_task_context(
        {
            "tasks": list(reversed(_plan()["tasks"])),
            **{k: v for k, v in _plan().items() if k != "tasks"},
        },
        _brief(),
        "task-api",
        include_dependents=True,
    )

    assert json.loads(json.dumps(payload, sort_keys=True)) == payload
    assert json.dumps(payload, sort_keys=True) == json.dumps(repeated, sort_keys=True)
    assert payload["validation_context"] == {
        "definition_of_done": ["Tests pass", "CLI output is documented"],
        "dependency_acceptance_criteria": [
            {
                "acceptance_criteria": ["Foundation exists"],
                "task_id": "task-setup",
            }
        ],
        "task_acceptance_criteria": [
            "Compacted context includes the selected task",
            "Validation context includes task acceptance criteria",
        ],
        "task_test_command": "poetry run pytest tests/test_task_context_compactor.py -o addopts=''",
        "test_strategy": "Run pytest",
        "validation_plan": "Run focused compactor tests",
    }


def _brief():
    return {
        "id": "brief-context",
        "title": "Compact task context",
        "status": "approved",
        "domain": "developer_tools",
        "problem_statement": "Full plans are too large for autonomous handoffs.",
        "mvp_goal": "Let agents work from a compact task context.",
        "scope": ["Task compaction"],
        "architecture_notes": "Use the existing store and CLI command patterns.",
        "risks": ["Context packing could omit validation details"],
        "validation_plan": "Run focused compactor tests",
        "definition_of_done": ["Tests pass", "CLI output is documented"],
        "source_payload": {"body": "large brief payload that should be omitted"},
    }


def _plan():
    return {
        "id": "plan-context",
        "implementation_brief_id": "brief-context",
        "status": "ready",
        "target_engine": "codex",
        "target_repo": "example/blueprint",
        "project_type": "python_package",
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement from the compact payload.",
        "generation_prompt": "large unrelated details that should stay out",
        "tasks": [
            _task(
                "task-setup",
                "Setup",
                acceptance_criteria=["Foundation exists"],
                files_or_modules=["pyproject.toml"],
                status="completed",
            ),
            _task(
                "task-api",
                "Compact context API",
                depends_on=["task-setup"],
                acceptance_criteria=[
                    "Compacted context includes the selected task",
                    "Validation context includes task acceptance criteria",
                ],
                files_or_modules=[
                    "src/blueprint/task_context_compactor.py",
                    "tests/test_task_context_compactor.py",
                ],
                test_command="poetry run pytest tests/test_task_context_compactor.py -o addopts=''",
                risk_level="low",
                metadata={"risks": ["Payload shape is too broad"]},
            ),
            _task(
                "task-ui",
                "Expose context",
                depends_on=["task-api"],
                description="Downstream task.",
            ),
            _task(
                "task-docs",
                "Document context",
                depends_on=["task-api", "task-api"],
                description="Another downstream task.",
            ),
        ],
    }


def _task(
    task_id,
    title,
    *,
    depends_on=None,
    acceptance_criteria=None,
    files_or_modules=None,
    description="Implement the task.",
    status="pending",
    test_command=None,
    risk_level=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description,
        "status": status,
        "milestone": "Implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "estimated_complexity": "small",
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules or ["src/app.py"],
        "acceptance_criteria": acceptance_criteria or [f"{title} is complete"],
        "notes": "large unrelated details that should stay out",
    }
    if test_command is not None:
        task["test_command"] = test_command
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
