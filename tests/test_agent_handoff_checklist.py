import json

from blueprint.agent_handoff_checklist import (
    build_agent_handoff_checklist,
    build_agent_handoff_checklists,
    checklists_to_dicts,
)


def test_dependency_aware_checklist_includes_dependency_completion_prechecks():
    checklists = build_agent_handoff_checklists(_execution_plan(), _implementation_brief())

    checklist = next(item for item in checklists if item.task_id == "task-api")

    assert checklist.files_to_inspect == (
        "src/app.py",
        "src/schema.py",
        "Repository: example/repo",
        "Implementation brief architecture notes",
    )
    assert (
        "Confirm dependency `task-setup` is completed or explicitly unblocked "
        "before starting; current status: completed."
    ) in checklist.prechecks
    assert (
        "Confirm dependency `task-schema` is completed or explicitly unblocked "
        "before starting; current status: unknown."
    ) in checklist.prechecks
    assert checklist.context.acceptance_criteria == (
        "API returns data",
        "Schema validates payloads",
    )


def test_missing_test_command_falls_back_to_plan_test_strategy():
    plan = _execution_plan()
    plan["tasks"][1]["test_command"] = None

    checklist = build_agent_handoff_checklist(
        task=plan["tasks"][1],
        execution_plan=plan,
        implementation_brief=_implementation_brief(),
        tasks_by_id={task["id"]: task for task in plan["tasks"]},
    )

    assert checklist.validation[0] == (
        "No task-specific test_command provided; use plan test strategy: Run pytest"
    )
    assert "Verify each acceptance criterion is satisfied." in checklist.validation


def test_missing_test_command_uses_generic_validation_when_no_plan_context():
    plan = _execution_plan()
    plan["test_strategy"] = None
    plan["tasks"][1]["test_command"] = None
    brief = _implementation_brief()
    brief["validation_plan"] = ""

    checklist = build_agent_handoff_checklist(
        task=plan["tasks"][1],
        execution_plan=plan,
        implementation_brief=brief,
    )

    assert checklist.validation[0] == (
        "No task-specific test_command provided; identify and run the narrowest "
        "relevant validation before handoff."
    )


def test_high_risk_task_includes_explicit_escalation_triggers():
    plan = _execution_plan()
    plan["tasks"][1]["risk_level"] = "high"

    checklist = build_agent_handoff_checklist(
        task=plan["tasks"][1],
        execution_plan=plan,
        implementation_brief=_implementation_brief(),
    )

    assert any("High-risk task `task-api`" in item for item in checklist.escalation)
    assert any("public contracts" in item for item in checklist.escalation)
    assert any("validation coverage" in item for item in checklist.escalation)


def test_serialization_is_json_compatible_and_deterministic():
    first = checklists_to_dicts(
        build_agent_handoff_checklists(_execution_plan(), _implementation_brief())
    )
    second = checklists_to_dicts(
        build_agent_handoff_checklists(_execution_plan(), _implementation_brief())
    )

    assert first == second
    assert list(first[1]) == [
        "task_id",
        "title",
        "context",
        "files_to_inspect",
        "prechecks",
        "implementation",
        "validation",
        "evidence",
        "escalation",
    ]
    assert list(first[1]["context"]) == [
        "plan_id",
        "brief_id",
        "target_engine",
        "target_repo",
        "project_type",
        "milestone",
        "task_status",
        "owner_type",
        "suggested_engine",
        "estimated_complexity",
        "risk_level",
        "description",
        "acceptance_criteria",
        "mvp_goal",
        "validation_plan",
        "handoff_prompt",
    ]
    assert json.loads(json.dumps(first)) == first


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Set up the project"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "tasks": [
            {
                "id": "task-setup",
                "title": "Setup project",
                "description": "Create the baseline project structure",
                "milestone": "Foundation",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["pyproject.toml"],
                "acceptance_criteria": ["Project installs"],
                "estimated_complexity": "low",
                "risk_level": "low",
                "test_command": "poetry run pytest tests/test_setup.py",
                "status": "completed",
            },
            {
                "id": "task-api",
                "title": "Build API",
                "description": "Implement the command API",
                "milestone": "Foundation",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": ["task-setup", "task-schema"],
                "files_or_modules": ["src/app.py", "src/schema.py"],
                "acceptance_criteria": ["API returns data", "Schema validates payloads"],
                "estimated_complexity": "medium",
                "risk_level": "medium",
                "test_command": "poetry run pytest tests/test_api.py",
                "status": "pending",
            },
        ],
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "title": "Test Brief",
        "mvp_goal": "Expose execution tasks as agent checklists",
        "architecture_notes": "Use structured dataclasses outside exporters",
        "validation_plan": "Run focused handoff checklist tests",
    }
