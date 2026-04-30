import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.execution_handoff_risk_register import (
    ExecutionHandoffRiskRecord,
    build_execution_handoff_risk_register,
    execution_handoff_risk_register_to_dict,
)


def test_dependency_chain_risk_identifies_downstream_tasks():
    register = build_execution_handoff_risk_register(
        _plan(
            [
                _task(
                    "task-contract",
                    "Update API contract",
                    files=["src/api/accounts.py"],
                    test_command="poetry run pytest tests/test_api.py",
                    owner_type="agent",
                    risk_level="medium",
                ),
                _task(
                    "task-client",
                    "Update API client",
                    depends_on=["task-contract"],
                    test_command="poetry run pytest tests/test_client.py",
                    owner_type="agent",
                ),
                _task(
                    "task-ui",
                    "Update account UI",
                    depends_on=["task-client"],
                    test_command="poetry run pytest tests/test_ui.py",
                    owner_type="agent",
                    risk_level="high",
                ),
            ]
        )
    )

    risk = _risk_by_id(register, "dependency-chain-contract-task")

    assert isinstance(risk, ExecutionHandoffRiskRecord)
    assert risk.category == "dependency"
    assert risk.severity == "high"
    assert risk.likelihood == "medium"
    assert risk.impacted_task_ids == ("task-contract", "task-client", "task-ui")
    assert risk.evidence == (
        "Task task-contract has 1 direct dependents.",
        "Dependency chain reaches 2 downstream tasks.",
    )
    assert "before dependent branches" in risk.mitigation


def test_missing_validation_and_blocked_status_create_distinct_task_risks():
    register = build_execution_handoff_risk_register(
        _plan(
            [
                _task(
                    "task-blocked",
                    "Wire billing sync",
                    status="blocked",
                    blocked_reason="Waiting on billing API token",
                    risk_level="medium",
                    owner_type="agent",
                )
            ]
        )
    )

    assert _risk_by_id(register, "blocked-blocked-task").to_dict() == {
        "risk_id": "blocked-blocked-task",
        "category": "blocked",
        "severity": "high",
        "likelihood": "high",
        "impacted_task_ids": ["task-blocked"],
        "evidence": [
            "Task task-blocked status is blocked.",
            "Blocked reason: Waiting on billing API token",
        ],
        "mitigation": (
            "Resolve the blocker for task-blocked or re-scope the branch before dispatch."
        ),
        "escalation_trigger": (
            "Escalate when task-blocked is still blocked at branch assignment time."
        ),
    }
    assert _risk_by_id(register, "validation-missing-blocked-task").category == "validation"


def test_file_contention_risk_groups_tasks_touching_same_module():
    register = build_execution_handoff_risk_register(
        _plan(
            [
                _task(
                    "task-parser",
                    "Refactor parser",
                    files=["src/parser.py"],
                    test_command="poetry run pytest tests/test_parser.py",
                    owner_type="agent",
                ),
                _task(
                    "task-export",
                    "Export parser metadata",
                    files=["src/parser.py"],
                    test_command="poetry run pytest tests/test_export.py",
                    owner_type="agent",
                    risk_level="high",
                ),
            ]
        )
    )

    risk = _risk_by_id(register, "contention-parser-py-src")

    assert risk.category == "contention"
    assert risk.severity == "high"
    assert risk.likelihood == "high"
    assert risk.impacted_task_ids == ("task-parser", "task-export")
    assert risk.evidence == (
        "Shared file or module 'src/parser.py' is assigned to task-parser, task-export.",
    )
    assert "Assign one owner for src/parser.py" in risk.mitigation


def test_inherited_brief_risks_link_to_matching_tasks_when_possible():
    register = build_execution_handoff_risk_register(
        _plan(
            [
                _task(
                    "task-webhook",
                    "Implement CRM webhook retries",
                    description="Handle CRM timeout and retry behavior.",
                    files=["src/integrations/crm_webhook.py"],
                    test_command="poetry run pytest tests/test_webhook.py",
                    owner_type="integration_agent",
                    risk_level="medium",
                ),
                _task(
                    "task-copy",
                    "Update empty state copy",
                    test_command="poetry run pytest tests/test_copy.py",
                    owner_type="agent",
                ),
            ]
        ),
        _brief(risks=["CRM webhook timeout can drop customer updates"]),
    )

    risk = next(item for item in register.risks if item.category == "brief")

    assert risk.severity == "medium"
    assert risk.likelihood == "medium"
    assert risk.impacted_task_ids == ("task-webhook",)
    assert risk.evidence == (
        "Brief risk: CRM webhook timeout can drop customer updates",
        "Matched task task-webhook: Implement CRM webhook retries",
    )
    assert "task-webhook handoff notes" in risk.mitigation


def test_aggregate_summaries_and_serialization_are_stable():
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-a",
                    "Update API",
                    files=["src/shared.py"],
                    test_command="poetry run pytest tests/test_a.py",
                    owner_type="agent",
                    risk_level="high",
                ),
                _task(
                    "task-b",
                    "Use API",
                    depends_on=["task-a"],
                    files=["src/shared.py"],
                    owner_type="agent",
                ),
                _task("task-c", "Missing owner", depends_on=["task-missing"]),
            ]
        )
    )
    brief_model = ImplementationBrief.model_validate(
        _brief(risks=["API contract may break customer integrations"])
    )

    register = build_execution_handoff_risk_register(plan_model, brief_model)
    payload = execution_handoff_risk_register_to_dict(register)

    assert payload == register.to_dict()
    assert payload["risk_count"] == len(payload["risks"])
    assert payload["counts_by_category"] == {
        "brief": 1,
        "contention": 1,
        "dependency": 1,
        "ownership": 1,
        "task_risk": 1,
        "validation": 2,
    }
    assert payload["counts_by_severity"] == {"high": 4, "low": 1, "medium": 2}
    assert list(payload) == [
        "plan_id",
        "risk_count",
        "counts_by_severity",
        "counts_by_category",
        "risks",
    ]
    assert list(payload["risks"][0]) == [
        "risk_id",
        "category",
        "severity",
        "likelihood",
        "impacted_task_ids",
        "evidence",
        "mitigation",
        "escalation_trigger",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _risk_by_id(register, risk_id):
    return next(risk for risk in register.risks if risk.risk_id == risk_id)


def _plan(tasks):
    return {
        "id": "handoff-plan",
        "implementation_brief_id": "handoff-brief",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [{"name": "Implementation"}],
        "test_strategy": "Run focused tests",
        "status": "draft",
        "metadata": {},
        "tasks": tasks,
    }


def _brief(*, risks):
    return {
        "id": "handoff-brief",
        "source_brief_id": "source-handoff",
        "title": "Handoff Brief",
        "problem_statement": "Agents need clear handoff risk context.",
        "mvp_goal": "Ship with coordinated execution.",
        "scope": ["Build risk register"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": risks,
        "validation_plan": "Run focused validation.",
        "definition_of_done": [],
        "status": "planned",
    }


def _task(
    task_id,
    title,
    *,
    description=None,
    depends_on=None,
    files=None,
    acceptance=None,
    test_command=None,
    owner_type=None,
    risk_level=None,
    status="pending",
    blocked_reason=None,
):
    task = {
        "id": task_id,
        "execution_plan_id": "handoff-plan",
        "title": title,
        "description": description or f"Implement {title}.",
        "depends_on": depends_on or [],
        "files_or_modules": files or [],
        "acceptance_criteria": acceptance or [f"{title} is complete"],
        "status": status,
    }
    if test_command is not None:
        task["test_command"] = test_command
    if owner_type is not None:
        task["owner_type"] = owner_type
    if risk_level is not None:
        task["risk_level"] = risk_level
    if blocked_reason is not None:
        task["blocked_reason"] = blocked_reason
    return task
