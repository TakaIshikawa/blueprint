import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_dependency_sla_matrix import (
    DependencySlaEdge,
    DependencySlaFinding,
    PlanDependencySlaMatrix,
    build_plan_dependency_sla_matrix,
    derive_plan_dependency_sla_matrix,
    plan_dependency_sla_matrix_to_dict,
    plan_dependency_sla_matrix_to_markdown,
)


def test_dependency_edges_are_extracted_in_deterministic_task_and_dependency_order():
    result = build_plan_dependency_sla_matrix(
        _plan(
            [
                _task("task-z", title="Zed"),
                _task("task-a", title="Alpha"),
                _task(
                    "task-final",
                    title="Final",
                    depends_on=["task-z", "task-a"],
                    metadata={"handoff_sla": "within 12 hours"},
                ),
            ]
        )
    )

    assert [edge.edge_id for edge in result.edges] == [
        "task-z->task-final",
        "task-a->task-final",
    ]
    assert [edge.coordination_risk for edge in result.edges] == ["low", "low"]
    assert {edge.expected_handoff_sla for edge in result.edges} == {"within 12 hours"}
    assert result.findings == ()


def test_cross_owner_and_engine_boundaries_raise_coordination_risk_and_findings():
    result = build_plan_dependency_sla_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    milestone="Foundation",
                    owner_type="agent",
                    suggested_engine="codex",
                    test_command="poetry run pytest tests/test_api.py",
                ),
                _task(
                    "task-copy",
                    milestone="Launch",
                    owner_type="human",
                    suggested_engine="manual",
                    depends_on=["task-api"],
                    metadata={"sla": "handoff by 2026-05-03 10:00 JST"},
                ),
            ]
        )
    )

    edge = result.edges[0]
    assert edge.coordination_risk == "high"
    assert edge.risk_reasons == (
        "cross-owner boundary",
        "cross-engine boundary",
        "cross-milestone handoff",
    )
    assert edge.validation_gates == ("poetry run pytest tests/test_api.py",)
    assert [finding.code for finding in result.findings] == ["cross_boundary_dependency"]


def test_missing_weak_and_blocked_sla_metadata_produce_actionable_findings():
    result = build_plan_dependency_sla_matrix(
        _plan(
            [
                _task("task-schema", estimated_complexity="high"),
                _task("task-api", depends_on=["task-schema"], risk_level="high"),
                _task(
                    "task-ui",
                    depends_on=["task-api"],
                    metadata={"sla": "soon", "blocked_until": "2026-05-04"},
                ),
            ]
        )
    )

    assert [edge.to_dict() for edge in result.edges] == [
        {
            "edge_id": "task-schema->task-api",
            "prerequisite_task_id": "task-schema",
            "dependent_task_id": "task-api",
            "prerequisite_title": "Task task-schema",
            "dependent_title": "Task task-api",
            "prerequisite_owner_type": "agent",
            "dependent_owner_type": "agent",
            "prerequisite_engine": "codex",
            "dependent_engine": "codex",
            "prerequisite_milestone": "Foundation",
            "dependent_milestone": "Foundation",
            "coordination_risk": "high",
            "risk_reasons": ["high task risk", "high complexity"],
            "expected_handoff_sla": "within 48 hours after prerequisite completion",
            "sla_source": "missing",
            "due_date": None,
            "blocked_until": None,
            "validation_gates": [],
        },
        {
            "edge_id": "task-api->task-ui",
            "prerequisite_task_id": "task-api",
            "dependent_task_id": "task-ui",
            "prerequisite_title": "Task task-api",
            "dependent_title": "Task task-ui",
            "prerequisite_owner_type": "agent",
            "dependent_owner_type": "agent",
            "prerequisite_engine": "codex",
            "dependent_engine": "codex",
            "prerequisite_milestone": "Foundation",
            "dependent_milestone": "Foundation",
            "coordination_risk": "high",
            "risk_reasons": ["high task risk", "blocked schedule"],
            "expected_handoff_sla": "soon",
            "sla_source": "metadata",
            "due_date": None,
            "blocked_until": "2026-05-04",
            "validation_gates": [],
        },
    ]
    assert [finding.code for finding in result.findings] == [
        "missing_handoff_sla",
        "high_risk_dependency_missing_validation_gate",
        "weak_handoff_sla",
        "high_risk_dependency_missing_validation_gate",
    ]
    assert result.findings[0].suggested_remediation.startswith("Add sla or handoff_sla")
    assert result.findings[2].suggested_remediation.startswith("Replace vague SLA text")


def test_due_dates_unknown_dependencies_and_markdown_summary_are_reported():
    result = build_plan_dependency_sla_matrix(
        _plan(
            [
                _task(
                    "task-ui",
                    depends_on=["task-missing"],
                    metadata={"due_date": "2026-05-05"},
                )
            ],
            plan_id="plan-sla",
        )
    )

    assert result.edges[0].to_dict() == {
        "edge_id": "task-missing->task-ui",
        "prerequisite_task_id": "task-missing",
        "dependent_task_id": "task-ui",
        "prerequisite_title": None,
        "dependent_title": "Task task-ui",
        "prerequisite_owner_type": "unknown",
        "dependent_owner_type": "agent",
        "prerequisite_engine": "unknown",
        "dependent_engine": "codex",
        "prerequisite_milestone": None,
        "dependent_milestone": "Foundation",
        "coordination_risk": "high",
        "risk_reasons": ["unknown prerequisite"],
        "expected_handoff_sla": "handoff due by 2026-05-05",
        "sla_source": "metadata",
        "due_date": "2026-05-05",
        "blocked_until": None,
        "validation_gates": [],
    }
    assert [finding.code for finding in result.findings] == [
        "unknown_dependency",
        "cross_boundary_dependency",
        "high_risk_dependency_missing_validation_gate",
    ]
    assert plan_dependency_sla_matrix_to_markdown(result) == "\n".join(
        [
            "# Plan Dependency SLA Matrix: plan-sla",
            "",
            "## Dependency Matrix",
            "",
            "| Edge | Prerequisite | Dependent | Boundary | Risk | SLA | Validation Gates |",
            "| --- | --- | --- | --- | --- | --- | --- |",
            (
                "| task-missing->task-ui | task-missing | task-ui | "
                "owner: unknown->agent; engine: unknown->codex; milestone: none->Foundation | "
                "high | handoff due by 2026-05-05 | none |"
            ),
            "",
            "## Findings Summary",
            "",
            (
                "- **error** `unknown_dependency`: Task task-ui depends on unknown task "
                "task-missing. Edges: task-missing->task-ui."
            ),
            (
                "- **warning** `cross_boundary_dependency`: Dependency task-missing->task-ui "
                "crosses owner or engine boundaries. Edges: task-missing->task-ui."
            ),
            (
                "- **error** `high_risk_dependency_missing_validation_gate`: High-risk dependency "
                "task-missing->task-ui has no validation gate. Edges: task-missing->task-ui."
            ),
        ]
    )


def test_serialization_aliases_model_input_and_empty_inputs_are_stable_without_mutation():
    plan = _plan(
        [
            _task("task-setup", test_command="make setup-test"),
            _task(
                "task-feature",
                depends_on=["task-setup"],
                metadata={"handoff_sla": "within 8 hours"},
            ),
        ],
        plan_id="plan-model",
    )
    original = copy.deepcopy(plan)

    result = build_plan_dependency_sla_matrix(ExecutionPlan.model_validate(plan))
    alias_result = derive_plan_dependency_sla_matrix(plan)
    empty = build_plan_dependency_sla_matrix({"id": "plan-empty", "tasks": []})
    payload = plan_dependency_sla_matrix_to_dict(result)

    assert plan == original
    assert isinstance(result, PlanDependencySlaMatrix)
    assert isinstance(DependencySlaEdge, type)
    assert isinstance(DependencySlaFinding, type)
    assert payload == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert alias_result.to_dict() == result.to_dict()
    assert list(payload) == ["plan_id", "edges", "findings"]
    assert list(payload["edges"][0]) == [
        "edge_id",
        "prerequisite_task_id",
        "dependent_task_id",
        "prerequisite_title",
        "dependent_title",
        "prerequisite_owner_type",
        "dependent_owner_type",
        "prerequisite_engine",
        "dependent_engine",
        "prerequisite_milestone",
        "dependent_milestone",
        "coordination_risk",
        "risk_reasons",
        "expected_handoff_sla",
        "sla_source",
        "due_date",
        "blocked_until",
        "validation_gates",
    ]
    assert plan_dependency_sla_matrix_to_markdown(empty) == "\n".join(
        [
            "# Plan Dependency SLA Matrix: plan-empty",
            "",
            "No dependency edges were found.",
            "",
            "## Findings Summary",
            "",
            "No findings.",
        ]
    )


def _plan(tasks, *, plan_id="plan-sla"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-sla",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation"}, {"name": "Launch"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    milestone="Foundation",
    owner_type="agent",
    suggested_engine="codex",
    depends_on=None,
    estimated_complexity="medium",
    risk_level="medium",
    test_command=None,
    validation_command=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or f"Task {task_id}",
        "description": description or f"Implement {task_id}.",
        "milestone": milestone,
        "owner_type": owner_type,
        "suggested_engine": suggested_engine,
        "depends_on": depends_on or [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{task_id} works"],
        "estimated_complexity": estimated_complexity,
        "risk_level": risk_level,
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
    if validation_command is not None:
        task["validation_command"] = validation_command
    return task
