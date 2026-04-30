import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_dependency_health import (
    DependencyChain,
    DependencyHotspot,
    MissingDependencyReference,
    build_plan_dependency_health,
    plan_dependency_health_to_dict,
)


def test_healthy_plan_reports_counts_readiness_and_chain():
    result = build_plan_dependency_health(
        _plan(
            [
                _task("task-schema", title="Schema", status="completed"),
                _task("task-api", title="API", depends_on=["task-schema"]),
                _task("task-ui", title="UI", depends_on=["task-api"]),
                _task("task-docs", title="Docs"),
            ]
        )
    )

    assert result.total_dependencies == 2
    assert result.missing_dependency_references == ()
    assert result.ready_task_ids == ("task-api", "task-docs")
    assert result.blocked_task_ids == ("task-ui",)
    assert result.root_task_ids == ("task-schema", "task-docs")
    assert result.leaf_task_ids == ("task-ui", "task-docs")
    assert result.longest_dependency_chain == DependencyChain(
        task_ids=("task-schema", "task-api", "task-ui"),
        length=3,
    )
    assert result.summary == {
        "task_count": 4,
        "total_dependencies": 2,
        "missing_dependency_count": 0,
        "blocked_task_count": 1,
        "ready_task_count": 2,
        "leaf_task_count": 2,
        "root_task_count": 2,
        "longest_dependency_chain_length": 3,
        "fan_in_hotspot_count": 0,
        "fan_out_hotspot_count": 0,
        "warning_count": 0,
    }


def test_missing_dependency_ids_are_reported_without_crashing():
    result = build_plan_dependency_health(
        _plan(
            [
                _task("task-api", title="API", depends_on=["task-missing"]),
                _task("task-ui", title="UI", depends_on=["task-api"]),
            ]
        )
    )

    assert result.missing_dependency_references == (
        MissingDependencyReference(
            task_id="task-api",
            dependency_id="task-missing",
        ),
    )
    assert result.blocked_task_ids == ("task-api", "task-ui")
    assert result.root_task_ids == ("task-api",)
    assert result.leaf_task_ids == ("task-ui",)
    assert result.summary["missing_dependency_count"] == 1


def test_blocked_chains_treat_incomplete_dependencies_as_not_ready():
    result = build_plan_dependency_health(
        _plan(
            [
                _task("task-setup", title="Setup", status="in_progress"),
                _task("task-service", title="Service", depends_on=["task-setup"]),
                _task("task-client", title="Client", depends_on=["task-service"]),
            ]
        )
    )

    assert result.ready_task_ids == ("task-setup",)
    assert result.blocked_task_ids == ("task-service", "task-client")
    assert result.longest_dependency_chain.task_ids == (
        "task-setup",
        "task-service",
        "task-client",
    )


def test_fan_in_and_fan_out_hotspots_are_ordered_deterministically():
    result = build_plan_dependency_health(
        _plan(
            [
                _task("task-core", title="Core", status="completed"),
                _task("task-contract", title="Contract", status="completed"),
                _task("task-api", title="API", depends_on=["task-core", "task-contract"]),
                _task("task-ui", title="UI", depends_on=["task-core"]),
                _task("task-worker", title="Worker", depends_on=["task-core"]),
            ]
        )
    )

    assert result.fan_in_hotspots == (
        DependencyHotspot(
            task_id="task-api",
            title="API",
            fan_in=2,
            fan_out=0,
        ),
    )
    assert result.fan_out_hotspots == (
        DependencyHotspot(
            task_id="task-core",
            title="Core",
            fan_in=0,
            fan_out=3,
        ),
    )


def test_cycle_like_dependencies_return_bounded_chain_and_warning():
    result = build_plan_dependency_health(
        _plan(
            [
                _task("task-a", title="A", depends_on=["task-b"]),
                _task("task-b", title="B", depends_on=["task-a"]),
            ]
        )
    )

    assert result.longest_dependency_chain == DependencyChain(
        task_ids=("task-a", "task-b"),
        length=2,
        bounded=True,
    )
    assert result.warnings == (
        "Dependency cycle detected; longest_dependency_chain is bounded to an "
        "acyclic traversal.",
    )
    assert result.blocked_task_ids == ("task-a", "task-b")


def test_empty_task_plan_and_model_input_serialize_stably():
    plan_model = ExecutionPlan.model_validate(_plan([]))

    result = build_plan_dependency_health(plan_model)
    payload = plan_dependency_health_to_dict(result)

    assert payload == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "total_dependencies",
        "missing_dependency_references",
        "blocked_task_ids",
        "ready_task_ids",
        "leaf_task_ids",
        "root_task_ids",
        "longest_dependency_chain",
        "fan_in_hotspots",
        "fan_out_hotspots",
        "warnings",
        "summary",
    ]
    assert list(payload["longest_dependency_chain"]) == [
        "task_ids",
        "length",
        "bounded",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert payload["summary"] == {
        "task_count": 0,
        "total_dependencies": 0,
        "missing_dependency_count": 0,
        "blocked_task_count": 0,
        "ready_task_count": 0,
        "leaf_task_count": 0,
        "root_task_count": 0,
        "longest_dependency_chain_length": 0,
        "fan_in_hotspot_count": 0,
        "fan_out_hotspot_count": 0,
        "warning_count": 0,
    }


def _plan(tasks):
    return {
        "id": "plan-dependency-health",
        "implementation_brief_id": "brief-dependency-health",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    depends_on=None,
    status="pending",
):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": f"Implement {task_id}.",
        "depends_on": depends_on or [],
        "acceptance_criteria": ["Done"],
        "status": status,
    }
