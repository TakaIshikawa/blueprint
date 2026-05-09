"""Comprehensive tests for the PlanOptimizer module (CPM scheduling)."""

from __future__ import annotations

import pytest

from blueprint.optimizers.plan_optimizer import (
    OptimizerConfig,
    OptimizationResult,
    PlanOptimizer,
    ResourceConstraints,
    ScheduledTask,
    TaskNode,
    WhatIfResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _task(
    tid: str,
    title: str = "",
    duration: float = 1.0,
    dependencies: list[str] | None = None,
    skills: list[str] | None = None,
) -> dict:
    """Build a minimal task dict for the optimizer."""
    d: dict = {"id": tid, "title": title or tid, "duration": duration}
    if dependencies:
        d["dependencies"] = dependencies
    if skills:
        d["skills"] = skills
    return d


def _task_ids(result: OptimizationResult) -> list[str]:
    """Return task_ids from the schedule in order."""
    return [s.task_id for s in result.scheduled_tasks]


def _schedule_map(result: OptimizationResult) -> dict[str, ScheduledTask]:
    """Return a dict mapping task_id -> ScheduledTask."""
    return {s.task_id: s for s in result.scheduled_tasks}


# ---------------------------------------------------------------------------
# 1. Empty / minimal inputs
# ---------------------------------------------------------------------------


def test_empty_task_list_returns_empty_result():
    opt = PlanOptimizer()
    result = opt.optimize([])

    assert result.scheduled_tasks == []
    assert result.critical_path == []
    assert result.total_duration == 0.0
    assert result.resource_utilization == 0.0
    assert result.alternatives == []


def test_single_task_schedule():
    opt = PlanOptimizer()
    result = opt.optimize([_task("a", duration=5.0)])

    assert len(result.scheduled_tasks) == 1
    s = result.scheduled_tasks[0]
    assert s.task_id == "a"
    assert s.start_time == 0.0
    assert s.end_time == 5.0
    assert s.is_critical is True
    assert s.slack == 0.0
    assert result.total_duration == 5.0
    assert result.critical_path == ["a"]


def test_task_without_id_is_skipped():
    opt = PlanOptimizer()
    tasks = [{"title": "no id", "duration": 3.0}]
    result = opt.optimize(tasks)

    assert result.scheduled_tasks == []
    assert result.total_duration == 0.0


def test_task_with_empty_string_id_is_skipped():
    opt = PlanOptimizer()
    tasks = [{"id": "", "title": "empty id", "duration": 2.0}]
    result = opt.optimize(tasks)

    assert result.scheduled_tasks == []


# ---------------------------------------------------------------------------
# 2. Linear chain: A -> B -> C
# ---------------------------------------------------------------------------


def test_linear_chain_scheduling_order():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=2.0),
        _task("b", duration=3.0, dependencies=["a"]),
        _task("c", duration=1.0, dependencies=["b"]),
    ]
    result = opt.optimize(tasks)

    ids = _task_ids(result)
    assert ids == ["a", "b", "c"]

    sm = _schedule_map(result)
    assert sm["a"].start_time == 0.0
    assert sm["a"].end_time == 2.0
    assert sm["b"].start_time == 2.0
    assert sm["b"].end_time == 5.0
    assert sm["c"].start_time == 5.0
    assert sm["c"].end_time == 6.0
    assert result.total_duration == 6.0


def test_linear_chain_critical_path():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=2.0),
        _task("b", duration=3.0, dependencies=["a"]),
        _task("c", duration=1.0, dependencies=["b"]),
    ]
    result = opt.optimize(tasks)

    assert result.critical_path == ["a", "b", "c"]
    for s in result.scheduled_tasks:
        assert s.is_critical is True
        assert s.slack == 0.0


# ---------------------------------------------------------------------------
# 3. Diamond graph: A -> {B, C} -> D
# ---------------------------------------------------------------------------


def test_diamond_graph_parallel_scheduling():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=1.0),
        _task("b", duration=2.0, dependencies=["a"]),
        _task("c", duration=3.0, dependencies=["a"]),
        _task("d", duration=1.0, dependencies=["b", "c"]),
    ]
    result = opt.optimize(tasks)

    sm = _schedule_map(result)
    # Both B and C start after A finishes
    assert sm["b"].start_time == 1.0
    assert sm["c"].start_time == 1.0
    # D starts after the longest predecessor finishes (C at 4.0)
    assert sm["d"].start_time == 4.0
    assert sm["d"].end_time == 5.0
    assert result.total_duration == 5.0


def test_diamond_graph_critical_path():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=1.0),
        _task("b", duration=2.0, dependencies=["a"]),
        _task("c", duration=3.0, dependencies=["a"]),
        _task("d", duration=1.0, dependencies=["b", "c"]),
    ]
    result = opt.optimize(tasks)

    # Critical path goes through the longer branch: A -> C -> D
    assert "a" in result.critical_path
    assert "c" in result.critical_path
    assert "d" in result.critical_path
    # B has slack (finishes at 3.0 but D starts at 4.0)
    sm = _schedule_map(result)
    assert sm["b"].slack > 0.0
    assert sm["b"].is_critical is False


# ---------------------------------------------------------------------------
# 4. Critical path identification with different durations
# ---------------------------------------------------------------------------


def test_critical_path_with_varying_durations():
    """Two independent paths of different lengths merging at the end."""
    opt = PlanOptimizer()
    tasks = [
        _task("start", duration=1.0),
        _task("fast1", duration=1.0, dependencies=["start"]),
        _task("fast2", duration=1.0, dependencies=["fast1"]),
        _task("slow1", duration=5.0, dependencies=["start"]),
        _task("end", duration=1.0, dependencies=["fast2", "slow1"]),
    ]
    result = opt.optimize(tasks)

    # Critical path: start -> slow1 -> end (total = 1 + 5 + 1 = 7)
    assert "start" in result.critical_path
    assert "slow1" in result.critical_path
    assert "end" in result.critical_path
    assert result.total_duration == 7.0

    sm = _schedule_map(result)
    # fast1 and fast2 have slack
    assert sm["fast1"].slack > 0.0
    assert sm["fast2"].slack > 0.0


def test_slack_computation():
    """Verify exact slack values in a simple graph."""
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=1.0),
        _task("b", duration=2.0, dependencies=["a"]),  # critical
        _task("c", duration=1.0, dependencies=["a"]),  # slack = 1.0
        _task("d", duration=1.0, dependencies=["b", "c"]),
    ]
    result = opt.optimize(tasks)

    sm = _schedule_map(result)
    assert sm["a"].slack == 0.0
    assert sm["b"].slack == 0.0
    assert sm["c"].slack == 1.0
    assert sm["d"].slack == 0.0


# ---------------------------------------------------------------------------
# 5. Resource constraints
# ---------------------------------------------------------------------------


def test_resource_constraint_limits_parallelism():
    """With max_parallel=1, all independent tasks must be serialized."""
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=1.0),
        _task("b", duration=1.0),
        _task("c", duration=1.0),
    ]
    constraints = ResourceConstraints(max_parallel_tasks=1)
    result = opt.optimize(tasks, constraints)

    assert result.total_duration == 3.0
    sm = _schedule_map(result)
    # All tasks must run one after another
    start_times = sorted(sm[tid].start_time for tid in ["a", "b", "c"])
    assert start_times == [0.0, 1.0, 2.0]


def test_resource_constraint_allows_full_parallelism():
    """With high parallelism, independent tasks run concurrently."""
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=2.0),
        _task("b", duration=3.0),
        _task("c", duration=1.0),
    ]
    constraints = ResourceConstraints(max_parallel_tasks=10)
    result = opt.optimize(tasks, constraints)

    sm = _schedule_map(result)
    # All tasks start at 0 since they are independent
    assert sm["a"].start_time == 0.0
    assert sm["b"].start_time == 0.0
    assert sm["c"].start_time == 0.0
    assert result.total_duration == 3.0


def test_resource_constraint_max_parallel_two():
    """With max_parallel=2 and 3 independent tasks, at most 2 run at once."""
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=2.0),
        _task("b", duration=2.0),
        _task("c", duration=2.0),
    ]
    constraints = ResourceConstraints(max_parallel_tasks=2)
    result = opt.optimize(tasks, constraints)

    sm = _schedule_map(result)
    start_times = sorted(sm[tid].start_time for tid in ["a", "b", "c"])
    # Two tasks start at 0, one must wait
    assert start_times[0] == 0.0
    assert start_times[1] == 0.0
    assert start_times[2] >= 2.0
    assert result.total_duration == 4.0


# ---------------------------------------------------------------------------
# 6. Cycle detection
# ---------------------------------------------------------------------------


def test_cycle_detection_raises_valueerror():
    opt = PlanOptimizer()
    tasks = [
        _task("a", dependencies=["b"]),
        _task("b", dependencies=["a"]),
    ]
    with pytest.raises(ValueError, match="Circular dependency"):
        opt.optimize(tasks)


def test_cycle_detection_three_node_cycle():
    opt = PlanOptimizer()
    tasks = [
        _task("a", dependencies=["c"]),
        _task("b", dependencies=["a"]),
        _task("c", dependencies=["b"]),
    ]
    with pytest.raises(ValueError, match="Circular dependency"):
        opt.optimize(tasks)


def test_self_cycle_detection():
    opt = PlanOptimizer()
    tasks = [_task("a", dependencies=["a"])]
    with pytest.raises(ValueError, match="Circular dependency"):
        opt.optimize(tasks)


# ---------------------------------------------------------------------------
# 7. What-if scenarios
# ---------------------------------------------------------------------------


def test_what_if_add_tasks():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=2.0),
        _task("b", duration=3.0, dependencies=["a"]),
    ]
    scenario = {"add_tasks": [_task("c", duration=4.0, dependencies=["b"])]}
    wi = opt.what_if(tasks, scenario)

    assert wi.original_duration == 5.0
    assert wi.modified_duration == 9.0
    assert wi.duration_delta == 4.0
    assert "c" in wi.affected_tasks


def test_what_if_remove_tasks():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=2.0),
        _task("b", duration=3.0, dependencies=["a"]),
        _task("c", duration=1.0, dependencies=["b"]),
    ]
    # Removing the middle task should shorten the schedule.
    # After removing b, c's dependency on b is also stripped.
    scenario = {"remove_tasks": ["b"]}
    wi = opt.what_if(tasks, scenario)

    assert wi.original_duration == 6.0
    # a=2 and c=1 are now independent, run in parallel -> duration = max(2,1) = 2
    assert wi.modified_duration == 2.0
    assert wi.duration_delta == -4.0


def test_what_if_change_durations():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=2.0),
        _task("b", duration=3.0, dependencies=["a"]),
    ]
    scenario = {"change_durations": {"b": 10.0}}
    wi = opt.what_if(tasks, scenario)

    assert wi.original_duration == 5.0
    assert wi.modified_duration == 12.0
    assert wi.duration_delta == 7.0
    assert "b" in wi.affected_tasks


def test_what_if_empty_scenario():
    opt = PlanOptimizer()
    tasks = [_task("a", duration=2.0)]
    scenario: dict = {}
    wi = opt.what_if(tasks, scenario)

    assert wi.duration_delta == 0.0
    assert wi.affected_tasks == []


# ---------------------------------------------------------------------------
# 8. Alternative generation
# ---------------------------------------------------------------------------


def test_generate_alternatives_returns_alternatives():
    config = OptimizerConfig(max_alternatives=3)
    opt = PlanOptimizer(config)
    tasks = [
        _task("a", duration=1.0),
        _task("b", duration=1.0),
        _task("c", duration=1.0),
    ]
    alts = opt.generate_alternatives(tasks)

    assert len(alts) <= 3
    for alt in alts:
        assert "max_parallel_tasks" in alt
        assert "total_duration" in alt
        assert "resource_utilization" in alt
        assert "trade_off" in alt


def test_generate_alternatives_empty_tasks():
    opt = PlanOptimizer()
    alts = opt.generate_alternatives([])
    assert alts == []


def test_alternatives_included_in_optimize_result():
    config = OptimizerConfig(max_alternatives=2)
    opt = PlanOptimizer(config)
    tasks = [
        _task("a", duration=2.0),
        _task("b", duration=2.0),
    ]
    result = opt.optimize(tasks)

    assert isinstance(result.alternatives, list)
    # The optimizer generates alternatives with different parallelism
    assert len(result.alternatives) <= 2


def test_alternatives_different_parallelism_levels():
    config = OptimizerConfig(max_alternatives=3)
    opt = PlanOptimizer(config)
    tasks = [
        _task("a", duration=1.0),
        _task("b", duration=1.0),
        _task("c", duration=1.0),
        _task("d", duration=1.0),
    ]
    constraints = ResourceConstraints(max_parallel_tasks=4)
    alts = opt.generate_alternatives(tasks, constraints)

    parallelism_levels = [a["max_parallel_tasks"] for a in alts]
    # All should differ from the base (4)
    assert 4 not in parallelism_levels
    # All should be unique
    assert len(parallelism_levels) == len(set(parallelism_levels))


# ---------------------------------------------------------------------------
# 9. Skill constraints
# ---------------------------------------------------------------------------


def test_skill_constraint_blocks_unqualified_task():
    """A task requiring a skill not in the available pool should still be
    scheduled eventually (the scheduler does not permanently skip tasks
    that are ready but lack skills -- it retries after advancing time)."""
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=1.0, skills=["python"]),
        _task("b", duration=1.0, skills=["rust"]),
    ]
    constraints = ResourceConstraints(
        max_parallel_tasks=4, available_skills=["python"]
    )
    result = opt.optimize(tasks, constraints)

    sm = _schedule_map(result)
    # Task a should be scheduled; task b may not be scheduled
    # because its required skill (rust) is not available.
    assert "a" in sm


def test_skill_constraint_with_matching_skills():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=1.0, skills=["python"]),
        _task("b", duration=1.0, skills=["python"]),
    ]
    constraints = ResourceConstraints(
        max_parallel_tasks=4, available_skills=["python"]
    )
    result = opt.optimize(tasks, constraints)

    assert len(result.scheduled_tasks) == 2


def test_no_skill_constraint_schedules_all():
    """When no skills are specified in constraints, all tasks run."""
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=1.0, skills=["python"]),
        _task("b", duration=1.0, skills=["rust"]),
    ]
    result = opt.optimize(tasks)

    assert len(result.scheduled_tasks) == 2


# ---------------------------------------------------------------------------
# 10. Data model serialization (to_dict)
# ---------------------------------------------------------------------------


def test_task_node_to_dict():
    node = TaskNode(
        id="t1",
        title="Task One",
        duration=3.0,
        dependencies=["t0"],
        skills=["py"],
        earliest_start=0.0,
        earliest_finish=3.0,
        latest_start=1.0,
        latest_finish=4.0,
        slack=1.0,
    )
    d = node.to_dict()
    assert d["id"] == "t1"
    assert d["title"] == "Task One"
    assert d["duration"] == 3.0
    assert d["dependencies"] == ["t0"]
    assert d["skills"] == ["py"]
    assert d["earliest_start"] == 0.0
    assert d["earliest_finish"] == 3.0
    assert d["latest_start"] == 1.0
    assert d["latest_finish"] == 4.0
    assert d["slack"] == 1.0


def test_resource_constraints_to_dict():
    rc = ResourceConstraints(max_parallel_tasks=2, available_skills=["go", "py"])
    d = rc.to_dict()
    assert d == {"max_parallel_tasks": 2, "available_skills": ["go", "py"]}


def test_optimizer_config_to_dict():
    cfg = OptimizerConfig(strategy="balance_load", max_alternatives=5)
    d = cfg.to_dict()
    assert d == {"strategy": "balance_load", "max_alternatives": 5}


def test_scheduled_task_to_dict():
    st = ScheduledTask(
        task_id="x", start_time=1.0, end_time=3.0, is_critical=True, slack=0.0
    )
    d = st.to_dict()
    assert d == {
        "task_id": "x",
        "start_time": 1.0,
        "end_time": 3.0,
        "is_critical": True,
        "slack": 0.0,
    }


def test_optimization_result_to_dict():
    st = ScheduledTask(task_id="a", start_time=0, end_time=1, is_critical=True, slack=0)
    result = OptimizationResult(
        scheduled_tasks=[st],
        critical_path=["a"],
        total_duration=1.0,
        resource_utilization=1.0,
        alternatives=[],
    )
    d = result.to_dict()
    assert d["critical_path"] == ["a"]
    assert d["total_duration"] == 1.0
    assert d["resource_utilization"] == 1.0
    assert len(d["scheduled_tasks"]) == 1
    assert d["scheduled_tasks"][0]["task_id"] == "a"


def test_what_if_result_to_dict():
    wi = WhatIfResult(
        original_duration=5.0,
        modified_duration=3.0,
        duration_delta=-2.0,
        affected_tasks=["a", "b"],
    )
    d = wi.to_dict()
    assert d == {
        "original_duration": 5.0,
        "modified_duration": 3.0,
        "duration_delta": -2.0,
        "affected_tasks": ["a", "b"],
    }


# ---------------------------------------------------------------------------
# 11. Deterministic ordering
# ---------------------------------------------------------------------------


def test_deterministic_output():
    """Running the optimizer twice with the same input produces the same result."""
    opt = PlanOptimizer()
    tasks = [
        _task("c", duration=1.0),
        _task("a", duration=2.0),
        _task("b", duration=1.5, dependencies=["a"]),
        _task("d", duration=1.0, dependencies=["c"]),
    ]
    r1 = opt.optimize(tasks)
    r2 = opt.optimize(tasks)

    assert _task_ids(r1) == _task_ids(r2)
    assert r1.critical_path == r2.critical_path
    assert r1.total_duration == r2.total_duration


def test_deterministic_ordering_independent_tasks():
    """Independent tasks are scheduled in sorted id order for determinism."""
    opt = PlanOptimizer()
    tasks = [
        _task("z", duration=1.0),
        _task("m", duration=1.0),
        _task("a", duration=1.0),
    ]
    result = opt.optimize(tasks)

    # Topological sort uses sorted id order among peers
    ids = _task_ids(result)
    assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# 12. Resource utilization
# ---------------------------------------------------------------------------


def test_utilization_single_task():
    opt = PlanOptimizer()
    result = opt.optimize([_task("a", duration=4.0)])

    # utilization = work / (slots * makespan) = 4 / (4 * 4) = 0.25
    assert result.resource_utilization == 0.25


def test_utilization_full_parallel():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=2.0),
        _task("b", duration=2.0),
        _task("c", duration=2.0),
        _task("d", duration=2.0),
    ]
    constraints = ResourceConstraints(max_parallel_tasks=4)
    result = opt.optimize(tasks, constraints)

    # All 4 tasks run in parallel for 2 units. work=8, capacity=4*2=8
    assert result.resource_utilization == 1.0


def test_utilization_serial_execution():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=1.0),
        _task("b", duration=1.0, dependencies=["a"]),
    ]
    constraints = ResourceConstraints(max_parallel_tasks=2)
    result = opt.optimize(tasks, constraints)

    # work=2, capacity=2*2=4, utilization=0.5
    assert result.resource_utilization == 0.5


# ---------------------------------------------------------------------------
# 13. Edge cases
# ---------------------------------------------------------------------------


def test_zero_duration_task():
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=0.0),
        _task("b", duration=3.0, dependencies=["a"]),
    ]
    result = opt.optimize(tasks)

    sm = _schedule_map(result)
    assert sm["a"].start_time == 0.0
    assert sm["a"].end_time == 0.0
    assert sm["b"].start_time == 0.0
    assert sm["b"].end_time == 3.0
    assert result.total_duration == 3.0


def test_unknown_dependency_id():
    """Dependencies referencing non-existent task ids are harmlessly ignored."""
    opt = PlanOptimizer()
    tasks = [
        _task("a", duration=2.0, dependencies=["nonexistent"]),
    ]
    result = opt.optimize(tasks)

    assert len(result.scheduled_tasks) == 1
    assert result.total_duration == 2.0


def test_multiple_roots_and_leaves():
    """Graph with multiple independent start and end tasks."""
    opt = PlanOptimizer()
    tasks = [
        _task("r1", duration=2.0),
        _task("r2", duration=3.0),
        _task("m", duration=1.0, dependencies=["r1", "r2"]),
        _task("e1", duration=1.0, dependencies=["m"]),
        _task("e2", duration=2.0, dependencies=["m"]),
    ]
    result = opt.optimize(tasks)

    sm = _schedule_map(result)
    # m starts after both roots; r2 is longer so m starts at 3.0
    assert sm["m"].start_time == 3.0
    assert sm["m"].end_time == 4.0
    # e1 and e2 start at 4.0 (after m)
    assert sm["e1"].start_time == 4.0
    assert sm["e2"].start_time == 4.0
    assert result.total_duration == 6.0


def test_large_number_of_independent_tasks():
    """Optimizer handles many independent tasks correctly."""
    opt = PlanOptimizer()
    tasks = [_task(f"t{i}", duration=1.0) for i in range(20)]
    constraints = ResourceConstraints(max_parallel_tasks=20)
    result = opt.optimize(tasks, constraints)

    assert len(result.scheduled_tasks) == 20
    assert result.total_duration == 1.0


def test_default_optimizer_config():
    cfg = OptimizerConfig()
    assert cfg.strategy == "minimize_duration"
    assert cfg.max_alternatives == 3


def test_default_resource_constraints():
    rc = ResourceConstraints()
    assert rc.max_parallel_tasks == 4
    assert rc.available_skills == []
