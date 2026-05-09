"""Tests for constraint-based plan optimizer."""

import pytest

from blueprint.optimizers.constraint_optimizer import (
    AssignmentPlan,
    Constraint,
    ConstraintKind,
    ConstraintOptimizer,
    ConstraintSet,
    ObjectiveType,
    OptimizedSchedule,
    ResourceAssignment,
    Solution,
    TaskSchedule,
    ValidationResult,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def optimizer() -> ConstraintOptimizer:
    return ConstraintOptimizer()


@pytest.fixture
def simple_plan() -> dict:
    return {
        "id": "plan-1",
        "tasks": [
            {"id": "t1", "title": "Design", "duration": 3, "dependencies": []},
            {"id": "t2", "title": "Implement", "duration": 5, "dependencies": ["t1"]},
            {"id": "t3", "title": "Test", "duration": 2, "dependencies": ["t2"]},
        ],
        "max_parallel": 2,
    }


@pytest.fixture
def complex_plan() -> dict:
    return {
        "id": "plan-2",
        "tasks": [
            {"id": "t1", "title": "Design API", "duration": 3, "dependencies": [],
             "required_skills": ["design"]},
            {"id": "t2", "title": "Design UI", "duration": 2, "dependencies": [],
             "required_skills": ["design", "frontend"]},
            {"id": "t3", "title": "Implement API", "duration": 5, "dependencies": ["t1"],
             "required_skills": ["backend"]},
            {"id": "t4", "title": "Implement UI", "duration": 4, "dependencies": ["t2"],
             "required_skills": ["frontend"]},
            {"id": "t5", "title": "Integration test", "duration": 3, "dependencies": ["t3", "t4"],
             "required_skills": ["testing"]},
        ],
        "resources": [
            {"id": "dev-1", "skills": ["design", "backend"]},
            {"id": "dev-2", "skills": ["frontend", "design"]},
            {"id": "dev-3", "skills": ["testing", "backend"]},
        ],
        "max_parallel": 3,
        "deadline": "2025-06-01",
    }


# ---------------------------------------------------------------------------
# Constraint definition
# ---------------------------------------------------------------------------

class TestDefineConstraints:
    def test_defines_hard_constraints(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        constraints = optimizer.define_constraints(simple_plan)
        assert isinstance(constraints, ConstraintSet)
        assert len(constraints.hard_constraints) > 0

    def test_dependency_constraints_defined(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        constraints = optimizer.define_constraints(simple_plan)
        dep_constraints = [
            c for c in constraints.hard_constraints
            if c.name == "dependency_ordering"
        ]
        # t2 depends on t1, t3 depends on t2
        assert len(dep_constraints) == 2

    def test_capacity_constraint_defined(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        constraints = optimizer.define_constraints(simple_plan)
        cap_constraints = [
            c for c in constraints.hard_constraints
            if c.name == "resource_capacity"
        ]
        assert len(cap_constraints) == 1

    def test_deadline_constraint_when_present(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        constraints = optimizer.define_constraints(complex_plan)
        deadline_constraints = [
            c for c in constraints.hard_constraints
            if c.name == "deadline"
        ]
        assert len(deadline_constraints) == 1

    def test_soft_constraints_defined(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        constraints = optimizer.define_constraints(simple_plan)
        assert len(constraints.soft_constraints) > 0

    def test_soft_constraint_types(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        constraints = optimizer.define_constraints(complex_plan)
        names = [c.name for c in constraints.soft_constraints]
        assert "minimize_duration" in names
        assert "balance_workload" in names
        assert "minimize_context_switching" in names

    def test_skill_match_constraint_when_resources(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        constraints = optimizer.define_constraints(complex_plan)
        names = [c.name for c in constraints.soft_constraints]
        assert "skill_match" in names

    def test_constraint_set_to_dict(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        constraints = optimizer.define_constraints(simple_plan)
        d = constraints.to_dict()
        assert "hard_constraints" in d
        assert "soft_constraints" in d
        assert d["total"] > 0

    def test_empty_plan(self, optimizer: ConstraintOptimizer) -> None:
        constraints = optimizer.define_constraints({"tasks": []})
        assert isinstance(constraints, ConstraintSet)


# ---------------------------------------------------------------------------
# Schedule optimization (minimize makespan)
# ---------------------------------------------------------------------------

class TestOptimizeSchedule:
    def test_optimizes_simple_plan(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        schedule = optimizer.optimize_schedule(simple_plan)
        assert isinstance(schedule, OptimizedSchedule)
        assert schedule.plan_id == "plan-1"
        assert len(schedule.tasks) == 3
        assert schedule.makespan > 0

    def test_respects_dependencies(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        schedule = optimizer.optimize_schedule(simple_plan)
        task_times = {t.task_id: t for t in schedule.tasks}
        # t2 must start after t1 ends
        assert task_times["t2"].start_time >= task_times["t1"].end_time
        # t3 must start after t2 ends
        assert task_times["t3"].start_time >= task_times["t2"].end_time

    def test_sequential_chain_makespan(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        # t1(3) -> t2(5) -> t3(2) = 10 days total
        schedule = optimizer.optimize_schedule(simple_plan)
        assert schedule.makespan == 10.0

    def test_parallel_tasks_reduce_makespan(
        self, optimizer: ConstraintOptimizer
    ) -> None:
        plan = {
            "id": "parallel",
            "tasks": [
                {"id": "t1", "title": "A", "duration": 5, "dependencies": []},
                {"id": "t2", "title": "B", "duration": 5, "dependencies": []},
            ],
            "max_parallel": 2,
        }
        schedule = optimizer.optimize_schedule(plan)
        # Both can run in parallel
        assert schedule.makespan == 5.0

    def test_resource_constrained_schedule(
        self, optimizer: ConstraintOptimizer
    ) -> None:
        plan = {
            "id": "constrained",
            "tasks": [
                {"id": "t1", "title": "A", "duration": 3, "dependencies": []},
                {"id": "t2", "title": "B", "duration": 3, "dependencies": []},
                {"id": "t3", "title": "C", "duration": 3, "dependencies": []},
            ],
            "max_parallel": 1,  # Only 1 at a time
        }
        schedule = optimizer.optimize_schedule(plan)
        assert schedule.makespan == 9.0  # 3 + 3 + 3

    def test_empty_task_list(self, optimizer: ConstraintOptimizer) -> None:
        schedule = optimizer.optimize_schedule({"id": "empty", "tasks": []})
        assert schedule.makespan == 0.0
        assert len(schedule.tasks) == 0

    def test_schedule_is_feasible(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        schedule = optimizer.optimize_schedule(simple_plan)
        assert schedule.is_feasible is True

    def test_schedule_to_dict(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        schedule = optimizer.optimize_schedule(simple_plan)
        d = schedule.to_dict()
        assert d["plan_id"] == "plan-1"
        assert len(d["tasks"]) == 3
        assert "makespan" in d


# ---------------------------------------------------------------------------
# Resource assignment optimization
# ---------------------------------------------------------------------------

class TestOptimizeAssignments:
    def test_assigns_all_tasks(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        resources = complex_plan["resources"]
        plan = optimizer.optimize_assignments(complex_plan, resources)
        assert isinstance(plan, AssignmentPlan)
        assert len(plan.assignments) == 5

    def test_balances_workload(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        resources = complex_plan["resources"]
        plan = optimizer.optimize_assignments(complex_plan, resources)
        assert 0.0 < plan.workload_balance <= 1.0

    def test_skill_matching(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        resources = complex_plan["resources"]
        plan = optimizer.optimize_assignments(complex_plan, resources)
        assert plan.avg_skill_match > 0.0

    def test_empty_resources(self, optimizer: ConstraintOptimizer) -> None:
        plan = optimizer.optimize_assignments({"tasks": [{"id": "t1"}]}, [])
        assert len(plan.assignments) == 0

    def test_empty_tasks(self, optimizer: ConstraintOptimizer) -> None:
        plan = optimizer.optimize_assignments(
            {"tasks": []},
            [{"id": "r1", "skills": ["python"]}],
        )
        assert len(plan.assignments) == 0

    def test_assignment_plan_to_dict(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        resources = complex_plan["resources"]
        plan = optimizer.optimize_assignments(complex_plan, resources)
        d = plan.to_dict()
        assert "assignments" in d
        assert "workload_balance" in d
        assert "avg_skill_match" in d
        assert "context_switches" in d


# ---------------------------------------------------------------------------
# Constraint validation
# ---------------------------------------------------------------------------

class TestValidateConstraints:
    def test_valid_schedule(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        schedule = optimizer.optimize_schedule(simple_plan)
        constraints = optimizer.define_constraints(simple_plan)
        result = optimizer.validate_constraints(schedule, constraints)
        assert isinstance(result, ValidationResult)
        assert result.is_valid is True

    def test_infeasible_schedule_detected(
        self, optimizer: ConstraintOptimizer
    ) -> None:
        plan = {
            "id": "tight",
            "tasks": [
                {"id": "t1", "title": "A", "duration": 10, "dependencies": []},
                {"id": "t2", "title": "B", "duration": 10, "dependencies": ["t1"]},
            ],
            "max_parallel": 1,
            "deadline_days": 5,
        }
        schedule = optimizer.optimize_schedule(plan)
        constraints = optimizer.define_constraints(plan)
        result = optimizer.validate_constraints(schedule, constraints)
        assert len(result.violations) > 0

    def test_soft_score_calculated(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        schedule = optimizer.optimize_schedule(simple_plan)
        constraints = optimizer.define_constraints(simple_plan)
        result = optimizer.validate_constraints(schedule, constraints)
        assert 0.0 <= result.soft_score <= 1.0

    def test_validation_to_dict(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        schedule = optimizer.optimize_schedule(simple_plan)
        constraints = optimizer.define_constraints(simple_plan)
        result = optimizer.validate_constraints(schedule, constraints)
        d = result.to_dict()
        assert "is_valid" in d
        assert "satisfied_hard" in d
        assert "soft_score" in d


# ---------------------------------------------------------------------------
# Multi-objective optimization (Pareto frontier)
# ---------------------------------------------------------------------------

class TestParetoOptimal:
    def test_finds_pareto_solutions(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        solutions = optimizer.find_pareto_optimal(
            complex_plan,
            [ObjectiveType.MINIMIZE_MAKESPAN, ObjectiveType.MINIMIZE_COST],
        )
        assert len(solutions) > 0
        for sol in solutions:
            assert isinstance(sol, Solution)
            assert sol.is_pareto_optimal is True

    def test_pareto_with_multiple_objectives(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        solutions = optimizer.find_pareto_optimal(
            complex_plan,
            [
                ObjectiveType.MINIMIZE_MAKESPAN,
                ObjectiveType.MINIMIZE_COST,
                ObjectiveType.BALANCE_LOAD,
            ],
        )
        assert len(solutions) >= 1

    def test_pareto_solutions_have_objective_values(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        solutions = optimizer.find_pareto_optimal(
            complex_plan,
            [ObjectiveType.MINIMIZE_MAKESPAN, ObjectiveType.MINIMIZE_COST],
        )
        for sol in solutions:
            assert "minimize_makespan" in sol.objective_values
            assert "minimize_cost" in sol.objective_values

    def test_pareto_empty_tasks(self, optimizer: ConstraintOptimizer) -> None:
        solutions = optimizer.find_pareto_optimal(
            {"tasks": []},
            [ObjectiveType.MINIMIZE_MAKESPAN],
        )
        assert solutions == []

    def test_pareto_empty_objectives(
        self, optimizer: ConstraintOptimizer, simple_plan: dict
    ) -> None:
        solutions = optimizer.find_pareto_optimal(simple_plan, [])
        assert solutions == []

    def test_solution_to_dict(
        self, optimizer: ConstraintOptimizer, complex_plan: dict
    ) -> None:
        solutions = optimizer.find_pareto_optimal(
            complex_plan,
            [ObjectiveType.MINIMIZE_MAKESPAN],
        )
        assert solutions
        d = solutions[0].to_dict()
        assert "id" in d
        assert "schedule" in d
        assert "objective_values" in d
        assert "is_pareto_optimal" in d


# ---------------------------------------------------------------------------
# Data class serialization
# ---------------------------------------------------------------------------

class TestSerialization:
    def test_constraint_to_dict(self) -> None:
        c = Constraint(
            id="c1", kind=ConstraintKind.HARD, name="dep",
            description="test", weight=1.5,
        )
        d = c.to_dict()
        assert d["kind"] == "hard"
        assert d["weight"] == 1.5

    def test_task_schedule_to_dict(self) -> None:
        ts = TaskSchedule(
            task_id="t1", title="Design", start_time=0.0,
            end_time=3.0, duration=3.0, assigned_to="dev-1",
        )
        d = ts.to_dict()
        assert d["task_id"] == "t1"
        assert d["duration"] == 3.0

    def test_resource_assignment_to_dict(self) -> None:
        ra = ResourceAssignment(
            resource_id="dev-1", task_id="t1",
            skill_match=0.8, load_contribution=5.0,
        )
        d = ra.to_dict()
        assert d["skill_match"] == 0.8


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_single_task_plan(self, optimizer: ConstraintOptimizer) -> None:
        plan = {
            "id": "single",
            "tasks": [{"id": "t1", "title": "Only task", "duration": 5}],
        }
        schedule = optimizer.optimize_schedule(plan)
        assert schedule.makespan == 5.0
        assert len(schedule.tasks) == 1

    def test_circular_dependency_handled(
        self, optimizer: ConstraintOptimizer
    ) -> None:
        """Circular dependencies should not cause infinite loops."""
        plan = {
            "id": "circular",
            "tasks": [
                {"id": "t1", "title": "A", "duration": 2, "dependencies": ["t2"]},
                {"id": "t2", "title": "B", "duration": 2, "dependencies": ["t1"]},
            ],
        }
        # Should not hang; topological sort will produce partial result
        schedule = optimizer.optimize_schedule(plan)
        assert isinstance(schedule, OptimizedSchedule)

    def test_zero_duration_tasks(self, optimizer: ConstraintOptimizer) -> None:
        plan = {
            "id": "zero",
            "tasks": [
                {"id": "t1", "title": "Milestone", "duration": 0},
                {"id": "t2", "title": "Work", "duration": 5, "dependencies": ["t1"]},
            ],
        }
        schedule = optimizer.optimize_schedule(plan)
        assert schedule.makespan == 5.0

    def test_many_independent_tasks(
        self, optimizer: ConstraintOptimizer
    ) -> None:
        tasks = [
            {"id": f"t{i}", "title": f"Task {i}", "duration": 1, "dependencies": []}
            for i in range(10)
        ]
        plan = {"id": "many", "tasks": tasks, "max_parallel": 5}
        schedule = optimizer.optimize_schedule(plan)
        # 10 tasks, 5 parallel = 2 rounds of 1 day = 2 days
        assert schedule.makespan == 2.0
