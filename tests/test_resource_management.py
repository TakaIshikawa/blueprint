"""Test resource capacity planning and allocation."""

from datetime import datetime, timedelta

import pytest

from blueprint.analytics.resource_management import (
    Allocation,
    AllocationPlan,
    Capacity,
    OverallocationWarning,
    ResourceManager,
    SkillLevel,
    Workload,
)


@pytest.fixture
def manager():
    """Create fresh resource manager."""
    return ResourceManager()


@pytest.fixture
def base_date():
    """Fixed base date for testing."""
    return datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def populated_manager(manager, base_date):
    """Manager with sample capacity and allocations."""
    manager.set_capacity(
        user_id="user-1",
        hours_per_week=40.0,
        skills={"python": "expert", "react": "advanced"},
        timezone="America/New_York",
    )

    manager.set_capacity(
        user_id="user-2",
        hours_per_week=40.0,
        skills={"python": "intermediate", "docker": "advanced"},
        timezone="America/Los_Angeles",
    )

    manager.set_capacity(
        user_id="user-3",
        hours_per_week=32.0,
        skills={"react": "expert", "css": "advanced"},
        timezone="UTC",
    )

    return manager


def test_set_capacity(manager):
    """Test setting user capacity."""
    capacity = manager.set_capacity(
        user_id="user-1",
        hours_per_week=40.0,
        skills={"python": "expert", "javascript": "advanced"},
        timezone="UTC",
    )

    assert isinstance(capacity, Capacity)
    assert capacity.user_id == "user-1"
    assert capacity.hours_per_week == 40.0
    assert capacity.timezone == "UTC"
    assert capacity.skills["python"] == SkillLevel.EXPERT
    assert capacity.skills["javascript"] == SkillLevel.ADVANCED


def test_set_capacity_with_pto(manager, base_date):
    """Test setting capacity with PTO dates."""
    pto_dates = [
        {"start": base_date.isoformat(), "end": (base_date + timedelta(days=5)).isoformat()}
    ]

    capacity = manager.set_capacity(
        user_id="user-1",
        hours_per_week=40.0,
        availability={"pto_dates": pto_dates},
    )

    assert len(capacity.pto_dates) == 1
    assert capacity.pto_dates[0][0] == base_date


def test_allocate_task(manager):
    """Test allocating task to user."""
    allocation = manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=100.0,
        estimated_hours=16.0,
        required_skills=["python"],
    )

    assert isinstance(allocation, Allocation)
    assert allocation.task_id == "task-1"
    assert allocation.user_id == "user-1"
    assert allocation.allocation_pct == 100.0
    assert allocation.estimated_hours == 16.0


def test_allocate_task_partial(manager):
    """Test partial task allocation."""
    allocation = manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=50.0,
        estimated_hours=20.0,
    )

    assert allocation.allocation_pct == 50.0


def test_allocate_task_invalid_percentage(manager):
    """Test allocation with invalid percentage."""
    with pytest.raises(ValueError, match="between 0 and 100"):
        manager.allocate_task(
            task_id="task-1",
            user_id="user-1",
            allocation_pct=150.0,
            estimated_hours=10.0,
        )


def test_calculate_workload(populated_manager, base_date):
    """Test calculating user workload."""
    start = base_date
    end = base_date + timedelta(days=14)

    populated_manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=100.0,
        estimated_hours=30.0,
        start_date=start,
        end_date=end,
    )

    workload = populated_manager.calculate_workload("user-1", (start, end))

    assert isinstance(workload, Workload)
    assert workload.user_id == "user-1"
    assert workload.total_allocated_hours == 30.0
    assert workload.available_capacity == 80.0
    assert workload.utilization_pct == pytest.approx(37.5, rel=0.01)
    assert not workload.overallocated


def test_calculate_workload_overallocated(populated_manager, base_date):
    """Test detecting overallocation."""
    start = base_date
    end = base_date + timedelta(days=14)

    populated_manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=100.0,
        estimated_hours=100.0,
        start_date=start,
        end_date=end,
    )

    workload = populated_manager.calculate_workload("user-1", (start, end))

    assert workload.overallocated is True
    assert workload.total_allocated_hours > workload.available_capacity


def test_calculate_workload_partial_allocation(populated_manager, base_date):
    """Test workload with partial allocation."""
    start = base_date
    end = base_date + timedelta(days=14)

    populated_manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=50.0,
        estimated_hours=40.0,
        start_date=start,
        end_date=end,
    )

    workload = populated_manager.calculate_workload("user-1", (start, end))

    assert workload.total_allocated_hours == 20.0


def test_calculate_workload_no_capacity(manager, base_date):
    """Test workload calculation for user without capacity."""
    start = base_date
    end = base_date + timedelta(days=14)

    workload = manager.calculate_workload("unknown-user", (start, end))

    assert workload.total_allocated_hours == 0.0
    assert workload.available_capacity == 0.0


def test_detect_overallocation(populated_manager, base_date):
    """Test detecting overallocated users."""
    start = base_date
    end = base_date + timedelta(days=14)

    populated_manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=100.0,
        estimated_hours=100.0,
        start_date=start,
        end_date=end,
    )

    warnings = populated_manager.detect_overallocation((start, end))

    assert len(warnings) > 0
    assert isinstance(warnings[0], OverallocationWarning)
    assert warnings[0].user_id == "user-1"
    assert warnings[0].overage_hours > 0


def test_detect_overallocation_none(populated_manager, base_date):
    """Test no overallocation warnings."""
    start = base_date
    end = base_date + timedelta(days=14)

    populated_manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=100.0,
        estimated_hours=20.0,
        start_date=start,
        end_date=end,
    )

    warnings = populated_manager.detect_overallocation((start, end))

    assert len(warnings) == 0


def test_optimize_allocation(populated_manager, base_date):
    """Test optimizing task allocation."""
    start = base_date
    end = base_date + timedelta(days=14)

    tasks = [
        {"id": "task-1", "estimated_hours": 20.0, "required_skills": ["python"]},
        {"id": "task-2", "estimated_hours": 15.0, "required_skills": ["react"]},
        {"id": "task-3", "estimated_hours": 10.0, "required_skills": ["python"]},
    ]

    plan = populated_manager.optimize_allocation(
        plan_id="plan-1",
        tasks=tasks,
        time_range=(start, end),
    )

    assert isinstance(plan, AllocationPlan)
    assert plan.plan_id == "plan-1"
    assert len(plan.allocations) > 0
    assert plan.total_hours > 0


def test_optimize_allocation_skill_matching(populated_manager, base_date):
    """Test allocation optimizer matches skills."""
    start = base_date
    end = base_date + timedelta(days=14)

    tasks = [
        {"id": "task-1", "estimated_hours": 20.0, "required_skills": ["python", "docker"]},
    ]

    plan = populated_manager.optimize_allocation(
        plan_id="plan-1",
        tasks=tasks,
        time_range=(start, end),
    )

    if plan.allocations:
        assert plan.allocations[0].user_id == "user-2"


def test_optimize_allocation_unassigned_tasks(populated_manager, base_date):
    """Test tasks without matching skills remain unassigned."""
    start = base_date
    end = base_date + timedelta(days=14)

    tasks = [
        {"id": "task-1", "estimated_hours": 20.0, "required_skills": ["rust", "golang"]},
    ]

    plan = populated_manager.optimize_allocation(
        plan_id="plan-1",
        tasks=tasks,
        time_range=(start, end),
    )

    assert "task-1" in plan.unassigned_tasks


def test_generate_capacity_report(populated_manager, base_date):
    """Test generating capacity report."""
    start = base_date
    end = base_date + timedelta(days=14)

    populated_manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=100.0,
        estimated_hours=30.0,
        start_date=start,
        end_date=end,
    )

    report = populated_manager.generate_capacity_report((start, end))

    assert "team_size" in report
    assert "total_capacity" in report
    assert "total_allocated" in report
    assert "overall_utilization" in report
    assert "user_workloads" in report
    assert report["team_size"] == 3


def test_get_skill_gap_analysis(populated_manager):
    """Test skill gap analysis."""
    required_skills = ["python", "react", "rust"]

    analysis = populated_manager.get_skill_gap_analysis(required_skills)

    assert "required_skills" in analysis
    assert "skill_coverage" in analysis
    assert "skill_gaps" in analysis
    assert "coverage_percentage" in analysis
    assert "rust" in analysis["skill_gaps"]


def test_skill_gap_no_gaps(populated_manager):
    """Test skill gap analysis with full coverage."""
    required_skills = ["python", "react"]

    analysis = populated_manager.get_skill_gap_analysis(required_skills)

    assert len([g for g in analysis["skill_gaps"] if "needs advanced" not in g]) == 0
    assert analysis["coverage_percentage"] == 100.0


def test_capacity_to_dict(manager):
    """Test serializing capacity to dict."""
    capacity = manager.set_capacity(
        user_id="user-1",
        hours_per_week=40.0,
        skills={"python": "expert"},
    )

    data = capacity.to_dict()

    assert "user_id" in data
    assert "hours_per_week" in data
    assert "timezone" in data
    assert "skills" in data
    assert data["skills"]["python"] == "expert"


def test_allocation_to_dict(manager, base_date):
    """Test serializing allocation to dict."""
    allocation = manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=75.0,
        estimated_hours=20.0,
        start_date=base_date,
        end_date=base_date + timedelta(days=10),
        required_skills=["python", "react"],
    )

    data = allocation.to_dict()

    assert "task_id" in data
    assert "user_id" in data
    assert "allocation_pct" in data
    assert "estimated_hours" in data
    assert "start_date" in data
    assert data["allocation_pct"] == 75.0


def test_workload_to_dict(populated_manager, base_date):
    """Test serializing workload to dict."""
    start = base_date
    end = base_date + timedelta(days=14)

    workload = populated_manager.calculate_workload("user-1", (start, end))

    data = workload.to_dict()

    assert "user_id" in data
    assert "time_range" in data
    assert "total_allocated_hours" in data
    assert "available_capacity" in data
    assert "utilization_pct" in data


def test_overallocation_warning_to_dict(populated_manager, base_date):
    """Test serializing overallocation warning to dict."""
    start = base_date
    end = base_date + timedelta(days=14)

    populated_manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=100.0,
        estimated_hours=100.0,
        start_date=start,
        end_date=end,
    )

    warnings = populated_manager.detect_overallocation((start, end))

    if warnings:
        data = warnings[0].to_dict()

        assert "user_id" in data
        assert "time_range" in data
        assert "allocated_hours" in data
        assert "available_hours" in data
        assert "overage_hours" in data


def test_allocation_plan_to_dict(populated_manager, base_date):
    """Test serializing allocation plan to dict."""
    start = base_date
    end = base_date + timedelta(days=14)

    tasks = [
        {"id": "task-1", "estimated_hours": 20.0, "required_skills": ["python"]},
    ]

    plan = populated_manager.optimize_allocation("plan-1", tasks, (start, end))

    data = plan.to_dict()

    assert "plan_id" in data
    assert "allocations" in data
    assert "total_hours" in data
    assert "average_utilization" in data


def test_workload_with_pto(manager, base_date):
    """Test workload calculation accounts for PTO."""
    start = base_date
    end = base_date + timedelta(days=14)

    pto_dates = [
        {"start": (base_date + timedelta(days=5)).isoformat(), "end": (base_date + timedelta(days=9)).isoformat()}
    ]

    manager.set_capacity(
        user_id="user-1",
        hours_per_week=40.0,
        availability={"pto_dates": pto_dates},
    )

    workload = manager.calculate_workload("user-1", (start, end))

    assert workload.available_capacity < 80.0


def test_multiple_allocations_same_task(manager, base_date):
    """Test multiple users allocated to same task."""
    start = base_date
    end = base_date + timedelta(days=14)

    manager.set_capacity("user-1", hours_per_week=40.0)
    manager.set_capacity("user-2", hours_per_week=40.0)

    manager.allocate_task("task-1", "user-1", 50.0, 20.0, start, end)
    manager.allocate_task("task-1", "user-2", 50.0, 20.0, start, end)

    assert len(manager._allocations["task-1"]) == 2


def test_capacity_report_utilization(populated_manager, base_date):
    """Test capacity report shows overall utilization."""
    start = base_date
    end = base_date + timedelta(days=14)

    populated_manager.allocate_task(
        task_id="task-1",
        user_id="user-1",
        allocation_pct=100.0,
        estimated_hours=40.0,
        start_date=start,
        end_date=end,
    )

    report = populated_manager.generate_capacity_report((start, end))

    assert report["overall_utilization"] > 0
    assert report["total_allocated"] == 40.0
