"""Resource capacity planning and allocation across team members.

Tracks team capacity, availability, workload, and skill matrices. Supports
partial task allocations, overallocation detection, and constraint-based
allocation optimization for balanced workload distribution.

Methodology
-----------
* **Capacity tracking** - weekly hours, PTO, skill matrix, timezone
* **Partial allocation** - assign users at percentage levels to tasks
* **Workload metrics** - allocated hours, available capacity, utilization
* **Conflict detection** - scheduling conflicts and overcommitments
* **Allocation optimizer** - constraint satisfaction for balanced distribution
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any


class SkillLevel(str, Enum):
    """Proficiency levels for skills."""

    BEGINNER = "beginner"
    INTERMEDIATE = "intermediate"
    ADVANCED = "advanced"
    EXPERT = "expert"


@dataclass(frozen=True, slots=True)
class Capacity:
    """User capacity configuration."""

    user_id: str
    hours_per_week: float
    timezone: str
    skills: dict[str, SkillLevel] = field(default_factory=dict)
    pto_dates: tuple[tuple[datetime, datetime], ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "hours_per_week": self.hours_per_week,
            "timezone": self.timezone,
            "skills": {k: v.value for k, v in self.skills.items()},
            "pto_dates": [
                {"start": start.isoformat(), "end": end.isoformat()}
                for start, end in self.pto_dates
            ],
        }


@dataclass(frozen=True, slots=True)
class Allocation:
    """Task allocation to a user."""

    task_id: str
    user_id: str
    allocation_pct: float
    estimated_hours: float
    start_date: datetime | None = None
    end_date: datetime | None = None
    required_skills: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "user_id": self.user_id,
            "allocation_pct": self.allocation_pct,
            "estimated_hours": self.estimated_hours,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "required_skills": list(self.required_skills),
        }


@dataclass(frozen=True, slots=True)
class Workload:
    """Workload metrics for a user."""

    user_id: str
    time_range: tuple[datetime, datetime]
    total_allocated_hours: float
    available_capacity: float
    utilization_pct: float
    overallocated: bool
    allocations: tuple[Allocation, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        start, end = self.time_range
        return {
            "user_id": self.user_id,
            "time_range": {
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
            "total_allocated_hours": self.total_allocated_hours,
            "available_capacity": self.available_capacity,
            "utilization_pct": self.utilization_pct,
            "overallocated": self.overallocated,
            "allocations": [a.to_dict() for a in self.allocations],
        }


@dataclass(frozen=True, slots=True)
class OverallocationWarning:
    """Warning about user overallocation."""

    user_id: str
    time_range: tuple[datetime, datetime]
    allocated_hours: float
    available_hours: float
    overage_hours: float
    overage_pct: float
    conflicting_tasks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        start, end = self.time_range
        return {
            "user_id": self.user_id,
            "time_range": {
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
            "allocated_hours": self.allocated_hours,
            "available_hours": self.available_hours,
            "overage_hours": self.overage_hours,
            "overage_pct": self.overage_pct,
            "conflicting_tasks": list(self.conflicting_tasks),
        }


@dataclass(frozen=True, slots=True)
class AllocationPlan:
    """Optimized allocation plan for a set of tasks."""

    plan_id: str
    allocations: tuple[Allocation, ...]
    total_hours: float
    average_utilization: float
    unassigned_tasks: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[OverallocationWarning, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "allocations": [a.to_dict() for a in self.allocations],
            "total_hours": self.total_hours,
            "average_utilization": self.average_utilization,
            "unassigned_tasks": list(self.unassigned_tasks),
            "warnings": [w.to_dict() for w in self.warnings],
        }


class ResourceManager:
    """Manage team capacity and task allocation."""

    def __init__(self) -> None:
        self._capacities: dict[str, Capacity] = {}
        self._allocations: dict[str, list[Allocation]] = {}

    def set_capacity(
        self,
        user_id: str,
        hours_per_week: float,
        availability: dict[str, Any] | None = None,
        skills: dict[str, str] | None = None,
        timezone: str = "UTC",
    ) -> Capacity:
        """Set user capacity configuration.

        Args:
            user_id: User identifier
            hours_per_week: Available hours per week
            availability: PTO dates and other availability info
            skills: Dictionary of skill name to proficiency level
            timezone: User timezone

        Returns:
            Capacity object
        """
        skill_map = {}
        if skills:
            for skill_name, level_str in skills.items():
                skill_map[skill_name] = SkillLevel(level_str.lower())

        pto_dates = []
        if availability and "pto_dates" in availability:
            for pto in availability["pto_dates"]:
                start = datetime.fromisoformat(pto["start"]) if isinstance(pto["start"], str) else pto["start"]
                end = datetime.fromisoformat(pto["end"]) if isinstance(pto["end"], str) else pto["end"]
                pto_dates.append((start, end))

        capacity = Capacity(
            user_id=user_id,
            hours_per_week=hours_per_week,
            timezone=timezone,
            skills=skill_map,
            pto_dates=tuple(pto_dates),
        )

        self._capacities[user_id] = capacity
        return capacity

    def allocate_task(
        self,
        task_id: str,
        user_id: str,
        allocation_pct: float,
        estimated_hours: float = 0.0,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        required_skills: list[str] | None = None,
    ) -> Allocation:
        """Allocate a task to a user.

        Args:
            task_id: Task identifier
            user_id: User identifier
            allocation_pct: Percentage allocation (0-100)
            estimated_hours: Estimated hours for task
            start_date: Task start date
            end_date: Task end date
            required_skills: Skills required for task

        Returns:
            Allocation object
        """
        if allocation_pct < 0 or allocation_pct > 100:
            raise ValueError("Allocation percentage must be between 0 and 100")

        allocation = Allocation(
            task_id=task_id,
            user_id=user_id,
            allocation_pct=allocation_pct,
            estimated_hours=estimated_hours,
            start_date=start_date,
            end_date=end_date,
            required_skills=tuple(required_skills or []),
        )

        if task_id not in self._allocations:
            self._allocations[task_id] = []
        self._allocations[task_id].append(allocation)

        return allocation

    def calculate_workload(
        self,
        user_id: str,
        time_range: tuple[datetime, datetime],
    ) -> Workload:
        """Calculate workload for a user in a time range.

        Args:
            user_id: User identifier
            time_range: (start, end) datetime tuple

        Returns:
            Workload metrics
        """
        start_date, end_date = time_range
        capacity = self._capacities.get(user_id)

        if not capacity:
            return Workload(
                user_id=user_id,
                time_range=time_range,
                total_allocated_hours=0.0,
                available_capacity=0.0,
                utilization_pct=0.0,
                overallocated=False,
            )

        user_allocations = []
        for allocations in self._allocations.values():
            for alloc in allocations:
                if alloc.user_id == user_id:
                    if self._overlaps_range(alloc, start_date, end_date):
                        user_allocations.append(alloc)

        total_allocated = sum(
            alloc.estimated_hours * (alloc.allocation_pct / 100)
            for alloc in user_allocations
        )

        weeks = (end_date - start_date).days / 7.0
        available_capacity = capacity.hours_per_week * weeks

        available_capacity = self._adjust_for_pto(
            capacity,
            start_date,
            end_date,
            available_capacity,
        )

        utilization = (total_allocated / available_capacity * 100) if available_capacity > 0 else 0.0
        overallocated = total_allocated > available_capacity

        return Workload(
            user_id=user_id,
            time_range=time_range,
            total_allocated_hours=total_allocated,
            available_capacity=available_capacity,
            utilization_pct=utilization,
            overallocated=overallocated,
            allocations=tuple(user_allocations),
        )

    def detect_overallocation(self, time_range: tuple[datetime, datetime] | None = None) -> list[OverallocationWarning]:
        """Detect overallocated users.

        Args:
            time_range: Optional time range to check

        Returns:
            List of overallocation warnings
        """
        if time_range is None:
            now = datetime.now()
            time_range = (now, now + timedelta(days=30))

        warnings = []
        for user_id in self._capacities:
            workload = self.calculate_workload(user_id, time_range)

            if workload.overallocated:
                overage = workload.total_allocated_hours - workload.available_capacity
                overage_pct = (overage / workload.available_capacity * 100) if workload.available_capacity > 0 else 0.0

                task_ids = tuple(a.task_id for a in workload.allocations)

                warning = OverallocationWarning(
                    user_id=user_id,
                    time_range=time_range,
                    allocated_hours=workload.total_allocated_hours,
                    available_hours=workload.available_capacity,
                    overage_hours=overage,
                    overage_pct=overage_pct,
                    conflicting_tasks=task_ids,
                )
                warnings.append(warning)

        return warnings

    def optimize_allocation(
        self,
        plan_id: str,
        tasks: list[dict[str, Any]],
        time_range: tuple[datetime, datetime],
    ) -> AllocationPlan:
        """Optimize task allocation across team members.

        Args:
            plan_id: Plan identifier
            tasks: List of task dictionaries with id, estimated_hours, required_skills
            time_range: Time range for allocation

        Returns:
            AllocationPlan with optimized assignments
        """
        allocations = []
        unassigned = []

        sorted_tasks = sorted(tasks, key=lambda t: t.get("estimated_hours", 0), reverse=True)

        for task in sorted_tasks:
            task_id = task["id"]
            estimated_hours = task.get("estimated_hours", 0.0)
            required_skills = task.get("required_skills", [])

            best_user = self._find_best_user(
                estimated_hours,
                required_skills,
                time_range,
            )

            if best_user:
                allocation = self.allocate_task(
                    task_id=task_id,
                    user_id=best_user,
                    allocation_pct=100.0,
                    estimated_hours=estimated_hours,
                    start_date=time_range[0],
                    end_date=time_range[1],
                    required_skills=required_skills,
                )
                allocations.append(allocation)
            else:
                unassigned.append(task_id)

        total_hours = sum(a.estimated_hours for a in allocations)

        user_workloads = [
            self.calculate_workload(user_id, time_range)
            for user_id in self._capacities
        ]

        avg_utilization = (
            sum(w.utilization_pct for w in user_workloads) / len(user_workloads)
            if user_workloads else 0.0
        )

        warnings = tuple(self.detect_overallocation(time_range))

        return AllocationPlan(
            plan_id=plan_id,
            allocations=tuple(allocations),
            total_hours=total_hours,
            average_utilization=avg_utilization,
            unassigned_tasks=tuple(unassigned),
            warnings=warnings,
        )

    def generate_capacity_report(
        self,
        time_range: tuple[datetime, datetime],
    ) -> dict[str, Any]:
        """Generate capacity report for all users.

        Args:
            time_range: Time range for report

        Returns:
            Dictionary with capacity metrics
        """
        user_workloads = []
        for user_id in self._capacities:
            workload = self.calculate_workload(user_id, time_range)
            user_workloads.append(workload.to_dict())

        total_capacity = sum(
            self._capacities[uid].hours_per_week * ((time_range[1] - time_range[0]).days / 7.0)
            for uid in self._capacities
        )

        total_allocated = sum(w["total_allocated_hours"] for w in user_workloads)

        return {
            "time_range": {
                "start": time_range[0].isoformat(),
                "end": time_range[1].isoformat(),
            },
            "team_size": len(self._capacities),
            "total_capacity": total_capacity,
            "total_allocated": total_allocated,
            "overall_utilization": (total_allocated / total_capacity * 100) if total_capacity > 0 else 0.0,
            "user_workloads": user_workloads,
        }

    def get_skill_gap_analysis(
        self,
        required_skills: list[str],
    ) -> dict[str, Any]:
        """Analyze skill gaps in the team.

        Args:
            required_skills: List of required skills

        Returns:
            Dictionary with skill coverage analysis
        """
        skill_coverage: dict[str, list[dict[str, Any]]] = {skill: [] for skill in required_skills}

        for user_id, capacity in self._capacities.items():
            for skill in required_skills:
                if skill in capacity.skills:
                    skill_coverage[skill].append({
                        "user_id": user_id,
                        "level": capacity.skills[skill].value,
                    })

        gaps = []
        for skill, users in skill_coverage.items():
            if not users:
                gaps.append(skill)
            elif all(u["level"] == "beginner" for u in users):
                gaps.append(f"{skill} (needs advanced)")

        return {
            "required_skills": required_skills,
            "skill_coverage": skill_coverage,
            "skill_gaps": gaps,
            "coverage_percentage": (
                (len([s for s in required_skills if skill_coverage[s]]) / len(required_skills) * 100)
                if required_skills else 100.0
            ),
        }

    def _overlaps_range(
        self,
        allocation: Allocation,
        start: datetime,
        end: datetime,
    ) -> bool:
        """Check if allocation overlaps with time range."""
        if allocation.start_date is None or allocation.end_date is None:
            return True

        return not (allocation.end_date < start or allocation.start_date > end)

    def _adjust_for_pto(
        self,
        capacity: Capacity,
        start: datetime,
        end: datetime,
        available_hours: float,
    ) -> float:
        """Adjust available capacity for PTO."""
        total_pto_days = 0

        for pto_start, pto_end in capacity.pto_dates:
            if not (pto_end < start or pto_start > end):
                overlap_start = max(pto_start, start)
                overlap_end = min(pto_end, end)
                pto_days = (overlap_end - overlap_start).days + 1
                total_pto_days += pto_days

        pto_hours = (total_pto_days / 7.0) * capacity.hours_per_week

        return max(0.0, available_hours - pto_hours)

    def _find_best_user(
        self,
        estimated_hours: float,
        required_skills: list[str],
        time_range: tuple[datetime, datetime],
    ) -> str | None:
        """Find best user for a task based on skills and capacity."""
        candidates = []

        for user_id, capacity in self._capacities.items():
            has_skills = all(skill in capacity.skills for skill in required_skills)

            if not has_skills:
                continue

            workload = self.calculate_workload(user_id, time_range)

            if workload.available_capacity - workload.total_allocated_hours >= estimated_hours:
                skill_score = sum(
                    self._skill_level_score(capacity.skills.get(skill, SkillLevel.BEGINNER))
                    for skill in required_skills
                ) / len(required_skills) if required_skills else 1.0

                candidates.append({
                    "user_id": user_id,
                    "utilization": workload.utilization_pct,
                    "skill_score": skill_score,
                })

        if not candidates:
            return None

        candidates.sort(key=lambda c: (c["utilization"], -c["skill_score"]))

        return candidates[0]["user_id"]

    def _skill_level_score(self, level: SkillLevel) -> float:
        """Convert skill level to numeric score."""
        scores = {
            SkillLevel.BEGINNER: 1.0,
            SkillLevel.INTERMEDIATE: 2.0,
            SkillLevel.ADVANCED: 3.0,
            SkillLevel.EXPERT: 4.0,
        }
        return scores.get(level, 0.0)
