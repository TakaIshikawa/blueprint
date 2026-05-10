"""Tests for portfolio manager covering metric aggregation and all features."""

import pytest

from blueprint.portfolio.portfolio_manager import PlanData, PortfolioManager
from blueprint.portfolio.portfolio_model import (
    HealthRating,
    PortfolioStatus,
    RiskLevel,
)


@pytest.fixture
def manager() -> PortfolioManager:
    return PortfolioManager()


@pytest.fixture
def portfolio_with_plans(manager: PortfolioManager):
    """Create a portfolio with two registered plans."""
    pf = manager.create_portfolio("Test Portfolio", owner="alice")
    manager.add_plan(pf.portfolio_id, "plan-1", "Backend API")
    manager.add_plan(pf.portfolio_id, "plan-2", "Frontend UI")
    manager.register_plan_data(
        PlanData(
            plan_id="plan-1",
            title="Backend API",
            status="on_track",
            total_tasks=10,
            completed_tasks=7,
            in_progress_tasks=2,
            budget=50000.0,
            spent=30000.0,
            start_date="2025-01-01",
            end_date="2025-06-30",
            milestones=[
                {"milestone_id": "ms-1", "title": "Alpha", "target_date": "2025-03-01"},
                {"milestone_id": "ms-2", "title": "Beta", "target_date": "2025-05-01"},
            ],
            risks=[{"risk_id": "r-1", "title": "Scope creep", "level": "medium"}],
            resources=[
                {
                    "resource_id": "dev-1",
                    "resource_name": "Senior Dev",
                    "resource_type": "person",
                    "total_capacity": 160.0,
                    "allocated": 120.0,
                }
            ],
            health_score=0.8,
        )
    )
    manager.register_plan_data(
        PlanData(
            plan_id="plan-2",
            title="Frontend UI",
            status="at_risk",
            total_tasks=8,
            completed_tasks=2,
            in_progress_tasks=3,
            budget=30000.0,
            spent=25000.0,
            start_date="2025-02-01",
            end_date="2025-07-31",
            milestones=[
                {"milestone_id": "ms-3", "title": "Design Complete", "target_date": "2025-02-15"},
            ],
            risks=[
                {"risk_id": "r-2", "title": "Late delivery", "level": "high"},
                {"risk_id": "r-3", "title": "Resource gap", "level": "critical"},
            ],
            resources=[
                {
                    "resource_id": "dev-1",
                    "resource_name": "Senior Dev",
                    "resource_type": "person",
                    "total_capacity": 160.0,
                    "allocated": 40.0,
                }
            ],
            health_score=0.3,
        )
    )
    return pf


# ------------------------------------------------------------------
# Portfolio CRUD
# ------------------------------------------------------------------


class TestPortfolioCRUD:
    def test_create_portfolio(self, manager: PortfolioManager):
        pf = manager.create_portfolio("My Portfolio", description="Test", owner="bob")
        assert pf.name == "My Portfolio"
        assert pf.owner == "bob"
        assert pf.status == PortfolioStatus.PLANNED

    def test_create_portfolio_with_goals(self, manager: PortfolioManager):
        pf = manager.create_portfolio(
            "Goal Portfolio",
            goals=[{"title": "Ship MVP", "target_date": "2025-06-01"}],
        )
        assert len(pf.goals) == 1
        assert pf.goals[0].title == "Ship MVP"

    def test_get_portfolio(self, manager: PortfolioManager):
        pf = manager.create_portfolio("Find Me")
        result = manager.get_portfolio(pf.portfolio_id)
        assert result is not None
        assert result.name == "Find Me"

    def test_get_missing_portfolio(self, manager: PortfolioManager):
        assert manager.get_portfolio("nonexistent") is None

    def test_list_portfolios(self, manager: PortfolioManager):
        manager.create_portfolio("A")
        manager.create_portfolio("B")
        assert len(manager.list_portfolios()) == 2

    def test_delete_portfolio(self, manager: PortfolioManager):
        pf = manager.create_portfolio("Delete Me")
        assert manager.delete_portfolio(pf.portfolio_id) is True
        assert manager.get_portfolio(pf.portfolio_id) is None

    def test_delete_missing_portfolio(self, manager: PortfolioManager):
        assert manager.delete_portfolio("missing") is False

    def test_update_status(self, manager: PortfolioManager):
        pf = manager.create_portfolio("Status Test")
        updated = manager.update_portfolio_status(pf.portfolio_id, PortfolioStatus.ACTIVE)
        assert updated is not None
        assert updated.status == PortfolioStatus.ACTIVE


# ------------------------------------------------------------------
# Plan membership & grouping
# ------------------------------------------------------------------


class TestPlanGrouping:
    def test_add_plan(self, manager: PortfolioManager):
        pf = manager.create_portfolio("Plans")
        result = manager.add_plan(pf.portfolio_id, "p1", "Plan One")
        assert result is not None
        assert len(result.member_plans) == 1
        assert result.member_plans[0].plan_id == "p1"

    def test_add_plan_with_parent(self, manager: PortfolioManager):
        pf = manager.create_portfolio("Hierarchy")
        manager.add_plan(pf.portfolio_id, "parent", "Parent Plan")
        manager.add_plan(pf.portfolio_id, "child", "Child Plan", parent_plan_id="parent")
        hierarchy = manager.get_plan_hierarchy(pf.portfolio_id)
        assert None in hierarchy  # root plans
        assert "parent" in hierarchy  # children of parent

    def test_remove_plan(self, manager: PortfolioManager):
        pf = manager.create_portfolio("Remove")
        manager.add_plan(pf.portfolio_id, "p1", "Plan One")
        result = manager.remove_plan(pf.portfolio_id, "p1")
        assert result is not None
        assert len(result.member_plans) == 0

    def test_remove_nonexistent_plan(self, manager: PortfolioManager):
        pf = manager.create_portfolio("NoRemove")
        result = manager.remove_plan(pf.portfolio_id, "nope")
        assert result is not None
        assert len(result.member_plans) == 0

    def test_add_plan_missing_portfolio(self, manager: PortfolioManager):
        assert manager.add_plan("missing", "p1", "Plan") is None


# ------------------------------------------------------------------
# Rollup metrics
# ------------------------------------------------------------------


class TestRollupMetrics:
    def test_rollup_with_plans(self, manager: PortfolioManager, portfolio_with_plans):
        metrics = manager.compute_rollup_metrics(portfolio_with_plans.portfolio_id)
        assert metrics is not None
        assert metrics.total_plans == 2
        assert metrics.total_tasks == 18
        assert metrics.completed_tasks == 9
        assert metrics.in_progress_tasks == 5
        assert metrics.completion_pct == 50.0
        assert metrics.total_budget == 80000.0
        assert metrics.spent_budget == 55000.0
        assert metrics.earliest_start == "2025-01-01"
        assert metrics.latest_end == "2025-07-31"
        assert metrics.on_track_plans == 1
        assert metrics.at_risk_plans == 1

    def test_rollup_empty_portfolio(self, manager: PortfolioManager):
        pf = manager.create_portfolio("Empty")
        metrics = manager.compute_rollup_metrics(pf.portfolio_id)
        assert metrics is not None
        assert metrics.total_plans == 0
        assert metrics.total_tasks == 0

    def test_rollup_missing_portfolio(self, manager: PortfolioManager):
        assert manager.compute_rollup_metrics("missing") is None

    def test_rollup_to_dict(self, manager: PortfolioManager, portfolio_with_plans):
        metrics = manager.compute_rollup_metrics(portfolio_with_plans.portfolio_id)
        d = metrics.to_dict()
        assert isinstance(d, dict)
        assert d["total_plans"] == 2
        assert d["completion_pct"] == 50.0

    def test_rollup_no_registered_data(self, manager: PortfolioManager):
        pf = manager.create_portfolio("NoData")
        manager.add_plan(pf.portfolio_id, "orphan", "Orphan Plan")
        metrics = manager.compute_rollup_metrics(pf.portfolio_id)
        assert metrics is not None
        assert metrics.total_plans == 1
        assert metrics.total_tasks == 0


# ------------------------------------------------------------------
# Health dashboard
# ------------------------------------------------------------------


class TestHealthDashboard:
    def test_dashboard_with_plans(self, manager: PortfolioManager, portfolio_with_plans):
        dashboard = manager.generate_health_dashboard(portfolio_with_plans.portfolio_id)
        assert dashboard is not None
        assert dashboard.portfolio_id == portfolio_with_plans.portfolio_id
        assert dashboard.overall_health in HealthRating
        assert len(dashboard.plan_health_scores) == 2

    def test_dashboard_missing_portfolio(self, manager: PortfolioManager):
        assert manager.generate_health_dashboard("missing") is None

    def test_dashboard_to_dict(self, manager: PortfolioManager, portfolio_with_plans):
        dashboard = manager.generate_health_dashboard(portfolio_with_plans.portfolio_id)
        d = dashboard.to_dict()
        assert isinstance(d, dict)
        assert "overall_health" in d

    def test_dashboard_red_risk(self, manager: PortfolioManager, portfolio_with_plans):
        """Critical risk should produce RED risk health."""
        dashboard = manager.generate_health_dashboard(portfolio_with_plans.portfolio_id)
        assert dashboard.risk_health == HealthRating.RED


# ------------------------------------------------------------------
# Resource utilization
# ------------------------------------------------------------------


class TestResourceUtilization:
    def test_resource_aggregation(self, manager: PortfolioManager, portfolio_with_plans):
        allocs = manager.compute_resource_utilization(portfolio_with_plans.portfolio_id)
        assert len(allocs) == 1
        dev = allocs[0]
        assert dev.resource_id == "dev-1"
        assert dev.allocated == 160.0
        assert dev.utilization_pct == 100.0
        assert len(dev.plan_allocations) == 2

    def test_resource_empty(self, manager: PortfolioManager):
        pf = manager.create_portfolio("Empty")
        assert manager.compute_resource_utilization(pf.portfolio_id) == []

    def test_resource_missing_portfolio(self, manager: PortfolioManager):
        assert manager.compute_resource_utilization("missing") == []


# ------------------------------------------------------------------
# Portfolio timeline
# ------------------------------------------------------------------


class TestPortfolioTimeline:
    def test_timeline_sorted(self, manager: PortfolioManager, portfolio_with_plans):
        timeline = manager.build_portfolio_timeline(portfolio_with_plans.portfolio_id)
        assert len(timeline) == 3
        dates = [e.target_date for e in timeline]
        assert dates == sorted(dates)

    def test_timeline_empty(self, manager: PortfolioManager):
        pf = manager.create_portfolio("Empty")
        assert manager.build_portfolio_timeline(pf.portfolio_id) == []


# ------------------------------------------------------------------
# Cross-plan dependencies
# ------------------------------------------------------------------


class TestDependencies:
    def test_add_dependency(self, manager: PortfolioManager, portfolio_with_plans):
        dep = manager.add_dependency("plan-1", "task-1", "plan-2", "task-5")
        assert dep.source_plan_id == "plan-1"
        assert dep.target_plan_id == "plan-2"

    def test_get_portfolio_dependencies(self, manager: PortfolioManager, portfolio_with_plans):
        manager.add_dependency("plan-1", "task-1", "plan-2", "task-5")
        manager.add_dependency("plan-3", "task-x", "plan-4", "task-y")  # unrelated
        deps = manager.get_portfolio_dependencies(portfolio_with_plans.portfolio_id)
        assert len(deps) == 1


# ------------------------------------------------------------------
# Risk assessment
# ------------------------------------------------------------------


class TestRiskAssessment:
    def test_risks_sorted_by_severity(self, manager: PortfolioManager, portfolio_with_plans):
        risks = manager.assess_portfolio_risks(portfolio_with_plans.portfolio_id)
        assert len(risks) == 3
        assert risks[0].level == RiskLevel.CRITICAL

    def test_risks_empty(self, manager: PortfolioManager):
        pf = manager.create_portfolio("NoRisks")
        assert manager.assess_portfolio_risks(pf.portfolio_id) == []


# ------------------------------------------------------------------
# Performance comparison
# ------------------------------------------------------------------


class TestPerformanceComparison:
    def test_compare_on_target(self, manager: PortfolioManager, portfolio_with_plans):
        targets = {"completion_pct": 40.0, "on_track_plans": 1.0}
        results = manager.compare_performance(portfolio_with_plans.portfolio_id, targets)
        assert len(results) == 2
        completion = next(r for r in results if r.metric_name == "completion_pct")
        assert completion.on_target is True
        assert completion.actual_value == 50.0

    def test_compare_off_target(self, manager: PortfolioManager, portfolio_with_plans):
        targets = {"completion_pct": 90.0}
        results = manager.compare_performance(portfolio_with_plans.portfolio_id, targets)
        assert results[0].on_target is False

    def test_compare_missing_portfolio(self, manager: PortfolioManager):
        assert manager.compare_performance("missing", {"x": 1.0}) == []


# ------------------------------------------------------------------
# Portfolio model serialization
# ------------------------------------------------------------------


class TestPortfolioModel:
    def test_portfolio_to_dict(self, manager: PortfolioManager):
        pf = manager.create_portfolio("Serialize", owner="bob")
        manager.add_plan(pf.portfolio_id, "p1", "Plan One")
        updated = manager.get_portfolio(pf.portfolio_id)
        d = updated.to_dict()
        assert d["name"] == "Serialize"
        assert len(d["member_plans"]) == 1
