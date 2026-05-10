"""Portfolio manager for aggregating multiple plans with rollup metrics.

Provides plan grouping, metric aggregation, health dashboards,
resource utilization views, cross-plan dependency tracking,
risk assessment, and performance comparison.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

from blueprint.portfolio.portfolio_model import (
    CrossPlanDependency,
    HealthDashboard,
    HealthRating,
    MilestoneEntry,
    PerformanceTarget,
    PlanReference,
    Portfolio,
    PortfolioGoal,
    PortfolioRisk,
    PortfolioStatus,
    ResourceAllocation,
    RiskLevel,
    RollupMetrics,
    _gen_id,
    _now_iso,
)


@dataclass
class PlanData:
    """Snapshot of a plan's state for metric computation."""

    plan_id: str
    title: str = ""
    status: str = "draft"
    owner: str = ""
    total_tasks: int = 0
    completed_tasks: int = 0
    in_progress_tasks: int = 0
    budget: float = 0.0
    spent: float = 0.0
    start_date: str | None = None
    end_date: str | None = None
    milestones: list[dict[str, Any]] = field(default_factory=list)
    risks: list[dict[str, Any]] = field(default_factory=list)
    resources: list[dict[str, Any]] = field(default_factory=list)
    health_score: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


class PortfolioManager:
    """Manages portfolios of execution plans with rollup metrics."""

    def __init__(self) -> None:
        self._portfolios: dict[str, Portfolio] = {}
        self._plan_data: dict[str, PlanData] = {}
        self._dependencies: list[CrossPlanDependency] = []

    # ------------------------------------------------------------------
    # Portfolio CRUD
    # ------------------------------------------------------------------

    def create_portfolio(
        self,
        name: str,
        *,
        description: str = "",
        owner: str = "",
        goals: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Portfolio:
        pid = _gen_id("pf")
        goal_objs = [
            PortfolioGoal(
                goal_id=_gen_id("goal"),
                title=g.get("title", ""),
                description=g.get("description", ""),
                target_date=g.get("target_date"),
                status=g.get("status", "open"),
            )
            for g in (goals or [])
        ]
        portfolio = Portfolio(
            portfolio_id=pid,
            name=name,
            description=description,
            owner=owner,
            goals=goal_objs,
            metadata=metadata or {},
        )
        self._portfolios[pid] = portfolio
        return portfolio

    def get_portfolio(self, portfolio_id: str) -> Portfolio | None:
        return self._portfolios.get(portfolio_id)

    def list_portfolios(self) -> list[Portfolio]:
        return list(self._portfolios.values())

    def delete_portfolio(self, portfolio_id: str) -> bool:
        return self._portfolios.pop(portfolio_id, None) is not None

    def update_portfolio_status(
        self, portfolio_id: str, status: PortfolioStatus
    ) -> Portfolio | None:
        portfolio = self._portfolios.get(portfolio_id)
        if portfolio is None:
            return None
        updated = replace(portfolio, status=status, updated_at=_now_iso())
        self._portfolios[portfolio_id] = updated
        return updated

    # ------------------------------------------------------------------
    # Plan membership & grouping
    # ------------------------------------------------------------------

    def add_plan(
        self,
        portfolio_id: str,
        plan_id: str,
        title: str,
        *,
        parent_plan_id: str | None = None,
        status: str = "draft",
        owner: str = "",
    ) -> Portfolio | None:
        portfolio = self._portfolios.get(portfolio_id)
        if portfolio is None:
            return None
        ref = PlanReference(
            plan_id=plan_id,
            title=title,
            parent_plan_id=parent_plan_id,
            status=status,
            owner=owner,
        )
        updated = replace(
            portfolio,
            member_plans=[*portfolio.member_plans, ref],
            updated_at=_now_iso(),
        )
        self._portfolios[portfolio_id] = updated
        return updated

    def remove_plan(self, portfolio_id: str, plan_id: str) -> Portfolio | None:
        portfolio = self._portfolios.get(portfolio_id)
        if portfolio is None:
            return None
        new_plans = [p for p in portfolio.member_plans if p.plan_id != plan_id]
        if len(new_plans) == len(portfolio.member_plans):
            return portfolio
        updated = replace(portfolio, member_plans=new_plans, updated_at=_now_iso())
        self._portfolios[portfolio_id] = updated
        return updated

    def get_plan_hierarchy(self, portfolio_id: str) -> dict[str | None, list[PlanReference]]:
        """Return plans grouped by parent_plan_id (None = root)."""
        portfolio = self._portfolios.get(portfolio_id)
        if portfolio is None:
            return {}
        hierarchy: dict[str | None, list[PlanReference]] = {}
        for ref in portfolio.member_plans:
            hierarchy.setdefault(ref.parent_plan_id, []).append(ref)
        return hierarchy

    # ------------------------------------------------------------------
    # Plan data registration (for metric computation)
    # ------------------------------------------------------------------

    def register_plan_data(self, data: PlanData) -> None:
        self._plan_data[data.plan_id] = data

    def get_plan_data(self, plan_id: str) -> PlanData | None:
        return self._plan_data.get(plan_id)

    # ------------------------------------------------------------------
    # Rollup metrics
    # ------------------------------------------------------------------

    def compute_rollup_metrics(self, portfolio_id: str) -> RollupMetrics | None:
        portfolio = self._portfolios.get(portfolio_id)
        if portfolio is None:
            return None

        plan_ids = {p.plan_id for p in portfolio.member_plans}
        plans = [self._plan_data[pid] for pid in plan_ids if pid in self._plan_data]

        if not plans:
            return RollupMetrics(total_plans=len(plan_ids))

        total_tasks = sum(p.total_tasks for p in plans)
        completed_tasks = sum(p.completed_tasks for p in plans)
        in_progress_tasks = sum(p.in_progress_tasks for p in plans)
        total_budget = sum(p.budget for p in plans)
        spent_budget = sum(p.spent for p in plans)

        starts = [p.start_date for p in plans if p.start_date]
        ends = [p.end_date for p in plans if p.end_date]

        on_track = sum(1 for p in plans if p.status in ("on_track", "completed"))
        at_risk = sum(1 for p in plans if p.status == "at_risk")
        blocked = sum(1 for p in plans if p.status == "blocked")

        return RollupMetrics(
            total_plans=len(plan_ids),
            total_tasks=total_tasks,
            completed_tasks=completed_tasks,
            in_progress_tasks=in_progress_tasks,
            completion_pct=(completed_tasks / total_tasks * 100) if total_tasks else 0.0,
            total_budget=total_budget,
            spent_budget=spent_budget,
            budget_utilization_pct=(spent_budget / total_budget * 100) if total_budget else 0.0,
            earliest_start=min(starts) if starts else None,
            latest_end=max(ends) if ends else None,
            on_track_plans=on_track,
            at_risk_plans=at_risk,
            blocked_plans=blocked,
        )

    # ------------------------------------------------------------------
    # Health dashboard
    # ------------------------------------------------------------------

    def generate_health_dashboard(self, portfolio_id: str) -> HealthDashboard | None:
        portfolio = self._portfolios.get(portfolio_id)
        if portfolio is None:
            return None

        metrics = self.compute_rollup_metrics(portfolio_id)
        if metrics is None:
            return None

        completion_health = self._rate_completion(metrics.completion_pct)
        budget_health = self._rate_budget(metrics.budget_utilization_pct)
        schedule_health = self._rate_schedule(metrics)
        risk_health = self._rate_risk(portfolio_id)

        plan_scores: dict[str, HealthRating] = {}
        for ref in portfolio.member_plans:
            pd = self._plan_data.get(ref.plan_id)
            if pd:
                plan_scores[ref.plan_id] = self._rate_plan_health(pd)

        ratings = [completion_health, budget_health, schedule_health, risk_health]
        overall = self._worst_rating(ratings)

        recommendations: list[str] = []
        if budget_health == HealthRating.RED:
            recommendations.append("Budget overrun detected. Review spend allocation.")
        if schedule_health == HealthRating.RED:
            recommendations.append("Multiple plans at risk. Reassess timelines.")
        if completion_health == HealthRating.YELLOW:
            recommendations.append("Completion rate below target. Check blockers.")

        return HealthDashboard(
            portfolio_id=portfolio_id,
            overall_health=overall,
            completion_health=completion_health,
            budget_health=budget_health,
            schedule_health=schedule_health,
            risk_health=risk_health,
            plan_health_scores=plan_scores,
            recommendations=recommendations,
        )

    # ------------------------------------------------------------------
    # Resource utilization
    # ------------------------------------------------------------------

    def compute_resource_utilization(self, portfolio_id: str) -> list[ResourceAllocation]:
        portfolio = self._portfolios.get(portfolio_id)
        if portfolio is None:
            return []

        resource_map: dict[str, dict[str, Any]] = {}
        for ref in portfolio.member_plans:
            pd = self._plan_data.get(ref.plan_id)
            if not pd:
                continue
            for res in pd.resources:
                rid = res.get("resource_id", "")
                if rid not in resource_map:
                    resource_map[rid] = {
                        "resource_name": res.get("resource_name", rid),
                        "resource_type": res.get("resource_type", "unknown"),
                        "total_capacity": res.get("total_capacity", 0.0),
                        "allocated": 0.0,
                        "plan_allocations": {},
                    }
                alloc = res.get("allocated", 0.0)
                resource_map[rid]["allocated"] += alloc
                resource_map[rid]["plan_allocations"][ref.plan_id] = alloc

        result: list[ResourceAllocation] = []
        for rid, info in resource_map.items():
            cap = info["total_capacity"]
            alloc = info["allocated"]
            result.append(
                ResourceAllocation(
                    resource_id=rid,
                    resource_name=info["resource_name"],
                    resource_type=info["resource_type"],
                    total_capacity=cap,
                    allocated=alloc,
                    utilization_pct=(alloc / cap * 100) if cap else 0.0,
                    plan_allocations=info["plan_allocations"],
                )
            )
        return result

    # ------------------------------------------------------------------
    # Portfolio timeline
    # ------------------------------------------------------------------

    def build_portfolio_timeline(self, portfolio_id: str) -> list[MilestoneEntry]:
        portfolio = self._portfolios.get(portfolio_id)
        if portfolio is None:
            return []

        entries: list[MilestoneEntry] = []
        for ref in portfolio.member_plans:
            pd = self._plan_data.get(ref.plan_id)
            if not pd:
                continue
            for ms in pd.milestones:
                entries.append(
                    MilestoneEntry(
                        milestone_id=ms.get("milestone_id", _gen_id("ms")),
                        plan_id=ref.plan_id,
                        plan_title=ref.title,
                        title=ms.get("title", ""),
                        target_date=ms.get("target_date", ""),
                        status=ms.get("status", "pending"),
                    )
                )
        entries.sort(key=lambda e: e.target_date)
        return entries

    # ------------------------------------------------------------------
    # Cross-plan dependencies
    # ------------------------------------------------------------------

    def add_dependency(
        self,
        source_plan_id: str,
        source_task_id: str,
        target_plan_id: str,
        target_task_id: str,
        *,
        dependency_type: str = "finish_to_start",
        description: str = "",
    ) -> CrossPlanDependency:
        dep = CrossPlanDependency(
            dependency_id=_gen_id("dep"),
            source_plan_id=source_plan_id,
            source_task_id=source_task_id,
            target_plan_id=target_plan_id,
            target_task_id=target_task_id,
            dependency_type=dependency_type,
            description=description,
        )
        self._dependencies.append(dep)
        return dep

    def get_portfolio_dependencies(self, portfolio_id: str) -> list[CrossPlanDependency]:
        portfolio = self._portfolios.get(portfolio_id)
        if portfolio is None:
            return []
        plan_ids = {p.plan_id for p in portfolio.member_plans}
        return [
            d
            for d in self._dependencies
            if d.source_plan_id in plan_ids or d.target_plan_id in plan_ids
        ]

    # ------------------------------------------------------------------
    # Risk assessment
    # ------------------------------------------------------------------

    def assess_portfolio_risks(self, portfolio_id: str) -> list[PortfolioRisk]:
        portfolio = self._portfolios.get(portfolio_id)
        if portfolio is None:
            return []

        risks: list[PortfolioRisk] = []
        for ref in portfolio.member_plans:
            pd = self._plan_data.get(ref.plan_id)
            if not pd:
                continue
            for r in pd.risks:
                level_str = r.get("level", "medium").lower()
                try:
                    level = RiskLevel(level_str)
                except ValueError:
                    level = RiskLevel.MEDIUM
                risks.append(
                    PortfolioRisk(
                        risk_id=r.get("risk_id", _gen_id("risk")),
                        plan_id=ref.plan_id,
                        title=r.get("title", ""),
                        level=level,
                        impact=r.get("impact", ""),
                        mitigation=r.get("mitigation", ""),
                        status=r.get("status", "open"),
                    )
                )
        risks.sort(key=lambda r: list(RiskLevel).index(r.level), reverse=True)
        return risks

    # ------------------------------------------------------------------
    # Performance comparison
    # ------------------------------------------------------------------

    def compare_performance(
        self, portfolio_id: str, targets: dict[str, float]
    ) -> list[PerformanceTarget]:
        metrics = self.compute_rollup_metrics(portfolio_id)
        if metrics is None:
            return []

        actuals = {
            "completion_pct": metrics.completion_pct,
            "budget_utilization_pct": metrics.budget_utilization_pct,
            "total_tasks": float(metrics.total_tasks),
            "completed_tasks": float(metrics.completed_tasks),
            "on_track_plans": float(metrics.on_track_plans),
        }

        results: list[PerformanceTarget] = []
        for metric_name, target_value in targets.items():
            actual = actuals.get(metric_name, 0.0)
            results.append(
                PerformanceTarget(
                    metric_name=metric_name,
                    target_value=target_value,
                    actual_value=actual,
                    on_target=actual >= target_value,
                )
            )
        return results

    # ------------------------------------------------------------------
    # Internal rating helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _rate_completion(pct: float) -> HealthRating:
        if pct >= 70:
            return HealthRating.GREEN
        if pct >= 40:
            return HealthRating.YELLOW
        return HealthRating.RED

    @staticmethod
    def _rate_budget(utilization_pct: float) -> HealthRating:
        if utilization_pct <= 85:
            return HealthRating.GREEN
        if utilization_pct <= 100:
            return HealthRating.YELLOW
        return HealthRating.RED

    @staticmethod
    def _rate_schedule(metrics: RollupMetrics) -> HealthRating:
        if metrics.total_plans == 0:
            return HealthRating.GREEN
        risk_ratio = metrics.at_risk_plans / metrics.total_plans
        if risk_ratio <= 0.1:
            return HealthRating.GREEN
        if risk_ratio <= 0.3:
            return HealthRating.YELLOW
        return HealthRating.RED

    def _rate_risk(self, portfolio_id: str) -> HealthRating:
        risks = self.assess_portfolio_risks(portfolio_id)
        critical = sum(1 for r in risks if r.level == RiskLevel.CRITICAL and r.status == "open")
        high = sum(1 for r in risks if r.level == RiskLevel.HIGH and r.status == "open")
        if critical > 0:
            return HealthRating.RED
        if high > 1:
            return HealthRating.YELLOW
        return HealthRating.GREEN

    @staticmethod
    def _rate_plan_health(pd: PlanData) -> HealthRating:
        if pd.health_score >= 0.7:
            return HealthRating.GREEN
        if pd.health_score >= 0.4:
            return HealthRating.YELLOW
        return HealthRating.RED

    @staticmethod
    def _worst_rating(ratings: list[HealthRating]) -> HealthRating:
        order = [HealthRating.GREEN, HealthRating.YELLOW, HealthRating.RED]
        worst = HealthRating.GREEN
        for r in ratings:
            if order.index(r) > order.index(worst):
                worst = r
        return worst


__all__ = [
    "PlanData",
    "PortfolioManager",
]
