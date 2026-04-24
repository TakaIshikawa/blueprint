"""Aggregate readiness gate for autonomous execution handoff."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import Any, Literal

from blueprint.audits.brief_plan_coherence import (
    BriefPlanCoherenceResult,
    audit_brief_plan_coherence,
)
from blueprint.audits.env_inventory import EnvInventoryResult, build_env_inventory
from blueprint.audits.plan_audit import PlanAuditResult, audit_execution_plan
from blueprint.audits.risk_coverage import RiskCoverageResult, audit_risk_coverage
from blueprint.audits.task_completeness import (
    TaskCompletenessResult,
    audit_task_completeness,
)


ReadinessComponent = Literal[
    "plan_audit",
    "task_completeness",
    "brief_plan_coherence",
    "risk_coverage",
    "env_inventory",
]


@dataclass(frozen=True)
class PlanReadinessBlockingReason:
    """A single reason an execution plan is not ready for handoff."""

    component: ReadinessComponent
    code: str
    message: str
    task_id: str | None = None
    item_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "component": self.component,
            "code": self.code,
            "message": self.message,
        }
        if self.task_id is not None:
            payload["task_id"] = self.task_id
        if self.item_name is not None:
            payload["item_name"] = self.item_name
        return payload


@dataclass(frozen=True)
class EnvInventoryCounts:
    """Summarized environment inventory counts for readiness output."""

    required: int = 0
    optional: int = 0
    unknown: int = 0
    missing_required: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "required": self.required,
            "optional": self.optional,
            "unknown": self.unknown,
            "missing_required": self.missing_required,
        }


@dataclass(frozen=True)
class PlanReadinessResult:
    """Aggregate readiness result for an execution plan."""

    plan_id: str
    implementation_brief_id: str
    plan_audit: PlanAuditResult
    task_completeness: TaskCompletenessResult
    brief_plan_coherence: BriefPlanCoherenceResult
    risk_coverage: RiskCoverageResult
    env_inventory: EnvInventoryResult
    env_inventory_counts: EnvInventoryCounts
    blocking_reasons: list[PlanReadinessBlockingReason] = field(default_factory=list)

    @property
    def ready(self) -> bool:
        return not self.blocking_reasons

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "implementation_brief_id": self.implementation_brief_id,
            "ready": self.ready,
            "blocking_reasons": [
                reason.to_dict() for reason in self.blocking_reasons
            ],
            "components": {
                "plan_audit": self.plan_audit.to_dict(),
                "task_completeness": self.task_completeness.to_dict(),
                "brief_plan_coherence": self.brief_plan_coherence.to_dict(),
                "risk_coverage": self.risk_coverage.to_dict(),
                "env_inventory": self.env_inventory.to_dict(),
                "env_inventory_counts": self.env_inventory_counts.to_dict(),
            },
        }


def evaluate_plan_readiness(
    plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> PlanReadinessResult:
    """Run all blocking handoff audits and return a single go/no-go result."""
    plan_audit = audit_execution_plan(plan)
    task_completeness = audit_task_completeness(plan)
    brief_plan_coherence = audit_brief_plan_coherence(plan, implementation_brief)
    risk_coverage = audit_risk_coverage(implementation_brief, plan)
    env_inventory = build_env_inventory(implementation_brief, plan)
    missing_required_items = _missing_required_env_items(env_inventory)
    env_inventory_counts = _env_inventory_counts(env_inventory, missing_required_items)

    blocking_reasons: list[PlanReadinessBlockingReason] = []
    blocking_reasons.extend(_plan_audit_blockers(plan_audit))
    blocking_reasons.extend(_task_completeness_blockers(task_completeness))
    blocking_reasons.extend(_coherence_blockers(brief_plan_coherence))
    blocking_reasons.extend(_risk_coverage_blockers(risk_coverage))
    blocking_reasons.extend(_env_inventory_blockers(missing_required_items))

    return PlanReadinessResult(
        plan_id=str(plan.get("id") or ""),
        implementation_brief_id=str(implementation_brief.get("id") or ""),
        plan_audit=plan_audit,
        task_completeness=task_completeness,
        brief_plan_coherence=brief_plan_coherence,
        risk_coverage=risk_coverage,
        env_inventory=env_inventory,
        env_inventory_counts=env_inventory_counts,
        blocking_reasons=blocking_reasons,
    )


def _plan_audit_blockers(
    result: PlanAuditResult,
) -> list[PlanReadinessBlockingReason]:
    return [
        PlanReadinessBlockingReason(
            component="plan_audit",
            code=issue.code,
            message=issue.message,
            task_id=issue.task_id,
        )
        for issue in result.issues
        if issue.severity == "error"
    ]


def _task_completeness_blockers(
    result: TaskCompletenessResult,
) -> list[PlanReadinessBlockingReason]:
    return [
        PlanReadinessBlockingReason(
            component="task_completeness",
            code=finding.code,
            message=finding.message,
            task_id=finding.task_id,
        )
        for finding in result.findings
        if finding.severity == "blocking"
    ]


def _coherence_blockers(
    result: BriefPlanCoherenceResult,
) -> list[PlanReadinessBlockingReason]:
    return [
        PlanReadinessBlockingReason(
            component="brief_plan_coherence",
            code=issue.code,
            message=issue.message,
            task_id=issue.task_id,
        )
        for issue in result.issues
        if issue.severity == "error"
    ]


def _risk_coverage_blockers(
    result: RiskCoverageResult,
) -> list[PlanReadinessBlockingReason]:
    return [
        PlanReadinessBlockingReason(
            component="risk_coverage",
            code="uncovered_risk",
            message=f"Implementation risk is not covered by any task: {risk.risk}",
        )
        for risk in result.uncovered_risks
    ]


def _missing_required_env_items(
    result: EnvInventoryResult,
) -> list[str]:
    return [
        item.name
        for item in result.items
        if item.item_type == "env_var"
        and item.status == "required"
        and not os.environ.get(item.name)
    ]


def _env_inventory_counts(
    result: EnvInventoryResult,
    missing_required_items: list[str],
) -> EnvInventoryCounts:
    by_status = result.items_by_status()
    return EnvInventoryCounts(
        required=len(by_status["required"]),
        optional=len(by_status["optional"]),
        unknown=len(by_status["unknown"]),
        missing_required=len(missing_required_items),
    )


def _env_inventory_blockers(
    missing_required_items: list[str],
) -> list[PlanReadinessBlockingReason]:
    return [
        PlanReadinessBlockingReason(
            component="env_inventory",
            code="missing_required_env_var",
            item_name=item_name,
            message=f"Required environment variable is not set: {item_name}",
        )
        for item_name in missing_required_items
    ]
