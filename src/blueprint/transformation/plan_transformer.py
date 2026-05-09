"""Plan transformation engine applying automated transformations via rules.

Supports task, dependency, timeline, resource, and structure
transformations with rule conditions, preview, and batch operations.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from blueprint.transformation.rules_engine import (
    TransformationRule,
    apply_action,
    matches_all_conditions,
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TransformationType(str, Enum):
    """Type of transformation."""

    TASK = "task"
    DEPENDENCY = "dependency"
    TIMELINE = "timeline"
    RESOURCE = "resource"
    STRUCTURE = "structure"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TransformPreview:
    """Preview showing before/after of a transformation."""

    preview_id: str
    plan_id: str
    rule_count: int = 0
    tasks_affected: int = 0
    before_summary: dict[str, Any] = field(default_factory=dict)
    after_summary: dict[str, Any] = field(default_factory=dict)
    changes: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class TransformationResult:
    """Result of applying transformations."""

    result_id: str
    plan_id: str
    rules_applied: int = 0
    tasks_modified: int = 0
    transformed_plan: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class RuleValidation:
    """Result of validating a rule set."""

    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Data store
# ---------------------------------------------------------------------------


@dataclass
class TransformDataStore:
    """In-memory store providing plan data for transformation operations."""

    plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)

    def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        return self.plans.get(plan_id)

    def update_plan(self, plan: dict[str, Any]) -> None:
        self.plans[plan["id"]] = plan


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_id(prefix: str = "txf") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _summarize_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Create a summary of a plan for preview comparison."""
    tasks = plan.get("tasks", [])
    statuses: dict[str, int] = {}
    tags: set[str] = set()
    for t in tasks:
        s = t.get("status", "unknown")
        statuses[s] = statuses.get(s, 0) + 1
        tags.update(t.get("tags", []))
    return {
        "task_count": len(tasks),
        "statuses": statuses,
        "tags": sorted(tags),
        "dependency_count": sum(len(t.get("depends_on", [])) for t in tasks),
    }


# ---------------------------------------------------------------------------
# PlanTransformer
# ---------------------------------------------------------------------------


class PlanTransformer:
    """Applies automated transformations to plans via configurable rules.

    Supports task, dependency, timeline, resource, and structure
    transformations with rule conditions, preview, and batch operations.
    """

    def __init__(self, store: TransformDataStore | None = None) -> None:
        self._store = store or TransformDataStore()
        self._rules: dict[str, TransformationRule] = {}

    def define_rule(self, rule_config: TransformationRule) -> TransformationRule:
        """Register a transformation rule."""
        self._rules[rule_config.rule_id] = rule_config
        return rule_config

    def apply_transformation(
        self,
        plan_id: str,
        rule_set: list[TransformationRule] | None = None,
    ) -> TransformationResult:
        """Apply transformation rules to a plan.

        Args:
            plan_id: ID of the plan to transform.
            rule_set: Rules to apply (if None, uses all registered rules).
        """
        plan = self._store.get_plan(plan_id)
        if plan is None:
            return TransformationResult(
                result_id=_generate_id(),
                plan_id=plan_id,
                errors=[f"Plan {plan_id} not found"],
                created_at=_now_iso(),
            )

        rules = rule_set if rule_set is not None else list(self._rules.values())
        rules = [r for r in rules if r.enabled]

        transformed_plan = dict(plan)
        tasks = [dict(t) for t in plan.get("tasks", [])]
        total_modified = 0
        rules_applied = 0

        for rule in rules:
            rule_modified = 0
            new_tasks = []
            for task in tasks:
                if matches_all_conditions(task, rule.conditions):
                    modified = task
                    for action in rule.actions:
                        modified = apply_action(modified, action)
                    new_tasks.append(modified)
                    if modified != task:
                        rule_modified += 1
                else:
                    new_tasks.append(task)
            tasks = new_tasks
            if rule_modified > 0:
                rules_applied += 1
                total_modified += rule_modified

        transformed_plan["tasks"] = tasks
        transformed_plan["updated_at"] = _now_iso()

        # Update the store
        self._store.update_plan(transformed_plan)

        return TransformationResult(
            result_id=_generate_id(),
            plan_id=plan_id,
            rules_applied=rules_applied,
            tasks_modified=total_modified,
            transformed_plan=transformed_plan,
            created_at=_now_iso(),
        )

    def preview_transformation(
        self,
        plan_id: str,
        rule_set: list[TransformationRule] | None = None,
    ) -> TransformPreview:
        """Preview transformations without modifying the plan."""
        plan = self._store.get_plan(plan_id)
        if plan is None:
            return TransformPreview(
                preview_id=_generate_id("prv"),
                plan_id=plan_id,
            )

        before = _summarize_plan(plan)

        rules = rule_set if rule_set is not None else list(self._rules.values())
        rules = [r for r in rules if r.enabled]

        tasks = [dict(t) for t in plan.get("tasks", [])]
        changes: list[dict[str, Any]] = []
        tasks_affected = 0

        for rule in rules:
            for i, task in enumerate(tasks):
                if matches_all_conditions(task, rule.conditions):
                    before_task = dict(task)
                    modified = task
                    for action in rule.actions:
                        modified = apply_action(modified, action)
                    if modified != before_task:
                        changes.append({
                            "task_id": task.get("id"),
                            "rule_id": rule.rule_id,
                            "before": before_task,
                            "after": modified,
                        })
                        tasks_affected += 1
                    tasks[i] = modified

        after_plan = dict(plan)
        after_plan["tasks"] = tasks
        after = _summarize_plan(after_plan)

        return TransformPreview(
            preview_id=_generate_id("prv"),
            plan_id=plan_id,
            rule_count=len(rules),
            tasks_affected=tasks_affected,
            before_summary=before,
            after_summary=after,
            changes=changes,
        )

    def batch_transform(
        self,
        plan_ids: list[str],
        rule_set: list[TransformationRule] | None = None,
    ) -> list[TransformationResult]:
        """Apply transformations to multiple plans."""
        return [
            self.apply_transformation(pid, rule_set)
            for pid in plan_ids
        ]

    def validate_rules(
        self,
        rule_set: list[TransformationRule],
    ) -> RuleValidation:
        """Validate a set of transformation rules."""
        errors: list[str] = []
        warnings: list[str] = []

        for rule in rule_set:
            if not rule.rule_id:
                errors.append("Rule must have a rule_id")
            if not rule.name:
                errors.append(f"Rule {rule.rule_id} must have a name")
            if not rule.actions:
                warnings.append(f"Rule {rule.rule_id} has no actions")
            if not rule.enabled:
                warnings.append(f"Rule {rule.rule_id} is disabled")

        return RuleValidation(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )
