"""Core rules engine for plan transformations.

Defines rule conditions, actions, and templates for rule-based plan
modifications.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ConditionOperator(str, Enum):
    """Operators for rule conditions."""

    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    CONTAINS = "contains"
    MATCHES = "matches"
    IN = "in"
    NOT_IN = "not_in"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"


class ActionType(str, Enum):
    """Types of rule actions."""

    SET_FIELD = "set_field"
    COPY_FIELD = "copy_field"
    DELETE_FIELD = "delete_field"
    ADD_TAG = "add_tag"
    REMOVE_TAG = "remove_tag"
    ADD_DEPENDENCY = "add_dependency"
    REMOVE_DEPENDENCY = "remove_dependency"
    SET_STATUS = "set_status"
    ADJUST_ESTIMATE = "adjust_estimate"
    RENAME = "rename"


@dataclass(frozen=True, slots=True)
class RuleCondition:
    """A condition that must match for a rule to apply."""

    field: str
    operator: ConditionOperator
    value: Any = None


@dataclass(frozen=True, slots=True)
class RuleAction:
    """An action to perform when a rule matches."""

    action_type: ActionType
    field: str = ""
    value: Any = None


@dataclass(frozen=True, slots=True)
class TransformationRule:
    """A complete rule with conditions and actions."""

    rule_id: str
    name: str
    conditions: list[RuleCondition] = field(default_factory=list)
    actions: list[RuleAction] = field(default_factory=list)
    description: str = ""
    enabled: bool = True


def matches_condition(task: dict[str, Any], condition: RuleCondition) -> bool:
    """Check if a task matches a single condition."""
    field_val = task.get(condition.field)

    if condition.operator == ConditionOperator.EXISTS:
        return condition.field in task
    if condition.operator == ConditionOperator.NOT_EXISTS:
        return condition.field not in task
    if condition.operator == ConditionOperator.EQUALS:
        return field_val == condition.value
    if condition.operator == ConditionOperator.NOT_EQUALS:
        return field_val != condition.value
    if condition.operator == ConditionOperator.CONTAINS:
        if isinstance(field_val, str):
            return condition.value in field_val
        if isinstance(field_val, list):
            return condition.value in field_val
        return False
    if condition.operator == ConditionOperator.MATCHES:
        if isinstance(field_val, str):
            return bool(re.search(str(condition.value), field_val))
        return False
    if condition.operator == ConditionOperator.IN:
        if isinstance(condition.value, list):
            return field_val in condition.value
        return False
    if condition.operator == ConditionOperator.NOT_IN:
        if isinstance(condition.value, list):
            return field_val not in condition.value
        return False

    return False


def matches_all_conditions(task: dict[str, Any], conditions: list[RuleCondition]) -> bool:
    """Check if a task matches all conditions in a rule."""
    return all(matches_condition(task, c) for c in conditions)


def apply_action(task: dict[str, Any], action: RuleAction) -> dict[str, Any]:
    """Apply a single action to a task, returning the modified task."""
    result = dict(task)

    if action.action_type == ActionType.SET_FIELD:
        result[action.field] = action.value
    elif action.action_type == ActionType.COPY_FIELD:
        if action.field in result:
            result[action.value] = result[action.field]
    elif action.action_type == ActionType.DELETE_FIELD:
        result.pop(action.field, None)
    elif action.action_type == ActionType.ADD_TAG:
        tags = list(result.get("tags", []))
        if action.value not in tags:
            tags.append(action.value)
        result["tags"] = tags
    elif action.action_type == ActionType.REMOVE_TAG:
        tags = [t for t in result.get("tags", []) if t != action.value]
        result["tags"] = tags
    elif action.action_type == ActionType.ADD_DEPENDENCY:
        deps = list(result.get("depends_on", []))
        if action.value not in deps:
            deps.append(action.value)
        result["depends_on"] = deps
    elif action.action_type == ActionType.REMOVE_DEPENDENCY:
        deps = [d for d in result.get("depends_on", []) if d != action.value]
        result["depends_on"] = deps
    elif action.action_type == ActionType.SET_STATUS:
        result["status"] = action.value
    elif action.action_type == ActionType.ADJUST_ESTIMATE:
        current = result.get("estimate", 0)
        if isinstance(current, (int, float)) and isinstance(action.value, (int, float)):
            result["estimate"] = current + action.value
    elif action.action_type == ActionType.RENAME:
        if isinstance(action.value, str) and "title" in result:
            result["title"] = re.sub(action.field, action.value, result["title"])

    return result


# -- predefined templates ------------------------------------------------

TEMPLATE_TAG_ALL = TransformationRule(
    rule_id="tpl-tag-all",
    name="Add tag to all tasks",
    conditions=[],
    actions=[RuleAction(action_type=ActionType.ADD_TAG, value="tagged")],
    description="Add a tag to every task in the plan",
)

TEMPLATE_SET_DRAFT = TransformationRule(
    rule_id="tpl-set-draft",
    name="Reset all to draft",
    conditions=[],
    actions=[RuleAction(action_type=ActionType.SET_STATUS, value="draft")],
    description="Reset all task statuses to draft",
)

PREDEFINED_TEMPLATES = {
    "tag_all": TEMPLATE_TAG_ALL,
    "set_draft": TEMPLATE_SET_DRAFT,
}
