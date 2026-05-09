"""Tests for plan transformation rules engine."""

import json
from datetime import datetime, timezone

from blueprint.transformation.plan_transformer import (
    PlanTransformer,
    RuleValidation,
    TransformationResult,
    TransformDataStore,
    TransformPreview,
)
from blueprint.transformation.rules_engine import (
    ActionType,
    ConditionOperator,
    RuleAction,
    RuleCondition,
    TransformationRule,
    apply_action,
    matches_condition,
    PREDEFINED_TEMPLATES,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_store() -> TransformDataStore:
    """Build a store with plans for transformation."""
    now = _now()
    store = TransformDataStore()

    store.plans["plan-1"] = {
        "id": "plan-1",
        "title": "Auth System",
        "status": "in_progress",
        "tags": ["backend"],
        "user_ids": ["user-1", "user-2"],
        "tasks": [
            {"id": "t-1", "title": "Login endpoint", "depends_on": [], "status": "completed", "tags": ["api"], "assignee": "user-1", "estimate": 3},
            {"id": "t-2", "title": "Token refresh", "depends_on": ["t-1"], "status": "pending", "tags": ["api"], "assignee": "user-1", "estimate": 5},
            {"id": "t-3", "title": "UI login form", "depends_on": [], "status": "pending", "tags": ["ui"], "assignee": "user-2", "estimate": 2},
            {"id": "t-4", "title": "Password reset", "depends_on": ["t-1"], "status": "blocked", "tags": ["api", "security"], "assignee": "user-2", "estimate": 4},
        ],
        "created_at": now,
        "updated_at": now,
    }

    return store


def _transformer(store: TransformDataStore | None = None) -> PlanTransformer:
    return PlanTransformer(store=store or _make_store())


# ---------------------------------------------------------------------------
# Rule condition tests
# ---------------------------------------------------------------------------


class TestRuleConditions:
    def test_equals_condition(self):
        task = {"status": "pending", "title": "Test"}
        cond = RuleCondition(field="status", operator=ConditionOperator.EQUALS, value="pending")
        assert matches_condition(task, cond) is True

    def test_not_equals_condition(self):
        task = {"status": "completed"}
        cond = RuleCondition(field="status", operator=ConditionOperator.NOT_EQUALS, value="pending")
        assert matches_condition(task, cond) is True

    def test_contains_string(self):
        task = {"title": "Login endpoint"}
        cond = RuleCondition(field="title", operator=ConditionOperator.CONTAINS, value="Login")
        assert matches_condition(task, cond) is True

    def test_contains_list(self):
        task = {"tags": ["api", "security"]}
        cond = RuleCondition(field="tags", operator=ConditionOperator.CONTAINS, value="api")
        assert matches_condition(task, cond) is True

    def test_matches_regex(self):
        task = {"title": "API endpoint v2"}
        cond = RuleCondition(field="title", operator=ConditionOperator.MATCHES, value=r"v\d+")
        assert matches_condition(task, cond) is True

    def test_in_operator(self):
        task = {"status": "pending"}
        cond = RuleCondition(field="status", operator=ConditionOperator.IN, value=["pending", "blocked"])
        assert matches_condition(task, cond) is True

    def test_not_in_operator(self):
        task = {"status": "completed"}
        cond = RuleCondition(field="status", operator=ConditionOperator.NOT_IN, value=["pending", "blocked"])
        assert matches_condition(task, cond) is True

    def test_exists_operator(self):
        task = {"title": "Test", "assignee": "user-1"}
        cond = RuleCondition(field="assignee", operator=ConditionOperator.EXISTS)
        assert matches_condition(task, cond) is True

    def test_not_exists_operator(self):
        task = {"title": "Test"}
        cond = RuleCondition(field="assignee", operator=ConditionOperator.NOT_EXISTS)
        assert matches_condition(task, cond) is True


# ---------------------------------------------------------------------------
# Rule action tests
# ---------------------------------------------------------------------------


class TestRuleActions:
    def test_set_field(self):
        task = {"status": "pending"}
        action = RuleAction(action_type=ActionType.SET_FIELD, field="status", value="in_progress")
        result = apply_action(task, action)
        assert result["status"] == "in_progress"

    def test_add_tag(self):
        task = {"tags": ["api"]}
        action = RuleAction(action_type=ActionType.ADD_TAG, value="reviewed")
        result = apply_action(task, action)
        assert "reviewed" in result["tags"]
        assert "api" in result["tags"]

    def test_remove_tag(self):
        task = {"tags": ["api", "deprecated"]}
        action = RuleAction(action_type=ActionType.REMOVE_TAG, value="deprecated")
        result = apply_action(task, action)
        assert "deprecated" not in result["tags"]
        assert "api" in result["tags"]

    def test_delete_field(self):
        task = {"title": "Test", "temp": "data"}
        action = RuleAction(action_type=ActionType.DELETE_FIELD, field="temp")
        result = apply_action(task, action)
        assert "temp" not in result

    def test_set_status(self):
        task = {"status": "pending"}
        action = RuleAction(action_type=ActionType.SET_STATUS, value="in_progress")
        result = apply_action(task, action)
        assert result["status"] == "in_progress"

    def test_adjust_estimate(self):
        task = {"estimate": 5}
        action = RuleAction(action_type=ActionType.ADJUST_ESTIMATE, value=2)
        result = apply_action(task, action)
        assert result["estimate"] == 7

    def test_add_dependency(self):
        task = {"depends_on": ["t-1"]}
        action = RuleAction(action_type=ActionType.ADD_DEPENDENCY, value="t-2")
        result = apply_action(task, action)
        assert "t-2" in result["depends_on"]

    def test_remove_dependency(self):
        task = {"depends_on": ["t-1", "t-2"]}
        action = RuleAction(action_type=ActionType.REMOVE_DEPENDENCY, value="t-1")
        result = apply_action(task, action)
        assert "t-1" not in result["depends_on"]
        assert "t-2" in result["depends_on"]

    def test_rename(self):
        task = {"title": "Old Name v1"}
        action = RuleAction(action_type=ActionType.RENAME, field="v1", value="v2")
        result = apply_action(task, action)
        assert result["title"] == "Old Name v2"

    def test_copy_field(self):
        task = {"title": "Test", "source": "data"}
        action = RuleAction(action_type=ActionType.COPY_FIELD, field="source", value="target")
        result = apply_action(task, action)
        assert result["target"] == "data"


# ---------------------------------------------------------------------------
# Transformation application tests
# ---------------------------------------------------------------------------


class TestTransformationApplication:
    def test_apply_simple_rule(self):
        transformer = _transformer()
        rule = TransformationRule(
            rule_id="r-1",
            name="Mark pending as in_progress",
            conditions=[RuleCondition(field="status", operator=ConditionOperator.EQUALS, value="pending")],
            actions=[RuleAction(action_type=ActionType.SET_STATUS, value="in_progress")],
        )
        result = transformer.apply_transformation("plan-1", rule_set=[rule])
        assert isinstance(result, TransformationResult)
        assert result.tasks_modified == 2  # t-2 and t-3 are pending

    def test_apply_conditional_tag(self):
        transformer = _transformer()
        rule = TransformationRule(
            rule_id="r-2",
            name="Tag API tasks",
            conditions=[RuleCondition(field="tags", operator=ConditionOperator.CONTAINS, value="api")],
            actions=[RuleAction(action_type=ActionType.ADD_TAG, value="reviewed")],
        )
        result = transformer.apply_transformation("plan-1", rule_set=[rule])
        api_tasks = [t for t in result.transformed_plan["tasks"] if "reviewed" in t.get("tags", [])]
        assert len(api_tasks) == 3  # t-1, t-2, t-4 have "api" tag

    def test_apply_multiple_rules(self):
        transformer = _transformer()
        rules = [
            TransformationRule(
                rule_id="r-1",
                name="Add tag",
                conditions=[],
                actions=[RuleAction(action_type=ActionType.ADD_TAG, value="processed")],
            ),
            TransformationRule(
                rule_id="r-2",
                name="Adjust estimates",
                conditions=[RuleCondition(field="status", operator=ConditionOperator.EQUALS, value="pending")],
                actions=[RuleAction(action_type=ActionType.ADJUST_ESTIMATE, value=1)],
            ),
        ]
        result = transformer.apply_transformation("plan-1", rule_set=rules)
        assert result.rules_applied == 2

    def test_apply_updates_store(self):
        store = _make_store()
        transformer = PlanTransformer(store=store)
        rule = TransformationRule(
            rule_id="r-1",
            name="Tag all",
            conditions=[],
            actions=[RuleAction(action_type=ActionType.ADD_TAG, value="processed")],
        )
        transformer.apply_transformation("plan-1", rule_set=[rule])
        updated_plan = store.get_plan("plan-1")
        assert updated_plan is not None
        for task in updated_plan["tasks"]:
            assert "processed" in task.get("tags", [])

    def test_apply_nonexistent_plan(self):
        transformer = _transformer()
        result = transformer.apply_transformation("nonexistent")
        assert len(result.errors) > 0

    def test_disabled_rules_skipped(self):
        transformer = _transformer()
        rule = TransformationRule(
            rule_id="r-disabled",
            name="Disabled Rule",
            conditions=[],
            actions=[RuleAction(action_type=ActionType.ADD_TAG, value="should-not-appear")],
            enabled=False,
        )
        result = transformer.apply_transformation("plan-1", rule_set=[rule])
        assert result.rules_applied == 0


# ---------------------------------------------------------------------------
# Preview tests
# ---------------------------------------------------------------------------


class TestTransformPreview:
    def test_preview_transformation(self):
        transformer = _transformer()
        rule = TransformationRule(
            rule_id="r-1",
            name="Tag all",
            conditions=[],
            actions=[RuleAction(action_type=ActionType.ADD_TAG, value="preview")],
        )
        preview = transformer.preview_transformation("plan-1", rule_set=[rule])
        assert isinstance(preview, TransformPreview)
        assert preview.tasks_affected == 4
        assert len(preview.changes) == 4

    def test_preview_does_not_modify(self):
        store = _make_store()
        transformer = PlanTransformer(store=store)
        rule = TransformationRule(
            rule_id="r-1",
            name="Tag all",
            conditions=[],
            actions=[RuleAction(action_type=ActionType.ADD_TAG, value="preview")],
        )
        transformer.preview_transformation("plan-1", rule_set=[rule])
        plan = store.get_plan("plan-1")
        assert plan is not None
        for task in plan["tasks"]:
            assert "preview" not in task.get("tags", [])

    def test_preview_before_after(self):
        transformer = _transformer()
        rule = TransformationRule(
            rule_id="r-1",
            name="Tag all",
            conditions=[],
            actions=[RuleAction(action_type=ActionType.ADD_TAG, value="tagged")],
        )
        preview = transformer.preview_transformation("plan-1", rule_set=[rule])
        assert "tags" in preview.after_summary
        assert "tagged" in preview.after_summary["tags"]

    def test_preview_nonexistent_plan(self):
        transformer = _transformer()
        preview = transformer.preview_transformation("nonexistent")
        assert preview.tasks_affected == 0


# ---------------------------------------------------------------------------
# Batch transformation tests
# ---------------------------------------------------------------------------


class TestBatchTransformation:
    def test_batch_transform(self):
        store = _make_store()
        store.plans["plan-2"] = {
            "id": "plan-2",
            "title": "Plan 2",
            "status": "draft",
            "tags": [],
            "user_ids": [],
            "tasks": [
                {"id": "t-10", "title": "Task 10", "depends_on": [], "status": "pending", "tags": []},
            ],
            "created_at": _now(),
            "updated_at": _now(),
        }
        transformer = PlanTransformer(store=store)
        rule = TransformationRule(
            rule_id="r-batch",
            name="Tag all",
            conditions=[],
            actions=[RuleAction(action_type=ActionType.ADD_TAG, value="batch")],
        )
        results = transformer.batch_transform(["plan-1", "plan-2"], rule_set=[rule])
        assert len(results) == 2
        assert all(isinstance(r, TransformationResult) for r in results)


# ---------------------------------------------------------------------------
# Rule validation tests
# ---------------------------------------------------------------------------


class TestRuleValidation:
    def test_validate_valid_rules(self):
        transformer = _transformer()
        rules = [
            TransformationRule(
                rule_id="r-1",
                name="Test Rule",
                conditions=[RuleCondition(field="status", operator=ConditionOperator.EQUALS, value="pending")],
                actions=[RuleAction(action_type=ActionType.SET_STATUS, value="in_progress")],
            ),
        ]
        result = transformer.validate_rules(rules)
        assert isinstance(result, RuleValidation)
        assert result.valid is True

    def test_validate_rule_without_actions(self):
        transformer = _transformer()
        rules = [
            TransformationRule(rule_id="r-empty", name="Empty Rule", conditions=[], actions=[]),
        ]
        result = transformer.validate_rules(rules)
        assert len(result.warnings) > 0

    def test_validate_disabled_rule_warning(self):
        transformer = _transformer()
        rules = [
            TransformationRule(
                rule_id="r-disabled",
                name="Disabled",
                conditions=[],
                actions=[RuleAction(action_type=ActionType.ADD_TAG, value="test")],
                enabled=False,
            ),
        ]
        result = transformer.validate_rules(rules)
        assert len(result.warnings) > 0


# ---------------------------------------------------------------------------
# Predefined templates tests
# ---------------------------------------------------------------------------


class TestPredefinedTemplates:
    def test_tag_all_template(self):
        transformer = _transformer()
        template = PREDEFINED_TEMPLATES["tag_all"]
        result = transformer.apply_transformation("plan-1", rule_set=[template])
        for task in result.transformed_plan["tasks"]:
            assert "tagged" in task.get("tags", [])

    def test_set_draft_template(self):
        transformer = _transformer()
        template = PREDEFINED_TEMPLATES["set_draft"]
        result = transformer.apply_transformation("plan-1", rule_set=[template])
        for task in result.transformed_plan["tasks"]:
            assert task["status"] == "draft"


# ---------------------------------------------------------------------------
# Registered rules tests
# ---------------------------------------------------------------------------


class TestRegisteredRules:
    def test_define_and_use_rule(self):
        transformer = _transformer()
        rule = TransformationRule(
            rule_id="r-custom",
            name="Custom Rule",
            conditions=[RuleCondition(field="status", operator=ConditionOperator.EQUALS, value="blocked")],
            actions=[RuleAction(action_type=ActionType.SET_STATUS, value="pending")],
        )
        transformer.define_rule(rule)
        result = transformer.apply_transformation("plan-1")
        assert result.tasks_modified == 1  # t-4 is blocked

    def test_define_multiple_rules(self):
        transformer = _transformer()
        for i in range(3):
            transformer.define_rule(TransformationRule(
                rule_id=f"r-{i}",
                name=f"Rule {i}",
                conditions=[],
                actions=[RuleAction(action_type=ActionType.ADD_TAG, value=f"tag-{i}")],
            ))
        result = transformer.apply_transformation("plan-1")
        assert result.rules_applied == 3
