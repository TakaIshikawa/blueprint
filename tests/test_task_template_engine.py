"""Tests for task template engine with parameterized generation."""

from __future__ import annotations

import pytest

from blueprint.templates.template_model import (
    ParameterType,
    TaskDefinition,
    TaskTemplate,
    TemplateParameter,
)
from blueprint.templates.task_template_engine import (
    BUILTIN_TEMPLATES,
    TaskTemplateEngine,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _simple_template() -> TaskTemplate:
    return TaskTemplate(
        name="test_template",
        description="A test template",
        parameters=(
            TemplateParameter(name="name", param_type=ParameterType.STRING),
            TemplateParameter(name="priority", param_type=ParameterType.SELECT, options=("low", "high")),
            TemplateParameter(name="optional_flag", param_type=ParameterType.STRING, required=False, default=""),
        ),
        task_definitions=(
            TaskDefinition(title_template="Setup {name}", description_template="Setup for {name} with {priority}", effort=1.0, tags=("setup",)),
            TaskDefinition(title_template="Execute {name}", description_template="Execute task", effort=3.0),
            TaskDefinition(title_template="Optional step for {name}", condition="optional_flag", effort=0.5),
        ),
    )


# ---------------------------------------------------------------------------
# Template model
# ---------------------------------------------------------------------------


class TestTemplateModel:
    def test_parameter_types(self) -> None:
        assert ParameterType.STRING.value == "string"
        assert ParameterType.NUMBER.value == "number"
        assert ParameterType.DATE.value == "date"
        assert ParameterType.SELECT.value == "select"
        assert ParameterType.LIST.value == "list"

    def test_template_parameter_names(self) -> None:
        tmpl = _simple_template()
        assert tmpl.parameter_names() == {"name", "priority", "optional_flag"}

    def test_template_to_dict(self) -> None:
        tmpl = _simple_template()
        d = tmpl.to_dict()
        assert d["name"] == "test_template"
        assert len(d["parameters"]) == 3
        assert len(d["task_definitions"]) == 3


# ---------------------------------------------------------------------------
# Parameter substitution
# ---------------------------------------------------------------------------


class TestParameterSubstitution:
    def test_basic_substitution(self) -> None:
        engine = TaskTemplateEngine()
        tmpl = _simple_template()
        tasks = engine.generate(tmpl, {"name": "Widget", "priority": "high"})
        assert tasks[0]["title"] == "Setup Widget"
        assert tasks[0]["description"] == "Setup for Widget with high"

    def test_missing_required_param_raises(self) -> None:
        engine = TaskTemplateEngine()
        tmpl = _simple_template()
        with pytest.raises(ValueError, match="Missing required parameter"):
            engine.generate(tmpl, {"name": "Widget"})  # missing priority

    def test_default_value_used(self) -> None:
        engine = TaskTemplateEngine()
        tmpl = _simple_template()
        tasks = engine.generate(tmpl, {"name": "X", "priority": "low"})
        # optional_flag defaults to "" (falsy) so conditional task excluded
        assert len(tasks) == 2


# ---------------------------------------------------------------------------
# Conditional task inclusion
# ---------------------------------------------------------------------------


class TestConditionalInclusion:
    def test_condition_falsy_excludes_task(self) -> None:
        engine = TaskTemplateEngine()
        tmpl = _simple_template()
        tasks = engine.generate(tmpl, {"name": "A", "priority": "low", "optional_flag": ""})
        titles = [t["title"] for t in tasks]
        assert "Optional step for A" not in titles

    def test_condition_truthy_includes_task(self) -> None:
        engine = TaskTemplateEngine()
        tmpl = _simple_template()
        tasks = engine.generate(tmpl, {"name": "A", "priority": "low", "optional_flag": "yes"})
        titles = [t["title"] for t in tasks]
        assert "Optional step for A" in titles

    def test_builtin_bug_fix_conditional_hotfix(self) -> None:
        engine = TaskTemplateEngine()
        # Without needs_hotfix
        tasks = engine.generate("bug_fix_workflow", {
            "bug_title": "NPE", "severity": "high", "component": "auth",
        })
        titles = [t["title"] for t in tasks]
        assert "Deploy hotfix for NPE" not in titles

        # With needs_hotfix
        tasks = engine.generate("bug_fix_workflow", {
            "bug_title": "NPE", "severity": "high", "component": "auth",
            "needs_hotfix": "yes",
        })
        titles = [t["title"] for t in tasks]
        assert "Deploy hotfix for NPE" in titles


# ---------------------------------------------------------------------------
# Template composition
# ---------------------------------------------------------------------------


class TestTemplateComposition:
    def test_compose_two_templates(self) -> None:
        engine = TaskTemplateEngine()
        tmpl_a = TaskTemplate(
            name="a",
            parameters=(TemplateParameter(name="x", param_type=ParameterType.STRING),),
            task_definitions=(TaskDefinition(title_template="A-{x}"),),
        )
        tmpl_b = TaskTemplate(
            name="b",
            parameters=(TemplateParameter(name="x", param_type=ParameterType.STRING),),
            task_definitions=(TaskDefinition(title_template="B-{x}"),),
        )
        engine.register(tmpl_a)
        engine.register(tmpl_b)
        tasks = engine.compose(["a", "b"], {"x": "val"})
        assert len(tasks) == 2
        assert tasks[0]["title"] == "A-val"
        assert tasks[1]["title"] == "B-val"


# ---------------------------------------------------------------------------
# Template registration and lookup
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_register_and_retrieve(self) -> None:
        engine = TaskTemplateEngine()
        tmpl = _simple_template()
        engine.register(tmpl)
        assert engine.get_template("test_template") is tmpl

    def test_generate_by_name(self) -> None:
        engine = TaskTemplateEngine()
        tmpl = _simple_template()
        engine.register(tmpl)
        tasks = engine.generate("test_template", {"name": "N", "priority": "high"})
        assert len(tasks) >= 2

    def test_unknown_template_raises(self) -> None:
        engine = TaskTemplateEngine()
        with pytest.raises(ValueError, match="not found"):
            engine.generate("nonexistent", {})

    def test_builtin_templates_registered(self) -> None:
        engine = TaskTemplateEngine()
        names = engine.list_templates()
        assert "bug_fix_workflow" in names
        assert "feature_development" in names
        assert "release_checklist" in names
        assert "incident_response" in names


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_valid_template(self) -> None:
        engine = TaskTemplateEngine()
        tmpl = _simple_template()
        errors = engine.validate_template(tmpl)
        assert errors == []

    def test_invalid_placeholder(self) -> None:
        engine = TaskTemplateEngine()
        tmpl = TaskTemplate(
            name="bad",
            parameters=(TemplateParameter(name="x", param_type=ParameterType.STRING),),
            task_definitions=(
                TaskDefinition(title_template="Use {x} and {y}"),
            ),
        )
        errors = engine.validate_template(tmpl)
        assert any("undefined parameter 'y'" in e for e in errors)

    def test_invalid_condition(self) -> None:
        engine = TaskTemplateEngine()
        tmpl = TaskTemplate(
            name="bad_cond",
            parameters=(TemplateParameter(name="x", param_type=ParameterType.STRING),),
            task_definitions=(
                TaskDefinition(title_template="{x}", condition="missing_param"),
            ),
        )
        errors = engine.validate_template(tmpl)
        assert any("missing_param" in e for e in errors)


# ---------------------------------------------------------------------------
# Batch generation
# ---------------------------------------------------------------------------


class TestBatchGeneration:
    def test_batch_multiple_param_sets(self) -> None:
        engine = TaskTemplateEngine()
        tasks = engine.generate_batch("release_checklist", [
            {"version": "1.0.0"},
            {"version": "2.0.0"},
        ])
        v1_titles = [t["title"] for t in tasks if "1.0.0" in t["title"]]
        v2_titles = [t["title"] for t in tasks if "2.0.0" in t["title"]]
        assert len(v1_titles) > 0
        assert len(v2_titles) > 0
        assert len(tasks) == len(v1_titles) + len(v2_titles)


# ---------------------------------------------------------------------------
# Task output structure
# ---------------------------------------------------------------------------


class TestTaskOutput:
    def test_task_has_expected_keys(self) -> None:
        engine = TaskTemplateEngine()
        tasks = engine.generate("release_checklist", {"version": "3.0"})
        for task in tasks:
            assert "title" in task
            assert "description" in task
            assert "effort" in task
            assert "tags" in task
            assert "source_template" in task
            assert task["source_template"] == "release_checklist"
