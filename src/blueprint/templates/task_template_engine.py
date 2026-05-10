"""Task template engine with parameterized generation.

Generates tasks from reusable templates by substituting parameter values
into title and description templates. Supports conditional task inclusion,
template composition, validation, and batch generation.
"""

from __future__ import annotations

import re
from typing import Any

from blueprint.templates.template_model import (
    ParameterType,
    TaskDefinition,
    TaskTemplate,
    TemplateParameter,
)


# ---------------------------------------------------------------------------
# Built-in templates
# ---------------------------------------------------------------------------

BUG_FIX_WORKFLOW = TaskTemplate(
    name="bug_fix_workflow",
    description="Standard bug fix workflow",
    parameters=(
        TemplateParameter(name="bug_title", param_type=ParameterType.STRING, description="Bug title"),
        TemplateParameter(name="severity", param_type=ParameterType.SELECT, options=("low", "medium", "high", "critical")),
        TemplateParameter(name="component", param_type=ParameterType.STRING, description="Affected component"),
        TemplateParameter(name="needs_hotfix", param_type=ParameterType.STRING, required=False, default=""),
    ),
    task_definitions=(
        TaskDefinition(title_template="Reproduce bug: {bug_title}", description_template="Reproduce the bug in {component}", effort=1.0, tags=("bug",)),
        TaskDefinition(title_template="Fix: {bug_title}", description_template="Fix {severity} bug in {component}", effort=3.0, tags=("bug", "fix")),
        TaskDefinition(title_template="Write regression test for {bug_title}", description_template="Add test covering {bug_title} in {component}", effort=2.0, tags=("bug", "test")),
        TaskDefinition(title_template="Deploy hotfix for {bug_title}", description_template="Deploy hotfix for {severity} bug", effort=1.0, tags=("bug", "deploy"), condition="needs_hotfix"),
    ),
)

FEATURE_DEVELOPMENT = TaskTemplate(
    name="feature_development",
    description="Standard feature development workflow",
    parameters=(
        TemplateParameter(name="feature_name", param_type=ParameterType.STRING),
        TemplateParameter(name="target_date", param_type=ParameterType.DATE, required=False, default=""),
        TemplateParameter(name="needs_migration", param_type=ParameterType.STRING, required=False, default=""),
    ),
    task_definitions=(
        TaskDefinition(title_template="Design: {feature_name}", description_template="Design specification for {feature_name}", effort=3.0, tags=("design",)),
        TaskDefinition(title_template="Implement: {feature_name}", description_template="Implement {feature_name}", effort=8.0, tags=("implementation",)),
        TaskDefinition(title_template="Write tests for {feature_name}", description_template="Unit and integration tests for {feature_name}", effort=3.0, tags=("test",)),
        TaskDefinition(title_template="Database migration for {feature_name}", description_template="Create migration scripts", effort=2.0, tags=("migration",), condition="needs_migration"),
        TaskDefinition(title_template="Documentation for {feature_name}", description_template="Update docs for {feature_name}", effort=2.0, tags=("docs",)),
    ),
)

RELEASE_CHECKLIST = TaskTemplate(
    name="release_checklist",
    description="Release preparation checklist",
    parameters=(
        TemplateParameter(name="version", param_type=ParameterType.STRING),
        TemplateParameter(name="release_date", param_type=ParameterType.DATE, required=False, default=""),
    ),
    task_definitions=(
        TaskDefinition(title_template="Freeze code for {version}", effort=0.5, tags=("release",)),
        TaskDefinition(title_template="Run full test suite for {version}", effort=2.0, tags=("release", "test")),
        TaskDefinition(title_template="Update changelog for {version}", effort=1.0, tags=("release", "docs")),
        TaskDefinition(title_template="Tag release {version}", effort=0.5, tags=("release",)),
        TaskDefinition(title_template="Deploy {version} to staging", effort=1.0, tags=("release", "deploy")),
        TaskDefinition(title_template="Deploy {version} to production", effort=1.0, tags=("release", "deploy")),
    ),
)

INCIDENT_RESPONSE = TaskTemplate(
    name="incident_response",
    description="Incident response workflow",
    parameters=(
        TemplateParameter(name="incident_title", param_type=ParameterType.STRING),
        TemplateParameter(name="severity", param_type=ParameterType.SELECT, options=("sev1", "sev2", "sev3")),
        TemplateParameter(name="needs_postmortem", param_type=ParameterType.STRING, required=False, default="yes"),
    ),
    task_definitions=(
        TaskDefinition(title_template="Triage: {incident_title}", description_template="Triage {severity} incident", effort=0.5, tags=("incident",)),
        TaskDefinition(title_template="Mitigate: {incident_title}", description_template="Apply mitigation for {incident_title}", effort=2.0, tags=("incident",)),
        TaskDefinition(title_template="Root cause analysis: {incident_title}", effort=3.0, tags=("incident", "rca")),
        TaskDefinition(title_template="Write postmortem for {incident_title}", effort=2.0, tags=("incident", "postmortem"), condition="needs_postmortem"),
    ),
)

BUILTIN_TEMPLATES: dict[str, TaskTemplate] = {
    "bug_fix_workflow": BUG_FIX_WORKFLOW,
    "feature_development": FEATURE_DEVELOPMENT,
    "release_checklist": RELEASE_CHECKLIST,
    "incident_response": INCIDENT_RESPONSE,
}

# Regex for {param_name} placeholders
_PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class TaskTemplateEngine:
    """Generate tasks from parameterized templates."""

    def __init__(self) -> None:
        self._templates: dict[str, TaskTemplate] = dict(BUILTIN_TEMPLATES)

    def register(self, template: TaskTemplate) -> None:
        """Register a custom template."""
        self._templates[template.name] = template

    def get_template(self, name: str) -> TaskTemplate | None:
        """Retrieve a registered template by name."""
        return self._templates.get(name)

    def list_templates(self) -> list[str]:
        """Return names of all registered templates."""
        return sorted(self._templates.keys())

    def validate_template(self, template: TaskTemplate) -> list[str]:
        """Validate that a template's task definitions only reference defined parameters.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors: list[str] = []
        param_names = template.parameter_names()
        for i, task_def in enumerate(template.task_definitions):
            for placeholder in _PLACEHOLDER_RE.findall(task_def.title_template):
                if placeholder not in param_names:
                    errors.append(
                        f"Task {i} title references undefined parameter '{placeholder}'"
                    )
            for placeholder in _PLACEHOLDER_RE.findall(task_def.description_template):
                if placeholder not in param_names:
                    errors.append(
                        f"Task {i} description references undefined parameter '{placeholder}'"
                    )
            if task_def.condition and task_def.condition not in param_names:
                errors.append(
                    f"Task {i} condition references undefined parameter '{task_def.condition}'"
                )
        return errors

    def generate(
        self,
        template: TaskTemplate | str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Generate tasks from a template with parameter substitution.

        Args:
            template: A ``TaskTemplate`` instance or the name of a registered template.
            params: Parameter values keyed by parameter name.

        Returns:
            List of generated task dictionaries.

        Raises:
            ValueError: If required parameters are missing or template not found.
        """
        tmpl = self._resolve_template(template)
        resolved_params = self._resolve_params(tmpl, params)
        tasks: list[dict[str, Any]] = []

        for task_def in tmpl.task_definitions:
            if not _condition_met(task_def, resolved_params):
                continue
            tasks.append(_render_task(task_def, resolved_params, tmpl.name))

        return tasks

    def generate_batch(
        self,
        template: TaskTemplate | str,
        param_sets: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Generate tasks for multiple parameter sets.

        Args:
            template: Template or template name.
            param_sets: List of parameter dictionaries.

        Returns:
            Flat list of all generated tasks.
        """
        all_tasks: list[dict[str, Any]] = []
        for params in param_sets:
            all_tasks.extend(self.generate(template, params))
        return all_tasks

    def compose(
        self,
        templates: list[TaskTemplate | str],
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Compose multiple templates into a single task list.

        Args:
            templates: List of templates or template names.
            params: Shared parameter values.

        Returns:
            Combined list of generated tasks.
        """
        combined: list[dict[str, Any]] = []
        for tmpl in templates:
            combined.extend(self.generate(tmpl, params))
        return combined

    def _resolve_template(self, template: TaskTemplate | str) -> TaskTemplate:
        if isinstance(template, str):
            resolved = self._templates.get(template)
            if resolved is None:
                raise ValueError(f"Template '{template}' not found")
            return resolved
        return template

    def _resolve_params(
        self, template: TaskTemplate, params: dict[str, Any]
    ) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for p in template.parameters:
            if p.name in params:
                resolved[p.name] = params[p.name]
            elif not p.required and p.default is not None:
                resolved[p.name] = p.default
            elif p.required:
                raise ValueError(f"Missing required parameter: '{p.name}'")
        return resolved


def _condition_met(task_def: TaskDefinition, params: dict[str, Any]) -> bool:
    if task_def.condition is None:
        return True
    value = params.get(task_def.condition, "")
    return bool(value)


def _render_task(
    task_def: TaskDefinition,
    params: dict[str, Any],
    template_name: str,
) -> dict[str, Any]:
    title = _substitute(task_def.title_template, params)
    description = _substitute(task_def.description_template, params)
    return {
        "title": title,
        "description": description,
        "effort": task_def.effort,
        "tags": list(task_def.tags),
        "source_template": template_name,
    }


def _substitute(template_str: str, params: dict[str, Any]) -> str:
    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        return str(params.get(key, match.group(0)))
    return _PLACEHOLDER_RE.sub(_replace, template_str)


__all__ = [
    "BUILTIN_TEMPLATES",
    "TaskTemplateEngine",
]
