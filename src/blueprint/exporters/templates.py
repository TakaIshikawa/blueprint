"""Markdown template loading and rendering for prompt exporters."""

from pathlib import Path
from string import Formatter
from typing import Any

from blueprint.config import Config, get_config


class TemplateRenderError(ValueError):
    """Raised when a configured export template cannot be rendered."""


class MarkdownTemplateRenderer:
    """Render configurable Markdown templates for prompt exporters."""

    def __init__(
        self,
        target: str,
        config: Config | None = None,
        default_task_template: str | None = None,
    ):
        self.target = target
        self.config = config or get_config()
        self.default_task_template = default_task_template or DEFAULT_TASK_TEMPLATE

    def render(
        self,
        default_content: str,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> str:
        """Render configured template content or return the built-in content."""
        template_path = self._configured_template_path("path")
        if template_path is None:
            return default_content

        template = self._load_configured_template(template_path, "path")
        context = {
            "brief": brief,
            "plan": plan,
            "tasks": self._render_tasks(plan, brief),
        }
        return render_template(template, context, str(template_path))

    def _render_tasks(self, plan: dict[str, Any], brief: dict[str, Any]) -> str:
        task_template_path = self._configured_template_path("task_path")
        if task_template_path is None:
            task_template = self.default_task_template
            template_name = f"{self.target} built-in task template"
        else:
            task_template = self._load_configured_template(
                task_template_path,
                "task_path",
            )
            template_name = str(task_template_path)

        rendered_tasks = []
        for task in plan.get("tasks", []):
            context = {"brief": brief, "plan": plan, "task": task}
            rendered_tasks.append(render_template(task_template, context, template_name))
        return "\n\n".join(rendered_tasks)

    def _configured_template_path(self, key: str) -> Path | None:
        configured = self._configured_template_value(key)
        if configured is None:
            return None
        if not isinstance(configured, str) or not configured.strip():
            raise TemplateRenderError(
                f"exports.templates.{self._config_target_name()}.{key} must be a non-empty string"
            )

        path = Path(configured).expanduser()
        if not path.is_absolute() and self.config.config_path:
            path = Path(self.config.config_path).expanduser().parent / path
        return path

    def _configured_template_value(self, key: str) -> Any:
        target = self._config_target_name()
        if key == "path":
            value = self.config.get(f"exports.templates.{target}")
            if isinstance(value, str):
                return value

        return (
            self.config.get(f"exports.templates.{target}.{key}")
            or self.config.get(f"exports.template_paths.{target}.{key}")
            or (self.config.get(f"exports.template_paths.{target}") if key == "path" else None)
        )

    def _load_configured_template(self, path: Path, key: str) -> str:
        if not path.exists():
            raise TemplateRenderError(
                f"Configured template {key} for {self.target} does not exist: {path}"
            )
        if not path.is_file():
            raise TemplateRenderError(
                f"Configured template {key} for {self.target} is not a file: {path}"
            )
        return path.read_text()

    def _config_target_name(self) -> str:
        return self.target.replace("-", "_")


DEFAULT_TASK_TEMPLATE = """### {task.title}

{task.description}

Files:
{task.files_or_modules}

Acceptance Criteria:
{task.acceptance_criteria}"""


def render_template(
    template: str,
    context: dict[str, Any],
    template_name: str,
) -> str:
    """Render a template with dotted placeholders such as {brief.title}."""
    rendered = []
    formatter = Formatter()

    for literal_text, field_name, format_spec, conversion in formatter.parse(template):
        rendered.append(literal_text)
        if field_name is None:
            continue
        if not field_name:
            raise TemplateRenderError(f"Missing placeholder name in template '{template_name}'")
        if format_spec:
            raise TemplateRenderError(
                f"Unsupported format specifier for placeholder '{{{field_name}:{format_spec}}}' "
                f"in template '{template_name}'"
            )
        if conversion:
            raise TemplateRenderError(
                f"Unsupported conversion for placeholder '{{{field_name}!{conversion}}}' "
                f"in template '{template_name}'"
            )

        value = _resolve_placeholder(field_name, context, template_name)
        rendered.append(_stringify_value(value))

    return "".join(rendered)


def _resolve_placeholder(
    placeholder: str,
    context: dict[str, Any],
    template_name: str,
) -> Any:
    current: Any = context
    for part in placeholder.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue

        raise TemplateRenderError(
            f"Missing placeholder '{{{placeholder}}}' in template '{template_name}'"
        )
    return current


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        if not value:
            return ""
        return "\n".join(f"- {_stringify_scalar(item)}" for item in value)
    if isinstance(value, dict):
        if not value:
            return ""
        return "\n".join(f"- {key}: {_stringify_scalar(item)}" for key, item in value.items())
    return str(value)


def _stringify_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return _stringify_value(value).replace("\n", "; ")
    return str(value)
