"""Assess execution tasks for prompt-injection readiness."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


PromptInjectionSurface = Literal[
    "llm",
    "agent",
    "retrieval",
    "tool_use",
    "user_uploaded_content",
    "external_documents",
    "prompt_template",
]
PromptInjectionControl = Literal[
    "input_source_isolation",
    "prompt_boundary_tests",
    "tool_allowlist",
    "retrieval_citation_checks",
    "output_validation",
    "secret_exfiltration_tests",
    "human_review_path",
]
PromptInjectionRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: dict[PromptInjectionSurface, int] = {
    "llm": 0,
    "agent": 1,
    "retrieval": 2,
    "tool_use": 3,
    "user_uploaded_content": 4,
    "external_documents": 5,
    "prompt_template": 6,
}
_CONTROL_ORDER: dict[PromptInjectionControl, int] = {
    "input_source_isolation": 0,
    "prompt_boundary_tests": 1,
    "tool_allowlist": 2,
    "retrieval_citation_checks": 3,
    "output_validation": 4,
    "secret_exfiltration_tests": 5,
    "human_review_path": 6,
}
_RISK_ORDER: dict[PromptInjectionRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_PATTERNS: dict[PromptInjectionSurface, tuple[re.Pattern[str], ...]] = {
    "llm": (
        re.compile(
            r"\b(?:llm|large language model|gpt-?4|gpt-?5|openai|anthropic|claude|chat completion|model inference)\b",
            re.I,
        ),
    ),
    "agent": (
        re.compile(
            r"\b(?:agent|agents|autonomous|planner|tool-using assistant|multi[- ]?step reasoning)\b",
            re.I,
        ),
    ),
    "retrieval": (
        re.compile(
            r"\b(?:rag|retrieval|retriever|retrieved context|grounding|vector store|vector database|semantic search|embedding|citation)\b",
            re.I,
        ),
    ),
    "tool_use": (
        re.compile(
            r"\b(?:tool call|tool calls|function call|function calling|plugin|connector|execute tool|tool execution|shell command|api action|write files?)\b",
            re.I,
        ),
    ),
    "user_uploaded_content": (
        re.compile(
            r"\b(?:user[- ]?uploaded|upload(?:ed|s)?|file upload|attachment|customer document|user document|imported file)\b",
            re.I,
        ),
    ),
    "external_documents": (
        re.compile(
            r"\b(?:external document|external documents|third[- ]party content|web page|web pages|website|url|pdf|html|email body|support ticket|slack message|confluence|notion)\b",
            re.I,
        ),
    ),
    "prompt_template": (
        re.compile(
            r"\b(?:prompt template|prompt templates|system prompt|system message|developer message|instruction template|few[- ]?shot|prompt)\b",
            re.I,
        ),
    ),
}
_PATH_SURFACE_PATTERNS: dict[PromptInjectionSurface, tuple[re.Pattern[str], ...]] = {
    "llm": (re.compile(r"(?:^|/)(?:llm|llms|ai|openai|anthropic)(?:/|$)|(?:llm|ai)[._-]", re.I),),
    "agent": (re.compile(r"(?:^|/)(?:agents?|planners?)(?:/|$)|agent", re.I),),
    "retrieval": (
        re.compile(
            r"(?:^|/)(?:rag|retrieval|retrievers?|vectorstores?|indexes?|search)(?:/|$)|retrieval|embedding",
            re.I,
        ),
    ),
    "tool_use": (
        re.compile(
            r"(?:^|/)(?:tools?|plugins?|connectors?|actions?)(?:/|$)|tool[-_]?call|function[-_]?call",
            re.I,
        ),
    ),
    "user_uploaded_content": (
        re.compile(r"(?:^|/)(?:uploads?|attachments?|imports?)(?:/|$)|upload", re.I),
    ),
    "external_documents": (
        re.compile(
            r"(?:^|/)(?:documents?|docs?|sources?|web|emails?|tickets?)(?:/|$)|external", re.I
        ),
    ),
    "prompt_template": (re.compile(r"(?:^|/)(?:prompts?|prompt_templates?)(?:/|$)|prompt", re.I),),
}
_UNTRUSTED_INPUT_RE = re.compile(
    r"\b(?:untrusted|external|third[- ]party|user[- ]?uploaded|upload(?:ed|s)?|attachment|"
    r"customer document|user document|web page|website|url|pdf|html|email body|support ticket|"
    r"slack message|confluence|notion|retrieved context|rag|scraped|imported file)\b",
    re.I,
)
_CONTROL_PATTERNS: dict[PromptInjectionControl, tuple[re.Pattern[str], ...]] = {
    "input_source_isolation": (
        re.compile(
            r"\b(?:input source isolation|source isolation|isolate untrusted|separate trusted and untrusted|content boundary|trust boundary)\b",
            re.I,
        ),
    ),
    "prompt_boundary_tests": (
        re.compile(
            r"\b(?:prompt boundary tests?|boundary tests?|prompt injection tests?|jailbreak tests?|instruction hierarchy tests?)\b",
            re.I,
        ),
    ),
    "tool_allowlist": (
        re.compile(
            r"\b(?:tool allowlist|allowlisted tools?|allowed tools?|tool permissions?|least privilege tools?|deny by default tools?)\b",
            re.I,
        ),
    ),
    "retrieval_citation_checks": (
        re.compile(
            r"\b(?:retrieval citation checks?|citation checks?|source citation|grounding checks?|cited sources?|source verification)\b",
            re.I,
        ),
    ),
    "output_validation": (
        re.compile(
            r"\b(?:output validation|validate outputs?|schema validation|structured output validation|post[- ]?generation validation|sanitize outputs?)\b",
            re.I,
        ),
    ),
    "secret_exfiltration_tests": (
        re.compile(
            r"\b(?:secret exfiltration tests?|exfiltration tests?|secrets? leakage|credential leakage|prompt leak tests?|data exfiltration tests?)\b",
            re.I,
        ),
    ),
    "human_review_path": (
        re.compile(
            r"\b(?:human review path|human review|manual review|review queue|escalation path|human approval|human[- ]in[- ]the[- ]loop)\b",
            re.I,
        ),
    ),
}


@dataclass(frozen=True, slots=True)
class TaskPromptInjectionReadinessRecommendation:
    """Prompt-injection readiness guidance for one execution task."""

    task_id: str
    title: str
    ai_surfaces: tuple[PromptInjectionSurface, ...] = field(default_factory=tuple)
    untrusted_input_signals: tuple[str, ...] = field(default_factory=tuple)
    missing_controls: tuple[PromptInjectionControl, ...] = field(default_factory=tuple)
    risk_level: PromptInjectionRiskLevel = "low"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "ai_surfaces": list(self.ai_surfaces),
            "untrusted_input_signals": list(self.untrusted_input_signals),
            "missing_controls": list(self.missing_controls),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskPromptInjectionReadinessPlan:
    """Plan-level prompt-injection readiness review."""

    plan_id: str | None = None
    recommendations: tuple[TaskPromptInjectionReadinessRecommendation, ...] = field(
        default_factory=tuple
    )
    ai_task_ids: tuple[str, ...] = field(default_factory=tuple)
    suppressed_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [record.to_dict() for record in self.recommendations],
            "ai_task_ids": list(self.ai_task_ids),
            "suppressed_task_ids": list(self.suppressed_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return prompt-injection recommendations as plain dictionaries."""
        return [record.to_dict() for record in self.recommendations]

    @property
    def records(self) -> tuple[TaskPromptInjectionReadinessRecommendation, ...]:
        """Compatibility view matching planners that name task rows records."""
        return self.recommendations

    def to_markdown(self) -> str:
        """Render the prompt-injection readiness plan as deterministic Markdown."""
        title = "# Task Prompt Injection Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('ai_task_count', 0)} AI-exposed tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(suppressed: {self.summary.get('suppressed_task_count', 0)})."
            ),
        ]
        if not self.recommendations:
            lines.extend(["", "No prompt-injection readiness recommendations were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Risk | AI Surfaces | Untrusted Input Signals | Missing Controls | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.ai_surfaces) or 'none')} | "
                f"{_markdown_cell('; '.join(record.untrusted_input_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_controls) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_prompt_injection_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskPromptInjectionReadinessPlan:
    """Build task-level prompt-injection readiness recommendations."""
    plan_id, tasks = _source_payload(source)
    all_records = [_task_recommendation(task, index) for index, task in enumerate(tasks, start=1)]
    recommendations = tuple(
        sorted(
            (record for record in all_records if record is not None),
            key=lambda record: (
                _RISK_ORDER[record.risk_level],
                len(record.missing_controls),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    ai_task_ids = tuple(record.task_id for record in recommendations)
    suppressed_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in set(ai_task_ids)
    )
    risk_counts = {
        risk: sum(1 for record in recommendations if record.risk_level == risk)
        for risk in _RISK_ORDER
    }
    surface_counts = {
        surface: sum(1 for record in recommendations if surface in record.ai_surfaces)
        for surface in _SURFACE_ORDER
    }
    missing_control_counts = {
        control: sum(1 for record in recommendations if control in record.missing_controls)
        for control in _CONTROL_ORDER
    }
    return TaskPromptInjectionReadinessPlan(
        plan_id=plan_id,
        recommendations=recommendations,
        ai_task_ids=ai_task_ids,
        suppressed_task_ids=suppressed_task_ids,
        summary={
            "task_count": len(tasks),
            "ai_task_count": len(ai_task_ids),
            "suppressed_task_count": len(suppressed_task_ids),
            "risk_counts": risk_counts,
            "surface_counts": surface_counts,
            "missing_control_counts": missing_control_counts,
        },
    )


def summarize_task_prompt_injection_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskPromptInjectionReadinessPlan:
    """Compatibility alias for building prompt-injection readiness plans."""
    return build_task_prompt_injection_readiness_plan(source)


def task_prompt_injection_readiness_plan_to_dict(
    result: TaskPromptInjectionReadinessPlan,
) -> dict[str, Any]:
    """Serialize a prompt-injection readiness plan to a plain dictionary."""
    return result.to_dict()


task_prompt_injection_readiness_plan_to_dict.__test__ = False


def task_prompt_injection_readiness_plan_to_markdown(
    result: TaskPromptInjectionReadinessPlan,
) -> str:
    """Render a prompt-injection readiness plan as Markdown."""
    return result.to_markdown()


task_prompt_injection_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[PromptInjectionSurface, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    untrusted_input_signals: tuple[str, ...] = field(default_factory=tuple)

    @property
    def has_ai_surface(self) -> bool:
        return bool(self.surfaces)


def _task_recommendation(
    task: Mapping[str, Any],
    index: int,
) -> TaskPromptInjectionReadinessRecommendation | None:
    signals = _signals(task)
    if not signals.has_ai_surface:
        return None

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    present_controls = _present_controls(task)
    missing_controls = tuple(
        control for control in _CONTROL_ORDER if control not in present_controls
    )
    return TaskPromptInjectionReadinessRecommendation(
        task_id=task_id,
        title=title,
        ai_surfaces=signals.surfaces,
        untrusted_input_signals=signals.untrusted_input_signals,
        missing_controls=missing_controls,
        risk_level=_risk_level(signals, missing_controls),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surfaces: set[PromptInjectionSurface] = set()
    evidence: list[str] = []
    untrusted: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        snippet = f"files_or_modules: {path}"
        for surface, patterns in _PATH_SURFACE_PATTERNS.items():
            if any(pattern.search(normalized) or pattern.search(path_text) for pattern in patterns):
                surfaces.add(surface)
                evidence.append(snippet)
        if _UNTRUSTED_INPUT_RE.search(path_text):
            untrusted.append(snippet)

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for surface, patterns in _SURFACE_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                surfaces.add(surface)
                evidence.append(snippet)
        if _UNTRUSTED_INPUT_RE.search(text):
            untrusted.append(snippet)

    for command in _validation_commands(task):
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        for surface, patterns in _SURFACE_PATTERNS.items():
            if any(pattern.search(command) or pattern.search(command_text) for pattern in patterns):
                surfaces.add(surface)
                evidence.append(snippet)
        if _UNTRUSTED_INPUT_RE.search(command) or _UNTRUSTED_INPUT_RE.search(command_text):
            untrusted.append(snippet)

    ordered_surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surfaces)
    return _Signals(
        surfaces=ordered_surfaces,
        evidence=tuple(_dedupe(evidence)),
        untrusted_input_signals=tuple(_dedupe(untrusted)),
    )


def _present_controls(task: Mapping[str, Any]) -> set[PromptInjectionControl]:
    present: set[PromptInjectionControl] = set()
    for _source_field, text in _candidate_texts(task):
        for control, patterns in _CONTROL_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                present.add(control)
    for command in _validation_commands(task):
        for control, patterns in _CONTROL_PATTERNS.items():
            if any(pattern.search(command) for pattern in patterns):
                present.add(control)
    return present


def _risk_level(
    signals: _Signals,
    missing_controls: tuple[PromptInjectionControl, ...],
) -> PromptInjectionRiskLevel:
    if not missing_controls:
        return "low"
    if "tool_use" in signals.surfaces:
        return "high"
    high_risk_surface = bool(
        {"user_uploaded_content", "external_documents"} & set(signals.surfaces)
    )
    if signals.untrusted_input_signals and high_risk_surface:
        return "high"
    if len(missing_controls) >= 4:
        return "medium"
    return "low"


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(
                    pattern.search(key_text)
                    for patterns in (*_SURFACE_PATTERNS.values(), *_CONTROL_PATTERNS.values())
                    for pattern in patterns
                ):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(
                pattern.search(key_text)
                for patterns in (*_SURFACE_PATTERNS.values(), *_CONTROL_PATTERNS.values())
                for pattern in patterns
            ):
                texts.append((field, str(key)))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "PromptInjectionControl",
    "PromptInjectionRiskLevel",
    "PromptInjectionSurface",
    "TaskPromptInjectionReadinessPlan",
    "TaskPromptInjectionReadinessRecommendation",
    "build_task_prompt_injection_readiness_plan",
    "summarize_task_prompt_injection_readiness",
    "task_prompt_injection_readiness_plan_to_dict",
    "task_prompt_injection_readiness_plan_to_markdown",
]
