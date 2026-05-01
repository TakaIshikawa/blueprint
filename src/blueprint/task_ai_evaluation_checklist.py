"""Derive AI evaluation checklist guidance for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


AIEvaluationRisk = Literal["high", "medium", "low", "not_ai_related"]
AISignal = Literal[
    "llm",
    "prompt",
    "embedding",
    "model_routing",
    "classifier",
    "retrieval",
    "summarization",
    "generated_output",
    "safety",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[AIEvaluationRisk, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
    "not_ai_related": 3,
}
_SIGNAL_ORDER: dict[AISignal, int] = {
    "llm": 0,
    "prompt": 1,
    "embedding": 2,
    "model_routing": 3,
    "classifier": 4,
    "retrieval": 5,
    "summarization": 6,
    "generated_output": 7,
    "safety": 8,
}
_PATH_SIGNAL_PATTERNS: tuple[tuple[AISignal, re.Pattern[str]], ...] = (
    ("llm", re.compile(r"(?:^|/)(?:llm|llms|ai|openai|anthropic)(?:/|$)|(?:llm|ai)[._-]", re.I)),
    ("prompt", re.compile(r"(?:^|/)(?:prompts?|prompt_templates?)(?:/|$)|prompt", re.I)),
    ("embedding", re.compile(r"(?:^|/)(?:embeddings?|vectors?|vectorstores?)(?:/|$)|embedding", re.I)),
    ("model_routing", re.compile(r"(?:^|/)(?:model[-_]?routing|routers?)(?:/|$)|model[-_]?router", re.I)),
    ("classifier", re.compile(r"(?:^|/)(?:classifiers?|classification)(?:/|$)|classifier", re.I)),
    ("retrieval", re.compile(r"(?:^|/)(?:rag|retrieval|retrievers?|search|indexes?)(?:/|$)|retrieval", re.I)),
    ("summarization", re.compile(r"(?:^|/)(?:summarizers?|summarization|summary)(?:/|$)|summar", re.I)),
    ("generated_output", re.compile(r"(?:^|/)(?:generators?|generation|outputs?)(?:/|$)|generated[-_]?output", re.I)),
    ("safety", re.compile(r"(?:^|/)(?:safety|moderation|guardrails?|refusals?)(?:/|$)|refusal", re.I)),
)
_TEXT_SIGNAL_PATTERNS: tuple[tuple[AISignal, re.Pattern[str]], ...] = (
    ("llm", re.compile(r"\b(?:llm|large language model|gpt-?4|gpt-?5|openai|anthropic|claude|model inference|chat completion)\b", re.I)),
    ("prompt", re.compile(r"\b(?:prompt|system message|few[- ]?shot|prompt template|instruction tuning)\b", re.I)),
    ("embedding", re.compile(r"\b(?:embedding|embeddings|vector store|vector database|semantic search|similarity search)\b", re.I)),
    ("model_routing", re.compile(r"\b(?:model routing|model router|fallback model|model fallback|route to model|provider fallback)\b", re.I)),
    ("classifier", re.compile(r"\b(?:classifier|classification|intent detection|sentiment model|toxicity score)\b", re.I)),
    ("retrieval", re.compile(r"\b(?:rag|retrieval|retriever|retrieved context|grounding|citation|context window)\b", re.I)),
    ("summarization", re.compile(r"\b(?:summari[sz]e|summari[sz]ation|summary generation|extractive summary)\b", re.I)),
    ("generated_output", re.compile(r"\b(?:ai[- ]?generated|generated output|generation quality|completion output|answer generation)\b", re.I)),
    ("safety", re.compile(r"\b(?:safety|refusal|refuse|moderation|jailbreak|prompt injection|harmful|policy violation|hallucination)\b", re.I)),
)
_HIGH_RISK_RE = re.compile(
    r"\b(?:safety|refusal|moderation|jailbreak|prompt injection|hallucination|customer[- ]?facing|"
    r"production|medical|legal|financial|pii|personal data|human review|fallback|model routing)\b",
    re.I,
)
_LOW_RISK_RE = re.compile(r"\b(?:docs?|documentation|readme|comment|test fixture|example|internal note)\b", re.I)


@dataclass(frozen=True, slots=True)
class TaskAIEvaluationChecklistRecord:
    """AI evaluation checklist guidance for one execution task."""

    task_id: str
    title: str
    evaluation_risk: AIEvaluationRisk
    ai_signals: tuple[AISignal, ...] = field(default_factory=tuple)
    required_artifacts: tuple[str, ...] = field(default_factory=tuple)
    recommended_test_cases: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "evaluation_risk": self.evaluation_risk,
            "ai_signals": list(self.ai_signals),
            "required_artifacts": list(self.required_artifacts),
            "recommended_test_cases": list(self.recommended_test_cases),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskAIEvaluationChecklistPlan:
    """Plan-level AI evaluation checklist review."""

    plan_id: str | None = None
    records: tuple[TaskAIEvaluationChecklistRecord, ...] = field(default_factory=tuple)
    ai_task_ids: tuple[str, ...] = field(default_factory=tuple)
    non_ai_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "ai_task_ids": list(self.ai_task_ids),
            "non_ai_task_ids": list(self.non_ai_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return AI evaluation records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the AI evaluation checklist as deterministic Markdown."""
        title = "# Task AI Evaluation Checklist"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No execution tasks were available for AI evaluation planning."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Risk | Signals | Required Artifacts | Recommended Test Cases |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{record.evaluation_risk} | "
                f"{_markdown_cell(', '.join(record.ai_signals) or 'none')} | "
                f"{_markdown_cell('; '.join(record.required_artifacts) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_test_cases) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_ai_evaluation_checklist_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskAIEvaluationChecklistPlan:
    """Build AI evaluation checklist guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (_task_record(task, index) for index, task in enumerate(tasks, start=1)),
            key=lambda record: (
                _RISK_ORDER[record.evaluation_risk],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    ai_task_ids = tuple(
        record.task_id for record in records if record.evaluation_risk != "not_ai_related"
    )
    non_ai_task_ids = tuple(
        record.task_id for record in records if record.evaluation_risk == "not_ai_related"
    )
    risk_counts = {
        risk: sum(1 for record in records if record.evaluation_risk == risk)
        for risk in _RISK_ORDER
    }
    signal_counts = {
        signal: sum(1 for record in records if signal in record.ai_signals)
        for signal in _SIGNAL_ORDER
    }
    return TaskAIEvaluationChecklistPlan(
        plan_id=plan_id,
        records=records,
        ai_task_ids=ai_task_ids,
        non_ai_task_ids=non_ai_task_ids,
        summary={
            "task_count": len(tasks),
            "ai_task_count": len(ai_task_ids),
            "non_ai_task_count": len(non_ai_task_ids),
            "risk_counts": risk_counts,
            "signal_counts": signal_counts,
        },
    )


def analyze_task_ai_evaluation_checklist(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskAIEvaluationChecklistPlan:
    """Compatibility alias for building AI evaluation checklists."""
    return build_task_ai_evaluation_checklist_plan(source)


def summarize_task_ai_evaluation_checklist(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskAIEvaluationChecklistPlan:
    """Compatibility alias for building AI evaluation checklists."""
    return build_task_ai_evaluation_checklist_plan(source)


def task_ai_evaluation_checklist_plan_to_dict(
    result: TaskAIEvaluationChecklistPlan,
) -> dict[str, Any]:
    """Serialize an AI evaluation checklist plan to a plain dictionary."""
    return result.to_dict()


task_ai_evaluation_checklist_plan_to_dict.__test__ = False


def task_ai_evaluation_checklist_plan_to_markdown(
    result: TaskAIEvaluationChecklistPlan,
) -> str:
    """Render an AI evaluation checklist plan as Markdown."""
    return result.to_markdown()


task_ai_evaluation_checklist_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[AISignal, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    high_risk_evidence: tuple[str, ...] = field(default_factory=tuple)
    validation_command_evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def has_ai_signal(self) -> bool:
        return bool(self.signals)


def _task_record(task: Mapping[str, Any], index: int) -> TaskAIEvaluationChecklistRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    risk = _evaluation_risk(signals)
    return TaskAIEvaluationChecklistRecord(
        task_id=task_id,
        title=title,
        evaluation_risk=risk,
        ai_signals=signals.signals if risk != "not_ai_related" else (),
        required_artifacts=_required_artifacts(signals, risk),
        recommended_test_cases=_recommended_test_cases(signals, risk),
        evidence=signals.evidence if risk != "not_ai_related" else (),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signals: set[AISignal] = set()
    evidence: list[str] = []
    high_risk_evidence: list[str] = []
    validation_command_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_evidence = f"files_or_modules: {path}"
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _PATH_SIGNAL_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                signals.add(signal)
                evidence.append(path_evidence)
        if _HIGH_RISK_RE.search(path_text):
            high_risk_evidence.append(path_evidence)

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for signal, pattern in _TEXT_SIGNAL_PATTERNS:
            if pattern.search(text):
                signals.add(signal)
                evidence.append(snippet)
        if _HIGH_RISK_RE.search(text):
            high_risk_evidence.append(snippet)

    for command in _validation_commands(task):
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _TEXT_SIGNAL_PATTERNS:
            if pattern.search(command) or pattern.search(command_text):
                signals.add(signal)
                evidence.append(snippet)
                validation_command_evidence.append(snippet)
        if _HIGH_RISK_RE.search(command) or _HIGH_RISK_RE.search(command_text):
            high_risk_evidence.append(snippet)

    ordered_signals = tuple(signal for signal in _SIGNAL_ORDER if signal in signals)
    return _Signals(
        signals=ordered_signals,
        evidence=tuple(_dedupe(evidence)),
        high_risk_evidence=tuple(_dedupe(high_risk_evidence)),
        validation_command_evidence=tuple(_dedupe(validation_command_evidence)),
    )


def _evaluation_risk(signals: _Signals) -> AIEvaluationRisk:
    if not signals.has_ai_signal:
        return "not_ai_related"
    if signals.high_risk_evidence or {"safety", "model_routing"} & set(signals.signals):
        return "high"
    if {"llm", "prompt", "retrieval", "summarization", "generated_output", "classifier"} & set(
        signals.signals
    ):
        return "medium"
    return "low"


def _required_artifacts(signals: _Signals, risk: AIEvaluationRisk) -> tuple[str, ...]:
    if risk == "not_ai_related":
        return ()
    artifacts = [
        "Golden evaluation dataset with representative inputs and expected outputs.",
        "Regression prompt suite covering current and previously fixed behaviors.",
        "Evaluation report with pass/fail thresholds, scorer notes, and sampled failures.",
        "Latency and cost tracking for model calls on the evaluation set.",
    ]
    if "retrieval" in signals.signals:
        artifacts.append(
            "Retrieval grounding dataset with expected sources, citation coverage, and irrelevant-context cases."
        )
    if "classifier" in signals.signals:
        artifacts.append(
            "Classifier confusion matrix with threshold justification and edge-case labels."
        )
    if "embedding" in signals.signals:
        artifacts.append(
            "Embedding similarity fixtures with nearest-neighbor expectations and drift checks."
        )
    if "model_routing" in signals.signals:
        artifacts.append(
            "Model fallback matrix covering provider errors, degraded models, and routing decisions."
        )
    if risk == "high" or "safety" in signals.signals:
        artifacts.extend(
            [
                "Safety and refusal case set covering prompt injection, harmful requests, and policy boundaries.",
                "Human review gate documenting reviewer, sampled outputs, and launch approval criteria.",
            ]
        )
    return tuple(_dedupe(artifacts))


def _recommended_test_cases(signals: _Signals, risk: AIEvaluationRisk) -> tuple[str, ...]:
    if risk == "not_ai_related":
        return ()
    cases = [
        "Golden prompts produce expected structured outputs within accepted quality thresholds.",
        "Regression prompts preserve behavior for previously accepted examples.",
        "Hallucination checks reject unsupported claims or require cited source evidence.",
        "Latency and cost checks stay within the task budget for representative load.",
    ]
    if "retrieval" in signals.signals:
        cases.append("Retrieval tests cover relevant, missing, stale, and conflicting source documents.")
    if "summarization" in signals.signals:
        cases.append("Summarization tests preserve required facts and avoid invented details.")
    if "classifier" in signals.signals:
        cases.append("Classifier tests cover boundary, ambiguous, adversarial, and out-of-domain inputs.")
    if "embedding" in signals.signals:
        cases.append("Embedding tests catch semantic drift, duplicate matches, and low-similarity negatives.")
    if "model_routing" in signals.signals:
        cases.append("Fallback tests cover model timeout, provider error, low-confidence, and retry paths.")
    if risk == "high" or "safety" in signals.signals:
        cases.append("Safety tests verify refusals, jailbreak resistance, and escalation to human review.")
    return tuple(_dedupe(cases))


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
                if any(pattern.search(key_text) for _, pattern in _TEXT_SIGNAL_PATTERNS):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for _, pattern in _TEXT_SIGNAL_PATTERNS):
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
    "AIEvaluationRisk",
    "AISignal",
    "TaskAIEvaluationChecklistPlan",
    "TaskAIEvaluationChecklistRecord",
    "analyze_task_ai_evaluation_checklist",
    "build_task_ai_evaluation_checklist_plan",
    "summarize_task_ai_evaluation_checklist",
    "task_ai_evaluation_checklist_plan_to_dict",
    "task_ai_evaluation_checklist_plan_to_markdown",
]
