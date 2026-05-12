"""Assess readiness for execution tasks that change producer/consumer data contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DataContractSignal = Literal[
    "event_contract",
    "payload_contract",
    "api_contract",
    "warehouse_contract",
    "stream_contract",
]
DataContractReadinessRequirement = Literal[
    "schema_ownership",
    "compatibility_mode",
    "fixture_updates",
    "consumer_notification",
    "contract_test_coverage",
]
DataContractReadinessRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[DataContractSignal, ...] = (
    "event_contract",
    "payload_contract",
    "api_contract",
    "warehouse_contract",
    "stream_contract",
)
_REQUIREMENT_ORDER: tuple[DataContractReadinessRequirement, ...] = (
    "schema_ownership",
    "compatibility_mode",
    "fixture_updates",
    "consumer_notification",
    "contract_test_coverage",
)
_RISK_ORDER: dict[DataContractReadinessRisk, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_PATTERNS: dict[DataContractSignal, re.Pattern[str]] = {
    "event_contract": re.compile(
        r"\b(?:event contract|event schema|event payload|domain event|integration event|"
        r"message contract|webhook event|producer event|consumer event)\b",
        re.I,
    ),
    "payload_contract": re.compile(
        r"\b(?:payload contract|payload schema|request payload|response payload|json payload|"
        r"message payload|contract payload|schema payload)\b",
        re.I,
    ),
    "api_contract": re.compile(
        r"\b(?:api contract|openapi contract|endpoint contract|request contract|response contract|"
        r"client contract|producer api|consumer api)\b",
        re.I,
    ),
    "warehouse_contract": re.compile(
        r"\b(?:warehouse contract|warehouse schema|analytics contract|data mart contract|"
        r"dbt contract|table contract|column contract|dataset contract|warehouse table)\b",
        re.I,
    ),
    "stream_contract": re.compile(
        r"\b(?:stream contract|stream schema|kafka schema|topic schema|topic contract|"
        r"pubsub schema|pub/sub schema|schema registry|avro schema|protobuf schema)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[DataContractSignal, re.Pattern[str]] = {
    "event_contract": re.compile(r"events?|messages?|webhooks?", re.I),
    "payload_contract": re.compile(r"payloads?|schemas?", re.I),
    "api_contract": re.compile(r"api|openapi|swagger", re.I),
    "warehouse_contract": re.compile(r"warehouse|dbt|analytics|datasets?|marts?|tables?", re.I),
    "stream_contract": re.compile(r"kafka|streams?|topics?|schema[-_]?registry|avro|proto", re.I),
}
_REQUIREMENT_PATTERNS: dict[DataContractReadinessRequirement, re.Pattern[str]] = {
    "schema_ownership": re.compile(
        r"\b(?:schema owner|schema ownership|contract owner|data owner|producer owner|"
        r"owning team|owner team|schema steward|domain owner)\b",
        re.I,
    ),
    "compatibility_mode": re.compile(
        r"\b(?:compatibility mode|backward compatible|backwards compatible|forward compatible|"
        r"full compatibility|non[- ]breaking|breaking change|versioned contract|"
        r"schema evolution|deprecat(?:e|ion)|dual read|dual write)\b",
        re.I,
    ),
    "fixture_updates": re.compile(
        r"\b(?:fixture updates?|fixtures? updated|sample payload|golden payload|example payload|"
        r"test fixture|consumer fixture|producer fixture|snapshot update|seed data)\b",
        re.I,
    ),
    "consumer_notification": re.compile(
        r"\b(?:consumer notification|notify consumers?|consumer comms?|consumer communication|"
        r"partner notification|migration notice|release note|announce to consumers?|"
        r"downstream notification|subscriber notification)\b",
        re.I,
    ),
    "contract_test_coverage": re.compile(
        r"\b(?:contract tests?|consumer[- ]driven contract|pact test|schema test|"
        r"compatibility test|producer contract test|consumer contract test|"
        r"contract coverage|test.{0,80}contract|validate.{0,80}contract)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:data contract|event contract|payload contract|"
    r"api contract|warehouse contract|stream contract|schema)\b.{0,80}"
    r"\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_ACTIONABLE_GAPS: dict[DataContractReadinessRequirement, str] = {
    "schema_ownership": "Name the schema or contract owner responsible for approving producer and consumer changes.",
    "compatibility_mode": "Define compatibility mode, versioning, and breaking-change handling before implementation.",
    "fixture_updates": "Update producer and consumer fixtures, golden payloads, or sample records with the new contract shape.",
    "consumer_notification": "Notify affected consumers with migration timing, release notes, and downstream action required.",
    "contract_test_coverage": "Add contract tests covering producer output, consumer expectations, and compatibility behavior.",
}


@dataclass(frozen=True, slots=True)
class TaskDataContractReadinessFinding:
    """Data contract readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[DataContractSignal, ...] = field(default_factory=tuple)
    present_requirements: tuple[DataContractReadinessRequirement, ...] = field(default_factory=tuple)
    missing_requirements: tuple[DataContractReadinessRequirement, ...] = field(default_factory=tuple)
    risk_level: DataContractReadinessRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_gaps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_requirements": list(self.present_requirements),
            "missing_requirements": list(self.missing_requirements),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "actionable_gaps": list(self.actionable_gaps),
        }


@dataclass(frozen=True, slots=True)
class TaskDataContractReadinessPlan:
    """Plan-level data contract readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskDataContractReadinessFinding, ...] = field(default_factory=tuple)
    data_contract_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskDataContractReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "data_contract_task_ids": list(self.data_contract_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    def to_markdown(self) -> str:
        """Render data contract readiness as deterministic Markdown."""
        title = "# Task Data Contract Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('total_task_count', 0)}",
            f"- Data contract task count: {self.summary.get('data_contract_task_count', 0)}",
            f"- Missing requirement count: {self.summary.get('missing_requirement_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.findings:
            lines.extend(["", "No data contract readiness findings were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Signals | Present Requirements | Missing Requirements | Gaps | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for finding in self.findings:
            lines.append(
                "| "
                f"`{_markdown_cell(finding.task_id)}` | "
                f"{_markdown_cell(finding.title)} | "
                f"{finding.risk_level} | "
                f"{_markdown_cell(', '.join(finding.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.present_requirements) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.missing_requirements) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.actionable_gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_data_contract_readiness_plan(source: Any) -> TaskDataContractReadinessPlan:
    """Build data contract readiness findings for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_finding_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    findings = tuple(
        sorted(
            (finding for finding in candidates if finding is not None),
            key=lambda finding: (_RISK_ORDER[finding.risk_level], finding.task_id, finding.title.casefold()),
        )
    )
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskDataContractReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        data_contract_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_data_contract_readiness(source: Any) -> TaskDataContractReadinessPlan:
    """Compatibility alias for building data contract readiness plans."""
    return build_task_data_contract_readiness_plan(source)


def summarize_task_data_contract_readiness(source: Any) -> TaskDataContractReadinessPlan:
    """Compatibility alias for building data contract readiness plans."""
    return build_task_data_contract_readiness_plan(source)


def extract_task_data_contract_readiness(source: Any) -> TaskDataContractReadinessPlan:
    """Compatibility alias for extracting data contract readiness plans."""
    return build_task_data_contract_readiness_plan(source)


def generate_task_data_contract_readiness(source: Any) -> TaskDataContractReadinessPlan:
    """Compatibility alias for generating data contract readiness plans."""
    return build_task_data_contract_readiness_plan(source)


def recommend_task_data_contract_readiness(source: Any) -> TaskDataContractReadinessPlan:
    """Compatibility alias for recommending data contract readiness gaps."""
    return build_task_data_contract_readiness_plan(source)


def task_data_contract_readiness_plan_to_dict(
    result: TaskDataContractReadinessPlan,
) -> dict[str, Any]:
    """Serialize a data contract readiness plan to a plain dictionary."""
    return result.to_dict()


task_data_contract_readiness_plan_to_dict.__test__ = False


def task_data_contract_readiness_plan_to_dicts(
    result: TaskDataContractReadinessPlan | Iterable[TaskDataContractReadinessFinding],
) -> list[dict[str, Any]]:
    """Serialize data contract readiness findings to plain dictionaries."""
    if isinstance(result, TaskDataContractReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_data_contract_readiness_plan_to_dicts.__test__ = False


def task_data_contract_readiness_plan_to_markdown(
    result: TaskDataContractReadinessPlan,
) -> str:
    """Render a data contract readiness plan as Markdown."""
    return result.to_markdown()


task_data_contract_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[DataContractSignal, ...] = field(default_factory=tuple)
    requirements: tuple[DataContractReadinessRequirement, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskDataContractReadinessFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    missing = tuple(
        requirement for requirement in _REQUIREMENT_ORDER if requirement not in signals.requirements
    )
    return TaskDataContractReadinessFinding(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        detected_signals=signals.signals,
        present_requirements=signals.requirements,
        missing_requirements=missing,
        risk_level=_risk_level(signals.signals, missing),
        evidence=signals.evidence,
        actionable_gaps=tuple(_ACTIONABLE_GAPS[requirement] for requirement in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[DataContractSignal] = set()
    requirement_hits: set[DataContractReadinessRequirement] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        matched = False
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for requirement, pattern in _REQUIREMENT_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                requirement_hits.add(requirement)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        matched = False
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for requirement, pattern in _REQUIREMENT_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                requirement_hits.add(requirement)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        requirements=tuple(requirement for requirement in _REQUIREMENT_ORDER if requirement in requirement_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _risk_level(
    signals: tuple[DataContractSignal, ...],
    missing: tuple[DataContractReadinessRequirement, ...],
) -> DataContractReadinessRisk:
    if not missing:
        return "low"
    if len(missing) >= 4:
        return "high"
    if "compatibility_mode" in missing and ("stream_contract" in signals or "api_contract" in signals):
        return "high"
    if "contract_test_coverage" in missing and len(missing) >= 3:
        return "high"
    return "medium"


def _summary(
    findings: tuple[TaskDataContractReadinessFinding, ...],
    *,
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "data_contract_task_count": len(findings),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_requirement_count": sum(len(finding.missing_requirements) for finding in findings),
        "risk_counts": {risk: sum(1 for finding in findings if finding.risk_level == risk) for risk in _RISK_ORDER},
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_requirement_counts": {
            requirement: sum(1 for finding in findings if requirement in finding.present_requirements)
            for requirement in _REQUIREMENT_ORDER
        },
        "missing_requirement_counts": {
            requirement: sum(1 for finding in findings if requirement in finding.missing_requirements)
            for requirement in _REQUIREMENT_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
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
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_REQUIREMENT_PATTERNS.values()])


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
    return str(PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")))


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "DataContractReadinessRequirement",
    "DataContractReadinessRisk",
    "DataContractSignal",
    "TaskDataContractReadinessFinding",
    "TaskDataContractReadinessPlan",
    "analyze_task_data_contract_readiness",
    "build_task_data_contract_readiness_plan",
    "extract_task_data_contract_readiness",
    "generate_task_data_contract_readiness",
    "recommend_task_data_contract_readiness",
    "summarize_task_data_contract_readiness",
    "task_data_contract_readiness_plan_to_dict",
    "task_data_contract_readiness_plan_to_dicts",
    "task_data_contract_readiness_plan_to_markdown",
]
