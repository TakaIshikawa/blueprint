"""Assess task-level readiness for CORS origin and browser client implementation work."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


CORSOriginSignal = Literal[
    "browser_client",
    "cross_origin_api",
    "trusted_origin",
    "credentialed_request",
    "preflight_headers",
    "wildcard_origin",
    "environment_origin",
]
CORSOriginSafeguard = Literal[
    "trusted_origin_allowlist",
    "credentials_policy",
    "preflight_headers",
    "wildcard_blocking",
    "environment_coverage",
    "browser_regression_tests",
]
CORSOriginReadiness = Literal["weak", "partial", "strong"]
CORSOriginImpact = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[CORSOriginReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_IMPACT_ORDER: dict[CORSOriginImpact, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[CORSOriginSignal, ...] = (
    "browser_client",
    "cross_origin_api",
    "trusted_origin",
    "credentialed_request",
    "preflight_headers",
    "wildcard_origin",
    "environment_origin",
)
_SAFEGUARD_ORDER: tuple[CORSOriginSafeguard, ...] = (
    "trusted_origin_allowlist",
    "credentials_policy",
    "preflight_headers",
    "wildcard_blocking",
    "environment_coverage",
    "browser_regression_tests",
)
_HIGH_IMPACT_SIGNALS: frozenset[CORSOriginSignal] = frozenset(
    {"browser_client", "cross_origin_api", "credentialed_request", "wildcard_origin"}
)

_SIGNAL_PATTERNS: dict[CORSOriginSignal, re.Pattern[str]] = {
    "browser_client": re.compile(
        r"\b(?:browser clients?|web clients?|frontend|front-end|spa|single page app|"
        r"react app|vue app|angular app|mobile web|web app|browser origin|browser request)\b",
        re.I,
    ),
    "cross_origin_api": re.compile(
        r"\b(?:cross[- ]origin|cross origin|cors|access-control|origin header|"
        r"same[- ]origin policy|cross domain|cross-domain api)\b",
        re.I,
    ),
    "trusted_origin": re.compile(
        r"\b(?:trusted origins?|allowed origins?|allowlist(?:ed)? origins?|origin allowlist|"
        r"approved origins?|specific origins?|explicit origins?|access-control-allow-origin)\b",
        re.I,
    ),
    "credentialed_request": re.compile(
        r"\b(?:credentialed requests?|with credentials|credentials include|include credentials|"
        r"access-control-allow-credentials|cookies?|session cookies?|authorization headers?|"
        r"bearer tokens?|csrf)\b",
        re.I,
    ),
    "preflight_headers": re.compile(
        r"\b(?:preflight|options request|options endpoint|http options|access-control-request-|"
        r"allowed methods?|allowed headers?|custom headers?|preflight cache|max-age)\b",
        re.I,
    ),
    "wildcard_origin": re.compile(
        r"\b(?:wildcard origins?|\*\s+origin|\*\s+for\s+origin|access-control-allow-origin:\s*\*|"
        r"no wildcard|not use wildcard|reject wildcard|block wildcard|disallow wildcard)\b",
        re.I,
    ),
    "environment_origin": re.compile(
        r"\b(?:dev(?:elopment)? origins?|staging origins?|production origins?|prod origins?|"
        r"preview origins?|environment[- ]specific origins?|per[- ]environment origins?|"
        r"localhost|127\.0\.0\.1|vercel preview|netlify preview)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[CORSOriginSignal, re.Pattern[str]] = {
    "browser_client": re.compile(r"browser|frontend|front[_-]?end|spa|web[_-]?app|client", re.I),
    "cross_origin_api": re.compile(r"cors|cross[_-]?origin|origin|access[_-]?control", re.I),
    "trusted_origin": re.compile(r"trusted|allowed|allowlist|origins?", re.I),
    "credentialed_request": re.compile(r"credential|cookie|auth|session|csrf", re.I),
    "preflight_headers": re.compile(r"preflight|options|headers?|methods?", re.I),
    "wildcard_origin": re.compile(r"wildcard|origin", re.I),
    "environment_origin": re.compile(r"env|dev|staging|stage|preview|prod|localhost", re.I),
}
_SAFEGUARD_PATTERNS: dict[CORSOriginSafeguard, re.Pattern[str]] = {
    "trusted_origin_allowlist": re.compile(
        r"\b(?:trusted origin(?:s)? allowlist|origin allowlist|allowed origins? list|"
        r"explicit allowlist|origin whitelist|approved origins? list|"
        r"specific origins? configuration|allowlisted domains?)\b",
        re.I,
    ),
    "credentials_policy": re.compile(
        r"\b(?:credentials? policy|cookie policy|csrf protection|same-?site|"
        r"access-control-allow-credentials policy|credentials? strategy|"
        r"cookie configuration|session configuration)\b",
        re.I,
    ),
    "preflight_headers": re.compile(
        r"\b(?:preflight headers?|options headers?|allowed methods?|allowed headers?|"
        r"access-control-allow-methods|access-control-allow-headers|"
        r"preflight configuration|options handling)\b",
        re.I,
    ),
    "wildcard_blocking": re.compile(
        r"\b(?:wildcard blocking|block wildcard|reject wildcard|no wildcard|disallow wildcard|"
        r"prevent wildcard|wildcard prevention|wildcard validation|"
        r"wildcard rejection|must not use \*)\b",
        re.I,
    ),
    "environment_coverage": re.compile(
        r"\b(?:environment[- ]specific origins?|per[- ]environment origins?|"
        r"dev(?:elopment)? origins?|staging origins?|prod(?:uction)? origins?|"
        r"preview origins?|environment coverage|origin configuration per environment)\b",
        re.I,
    ),
    "browser_regression_tests": re.compile(
        r"\b(?:browser tests?|browser regression|cors tests?|origin tests?|"
        r"cross[- ]origin tests?|preflight tests?|browser integration tests?|"
        r"end[- ]to[- ]end tests?|e2e tests?|browser coverage)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[CORSOriginSafeguard, str] = {
    "trusted_origin_allowlist": "Configure explicit trusted-origin allowlists for API endpoints and avoid wildcard origins on browser surfaces.",
    "credentials_policy": "Define and test credentials policy (SameSite, cookie behavior, Access-Control-Allow-Credentials) for credentialed browser requests.",
    "preflight_headers": "Implement OPTIONS handling with allowed methods, allowed headers, cache duration, and preflight test coverage.",
    "wildcard_blocking": "Block wildcard origins on browser-facing or credentialed API surfaces; require explicit allowed origins.",
    "environment_coverage": "Maintain dev, staging, preview, and production origin lists separately and ensure environment-specific origin coverage.",
    "browser_regression_tests": "Add browser regression tests covering CORS headers, cross-origin requests, preflight behavior, and credentials handling.",
}


@dataclass(frozen=True, slots=True)
class TaskCORSOriginReadinessRecord:
    """CORS origin readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[CORSOriginSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[CORSOriginSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CORSOriginSafeguard, ...] = field(default_factory=tuple)
    readiness: CORSOriginReadiness = "weak"
    impact: CORSOriginImpact = "medium"
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def signals(self) -> tuple[CORSOriginSignal, ...]:
        return self.detected_signals

    @property
    def safeguards(self) -> tuple[CORSOriginSafeguard, ...]:
        return self.present_safeguards

    @property
    def recommendations(self) -> tuple[str, ...]:
        return self.recommended_checks

    @property
    def recommended_actions(self) -> tuple[str, ...]:
        return self.recommended_checks

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "impact": self.impact,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskCORSOriginReadinessPlan:
    """Plan-level CORS origin readiness review."""

    plan_id: str | None = None
    records: tuple[TaskCORSOriginReadinessRecord, ...] = field(default_factory=tuple)
    cors_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskCORSOriginReadinessRecord, ...]:
        return self.records

    @property
    def recommendations(self) -> tuple[TaskCORSOriginReadinessRecord, ...]:
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        return self.cors_task_ids

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "cors_task_ids": list(self.cors_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        title = "# Task CORS Origin Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        impact_counts = self.summary.get("impact_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- CORS task count: {self.summary.get('cors_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Impact counts: " + ", ".join(f"{level} {impact_counts.get(level, 0)}" for level in _IMPACT_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task CORS origin readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Impact | Detected Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{record.impact} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_cors_origin_readiness_plan(source: Any) -> TaskCORSOriginReadinessPlan:
    """Build CORS origin readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _task_record(task, index)) is not None
            ),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                _IMPACT_ORDER[record.impact],
                -len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    cors_task_ids = tuple(record.task_id for record in records)
    cors_task_id_set = set(cors_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in cors_task_id_set
    )
    return TaskCORSOriginReadinessPlan(
        plan_id=plan_id,
        records=records,
        cors_task_ids=cors_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_cors_origin_readiness(source: Any) -> TaskCORSOriginReadinessPlan:
    return build_task_cors_origin_readiness_plan(source)


def recommend_task_cors_origin_readiness(source: Any) -> TaskCORSOriginReadinessPlan:
    return build_task_cors_origin_readiness_plan(source)


def summarize_task_cors_origin_readiness(source: Any) -> TaskCORSOriginReadinessPlan:
    return build_task_cors_origin_readiness_plan(source)


def generate_task_cors_origin_readiness(source: Any) -> TaskCORSOriginReadinessPlan:
    return build_task_cors_origin_readiness_plan(source)


def extract_task_cors_origin_readiness(source: Any) -> TaskCORSOriginReadinessPlan:
    return build_task_cors_origin_readiness_plan(source)


def derive_task_cors_origin_readiness(source: Any) -> TaskCORSOriginReadinessPlan:
    return build_task_cors_origin_readiness_plan(source)


def task_cors_origin_readiness_plan_to_dict(result: TaskCORSOriginReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_cors_origin_readiness_plan_to_dict.__test__ = False


def task_cors_origin_readiness_plan_to_dicts(
    result: TaskCORSOriginReadinessPlan | Iterable[TaskCORSOriginReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, TaskCORSOriginReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_cors_origin_readiness_plan_to_dicts.__test__ = False
task_cors_origin_readiness_to_dicts = task_cors_origin_readiness_plan_to_dicts
task_cors_origin_readiness_to_dicts.__test__ = False


def task_cors_origin_readiness_plan_to_markdown(result: TaskCORSOriginReadinessPlan) -> str:
    return result.to_markdown()


task_cors_origin_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[CORSOriginSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[CORSOriginSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskCORSOriginReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing: tuple[CORSOriginSafeguard, ...] = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    return TaskCORSOriginReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.present_safeguards, missing),
        impact=_impact(signals.signals, missing),
        recommended_checks=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[CORSOriginSignal] = set()
    safeguard_hits: set[CORSOriginSafeguard] = set()
    evidence: list[str] = []

    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_file_paths")
        or task.get("expected_files")
        or task.get("paths")
    ):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(_dedupe(evidence)),
    )


def _readiness(
    present: tuple[CORSOriginSafeguard, ...],
    missing: tuple[CORSOriginSafeguard, ...],
) -> CORSOriginReadiness:
    if not missing:
        return "strong"
    if len(present) >= 3:
        return "partial"
    return "weak"


def _impact(
    signals: tuple[CORSOriginSignal, ...],
    missing: tuple[CORSOriginSafeguard, ...],
) -> CORSOriginImpact:
    high_signal = any(signal in _HIGH_IMPACT_SIGNALS for signal in signals)
    if high_signal and (
        len(missing) >= 3
        or "trusted_origin_allowlist" in missing
        or "credentials_policy" in missing and "credentialed_request" in signals
    ):
        return "high"
    if high_signal or len(missing) >= 3:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskCORSOriginReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "cors_task_count": len(records),
        "cors_task_ids": [record.task_id for record in records],
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_count": len(no_impact_task_ids),
        "no_impact_task_ids": list(no_impact_task_ids),
        "signal_count": sum(len(record.detected_signals) for record in records),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "impact_counts": {
            impact: sum(1 for record in records if record.impact == impact)
            for impact in _IMPACT_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
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


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
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
        "expected_file_paths",
        "expected_files",
        "paths",
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
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
    ):
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value) or _strings(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value) or _strings(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
    return tuple(("validation_commands", command) for command in _dedupe(commands))


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
    path = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
    return str(PurePosixPath(path)) if path else ""


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


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
    "CORSOriginImpact",
    "CORSOriginReadiness",
    "CORSOriginSafeguard",
    "CORSOriginSignal",
    "TaskCORSOriginReadinessPlan",
    "TaskCORSOriginReadinessRecord",
    "analyze_task_cors_origin_readiness",
    "build_task_cors_origin_readiness_plan",
    "derive_task_cors_origin_readiness",
    "extract_task_cors_origin_readiness",
    "generate_task_cors_origin_readiness",
    "recommend_task_cors_origin_readiness",
    "summarize_task_cors_origin_readiness",
    "task_cors_origin_readiness_plan_to_dict",
    "task_cors_origin_readiness_plan_to_dicts",
    "task_cors_origin_readiness_plan_to_markdown",
    "task_cors_origin_readiness_to_dicts",
]
