"""Recommend CSRF protection readiness safeguards for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CsrfProtectionSignal = Literal[
    "cookie_session",
    "form_post",
    "unsafe_method",
    "admin_mutation",
    "same_site_cookie",
    "csrf_token",
    "origin_check",
    "double_submit_cookie",
]
CsrfProtectionSafeguard = Literal[
    "csrf_token_validation",
    "same_site_policy",
    "origin_referer_validation",
    "idempotency_or_confirmation",
    "integration_tests",
    "admin_action_coverage",
]
CsrfProtectionRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[CsrfProtectionRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[CsrfProtectionSignal, ...] = (
    "cookie_session",
    "form_post",
    "unsafe_method",
    "admin_mutation",
    "same_site_cookie",
    "csrf_token",
    "origin_check",
    "double_submit_cookie",
)
_SAFEGUARD_ORDER: tuple[CsrfProtectionSafeguard, ...] = (
    "csrf_token_validation",
    "same_site_policy",
    "origin_referer_validation",
    "idempotency_or_confirmation",
    "integration_tests",
    "admin_action_coverage",
)
_STATE_CHANGING_SIGNALS = {
    "cookie_session",
    "form_post",
    "unsafe_method",
    "admin_mutation",
}
_PROTECTION_SIGNALS = {
    "same_site_cookie",
    "csrf_token",
    "origin_check",
    "double_submit_cookie",
}
_SIGNAL_PATTERNS: dict[CsrfProtectionSignal, re.Pattern[str]] = {
    "cookie_session": re.compile(
        r"\b(?:cookie[- ]backed sessions?|session cookies?|cookie auth|browser session|"
        r"browser sessions?|authenticated sessions?|logged[- ]in sessions?|"
        r"same[- ]site session cookie)\b",
        re.I,
    ),
    "form_post": re.compile(
        r"\b(?:form posts?|post forms?|html forms?|browser forms?|settings form|profile form|"
        r"checkout form|signup form|admin form|submit form|form submission)\b",
        re.I,
    ),
    "unsafe_method": re.compile(
        r"\b(?:unsafe http methods?|unsafe methods?|POST|PUT|PATCH|DELETE|state[- ]changing "
        r"(?:request|endpoint|route|action)s?|mutating (?:request|endpoint|route)s?|"
        r"create/update/delete|write endpoint|non[- ]idempotent)\b",
        re.I,
    ),
    "admin_mutation": re.compile(
        r"\b(?:admin mutations?|admin actions?|administrator actions?|staff actions?|admin console|"
        r"admin panel|moderation actions?|delete user|disable account|impersonation|privileged action)\b",
        re.I,
    ),
    "same_site_cookie": re.compile(
        r"\b(?:same[- ]?site(?:=?(?:lax|strict|none))?|samesite cookie|same[- ]site policy|"
        r"cookie same[- ]site)\b",
        re.I,
    ),
    "csrf_token": re.compile(
        r"\b(?:csrf tokens?|xsrf tokens?|anti[- ]csrf tokens?|csrf validation|csrf middleware|"
        r"authenticity token|request verification token)\b",
        re.I,
    ),
    "origin_check": re.compile(
        r"\b(?:origin checks?|referer checks?|referrer checks?|origin validation|referer validation|"
        r"referrer validation|validate origin|validate referer|trusted origins?)\b",
        re.I,
    ),
    "double_submit_cookie": re.compile(
        r"\b(?:double[- ]submit cookies?|double submit cookie|xsrf cookie|csrf cookie paired token|"
        r"cookie[- ]to[- ]header token)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[CsrfProtectionSignal, re.Pattern[str]] = {
    "cookie_session": re.compile(
        r"(?:^|/)(?:sessions?|cookies?|auth|authentication)(?:/|\.|_|-|$)", re.I
    ),
    "form_post": re.compile(r"(?:^|/)(?:forms?|views?|templates?)(?:/|\.|_|-|$)", re.I),
    "unsafe_method": re.compile(
        r"(?:^|/)(?:controllers?|routes?|endpoints?|mutations?|handlers?)(?:/|\.|_|-|$)",
        re.I,
    ),
    "admin_mutation": re.compile(
        r"(?:^|/)(?:admin|staff|moderation|backoffice)(?:/|\.|_|-|$)", re.I
    ),
    "same_site_cookie": re.compile(r"(?:same[_-]?site|samesite|cookie[_-]?policy)", re.I),
    "csrf_token": re.compile(r"(?:csrf|xsrf|authenticity[_-]?token)", re.I),
    "origin_check": re.compile(r"(?:origin|referer|referrer|trusted[_-]?origin)", re.I),
    "double_submit_cookie": re.compile(r"(?:double[_-]?submit|xsrf[_-]?cookie)", re.I),
}
_SAFEGUARD_PATTERNS: dict[CsrfProtectionSafeguard, re.Pattern[str]] = {
    "csrf_token_validation": re.compile(
        r"\b(?:csrf tokens?|xsrf tokens?|anti[- ]csrf tokens?|csrf validation|csrf middleware|"
        r"authenticity token|request verification token|double[- ]submit cookies?)\b",
        re.I,
    ),
    "same_site_policy": re.compile(
        r"\b(?:same[- ]?site(?:=?(?:lax|strict|none))?|samesite cookie|same[- ]site policy|"
        r"cookie policy)\b",
        re.I,
    ),
    "origin_referer_validation": re.compile(
        r"\b(?:origin checks?|referer checks?|referrer checks?|origin validation|referer validation|"
        r"referrer validation|trusted origins?)\b",
        re.I,
    ),
    "idempotency_or_confirmation": re.compile(
        r"\b(?:idempotency|idempotency key|confirmation|confirm destructive|confirm admin|"
        r"are you sure|two[- ]step confirmation|undo window)\b",
        re.I,
    ),
    "integration_tests": re.compile(
        r"\b(?:csrf tests?|xsrf tests?|csrf integration tests?|xsrf integration tests?|"
        r"integration tests?.{0,80}(?:csrf|xsrf|cross[- ]origin|origin|referer|referrer)|"
        r"(?:csrf|xsrf|cross[- ]origin|origin|referer|referrer).{0,80}integration tests?|"
        r"e2e tests?.{0,80}(?:csrf|xsrf|cross[- ]origin)|browser tests?.{0,80}(?:csrf|xsrf)|"
        r"request specs?.{0,80}(?:csrf|xsrf))\b",
        re.I,
    ),
    "admin_action_coverage": re.compile(
        r"\b(?:admin action coverage|admin csrf coverage|privileged action coverage|staff action tests?|"
        r"admin mutation tests?|moderation action tests?)\b",
        re.I,
    ),
}
_RECOMMENDATIONS: dict[CsrfProtectionSafeguard, str] = {
    "csrf_token_validation": "Require CSRF token validation or an equivalent double-submit token for unsafe browser requests.",
    "same_site_policy": "Define SameSite cookie policy for session cookies and document any cross-site exceptions.",
    "origin_referer_validation": "Validate Origin or Referer headers for unsafe authenticated browser requests.",
    "idempotency_or_confirmation": "Add idempotency, confirmation, or undo coverage for destructive state changes.",
    "integration_tests": "Add integration tests that reject missing, invalid, and cross-origin CSRF attempts.",
    "admin_action_coverage": "Cover admin and privileged state changes with explicit CSRF acceptance criteria.",
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:csrf|browser forms?|cookie sessions?|"
    r"authenticated state changes?|state[- ]changing|unsafe methods?|admin actions?)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskCsrfProtectionReadinessRecord:
    """CSRF protection readiness guidance for one execution task."""

    task_id: str
    title: str
    csrf_signals: tuple[CsrfProtectionSignal, ...]
    risk_level: CsrfProtectionRiskLevel
    present_safeguards: tuple[CsrfProtectionSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CsrfProtectionSafeguard, ...] = field(default_factory=tuple)
    recommended_safeguards: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "csrf_signals": list(self.csrf_signals),
            "risk_level": self.risk_level,
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "recommended_safeguards": list(self.recommended_safeguards),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskCsrfProtectionReadinessPlan:
    """Task-level CSRF protection readiness recommendations."""

    plan_id: str | None = None
    records: tuple[TaskCsrfProtectionReadinessRecord, ...] = field(default_factory=tuple)
    csrf_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskCsrfProtectionReadinessRecord, ...]:
        """Compatibility view matching planners that call records recommendations."""
        return self.records

    @property
    def findings(self) -> tuple[TaskCsrfProtectionReadinessRecord, ...]:
        """Compatibility view matching planners that call records findings."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "csrf_task_ids": list(self.csrf_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return CSRF readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render CSRF protection readiness as deterministic Markdown."""
        title = "# Task CSRF Protection Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- CSRF-relevant task count: {self.summary.get('csrf_task_count', 0)}",
            f"- Missing safeguards count: {self.summary.get('missing_safeguards_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No CSRF protection readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(
                    ["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | CSRF Signals | Present Safeguards | Missing Safeguards | Recommended Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.csrf_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(
                ["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"]
            )
        return "\n".join(lines)


def build_task_csrf_protection_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskCsrfProtectionReadinessPlan:
    """Build CSRF protection readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _RISK_ORDER[record.risk_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    csrf_task_ids = tuple(record.task_id for record in records)
    csrf_task_id_set = set(csrf_task_ids)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in csrf_task_id_set
    )
    return TaskCsrfProtectionReadinessPlan(
        plan_id=plan_id,
        records=records,
        csrf_task_ids=csrf_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_csrf_protection_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskCsrfProtectionReadinessPlan:
    """Compatibility alias for building CSRF protection readiness plans."""
    return build_task_csrf_protection_readiness_plan(source)


def extract_task_csrf_protection_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskCsrfProtectionReadinessPlan:
    """Compatibility alias for extracting CSRF protection readiness plans."""
    return build_task_csrf_protection_readiness_plan(source)


def generate_task_csrf_protection_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskCsrfProtectionReadinessPlan:
    """Compatibility alias for generating CSRF protection readiness plans."""
    return build_task_csrf_protection_readiness_plan(source)


def recommend_task_csrf_protection_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskCsrfProtectionReadinessPlan:
    """Compatibility alias for recommending CSRF protection readiness plans."""
    return build_task_csrf_protection_readiness_plan(source)


def task_csrf_protection_readiness_plan_to_dict(
    result: TaskCsrfProtectionReadinessPlan,
) -> dict[str, Any]:
    """Serialize a CSRF protection readiness plan to a plain dictionary."""
    return result.to_dict()


task_csrf_protection_readiness_plan_to_dict.__test__ = False


def task_csrf_protection_readiness_plan_to_dicts(
    result: TaskCsrfProtectionReadinessPlan
    | Iterable[TaskCsrfProtectionReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize CSRF protection readiness records to plain dictionaries."""
    if isinstance(result, TaskCsrfProtectionReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_csrf_protection_readiness_plan_to_dicts.__test__ = False


def task_csrf_protection_readiness_plan_to_markdown(
    result: TaskCsrfProtectionReadinessPlan,
) -> str:
    """Render a CSRF protection readiness plan as Markdown."""
    return result.to_markdown()


task_csrf_protection_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    csrf_signals: tuple[CsrfProtectionSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[CsrfProtectionSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _record(task: Mapping[str, Any], index: int) -> TaskCsrfProtectionReadinessRecord | None:
    signals = _signals(task)
    signal_set = set(signals.csrf_signals)
    if signals.explicitly_no_impact:
        return None
    if not (signal_set & _STATE_CHANGING_SIGNALS):
        return None

    present = signals.present_safeguards
    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in present)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskCsrfProtectionReadinessRecord(
        task_id=task_id,
        title=title,
        csrf_signals=signals.csrf_signals,
        risk_level=_risk_level(signal_set, set(present), missing),
        present_safeguards=present,
        missing_safeguards=missing,
        recommended_safeguards=tuple(_RECOMMENDATIONS[safeguard] for safeguard in missing),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[CsrfProtectionSignal] = set()
    safeguard_hits: set[CsrfProtectionSafeguard] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or _SIGNAL_PATTERNS[signal].search(searchable):
                signal_hits.add(signal)
                path_matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                path_matched = True
        if path_matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        snippet = _evidence_snippet(source_field, text)
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(snippet)

    if "double_submit_cookie" in signal_hits:
        safeguard_hits.add("csrf_token_validation")
    if "csrf_token" in signal_hits:
        safeguard_hits.add("csrf_token_validation")
    if "same_site_cookie" in signal_hits:
        safeguard_hits.add("same_site_policy")
    if "origin_check" in signal_hits:
        safeguard_hits.add("origin_referer_validation")

    return _Signals(
        csrf_signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        present_safeguards=tuple(
            safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits
        ),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _risk_level(
    signal_set: set[CsrfProtectionSignal],
    present_set: set[CsrfProtectionSafeguard],
    missing: tuple[CsrfProtectionSafeguard, ...],
) -> CsrfProtectionRiskLevel:
    unsafe = bool(signal_set & _STATE_CHANGING_SIGNALS)
    token_or_origin = bool(present_set & {"csrf_token_validation", "origin_referer_validation"})
    if unsafe and not token_or_origin:
        return "high"
    if "admin_mutation" in signal_set and "admin_action_coverage" in missing:
        return "high" if "csrf_token_validation" in missing else "medium"
    if len(missing) >= 3:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskCsrfProtectionReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "csrf_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguards_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk) for risk in _RISK_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.csrf_signals)
            for signal in _SIGNAL_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "csrf_task_ids": [record.task_id for record in records],
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
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
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
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
        "dependencies",
        "files_or_modules",
        "files",
        "paths",
        "acceptance_criteria",
        "validation_plan",
        "validation_commands",
        "test_command",
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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "validation_plan",
        "validation_commands",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
        "dependencies",
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
    return any(
        pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()]
    )


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
    return str(
        PurePosixPath(
            value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
        )
    )


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
    "CsrfProtectionRiskLevel",
    "CsrfProtectionSafeguard",
    "CsrfProtectionSignal",
    "TaskCsrfProtectionReadinessPlan",
    "TaskCsrfProtectionReadinessRecord",
    "analyze_task_csrf_protection_readiness",
    "build_task_csrf_protection_readiness_plan",
    "extract_task_csrf_protection_readiness",
    "generate_task_csrf_protection_readiness",
    "recommend_task_csrf_protection_readiness",
    "task_csrf_protection_readiness_plan_to_dict",
    "task_csrf_protection_readiness_plan_to_dicts",
    "task_csrf_protection_readiness_plan_to_markdown",
]
