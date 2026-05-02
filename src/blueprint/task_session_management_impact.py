"""Plan session management impact safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SessionSurface = Literal[
    "login_session",
    "session_cookie",
    "refresh_token",
    "remember_me",
    "session_invalidation",
    "device_session",
    "logout",
]
SessionControl = Literal[
    "cookie_attributes",
    "token_rotation",
    "logout_invalidation",
    "device_revocation",
    "idle_or_absolute_timeout",
    "csrf_or_replay_guard",
    "audit_logging",
]
SessionRiskLevel = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[SessionRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: dict[SessionSurface, int] = {
    "login_session": 0,
    "session_cookie": 1,
    "refresh_token": 2,
    "remember_me": 3,
    "session_invalidation": 4,
    "device_session": 5,
    "logout": 6,
}
_CONTROL_ORDER: tuple[SessionControl, ...] = (
    "cookie_attributes",
    "token_rotation",
    "logout_invalidation",
    "device_revocation",
    "idle_or_absolute_timeout",
    "csrf_or_replay_guard",
    "audit_logging",
)
_SURFACE_PATTERNS: dict[SessionSurface, re.Pattern[str]] = {
    "login_session": re.compile(
        r"\b(?:login session|log[- ]?in session|authenticated session|user session|"
        r"session management|session state|session store|session id|session token)\b",
        re.I,
    ),
    "session_cookie": re.compile(
        r"\b(?:session cookie|auth cookie|secure cookie|http[- ]?only cookie|"
        r"samesite|cookie attributes?|set-cookie|cookie-based session)\b",
        re.I,
    ),
    "refresh_token": re.compile(
        r"\b(?:refresh tokens?|token refresh|rotate refresh|long[- ]?lived token|"
        r"offline token|refresh grant)\b",
        re.I,
    ),
    "remember_me": re.compile(
        r"\b(?:remember[- ]?me|remember me|persistent login|persistent session|"
        r"keep me signed in|stay signed in)\b",
        re.I,
    ),
    "session_invalidation": re.compile(
        r"\b(?:session invalidation|invalidate sessions?|revoke sessions?|terminate sessions?|"
        r"force logout|sign out everywhere|password change.*sessions?|sessions?.*password change)\b",
        re.I,
    ),
    "device_session": re.compile(
        r"\b(?:device sessions?|trusted devices?|active devices?|session devices?|"
        r"device management|manage devices?|logged[- ]?in devices?)\b",
        re.I,
    ),
    "logout": re.compile(r"\b(?:logout|log out|sign[- ]?out|sign out|end session)\b", re.I),
}
_PATH_PATTERNS: dict[SessionSurface, re.Pattern[str]] = {
    "login_session": re.compile(r"(?:^|/)(?:sessions?|session_store|auth|login)(?:/|$)|session", re.I),
    "session_cookie": re.compile(r"cookies?|set[_-]?cookie|same[_-]?site|http[_-]?only", re.I),
    "refresh_token": re.compile(r"refresh[_-]?tokens?|token[_-]?refresh|offline[_-]?token", re.I),
    "remember_me": re.compile(r"remember[_-]?me|persistent[_-]?(?:login|session)", re.I),
    "session_invalidation": re.compile(r"invalidat|revoke[_-]?session|session[_-]?revocation", re.I),
    "device_session": re.compile(r"device[_-]?sessions?|trusted[_-]?devices?|active[_-]?devices?", re.I),
    "logout": re.compile(r"(?:^|/)(?:logout|signout|sign_out)(?:/|$)|logout|sign[_-]?out", re.I),
}
_CONTROL_PATTERNS: dict[SessionControl, re.Pattern[str]] = {
    "cookie_attributes": re.compile(
        r"\b(?:secure|httponly|http only|samesite|domain|path|max[- ]?age|expires)\b.*\bcookie\b|"
        r"\bcookie\b.*\b(?:secure|httponly|http only|samesite|domain|path|max[- ]?age|expires)\b",
        re.I,
    ),
    "token_rotation": re.compile(
        r"\b(?:token rotation|rotate refresh tokens?|refresh token rotation|one[- ]?time refresh|"
        r"refresh token reuse detection|token family)\b",
        re.I,
    ),
    "logout_invalidation": re.compile(
        r"\b(?:logout invalidation|invalidate on logout|logout revokes?|log out.*invalidate|"
        r"sign out.*invalidate|server[- ]?side logout|clear server session)\b",
        re.I,
    ),
    "device_revocation": re.compile(
        r"\b(?:device revocation|revoke devices?|remove devices?|sign out device|"
        r"terminate device sessions?|trusted device removal)\b",
        re.I,
    ),
    "idle_or_absolute_timeout": re.compile(
        r"\b(?:idle timeout|absolute timeout|session timeout|inactivity timeout|max session age|"
        r"session ttl|time[- ]?to[- ]?live|expires after)\b",
        re.I,
    ),
    "csrf_or_replay_guard": re.compile(
        r"\b(?:csrf|xsrf|replay guard|replay protection|nonce|anti[- ]?replay|state parameter|"
        r"double submit)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit logs?|audit events?|security logs?|login history|session history|"
        r"log session|log logout|log revocation)\b",
        re.I,
    ),
}
_USER_FACING_SURFACES: set[SessionSurface] = {
    "session_cookie",
    "refresh_token",
    "remember_me",
    "session_invalidation",
    "device_session",
    "logout",
}


@dataclass(frozen=True, slots=True)
class TaskSessionManagementImpactRecommendation:
    """Session management impact guidance for one affected execution task."""

    task_id: str
    title: str
    session_surfaces: tuple[SessionSurface, ...] = field(default_factory=tuple)
    missing_controls: tuple[SessionControl, ...] = field(default_factory=tuple)
    risk_level: SessionRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "session_surfaces": list(self.session_surfaces),
            "missing_controls": list(self.missing_controls),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskSessionManagementImpactPlan:
    """Plan-level session management impact summary."""

    plan_id: str | None = None
    recommendations: tuple[TaskSessionManagementImpactRecommendation, ...] = field(default_factory=tuple)
    flagged_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskSessionManagementImpactRecommendation, ...]:
        """Compatibility view for report helpers that expose records."""
        return self.recommendations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [record.to_dict() for record in self.recommendations],
            "flagged_task_ids": list(self.flagged_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return recommendations as plain dictionaries."""
        return [record.to_dict() for record in self.recommendations]

    def to_markdown(self) -> str:
        """Render session management impact as deterministic Markdown."""
        title = "# Task Session Management Impact"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('total_task_count', 0)}",
            f"- Flagged task count: {self.summary.get('flagged_task_count', 0)}",
            f"- Missing control count: {self.summary.get('missing_control_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{level} {risk_counts.get(level, 0)}" for level in _RISK_ORDER),
        ]
        if not self.recommendations:
            lines.extend(["", "No session management impact recommendations were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Session Surfaces | Missing Controls | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.session_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_controls) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_session_management_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSessionManagementImpactPlan:
    """Build session management impact recommendations for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _record_for_task(task, index)) is not None
            ),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    return TaskSessionManagementImpactPlan(
        plan_id=plan_id,
        recommendations=records,
        flagged_task_ids=tuple(record.task_id for record in records),
        summary=_summary(records, total_task_count=len(tasks)),
    )


def generate_task_session_management_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[TaskSessionManagementImpactRecommendation, ...]:
    """Return session management impact recommendations for relevant execution tasks."""
    return build_task_session_management_impact_plan(source).recommendations


def task_session_management_impact_to_dicts(
    records: (
        tuple[TaskSessionManagementImpactRecommendation, ...]
        | list[TaskSessionManagementImpactRecommendation]
        | TaskSessionManagementImpactPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize session management impact recommendations to dictionaries."""
    if isinstance(records, TaskSessionManagementImpactPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


def task_session_management_impact_plan_to_dict(
    result: TaskSessionManagementImpactPlan,
) -> dict[str, Any]:
    """Serialize a session management impact plan to a plain dictionary."""
    return result.to_dict()


task_session_management_impact_plan_to_dict.__test__ = False


def task_session_management_impact_plan_to_markdown(
    result: TaskSessionManagementImpactPlan,
) -> str:
    """Serialize a session management impact plan to Markdown."""
    return result.to_markdown()


task_session_management_impact_plan_to_markdown.__test__ = False


def summarize_task_session_management_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSessionManagementImpactPlan:
    """Compatibility alias for building session management impact plans."""
    return build_task_session_management_impact_plan(source)


def _record_for_task(
    task: Mapping[str, Any],
    index: int,
) -> TaskSessionManagementImpactRecommendation | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    surfaces: dict[SessionSurface, list[str]] = {}
    controls: set[SessionControl] = set()

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _inspect_path(path, surfaces, controls)

    for source_field, text in _candidate_texts(task):
        _inspect_text(source_field, text, surfaces, controls)

    if not surfaces:
        return None

    session_surfaces = tuple(sorted(surfaces, key=lambda item: _SURFACE_ORDER[item]))
    missing_controls = tuple(control for control in _CONTROL_ORDER if control not in controls)
    return TaskSessionManagementImpactRecommendation(
        task_id=task_id,
        title=title,
        session_surfaces=session_surfaces,
        missing_controls=missing_controls,
        risk_level=_risk_level(session_surfaces, missing_controls),
        evidence=tuple(
            _dedupe(
                evidence
                for surface in session_surfaces
                for evidence in surfaces.get(surface, [])
            )
        ),
    )


def _inspect_path(
    path: str,
    surfaces: dict[SessionSurface, list[str]],
    controls: set[SessionControl],
) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    lowered = normalized.casefold()
    evidence = f"files_or_modules: {path}"
    parts = set(PurePosixPath(lowered).parts)
    for surface, pattern in _PATH_PATTERNS.items():
        if pattern.search(lowered):
            surfaces.setdefault(surface, []).append(evidence)
    if bool({"sessions", "session", "auth", "login"} & parts):
        surfaces.setdefault("login_session", []).append(evidence)
    if any(token in lowered for token in ("csrf", "xsrf", "replay", "audit", "timeout", "ttl")):
        _inspect_text("files_or_modules", normalized, surfaces, controls)


def _inspect_text(
    source_field: str,
    text: str,
    surfaces: dict[SessionSurface, list[str]],
    controls: set[SessionControl],
) -> None:
    evidence = _evidence_snippet(source_field, text)
    for surface, pattern in _SURFACE_PATTERNS.items():
        if pattern.search(text):
            surfaces.setdefault(surface, []).append(evidence)
    for control, pattern in _CONTROL_PATTERNS.items():
        if pattern.search(text):
            controls.add(control)


def _risk_level(
    session_surfaces: tuple[SessionSurface, ...],
    missing_controls: tuple[SessionControl, ...],
) -> SessionRiskLevel:
    if not missing_controls:
        return "low"
    if any(surface in _USER_FACING_SURFACES for surface in session_surfaces):
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskSessionManagementImpactRecommendation, ...],
    *,
    total_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "flagged_task_count": len(records),
        "unrelated_task_count": max(total_task_count - len(records), 0),
        "missing_control_count": sum(len(record.missing_controls) for record in records),
        "risk_counts": {
            level: sum(1 for record in records if record.risk_level == level)
            for level in _RISK_ORDER
        },
        "missing_control_counts": {
            control: sum(1 for record in records if control in record.missing_controls)
            for control in _CONTROL_ORDER
        },
        "session_surface_counts": {
            surface: sum(1 for record in records if surface in record.session_surfaces)
            for surface in _SURFACE_ORDER
        },
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

    try:
        iterator = iter(source)  # type: ignore[arg-type]
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
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        patterns = (*_SURFACE_PATTERNS.values(), *_CONTROL_PATTERNS.values())
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in patterns):
                texts.append((field, key_text))
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


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "SessionControl",
    "SessionRiskLevel",
    "SessionSurface",
    "TaskSessionManagementImpactPlan",
    "TaskSessionManagementImpactRecommendation",
    "build_task_session_management_impact_plan",
    "generate_task_session_management_impact",
    "summarize_task_session_management_impact",
    "task_session_management_impact_plan_to_dict",
    "task_session_management_impact_plan_to_markdown",
    "task_session_management_impact_to_dicts",
]
