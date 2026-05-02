"""Identify task-level session revocation and authentication lifecycle risks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SessionRevocationImpactLevel = Literal["high", "medium", "low"]
SessionRevocationSurface = Literal[
    "login",
    "logout",
    "token_refresh",
    "session_cookie",
    "password_reset",
    "impersonation",
    "device_trust",
    "admin_session",
]
SessionRevocationSafeguard = Literal[
    "server_side_revocation",
    "cookie_invalidation",
    "refresh_token_rotation",
    "audit_logging",
    "replay_protection",
    "cross_device_logout",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[SessionRevocationImpactLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: dict[SessionRevocationSurface, int] = {
    "login": 0,
    "logout": 1,
    "token_refresh": 2,
    "session_cookie": 3,
    "password_reset": 4,
    "impersonation": 5,
    "device_trust": 6,
    "admin_session": 7,
}
_SAFEGUARD_ORDER: dict[SessionRevocationSafeguard, int] = {
    "server_side_revocation": 0,
    "cookie_invalidation": 1,
    "refresh_token_rotation": 2,
    "audit_logging": 3,
    "replay_protection": 4,
    "cross_device_logout": 5,
}
_SURFACE_PATTERNS: dict[SessionRevocationSurface, re.Pattern[str]] = {
    "login": re.compile(r"\b(?:log ?in|sign ?in|authenticate|authentication flow|mfa challenge)\b", re.I),
    "logout": re.compile(r"\b(?:log ?out|sign ?out|terminate session|end session|session revocation|revoke sessions?)\b", re.I),
    "token_refresh": re.compile(r"\b(?:refresh token|token refresh|rotate token|access token|jwt refresh|oauth token)\b", re.I),
    "session_cookie": re.compile(r"\b(?:session cookie|auth cookie|cookie session|same ?site|httponly|secure cookie)\b", re.I),
    "password_reset": re.compile(r"\b(?:password reset|reset password|forgot password|change password|credential reset)\b", re.I),
    "impersonation": re.compile(r"\b(?:impersonat(?:e|ion)|sudo as|login as|act as user|support access)\b", re.I),
    "device_trust": re.compile(r"\b(?:trusted device|device trust|remember this device|device binding|device session)\b", re.I),
    "admin_session": re.compile(r"\b(?:admin session|administrator session|privileged session|staff session|backoffice session)\b", re.I),
}
_PATH_PATTERNS: dict[SessionRevocationSurface, re.Pattern[str]] = {
    "login": re.compile(r"(?:^|/)(?:login|signin|sign[_-]?in|authentication)(?:[_.-]|/|$)", re.I),
    "logout": re.compile(r"logout|signout|session[_-]?revocation|revoke[_-]?session", re.I),
    "token_refresh": re.compile(r"refresh[_-]?token|token[_-]?refresh|access[_-]?token|jwt", re.I),
    "session_cookie": re.compile(r"session[_-]?cookie|auth[_-]?cookie|cookie[_-]?session", re.I),
    "password_reset": re.compile(r"password[_-]?reset|reset[_-]?password|forgot[_-]?password", re.I),
    "impersonation": re.compile(r"impersonat|sudo[_-]?as|login[_-]?as|support[_-]?access", re.I),
    "device_trust": re.compile(r"trusted[_-]?device|device[_-]?trust|remember[_-]?device", re.I),
    "admin_session": re.compile(r"admin[_-]?session|privileged[_-]?session|staff[_-]?session", re.I),
}
_SAFEGUARD_PATTERNS: dict[SessionRevocationSafeguard, re.Pattern[str]] = {
    "server_side_revocation": re.compile(
        r"\b(?:server[- ]side revocation|server revocation|revocation list|session store|"
        r"denylist|blacklist|invalidate server session|delete server session)\b",
        re.I,
    ),
    "cookie_invalidation": re.compile(
        r"\b(?:cookie invalidation|clear cookie|expire cookie|delete cookie|set-cookie.*max-age=0|"
        r"remove auth cookie)\b",
        re.I,
    ),
    "refresh_token_rotation": re.compile(
        r"\b(?:refresh[- ]token rotation|rotate refresh tokens?|single[- ]use refresh token|"
        r"token family|reuse detection)\b",
        re.I,
    ),
    "audit_logging": re.compile(r"\b(?:audit log|audit event|audit trail|security log|activity log)\b", re.I),
    "replay_protection": re.compile(
        r"\b(?:replay protection|nonce|one[- ]time token|anti[- ]replay|csrf|pkce|jti|token binding)\b",
        re.I,
    ),
    "cross_device_logout": re.compile(
        r"\b(?:cross[- ]device logout|logout all devices|sign out all devices|all sessions|other devices)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class TaskSessionRevocationImpactFinding:
    """Session lifecycle guidance for one affected execution task."""

    task_id: str
    title: str
    impacted_surfaces: tuple[SessionRevocationSurface, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[SessionRevocationSafeguard, ...] = field(default_factory=tuple)
    impact_level: SessionRevocationImpactLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def risk_level(self) -> SessionRevocationImpactLevel:
        """Compatibility alias used by other task impact planners."""
        return self.impact_level

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impacted_surfaces": list(self.impacted_surfaces),
            "missing_safeguards": list(self.missing_safeguards),
            "impact_level": self.impact_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskSessionRevocationImpactPlan:
    """Plan-level summary of session revocation impact."""

    plan_id: str | None = None
    findings: tuple[TaskSessionRevocationImpactFinding, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskSessionRevocationImpactFinding, ...]:
        """Compatibility view matching planners that name task findings records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "impacted_task_ids": list(self.impacted_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return session revocation findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    def to_markdown(self) -> str:
        """Render a deterministic Markdown handoff summary."""
        title = "# Task Session Revocation Impact Plan"
        if self.plan_id:
            title = f"{title}: {_markdown_cell(self.plan_id)}"
        lines = [title]
        if not self.findings:
            lines.extend(["", "No session revocation impact findings were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Impact | Surfaces | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for finding in self.findings:
            lines.append(
                "| "
                f"`{_markdown_cell(finding.task_id)}` {_markdown_cell(finding.title)} | "
                f"{finding.impact_level} | "
                f"{_markdown_cell(', '.join(finding.impacted_surfaces) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_session_revocation_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> TaskSessionRevocationImpactPlan:
    """Build task-level session revocation impact recommendations."""
    plan_id, tasks = _source_payload(source)
    findings = tuple(
        sorted(
            (
                finding
                for index, task in enumerate(tasks, start=1)
                if (finding := _finding_for_task(task, index)) is not None
            ),
            key=lambda finding: (
                _RISK_ORDER[finding.impact_level],
                -len(finding.missing_safeguards),
                finding.task_id,
                finding.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(finding.task_id for finding in findings)
    impacted_task_id_set = set(impacted_task_ids)
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted_task_id_set
    )
    return TaskSessionRevocationImpactPlan(
        plan_id=plan_id,
        findings=findings,
        impacted_task_ids=impacted_task_ids,
        ignored_task_ids=ignored_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), ignored_task_count=len(ignored_task_ids)),
    )


def analyze_task_session_revocation_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> tuple[TaskSessionRevocationImpactFinding, ...]:
    """Return session revocation impact findings for relevant execution tasks."""
    return build_task_session_revocation_impact_plan(source).findings


def summarize_task_session_revocation_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> TaskSessionRevocationImpactPlan:
    """Compatibility alias for building a session revocation impact plan."""
    return build_task_session_revocation_impact_plan(source)


def summarize_task_session_revocation_impacts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> TaskSessionRevocationImpactPlan:
    """Compatibility plural alias for building a session revocation impact plan."""
    return build_task_session_revocation_impact_plan(source)


def task_session_revocation_impact_plan_to_dict(
    result: TaskSessionRevocationImpactPlan,
) -> dict[str, Any]:
    """Serialize a session revocation impact plan to a plain dictionary."""
    return result.to_dict()


task_session_revocation_impact_plan_to_dict.__test__ = False


def task_session_revocation_impact_plan_to_markdown(
    result: TaskSessionRevocationImpactPlan,
) -> str:
    """Render a session revocation impact plan as Markdown."""
    return result.to_markdown()


task_session_revocation_impact_plan_to_markdown.__test__ = False


def _finding_for_task(
    task: Mapping[str, Any],
    index: int,
) -> TaskSessionRevocationImpactFinding | None:
    surfaces: dict[SessionRevocationSurface, list[str]] = {}
    safeguards: set[SessionRevocationSafeguard] = set()

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _inspect_path(path, surfaces)
    for source_field, text in _candidate_texts(task):
        _inspect_text(source_field, text, surfaces, safeguards)

    if not surfaces:
        return None

    impacted_surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surfaces)
    missing_safeguards = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in safeguards)
    task_id = _task_id(task, index)
    return TaskSessionRevocationImpactFinding(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        impacted_surfaces=impacted_surfaces,
        missing_safeguards=missing_safeguards,
        impact_level=_impact_level(impacted_surfaces, missing_safeguards),
        evidence=tuple(
            _dedupe(
                evidence
                for surface in impacted_surfaces
                for evidence in surfaces.get(surface, [])
            )
        ),
    )


def _inspect_path(
    path: str,
    surfaces: dict[SessionRevocationSurface, list[str]],
) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    evidence = f"files_or_modules: {path}"
    searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    for surface, pattern in _PATH_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(searchable):
            surfaces.setdefault(surface, []).append(evidence)


def _inspect_text(
    source_field: str,
    text: str,
    surfaces: dict[SessionRevocationSurface, list[str]],
    safeguards: set[SessionRevocationSafeguard],
) -> None:
    evidence = _evidence_snippet(source_field, text)
    for surface, pattern in _SURFACE_PATTERNS.items():
        if pattern.search(text):
            surfaces.setdefault(surface, []).append(evidence)
    for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
        if pattern.search(text):
            safeguards.add(safeguard)


def _impact_level(
    impacted_surfaces: tuple[SessionRevocationSurface, ...],
    missing_safeguards: tuple[SessionRevocationSafeguard, ...],
) -> SessionRevocationImpactLevel:
    if not missing_safeguards:
        return "low"
    if any(
        surface in impacted_surfaces
        for surface in ("logout", "token_refresh", "password_reset", "impersonation", "admin_session")
    ) and any(
        safeguard in missing_safeguards
        for safeguard in ("server_side_revocation", "refresh_token_rotation", "replay_protection", "audit_logging")
    ):
        return "high"
    return "medium"


def _summary(
    findings: tuple[TaskSessionRevocationImpactFinding, ...],
    *,
    total_task_count: int,
    ignored_task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": total_task_count,
        "impacted_task_count": len(findings),
        "ignored_task_count": ignored_task_count,
        "impact_counts": {
            level: sum(1 for finding in findings if finding.impact_level == level)
            for level in ("high", "medium", "low")
        },
        "surface_counts": {
            surface: sum(1 for finding in findings if surface in finding.impacted_surfaces)
            for surface in _SURFACE_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
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
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            return _source_payload(value)
    if _looks_like_task(source):
        return None, [_object_task_payload(source)]

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
        elif _looks_like_task(item):
            tasks.append(_object_task_payload(item))
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
        elif _looks_like_task(item):
            tasks.append(_object_task_payload(item))
    return tasks


def _object_task_payload(value: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for field_name in (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "metadata",
    ):
        if hasattr(value, field_name):
            payload[field_name] = getattr(value, field_name)
    return payload


def _looks_like_task(value: Any) -> bool:
    return any(hasattr(value, field_name) for field_name in ("id", "title", "description", "acceptance_criteria"))


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
        patterns = (*_SURFACE_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            try:
                child = value[key]
            except (KeyError, TypeError):
                continue
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
            try:
                strings.extend(_strings(value[key]))
            except (KeyError, TypeError):
                continue
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
    return (
        _text(value)
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\n", "<br>")
    )


__all__ = [
    "SessionRevocationImpactLevel",
    "SessionRevocationSafeguard",
    "SessionRevocationSurface",
    "TaskSessionRevocationImpactFinding",
    "TaskSessionRevocationImpactPlan",
    "analyze_task_session_revocation_impact",
    "build_task_session_revocation_impact_plan",
    "summarize_task_session_revocation_impact",
    "summarize_task_session_revocation_impacts",
    "task_session_revocation_impact_plan_to_dict",
    "task_session_revocation_impact_plan_to_markdown",
]
