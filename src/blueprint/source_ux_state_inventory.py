"""Extract product UI state requirements from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceUxState = Literal[
    "loading",
    "empty",
    "error",
    "offline",
    "permission_denied",
    "success",
    "partial_data",
    "disabled",
    "first_run",
    "upgrade_required",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_STATE_ORDER: dict[SourceUxState, int] = {
    "loading": 0,
    "empty": 1,
    "error": 2,
    "offline": 3,
    "permission_denied": 4,
    "success": 5,
    "partial_data": 6,
    "disabled": 7,
    "first_run": 8,
    "upgrade_required": 9,
}
_STATE_PATTERNS: dict[SourceUxState, re.Pattern[str]] = {
    "loading": re.compile(
        r"\b(?:loading|loads?|loader|spinner|skeleton|pending state|in progress|progress indicator|"
        r"while (?:data|content|results?) (?:loads?|is loading)|fetching)\b",
        re.I,
    ),
    "empty": re.compile(
        r"\b(?:empty state|zero state|blank state|no results?|no data|nothing to show|no items?|"
        r"when (?:there are )?no\b)",
        re.I,
    ),
    "error": re.compile(
        r"\b(?:error state|errors?|failed|failure|retry|server error|validation error|unable to load|"
        r"could not load|something went wrong)\b",
        re.I,
    ),
    "offline": re.compile(
        r"\b(?:offline|no connection|network unavailable|network error|disconnected|reconnect|connectivity)\b",
        re.I,
    ),
    "permission_denied": re.compile(
        r"\b(?:permission denied|access denied|unauthori[sz]ed|forbidden|not allowed|"
        r"insufficient permissions?|missing permissions?|restricted access)\b",
        re.I,
    ),
    "success": re.compile(
        r"\b(?:success state|success message|confirmation|confirmed|completed|saved|submitted|"
        r"done state|toast confirms?|receipt)\b",
        re.I,
    ),
    "partial_data": re.compile(
        r"\b(?:partial data|partially loaded|incomplete results?|some data unavailable|degraded|"
        r"stale data|cached data|limited results?|partial results?)\b",
        re.I,
    ),
    "disabled": re.compile(
        r"\b(?:disabled state|disabled button|disabled control|disabled action|unavailable action|"
        r"inactive control|greyed out|grayed out|read[- ]only)\b",
        re.I,
    ),
    "first_run": re.compile(
        r"\b(?:first run|first[- ]time|new users?|onboarding|getting started|welcome state|"
        r"initial setup|first use)\b",
        re.I,
    ),
    "upgrade_required": re.compile(
        r"\b(?:upgrade required|upgrade prompt|upgrade plan|paywall|paid plan|premium plan|"
        r"subscription required|billing required|plan limit|upsell)\b",
        re.I,
    ),
}
_AUDIENCE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b(?:customer admins?|admin users?|admins?)\b", re.I), "admins"),
    (re.compile(r"\b(?:anonymous users?|unauthenticated users?|guests?)\b", re.I), "unauthenticated users"),
    (re.compile(r"\b(?:free users?|free plan users?|trial users?)\b", re.I), "free users"),
    (re.compile(r"\b(?:paid users?|premium users?|subscribers?)\b", re.I), "paid users"),
    (re.compile(r"\b(?:first[- ]time users?|new users?)\b", re.I), "new users"),
    (re.compile(r"\b(?:mobile users?|desktop users?|read[- ]only users?)\b", re.I), "users"),
    (re.compile(r"\b(?:viewers?|editors?|owners?|operators?|agents?|members?|customers?)\b", re.I), "users"),
)
_STRUCTURED_CONFIDENCE_FIELDS = {
    "acceptance_criteria",
    "criteria",
    "definition_of_done",
    "constraints",
    "personas",
    "persona",
    "ux_states",
    "ui_states",
    "states",
}
_BRIEF_TEXT_FIELDS = (
    "title",
    "summary",
    "domain",
    "target_user",
    "buyer",
    "workflow_context",
    "problem_statement",
    "mvp_goal",
    "product_surface",
    "architecture_notes",
    "data_requirements",
    "validation_plan",
    "generation_prompt",
)
_BRIEF_LIST_FIELDS = (
    "personas",
    "constraints",
    "acceptance_criteria",
    "criteria",
    "notes",
    "scope",
    "non_goals",
    "assumptions",
    "risks",
    "definition_of_done",
    "integration_points",
)
_PAYLOAD_FIELDS = ("source_payload", "metadata", "payload")


@dataclass(frozen=True, slots=True)
class SourceUxStateRequirement:
    """One UI state requirement inferred from source brief evidence."""

    state: SourceUxState
    audience: str
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_acceptance_criterion: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "state": self.state,
            "audience": self.audience,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "suggested_acceptance_criterion": self.suggested_acceptance_criterion,
        }


@dataclass(frozen=True, slots=True)
class SourceUxStateInventory:
    """Inventory of UI state requirements found in a source brief."""

    source_id: str | None = None
    requirements: tuple[SourceUxStateRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return UI state requirements as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    @property
    def records(self) -> tuple[SourceUxStateRequirement, ...]:
        """Compatibility view matching inventories that name rows records."""
        return self.requirements


def build_source_ux_state_inventory(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief,
) -> SourceUxStateInventory:
    """Build a UI state inventory from a source or implementation brief."""
    payload = _source_payload(source)
    source_id = _source_id(payload)
    detected: dict[tuple[SourceUxState, str], list[tuple[str, float]]] = {}

    for source_field, text in _candidate_texts(payload):
        audience = _audience_for(source_field, text, payload)
        confidence = _base_confidence(source_field)
        evidence = _evidence_snippet(source_field, text)
        for state, pattern in _STATE_PATTERNS.items():
            if pattern.search(text):
                detected.setdefault((state, audience), []).append((evidence, confidence))

    requirements = tuple(
        SourceUxStateRequirement(
            state=state,
            audience=audience,
            confidence=_confidence(evidence_confidences),
            evidence=tuple(_dedupe(evidence for evidence, _ in evidence_confidences)),
            suggested_acceptance_criterion=_suggested_acceptance_criterion(state, audience),
        )
        for (state, audience), evidence_confidences in sorted(
            detected.items(),
            key=lambda item: (_STATE_ORDER[item[0][0]], item[0][1].casefold()),
        )
    )
    return SourceUxStateInventory(
        source_id=source_id,
        requirements=requirements,
        summary=_summary(requirements, evidence_count=sum(len(items) for items in detected.values())),
    )


def extract_source_ux_state_inventory(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief,
) -> SourceUxStateInventory:
    """Compatibility alias for building a UI state inventory."""
    return build_source_ux_state_inventory(source)


def source_ux_state_inventory_to_dict(result: SourceUxStateInventory) -> dict[str, Any]:
    """Serialize a UI state inventory to a plain dictionary."""
    return result.to_dict()


source_ux_state_inventory_to_dict.__test__ = False


def _source_payload(source: Mapping[str, Any] | SourceBrief | ImplementationBrief) -> dict[str, Any]:
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        return source.model_dump(mode="python")
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                value = model.model_validate(source).model_dump(mode="python")
                return dict(value) if isinstance(value, Mapping) else {}
            except (TypeError, ValueError, ValidationError):
                continue
        return dict(source)
    return {}


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in _BRIEF_TEXT_FIELDS:
        if text := _optional_text(payload.get(field_name)):
            texts.append((field_name, text))
    for field_name in _BRIEF_LIST_FIELDS:
        for source_field, text in _nested_texts(payload.get(field_name), field_name):
            texts.append((source_field, text))
    for field_name in _PAYLOAD_FIELDS:
        for source_field, text in _nested_texts(payload.get(field_name), field_name):
            texts.append((source_field, text))
    return texts


def _nested_texts(value: Any, prefix: str) -> list[tuple[str, str]]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in _STATE_PATTERNS.values()):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in _STATE_PATTERNS.values()):
                texts.append((field, key_text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            texts.extend(_nested_texts(item, f"{prefix}[{index}]"))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _audience_for(source_field: str, text: str, payload: Mapping[str, Any]) -> str:
    if _audience_field(source_field):
        return _audience_text(text)
    for pattern, audience in _AUDIENCE_PATTERNS:
        if pattern.search(text):
            return audience
    for field_name in ("target_user", "buyer"):
        if value := _optional_text(payload.get(field_name)):
            return _audience_text(value)
    return "users"


def _audience_field(source_field: str) -> bool:
    folded = source_field.casefold()
    return any(token in folded for token in ("persona", "audience", "target_user", "buyer", "user_role"))


def _audience_text(value: str) -> str:
    text = _text(value)
    for pattern, audience in _AUDIENCE_PATTERNS:
        if pattern.search(text):
            return audience
    if len(text) <= 48:
        return text.rstrip(".:").casefold()
    return "users"


def _base_confidence(source_field: str) -> float:
    field = source_field.split("[", 1)[0].split(".", 1)[0]
    if field in _STRUCTURED_CONFIDENCE_FIELDS:
        return 0.9
    if source_field.startswith(_PAYLOAD_FIELDS):
        return 0.84
    if field in {"summary", "problem_statement", "workflow_context", "mvp_goal"}:
        return 0.8
    return 0.72


def _confidence(evidence_confidences: list[tuple[str, float]]) -> float:
    unique_evidence_count = len(_dedupe(evidence for evidence, _ in evidence_confidences))
    best = max((confidence for _, confidence in evidence_confidences), default=0.0)
    boosted = best + min(0.08, max(0, unique_evidence_count - 1) * 0.04)
    return round(min(0.99, boosted), 2)


def _suggested_acceptance_criterion(state: SourceUxState, audience: str) -> str:
    label = state.replace("_", " ")
    return (
        f"Add acceptance criteria proving the {label} UI state is visible, actionable, "
        f"and accessible for {audience}."
    )


def _summary(
    requirements: tuple[SourceUxStateRequirement, ...],
    *,
    evidence_count: int,
) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "states": [state for state in _STATE_ORDER if any(item.state == state for item in requirements)],
        "state_counts": {
            state: sum(1 for item in requirements if item.state == state)
            for state in _STATE_ORDER
        },
        "audiences": list(_dedupe(item.audience for item in requirements)),
        "evidence_count": evidence_count,
    }


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
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
    "SourceUxState",
    "SourceUxStateInventory",
    "SourceUxStateRequirement",
    "build_source_ux_state_inventory",
    "extract_source_ux_state_inventory",
    "source_ux_state_inventory_to_dict",
]
