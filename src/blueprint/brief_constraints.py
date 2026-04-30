"""Extract implementation constraints from brief records."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief


ConstraintCategory = Literal[
    "technical",
    "data",
    "integration",
    "security",
    "scope",
    "validation",
]
ConstraintSeverity = Literal["medium", "high"]

_FIELD_CATEGORIES: tuple[tuple[str, ConstraintCategory], ...] = (
    ("assumptions", "technical"),
    ("non_goals", "scope"),
    ("architecture_notes", "technical"),
    ("data_requirements", "data"),
    ("integration_points", "integration"),
    ("risks", "technical"),
    ("validation_plan", "validation"),
)
_SPLIT_RE = re.compile(r"(?:\r?\n|;)+")
_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")

_SECURITY_KEYWORDS = {
    "auth",
    "authentication",
    "authorization",
    "encrypt",
    "encrypted",
    "encryption",
    "oauth",
    "permission",
    "permissions",
    "pii",
    "secret",
    "secrets",
    "security",
    "token",
}
_DATA_KEYWORDS = {
    "cache",
    "database",
    "data",
    "dataset",
    "migration",
    "payload",
    "persist",
    "persistence",
    "schema",
    "storage",
}
_INTEGRATION_KEYWORDS = {
    "api",
    "callback",
    "cli",
    "event",
    "events",
    "export",
    "github",
    "import",
    "integration",
    "queue",
    "webhook",
}


@dataclass(frozen=True, slots=True)
class ImplementationConstraint:
    """One actionable constraint extracted from an implementation brief."""

    category: ConstraintCategory
    source_field: str
    text: str
    severity: ConstraintSeverity = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "text": self.text,
            "severity": self.severity,
        }


def extract_implementation_constraints(
    brief: Mapping[str, Any] | ImplementationBrief,
) -> tuple[ImplementationConstraint, ...]:
    """Extract deterministic implementation constraints from a brief-like object."""
    payload = _brief_payload(brief)
    constraints: list[ImplementationConstraint] = []
    seen_texts: set[str] = set()

    for field_name, default_category in _FIELD_CATEGORIES:
        for text in _strings(payload.get(field_name)):
            dedupe_key = _dedupe_key(text)
            if dedupe_key in seen_texts:
                continue
            category = _category_for_text(text, default_category)
            constraints.append(
                ImplementationConstraint(
                    category=category,
                    source_field=field_name,
                    text=text,
                    severity=_severity_for(category, default_category),
                )
            )
            seen_texts.add(dedupe_key)

    return tuple(constraints)


def implementation_constraints_to_dicts(
    constraints: tuple[ImplementationConstraint, ...] | list[ImplementationConstraint],
) -> list[dict[str, Any]]:
    """Serialize implementation constraints to dictionaries."""
    return [constraint.to_dict() for constraint in constraints]


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(brief)


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [text for part in _SPLIT_RE.split(value) if (text := _clean_text(part))]
    if isinstance(value, (list, tuple)):
        return [text for item in value if (text := _clean_text(item))]
    if isinstance(value, set):
        return [text for item in sorted(value, key=str) if (text := _clean_text(item))]
    return []


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = _SPACE_RE.sub(" ", value).strip()
    return text or None


def _category_for_text(text: str, default_category: ConstraintCategory) -> ConstraintCategory:
    tokens = set(_TOKEN_RE.findall(text.lower()))
    if tokens & _SECURITY_KEYWORDS:
        return "security"
    if default_category == "scope":
        return "scope"
    if default_category == "validation":
        return "validation"
    if tokens & _DATA_KEYWORDS:
        return "data"
    if tokens & _INTEGRATION_KEYWORDS:
        return "integration"
    return default_category


def _severity_for(
    category: ConstraintCategory, default_category: ConstraintCategory
) -> ConstraintSeverity:
    if category in {"security", "data", "integration"} and category != default_category:
        return "high"
    if category == "security":
        return "high"
    return "medium"


def _dedupe_key(text: str) -> str:
    return " ".join(_TOKEN_RE.findall(text.lower()))


__all__ = [
    "ConstraintCategory",
    "ConstraintSeverity",
    "ImplementationConstraint",
    "extract_implementation_constraints",
    "implementation_constraints_to_dicts",
]
