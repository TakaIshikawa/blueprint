"""Build dependency maps from implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief


DependencyCategory = Literal["integration", "data"]

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SPLIT_RE = re.compile(r"(?:\r?\n|;)+")
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}


@dataclass(frozen=True, slots=True)
class BriefDependencyEntry:
    """One dependency surfaced from an implementation brief."""

    id: str
    name: str
    category: DependencyCategory
    evidence: tuple[str, ...] = field(default_factory=tuple)
    risk_hints: tuple[str, ...] = field(default_factory=tuple)
    validation_covered: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "id": self.id,
            "name": self.name,
            "category": self.category,
            "evidence": list(self.evidence),
            "risk_hints": list(self.risk_hints),
            "validation_covered": self.validation_covered,
        }


@dataclass(frozen=True, slots=True)
class BriefDependencyMap:
    """Dependency inventory for planning an implementation brief."""

    brief_id: str | None
    dependencies: tuple[BriefDependencyEntry, ...] = field(default_factory=tuple)
    uncovered_dependency_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "dependencies": [dependency.to_dict() for dependency in self.dependencies],
            "uncovered_dependency_ids": list(self.uncovered_dependency_ids),
        }


def build_brief_dependency_map(
    brief: Mapping[str, Any] | ImplementationBrief,
) -> BriefDependencyMap:
    """Build a dependency map from an implementation brief."""
    payload = _brief_payload(brief)
    assumptions = _strings(payload.get("assumptions"))
    architecture_notes = _strings(payload.get("architecture_notes"))
    risks = _strings(payload.get("risks"))
    validation_plan = _optional_text(payload.get("validation_plan"))
    validation_texts = [validation_plan] if validation_plan else []

    dependencies: list[BriefDependencyEntry] = []
    seen_ids: set[str] = set()

    for name in _strings(payload.get("integration_points")):
        dependency = _dependency_entry(
            name=name,
            category="integration",
            source_field="integration_points",
            assumptions=assumptions,
            architecture_notes=architecture_notes,
            risks=risks,
            validation_texts=validation_texts,
            seen_ids=seen_ids,
        )
        dependencies.append(dependency)

    for name in _strings(payload.get("data_requirements")):
        dependency = _dependency_entry(
            name=name,
            category="data",
            source_field="data_requirements",
            assumptions=assumptions,
            architecture_notes=architecture_notes,
            risks=risks,
            validation_texts=validation_texts,
            seen_ids=seen_ids,
        )
        dependencies.append(dependency)

    uncovered_dependency_ids = tuple(
        dependency.id
        for dependency in dependencies
        if not dependency.validation_covered
    )
    return BriefDependencyMap(
        brief_id=_optional_text(payload.get("id")),
        dependencies=tuple(dependencies),
        uncovered_dependency_ids=uncovered_dependency_ids,
    )


def brief_dependency_map_to_dict(dependency_map: BriefDependencyMap) -> dict[str, Any]:
    """Serialize a brief dependency map to a dictionary."""
    return dependency_map.to_dict()


brief_dependency_map_to_dict.__test__ = False


def _dependency_entry(
    *,
    name: str,
    category: DependencyCategory,
    source_field: str,
    assumptions: list[str],
    architecture_notes: list[str],
    risks: list[str],
    validation_texts: list[str],
    seen_ids: set[str],
) -> BriefDependencyEntry:
    dependency_id = _unique_id(f"{category}-{_slugify(name)}", seen_ids)
    evidence = [f"{source_field}: {name}"]

    for text in assumptions:
        if _mentions_dependency(text, name):
            evidence.append(f"assumptions: {text}")

    for text in architecture_notes:
        if _mentions_dependency(text, name):
            evidence.append(f"architecture_notes: {text}")

    validation_matches = [
        text for text in validation_texts if _mentions_dependency(text, name)
    ]
    evidence.extend(f"validation_plan: {text}" for text in validation_matches)

    return BriefDependencyEntry(
        id=dependency_id,
        name=name,
        category=category,
        evidence=tuple(evidence),
        risk_hints=tuple(text for text in risks if _mentions_dependency(text, name)),
        validation_covered=bool(validation_matches),
    )


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
        return [item for part in _SPLIT_RE.split(value) if (item := _optional_text(part))]
    if isinstance(value, (list, tuple, set)):
        return [text for item in value if (text := _optional_text(item))]
    return []


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _mentions_dependency(text: str, dependency_name: str) -> bool:
    normalized_text = _normalized(text)
    normalized_name = _normalized(dependency_name)
    if not normalized_text or not normalized_name:
        return False
    if normalized_name in normalized_text:
        return True

    name_tokens = _significant_tokens(dependency_name)
    text_tokens = set(_tokens(text))
    return bool(name_tokens) and all(token in text_tokens for token in name_tokens)


def _normalized(value: str) -> str:
    return " ".join(_tokens(value))


def _significant_tokens(value: str) -> list[str]:
    return [token for token in _tokens(value) if token not in _STOP_WORDS]


def _tokens(value: str) -> list[str]:
    return _TOKEN_RE.findall(value.lower())


def _slugify(value: str) -> str:
    return "-".join(_significant_tokens(value)) or "dependency"


def _unique_id(base_id: str, seen_ids: set[str]) -> str:
    dependency_id = base_id
    suffix = 2
    while dependency_id in seen_ids:
        dependency_id = f"{base_id}-{suffix}"
        suffix += 1
    seen_ids.add(dependency_id)
    return dependency_id


__all__ = [
    "BriefDependencyEntry",
    "BriefDependencyMap",
    "build_brief_dependency_map",
    "brief_dependency_map_to_dict",
]
