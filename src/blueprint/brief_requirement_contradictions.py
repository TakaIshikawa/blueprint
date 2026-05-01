"""Detect contradictory requirements inside brief records."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ContradictionSeverity = Literal["high", "medium", "low"]
ContradictionType = Literal["direct_negation", "deadline_mismatch", "platform_mismatch"]

_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SPLIT_RE = re.compile(r"(?:\r?\n|;)+")
_NEGATION_RE = re.compile(
    r"^(?:do\s+not|don't|dont|must\s+not|should\s+not|will\s+not|won't|never|no|"
    r"avoid|exclude|excluding|out\s+of\s+scope|not)\s+",
    re.IGNORECASE,
)
_EXCLUSION_FIELD_RE = re.compile(r"(?:non_goals?|exclusions?|out_of_scope)", re.I)
_BEFORE_RE = re.compile(
    r"\b(?:by|before|no\s+later\s+than|due\s+by|deadline(?:\s+is)?|target(?:\s+date)?(?:\s+is)?)"
    r"\s+(?P<date>\d{4}-\d{1,2}-\d{1,2}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})\b",
    re.IGNORECASE,
)
_AFTER_RE = re.compile(
    r"\b(?:after|not\s+before|not\s+be\s+available\s+before|"
    r"will\s+not\s+be\s+available\s+before|no\s+earlier\s+than|starting|start\s+after)"
    r"\s+(?P<date>\d{4}-\d{1,2}-\d{1,2}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})\b",
    re.IGNORECASE,
)
_DATE_VALUE_RE = re.compile(
    r"^\s*(?P<date>\d{4}-\d{1,2}-\d{1,2}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})\s*$",
    re.IGNORECASE,
)
_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_PLATFORM_GROUPS: dict[str, set[str]] = {
    "web": {"web", "browser", "desktop web", "responsive web", "admin dashboard"},
    "ios": {"ios", "iphone", "ipad"},
    "android": {"android"},
    "mobile": {"mobile", "native mobile", "mobile app"},
    "desktop": {"desktop", "electron"},
    "cli": {"cli", "command line", "terminal"},
}
_PLATFORM_EXCLUSIVE = {
    frozenset(("web", "ios")),
    frozenset(("web", "android")),
    frozenset(("web", "mobile")),
    frozenset(("cli", "web")),
    frozenset(("cli", "ios")),
    frozenset(("cli", "android")),
    frozenset(("cli", "mobile")),
    frozenset(("desktop", "ios")),
    frozenset(("desktop", "android")),
    frozenset(("desktop", "mobile")),
}
_ONLY_RE = re.compile(r"\b(?:only|web-only|ios-only|android-only|cli-only|desktop-only)\b", re.I)
_NO_PLATFORM_RE = re.compile(
    r"\b(?:no|not|without|avoid|exclude|excluding|must\s+not|do\s+not)\s+"
    r"(?:native\s+)?(?:mobile|ios|android|web|browser|desktop|cli|command line)\b",
    re.IGNORECASE,
)
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "by",
    "can",
    "do",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "its",
    "must",
    "not",
    "of",
    "on",
    "or",
    "our",
    "scope",
    "should",
    "than",
    "the",
    "their",
    "this",
    "to",
    "with",
    "without",
}
_BOILERPLATE_TERMS = {
    "acceptance",
    "add",
    "avoid",
    "brief",
    "build",
    "constraint",
    "create",
    "deliver",
    "done",
    "enable",
    "exclude",
    "goal",
    "implement",
    "include",
    "keep",
    "mvp",
    "new",
    "plan",
    "provide",
    "requirement",
    "support",
    "update",
    "use",
    "work",
}


@dataclass(frozen=True, slots=True)
class BriefRequirementContradictionFinding:
    """One pair of brief requirements that conflict."""

    conflict_type: ContradictionType
    field_names: tuple[str, str]
    matched_text: tuple[str, str]
    matched_terms: tuple[str, ...]
    severity: ContradictionSeverity
    clarification_question: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "conflict_type": self.conflict_type,
            "field_names": list(self.field_names),
            "matched_text": list(self.matched_text),
            "matched_terms": list(self.matched_terms),
            "severity": self.severity,
            "clarification_question": self.clarification_question,
        }


@dataclass(frozen=True, slots=True)
class BriefRequirementContradictionReport:
    """Requirement contradiction findings for a source or implementation brief."""

    brief_id: str | None = None
    findings: tuple[BriefRequirementContradictionFinding, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return finding records as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]


@dataclass(frozen=True, slots=True)
class _RequirementText:
    field_name: str
    text: str


def detect_brief_requirement_contradictions(
    brief: Mapping[str, Any] | ImplementationBrief | SourceBrief,
) -> BriefRequirementContradictionReport:
    """Find contradictions across goals, constraints, assumptions, exclusions, and requirements."""
    payload = _brief_payload(brief)
    requirements = _requirement_texts(payload)
    findings = []
    seen: set[tuple[str, str, str, str, str]] = set()

    for left_index, left in enumerate(requirements):
        for right in requirements[left_index + 1 :]:
            for finding in _findings_for_pair(left, right):
                key = (
                    finding.conflict_type,
                    finding.field_names[0],
                    finding.field_names[1],
                    _dedupe_key(finding.matched_text[0]),
                    _dedupe_key(finding.matched_text[1]),
                )
                if key in seen:
                    continue
                findings.append(finding)
                seen.add(key)

    findings.sort(
        key=lambda item: (
            {"high": 0, "medium": 1, "low": 2}[item.severity],
            item.conflict_type,
            item.field_names,
            item.matched_text,
        )
    )
    result = tuple(findings)
    return BriefRequirementContradictionReport(
        brief_id=_optional_text(payload.get("id")) or _optional_text(payload.get("source_id")),
        findings=result,
        summary={
            "requirement_count": len(requirements),
            "finding_count": len(result),
            "high_severity_count": sum(1 for item in result if item.severity == "high"),
            "medium_severity_count": sum(1 for item in result if item.severity == "medium"),
            "low_severity_count": sum(1 for item in result if item.severity == "low"),
        },
    )


def brief_requirement_contradictions_to_dict(
    report: BriefRequirementContradictionReport,
) -> dict[str, Any]:
    """Serialize a requirement contradiction report to a plain dictionary."""
    return report.to_dict()


brief_requirement_contradictions_to_dict.__test__ = False


def find_brief_requirement_contradictions(
    brief: Mapping[str, Any] | ImplementationBrief | SourceBrief,
) -> BriefRequirementContradictionReport:
    """Compatibility alias for detecting brief requirement contradictions."""
    return detect_brief_requirement_contradictions(brief)


def _findings_for_pair(
    left: _RequirementText,
    right: _RequirementText,
) -> tuple[BriefRequirementContradictionFinding, ...]:
    findings: list[BriefRequirementContradictionFinding] = []
    if direct := _direct_negation(left, right):
        findings.append(direct)
    if deadline := _deadline_mismatch(left, right):
        findings.append(deadline)
    if platform := _platform_mismatch(left, right):
        findings.append(platform)
    return tuple(findings)


def _direct_negation(
    left: _RequirementText,
    right: _RequirementText,
) -> BriefRequirementContradictionFinding | None:
    left_phrase = _requirement_phrase(left)
    right_phrase = _requirement_phrase(right)
    if not left_phrase or not right_phrase:
        return None

    left_negative = _is_negative(left)
    right_negative = _is_negative(right)
    if left_negative == right_negative:
        return None

    left_tokens = _significant_tokens(left_phrase)
    right_tokens = _significant_tokens(right_phrase)
    overlap = left_tokens & right_tokens
    if _normalized(left_phrase) != _normalized(right_phrase) and not _is_meaningful_overlap(
        left_tokens | right_tokens, overlap
    ):
        return None

    terms = tuple(_dedupe(sorted(overlap) or [_normalized(left_phrase)]))
    severity: ContradictionSeverity = "high" if _is_exclusion(left) or _is_exclusion(right) else "medium"
    return _finding(
        conflict_type="direct_negation",
        left=left,
        right=right,
        matched_terms=terms,
        severity=severity,
        question=(
            f"Should `{_snippet(_positive_text(left.text), 80)}` be included or excluded "
            "from this brief?"
        ),
    )


def _deadline_mismatch(
    left: _RequirementText,
    right: _RequirementText,
) -> BriefRequirementContradictionFinding | None:
    left_deadlines = _deadline_constraints(left)
    right_deadlines = _deadline_constraints(right)
    for left_kind, left_date in left_deadlines:
        for right_kind, right_date in right_deadlines:
            if left_kind == right_kind:
                continue
            if left_kind == "before" and right_kind == "after" and left_date < right_date:
                return _deadline_finding(left, right, left_date, right_date)
            if left_kind == "after" and right_kind == "before" and right_date < left_date:
                return _deadline_finding(left, right, right_date, left_date)
    return None


def _deadline_finding(
    left: _RequirementText,
    right: _RequirementText,
    before_date: date,
    after_date: date,
) -> BriefRequirementContradictionFinding:
    return _finding(
        conflict_type="deadline_mismatch",
        left=left,
        right=right,
        matched_terms=(before_date.isoformat(), after_date.isoformat()),
        severity="high",
        question=(
            f"Which deadline should govern: completion by {before_date.isoformat()} "
            f"or no earlier than {after_date.isoformat()}?"
        ),
    )


def _platform_mismatch(
    left: _RequirementText,
    right: _RequirementText,
) -> BriefRequirementContradictionFinding | None:
    left_platforms = _platforms(left.text)
    right_platforms = _platforms(right.text)
    if not left_platforms or not right_platforms:
        return None

    left_exclusive = _platform_exclusive_requirement(left)
    right_exclusive = _platform_exclusive_requirement(right)
    for left_platform in left_platforms:
        for right_platform in right_platforms:
            if left_platform == right_platform:
                continue
            if frozenset((left_platform, right_platform)) not in _PLATFORM_EXCLUSIVE:
                continue
            if not (left_exclusive or right_exclusive or _is_negative(left) or _is_negative(right)):
                continue
            return _finding(
                conflict_type="platform_mismatch",
                left=left,
                right=right,
                matched_terms=tuple(sorted({left_platform, right_platform})),
                severity="high",
                question=(
                    "Which platform requirement is authoritative: "
                    f"{left_platform} or {right_platform}?"
                ),
            )
    return None


def _finding(
    *,
    conflict_type: ContradictionType,
    left: _RequirementText,
    right: _RequirementText,
    matched_terms: tuple[str, ...],
    severity: ContradictionSeverity,
    question: str,
) -> BriefRequirementContradictionFinding:
    return BriefRequirementContradictionFinding(
        conflict_type=conflict_type,
        field_names=(left.field_name, right.field_name),
        matched_text=(left.text, right.text),
        matched_terms=matched_terms,
        severity=severity,
        clarification_question=question,
    )


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief | SourceBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        value = brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(brief, Mapping):
        try:
            return ImplementationBrief.model_validate(brief).model_dump(mode="python")
        except (TypeError, ValueError, ValidationError):
            try:
                return SourceBrief.model_validate(brief).model_dump(mode="python")
            except (TypeError, ValueError, ValidationError):
                return dict(brief)
    return {}


def _requirement_texts(payload: Mapping[str, Any]) -> list[_RequirementText]:
    texts: list[_RequirementText] = []
    for field_name in (
        "mvp_goal",
        "goals",
        "scope",
        "constraints",
        "requirements",
        "acceptance_criteria",
        "definition_of_done",
        "assumptions",
        "non_goals",
        "exclusions",
        "out_of_scope",
        "deadline",
        "due_date",
        "target_date",
        "platform",
        "platforms",
        "product_surface",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
    ):
        _append_field_texts(texts, field_name, payload.get(field_name))

    source_payload = payload.get("source_payload")
    if isinstance(source_payload, Mapping):
        normalized = source_payload.get("normalized")
        if isinstance(normalized, Mapping):
            for field_name, value in normalized.items():
                if _is_requirement_field(field_name):
                    _append_field_texts(texts, field_name, value)
        elif isinstance(normalized, str):
            _append_field_texts(texts, "source_payload.normalized", normalized)

    return texts


def _append_field_texts(
    texts: list[_RequirementText],
    field_name: str,
    value: Any,
) -> None:
    for index, text in enumerate(_strings(value)):
        item_field = field_name if index == 0 and not isinstance(value, (list, tuple, set)) else f"{field_name}[{index}]"
        texts.append(_RequirementText(item_field, text))


def _is_requirement_field(field_name: str) -> bool:
    normalized = field_name.lower()
    return any(
        key in normalized
        for key in (
            "goal",
            "scope",
            "constraint",
            "requirement",
            "acceptance",
            "done",
            "assumption",
            "exclusion",
            "deadline",
            "due",
            "target_date",
            "platform",
        )
    )


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [text for part in _SPLIT_RE.split(value) if (text := _optional_text(part))]
    if isinstance(value, (list, tuple)):
        return [text for item in value if (text := _optional_text(item))]
    if isinstance(value, set):
        return [text for item in sorted(value, key=str) if (text := _optional_text(item))]
    if isinstance(value, (date,)):
        return [value.isoformat()]
    return []


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = _SPACE_RE.sub(" ", value).strip()
        return text or None
    return None


def _is_negative(requirement: _RequirementText) -> bool:
    return bool(_NEGATION_RE.search(requirement.text)) or _is_exclusion(requirement)


def _is_exclusion(requirement: _RequirementText) -> bool:
    return bool(_EXCLUSION_FIELD_RE.search(requirement.field_name))


def _requirement_phrase(requirement: _RequirementText) -> str:
    if _is_exclusion(requirement):
        return _positive_text(requirement.text)
    return _positive_text(requirement.text) if _NEGATION_RE.search(requirement.text) else requirement.text


def _positive_text(text: str) -> str:
    return _NEGATION_RE.sub("", text).strip(" .,:;")


def _deadline_constraints(requirement: _RequirementText) -> list[tuple[Literal["before", "after"], date]]:
    constraints: list[tuple[Literal["before", "after"], date]] = []
    for match in _BEFORE_RE.finditer(requirement.text):
        if parsed := _parse_date(match.group("date")):
            constraints.append(("before", parsed))
    for match in _AFTER_RE.finditer(requirement.text):
        if parsed := _parse_date(match.group("date")):
            constraints.append(("after", parsed))
    if _deadline_field(requirement.field_name) and (match := _DATE_VALUE_RE.match(requirement.text)):
        if parsed := _parse_date(match.group("date")):
            constraints.append(("before", parsed))
    return constraints


def _deadline_field(field_name: str) -> bool:
    lowered = field_name.lower()
    return "deadline" in lowered or "due_date" in lowered or "target_date" in lowered


def _parse_date(value: str) -> date | None:
    text = value.strip().replace(",", "")
    iso_match = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", text)
    if iso_match:
        try:
            return date(int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3)))
        except ValueError:
            return None

    parts = text.split()
    if len(parts) == 3 and parts[0].lower() in _MONTHS:
        try:
            return date(int(parts[2]), _MONTHS[parts[0].lower()], int(parts[1]))
        except ValueError:
            return None
    return None


def _platforms(text: str) -> set[str]:
    normalized = _normalized(text)
    platforms: set[str] = set()
    for platform, phrases in _PLATFORM_GROUPS.items():
        if any(_boundary_search(normalized, _normalized(phrase)) for phrase in phrases):
            platforms.add(platform)
    if "ios" in platforms or "android" in platforms:
        platforms.add("mobile")
    return platforms


def _platform_exclusive_requirement(requirement: _RequirementText) -> bool:
    return bool(_ONLY_RE.search(requirement.text) or _NO_PLATFORM_RE.search(requirement.text))


def _normalized(value: str) -> str:
    return " ".join(_tokens(value))


def _tokens(value: str) -> list[str]:
    return _TOKEN_RE.findall(value.lower())


def _significant_tokens(value: str) -> set[str]:
    return {
        token
        for token in _tokens(value)
        if token not in _STOP_WORDS and token not in _BOILERPLATE_TERMS
    }


def _is_meaningful_overlap(all_tokens: set[str], overlap: set[str]) -> bool:
    if not overlap:
        return False
    if len(overlap) >= 2:
        return True
    return len(all_tokens) <= 2 and bool(overlap)


def _boundary_search(text: str, phrase: str) -> bool:
    if not text or not phrase:
        return False
    return bool(re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", text))


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _dedupe_key(value: str) -> str:
    return " ".join(_tokens(value))


def _snippet(value: str, limit: int) -> str:
    text = _SPACE_RE.sub(" ", value).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


__all__ = [
    "BriefRequirementContradictionFinding",
    "BriefRequirementContradictionReport",
    "ContradictionSeverity",
    "ContradictionType",
    "brief_requirement_contradictions_to_dict",
    "detect_brief_requirement_contradictions",
    "find_brief_requirement_contradictions",
]
