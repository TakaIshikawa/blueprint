"""Extract source-level offline availability and sync requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


OfflineSyncCategory = Literal[
    "offline_mode",
    "local_cache",
    "sync_conflict",
    "background_sync",
    "reconnect_behavior",
    "partial_connectivity",
    "stale_data_indicator",
]
OfflineSyncMissingDetail = Literal["missing_conflict_resolution", "missing_offline_duration"]
OfflineSyncConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[OfflineSyncCategory, ...] = (
    "offline_mode",
    "local_cache",
    "sync_conflict",
    "background_sync",
    "reconnect_behavior",
    "partial_connectivity",
    "stale_data_indicator",
)
_MISSING_DETAIL_ORDER: tuple[OfflineSyncMissingDetail, ...] = (
    "missing_conflict_resolution",
    "missing_offline_duration",
)
_CONFIDENCE_ORDER: dict[OfflineSyncConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_OFFLINE_SYNC_CONTEXT_RE = re.compile(
    r"\b(?:offline|local cache|cache|caching|sync|synchroniz|"
    r"conflict|merge|reconnect|background|"
    r"network|connectivity|connection|disconnected|"
    r"stale|outdated|partial|eventual consistency)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:offline|sync|cache|network|connectivity|"
    r"requirements?|acceptance|done)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|include|return|expose|follow|implement|"
    r"acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:offline|local cache|sync|synchronization|"
    r"background sync|reconnect|conflict resolution)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:offline|local cache|sync|synchronization|"
    r"background sync|reconnect|conflict resolution)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_OFFLINE_SYNC_RE = re.compile(
    r"\b(?:no offline|no local cache|no sync|no synchronization|"
    r"no background sync|no reconnect|no conflict|"
    r"offline is out of scope|always online|online only)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:lip sync|music sync|audio sync)\b",
    re.I,
)
_CONFLICT_RESOLUTION_RE = re.compile(
    r"\b(?:conflict resolution|resolve conflicts?|merge strategy|"
    r"last write wins|version control|conflict handling)\b",
    re.I,
)
_OFFLINE_DURATION_RE = re.compile(
    r"\b(?:offline duration|offline period|offline time|"
    r"days? offline|hours? offline|offline limit)\b",
    re.I,
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
    "id",
    "source_id",
    "source_brief_id",
    "status",
    "created_by",
    "updated_by",
    "owner",
    "last_editor",
}
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "scope",
    "non_goals",
    "assumptions",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[OfflineSyncCategory, re.Pattern[str]] = {
    "offline_mode": re.compile(
        r"\b(?:offline(?: mode| support| capability| access)?|"
        r"work offline|offline[- ]first|offline functionality|"
        r"(?:without|no) (?:network|connection|connectivity)|disconnected mode)\b",
        re.I,
    ),
    "local_cache": re.compile(
        r"\b(?:local cache|local storage|cache|caching|"
        r"client[- ]side (?:cache|storage)|persist(?:ence)? locally|"
        r"IndexedDB|localStorage|SQLite|local database)\b",
        re.I,
    ),
    "sync_conflict": re.compile(
        r"\b(?:(?:sync|synchronization|merge) conflict|"
        r"conflict(?: resolution| handling| detection)?|"
        r"(?:resolve|handle|detect) conflicts?|merge strategy|"
        r"concurrent (?:edit|update|modification)|last write wins)\b",
        re.I,
    ),
    "background_sync": re.compile(
        r"\b(?:background sync(?:hronization)?|sync (?:in )?background|"
        r"(?:async|asynchronous) sync|automatic sync|"
        r"sync (?:when|after) (?:online|reconnect)|queue(?:d)? sync)\b",
        re.I,
    ),
    "reconnect_behavior": re.compile(
        r"\b(?:reconnect(?:ion)?(?: behavior| handling| strategy)?|"
        r"(?:on|after) reconnect(?:ion)?|network recovery|"
        r"(?:restore|resume) connection|connectivity (?:restored|recovered)|"
        r"automatic reconnect|retry (?:connection|sync))\b",
        re.I,
    ),
    "partial_connectivity": re.compile(
        r"\b(?:partial connectivity|limited connectivity|"
        r"(?:slow|poor|degraded|intermittent) (?:network|connection)|"
        r"flaky network|spotty connection|low bandwidth)\b",
        re.I,
    ),
    "stale_data_indicator": re.compile(
        r"\b(?:stale(?: data| indicator)?|outdated data|"
        r"(?:show|display|indicate) (?:stale|outdated|old)|"
        r"data (?:freshness|staleness|age)|timestamp|"
        r"last (?:sync(?:ed)?|updated)|offline indicator)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[OfflineSyncCategory, tuple[str, ...]] = {
    "offline_mode": ("frontend", "mobile", "platform"),
    "local_cache": ("frontend", "mobile", "backend"),
    "sync_conflict": ("backend", "platform", "data_engineering"),
    "background_sync": ("backend", "platform", "mobile"),
    "reconnect_behavior": ("frontend", "mobile", "backend"),
    "partial_connectivity": ("frontend", "mobile", "backend"),
    "stale_data_indicator": ("frontend", "mobile", "ux"),
}
_PLANNING_NOTES: dict[OfflineSyncCategory, tuple[str, ...]] = {
    "offline_mode": ("Define offline mode scope, what features work offline, and data synchronization strategy.",),
    "local_cache": ("Specify local cache implementation, storage mechanism, size limits, and expiration policy.",),
    "sync_conflict": ("Document conflict resolution strategy, merge rules, conflict detection, and user conflict resolution flow.",),
    "background_sync": ("Plan background sync triggers, queue management, retry policy, and network condition handling.",),
    "reconnect_behavior": ("Define reconnect detection, automatic sync on reconnect, state restoration, and error recovery.",),
    "partial_connectivity": ("Specify behavior under degraded network conditions, fallback strategies, and timeout handling.",),
    "stale_data_indicator": ("Document stale data indication strategy, timestamp display, refresh UI, and user messaging.",),
}
_GAP_MESSAGES: dict[OfflineSyncMissingDetail, str] = {
    "missing_conflict_resolution": "Specify conflict resolution strategy, merge rules, and user conflict handling.",
    "missing_offline_duration": "Define how long data remains available offline and cache expiration policy.",
}


@dataclass(frozen=True, slots=True)
class SourceOfflineSyncRequirement:
    """One source-backed offline sync requirement."""

    category: OfflineSyncCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: OfflineSyncConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> OfflineSyncCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> OfflineSyncCategory:
        """Compatibility view for extractors that expose concern naming."""
        return self.category

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        """Compatibility view matching adjacent source extractors."""
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
            "gap_messages": list(self.gap_messages),
        }


@dataclass(frozen=True, slots=True)
class SourceOfflineSyncRequirementsReport:
    """Source-level offline sync requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceOfflineSyncRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceOfflineSyncRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceOfflineSyncRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return offline sync requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Offline Sync Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source offline sync requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
            ]
        )
        for req in self.requirements:
            lines.extend(
                [
                    f"### {req.category}",
                    "",
                    f"- Source field: `{req.source_field}`",
                    f"- Confidence: {req.confidence}",
                ]
            )
            if req.value:
                lines.append(f"- Value: {req.value}")
            if req.evidence:
                lines.extend(["- Evidence:", *[f"  - {ev}" for ev in req.evidence]])
            if req.suggested_owners:
                lines.append(f"- Suggested owners: {', '.join(req.suggested_owners)}")
            if req.planning_notes:
                lines.extend(["- Planning notes:", *[f"  - {note}" for note in req.planning_notes]])
            if req.gap_messages:
                lines.extend(["- Gaps:", *[f"  - {gap}" for gap in req.gap_messages]])
            lines.append("")
        return "\n".join(lines)


def extract_source_offline_sync_requirements(
    brief: SourceBrief | ImplementationBrief | Mapping[str, Any] | str | object,
) -> SourceOfflineSyncRequirementsReport:
    """Extract source offline sync requirements from a source or implementation brief."""
    brief_id, title, payload = _brief_payload(brief)
    if _has_negated_scope(payload):
        return SourceOfflineSyncRequirementsReport(
            brief_id=brief_id,
            title=title,
            requirements=tuple(),
            summary=_empty_summary(),
        )

    requirements: list[SourceOfflineSyncRequirement] = []
    seen_categories: set[OfflineSyncCategory] = set()

    for category in _CATEGORY_ORDER:
        if category in seen_categories:
            continue
        matches = _find_category_matches(payload, category)
        if not matches:
            continue
        seen_categories.add(category)
        evidence, source_field, confidence, value = _best_match(matches, category)
        gaps = _detect_gaps(payload, category)
        requirements.append(
            SourceOfflineSyncRequirement(
                category=category,
                source_field=source_field,
                evidence=evidence,
                confidence=confidence,
                value=value,
                suggested_owners=_OWNER_SUGGESTIONS.get(category, tuple()),
                planning_notes=_PLANNING_NOTES.get(category, tuple()),
                gap_messages=tuple(_GAP_MESSAGES[g] for g in gaps),
            )
        )

    return SourceOfflineSyncRequirementsReport(
        brief_id=brief_id,
        title=title,
        requirements=tuple(requirements),
        summary=_compute_summary(requirements),
    )


def _brief_payload(
    brief: SourceBrief | ImplementationBrief | Mapping[str, Any] | str | object,
) -> tuple[str | None, str | None, dict[str, Any]]:
    if isinstance(brief, (SourceBrief, ImplementationBrief)):
        return brief.id, getattr(brief, "title", None), dict(brief.model_dump(mode="python"))
    if isinstance(brief, str):
        return None, None, {"body": brief}
    if isinstance(brief, Mapping):
        try:
            validated = SourceBrief.model_validate(brief)
            return validated.id, getattr(validated, "title", None), dict(validated.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            pass
        try:
            validated = ImplementationBrief.model_validate(brief)
            return validated.id, getattr(validated, "title", None), dict(validated.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            pass
        return brief.get("id"), brief.get("title"), dict(brief)
    if hasattr(brief, "id"):
        payload = {}
        for field in _SCANNED_FIELDS:
            if hasattr(brief, field):
                payload[field] = getattr(brief, field)
        return getattr(brief, "id", None), getattr(brief, "title", None), payload
    return None, None, {}


def _has_negated_scope(payload: Mapping[str, Any]) -> bool:
    def _collect_texts(value: Any) -> list[str]:
        if isinstance(value, dict):
            return [text for v in value.values() for text in _collect_texts(v)]
        if isinstance(value, (list, tuple)):
            return [text for item in value for text in _collect_texts(item)]
        if value and not isinstance(value, (bool, int, float)):
            return [str(value)]
        return []

    # Check top-level fields and nested source_payload for scoping statements
    all_texts = _collect_texts(payload)
    for text in all_texts:
        if _NO_OFFLINE_SYNC_RE.search(text):
            return True
    # Only check NEGATED_SCOPE_RE on top-level scoping fields to avoid false positives
    scoping_texts = [
        str(payload.get("summary", "")),
        str(payload.get("scope", "")),
        str(payload.get("non_goals", "")),
        str(payload.get("constraints", "")),
    ]
    for text in scoping_texts:
        if _NEGATED_SCOPE_RE.search(text):
            return True
    return False


def _find_category_matches(payload: Mapping[str, Any], category: OfflineSyncCategory) -> list[tuple[str, str, str]]:
    pattern = _CATEGORY_PATTERNS[category]
    matches: list[tuple[str, str, str]] = []

    def _scan_value(field_name: str, value: Any, parent_has_context: bool = False) -> None:
        if isinstance(value, dict):
            # Recursively scan nested dictionaries
            # Check if this dict level has offline sync context
            dict_text = " ".join(str(v) for v in value.values() if v)
            has_context = parent_has_context or bool(_OFFLINE_SYNC_CONTEXT_RE.search(dict_text))
            for nested_key, nested_value in value.items():
                nested_field = f"{field_name}.{nested_key}" if field_name else nested_key
                _scan_value(nested_field, nested_value, has_context)
        elif isinstance(value, (list, tuple)):
            # Scan list/tuple items
            for item in value:
                _scan_value(field_name, item, parent_has_context)
        elif value:
            text = str(value)
            if _UNRELATED_RE.search(text):
                return
            # Only require context if parent doesn't have it and text is long enough
            if not parent_has_context and len(text) > 50 and not _OFFLINE_SYNC_CONTEXT_RE.search(text):
                return

            for match in pattern.finditer(text):
                snippet = text[max(0, match.start() - 40) : min(len(text), match.end() + 40)]
                snippet = _SPACE_RE.sub(" ", snippet).strip()
                matches.append((field_name, snippet, match.group(0)))

    for field_name in _SCANNED_FIELDS:
        if field_name in _IGNORED_FIELDS:
            continue
        value = payload.get(field_name)
        if value:
            _scan_value(field_name, value)

    return matches


def _best_match(
    matches: list[tuple[str, str, str]], category: OfflineSyncCategory
) -> tuple[tuple[str, ...], str, OfflineSyncConfidence, str | None]:
    if not matches:
        return tuple(), "", "low", None

    field_name, snippet, keyword = matches[0]
    evidence = tuple(f"{field_name}: ...{snippet}..." for field_name, snippet, _ in matches[:3])

    confidence: OfflineSyncConfidence = "medium"
    if _REQUIREMENT_RE.search(snippet):
        confidence = "high"
    elif not _STRUCTURED_FIELD_RE.search(field_name):
        confidence = "low"

    value = None

    return evidence, field_name, confidence, value


def _detect_gaps(payload: Mapping[str, Any], category: OfflineSyncCategory) -> list[OfflineSyncMissingDetail]:
    gaps: list[OfflineSyncMissingDetail] = []
    searchable = " ".join(str(v) for v in payload.values() if v)

    if category == "sync_conflict":
        if not _CONFLICT_RESOLUTION_RE.search(searchable):
            gaps.append("missing_conflict_resolution")

    if category in ("offline_mode", "local_cache"):
        if not _OFFLINE_DURATION_RE.search(searchable):
            gaps.append("missing_offline_duration")

    return gaps


def _compute_summary(requirements: list[SourceOfflineSyncRequirement]) -> dict[str, Any]:
    category_counts = {category: 0 for category in _CATEGORY_ORDER}
    confidence_counts = {"high": 0, "medium": 0, "low": 0}
    missing_detail_flags: set[str] = set()

    for req in requirements:
        category_counts[req.category] += 1
        confidence_counts[req.confidence] += 1
        for gap_msg in req.gap_messages:
            for detail, msg in _GAP_MESSAGES.items():
                if msg == gap_msg:
                    missing_detail_flags.add(detail)

    return {
        "requirement_count": len(requirements),
        "category_counts": category_counts,
        "confidence_counts": confidence_counts,
        "missing_detail_flags": sorted(missing_detail_flags),
    }


def _empty_summary() -> dict[str, Any]:
    return {
        "requirement_count": 0,
        "category_counts": {category: 0 for category in _CATEGORY_ORDER},
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
    }


__all__ = [
    "OfflineSyncCategory",
    "OfflineSyncMissingDetail",
    "OfflineSyncConfidence",
    "SourceOfflineSyncRequirement",
    "SourceOfflineSyncRequirementsReport",
    "extract_source_offline_sync_requirements",
]
