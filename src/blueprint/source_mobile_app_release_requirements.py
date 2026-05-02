"""Extract source-level mobile app release requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


MobileAppReleaseCategory = Literal[
    "app_store_review",
    "versioning_build_numbers",
    "phased_release",
    "device_os_support",
    "crash_free_threshold",
    "privacy_manifest",
    "release_notes",
    "hotfix_rollback",
]
MobileAppReleaseConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[MobileAppReleaseCategory, ...] = (
    "app_store_review",
    "versioning_build_numbers",
    "phased_release",
    "device_os_support",
    "crash_free_threshold",
    "privacy_manifest",
    "release_notes",
    "hotfix_rollback",
)
_CONFIDENCE_ORDER: dict[MobileAppReleaseConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SUGGESTED_OWNERS: dict[MobileAppReleaseCategory, str] = {
    "app_store_review": "Release engineering",
    "versioning_build_numbers": "Mobile engineering",
    "phased_release": "Release engineering",
    "device_os_support": "Mobile engineering",
    "crash_free_threshold": "Quality engineering",
    "privacy_manifest": "Privacy and compliance",
    "release_notes": "Product marketing",
    "hotfix_rollback": "Incident response",
}
_PLANNING_NOTES: dict[MobileAppReleaseCategory, str] = {
    "app_store_review": "Plan App Store Connect and Google Play review submission timing, reviewer access, metadata, and approval gates.",
    "versioning_build_numbers": "Plan version name, build number, version code, signing, and release branch rules before packaging.",
    "phased_release": "Plan staged rollout percentages, hold points, metrics review, and pause or accelerate criteria.",
    "device_os_support": "Plan supported iOS, Android, SDK, device class, and deprecated OS constraints for launch.",
    "crash_free_threshold": "Plan crash-free, ANR, and stability gates with telemetry ownership before widening rollout.",
    "privacy_manifest": "Plan privacy manifests, Data Safety, nutrition labels, ATT, and SDK data disclosure updates.",
    "release_notes": "Plan store release notes, changelog copy, localization, and customer-facing update messaging.",
    "hotfix_rollback": "Plan hotfix, rollback, expedited review, kill switch, and previous-build recovery paths.",
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_MOBILE_CONTEXT_RE = re.compile(
    r"\b(?:mobile|mobile app|native app|app release|app launch|ios|iphone|ipad|android|"
    r"app store|appstore|app store connect|google play|play store|play console|"
    r"testflight|firebase app distribution|apk|aab|ipa|sdk|tablet|handset)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:mobile|native[-_ ]?app|app[-_ ]?release|ios|android|app[-_ ]?store|"
    r"play[-_ ]?store|review|version|build|rollout|phased|staged|device|os|sdk|"
    r"crash|stability|privacy|manifest|data[-_ ]?safety|release[-_ ]?notes|"
    r"changelog|hotfix|rollback|requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|"
    r"metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"provide|include|submit|approve|review|gate|block|cannot ship|ship only|"
    r"before launch|before release|acceptance|support|target|minimum|min|set|"
    r"define|document|publish|roll out|rollout|monitor|pause|rollback|hotfix)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,120}"
    r"\b(?:mobile|native app|mobile app|ios|android|app store|play store|mobile release|app release)\b"
    r".{0,120}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:mobile|native app|mobile app|ios|android|app store|play store|mobile release|app release)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|no work|non-goal|non goal)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+(?:\.\d+)?\s?%|"
    r"\d+(?:\.\d+)?\s*(?:crash[- ]?free|anr|crash rate)|"
    r"(?:ios|ipados|android)\s*\d+(?:\.\d+)?\+?|"
    r"(?:min(?:imum)?|target)\s*sdk\s*\d+|"
    r"(?:version(?:code|name)?|build number)\s*[:#]?\s*[A-Za-z0-9._-]+|"
    r"(?:build|version)\s+\d+(?:\.\d+){0,3}|"
    r"(?:app store connect|google play|play console|testflight|data safety|privacy manifest|"
    r"release notes|what'?s new|changelog|hotfix|rollback|expedited review))\b",
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
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "mobile",
    "mobile_app",
    "mobile_release",
    "app_release",
    "release",
    "ios",
    "android",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[MobileAppReleaseCategory, re.Pattern[str]] = {
    "app_store_review": re.compile(
        r"\b(?:app store review|store review|app review|review submission|store approval|"
        r"app store connect|google play review|play store review|play console review|"
        r"testflight review|reviewer access|review notes|store metadata|store listing)\b",
        re.I,
    ),
    "versioning_build_numbers": re.compile(
        r"\b(?:version(?:ing)?|build number|build numbers|version code|versioncode|"
        r"version name|versionname|cfbundleversion|cfbundleshortversionstring|"
        r"semantic version|semver|release version|increment build|bump build)\b",
        re.I,
    ),
    "phased_release": re.compile(
        r"\b(?:phased release|phased rollout|staged rollout|gradual rollout|canary rollout|"
        r"rollout percentage|rollout percent|release to \d+\s?%|\d+\s?% rollout|"
        r"pause rollout|accelerate rollout|widen rollout)\b",
        re.I,
    ),
    "device_os_support": re.compile(
        r"\b(?:device support|os support|supported devices?|supported os|minimum os|min os|"
        r"ios\s*\d+(?:\.\d+)?\+?|ipados\s*\d+(?:\.\d+)?\+?|android\s*\d+(?:\.\d+)?\+?|"
        r"min(?:imum)? sdk|target sdk|sdk\s*\d+|phones?|tablets?|ipad|iphone|"
        r"deprecated os|unsupported devices?)\b",
        re.I,
    ),
    "crash_free_threshold": re.compile(
        r"\b(?:crash[- ]?free|crash free|crash rate|crash threshold|stability threshold|"
        r"stability gate|anr rate|anrs?|crashlytics|sentry|datadog rum|"
        r"no new crashes|fatal crashes)\b",
        re.I,
    ),
    "privacy_manifest": re.compile(
        r"\b(?:privacy manifest|privacy nutrition label|nutrition label|data safety|"
        r"app privacy|privacy label|att prompt|app tracking transparency|tracking permission|"
        r"required reason api|sdk privacy|privacy disclosure|data collection disclosure)\b",
        re.I,
    ),
    "release_notes": re.compile(
        r"\b(?:release notes|store notes|what'?s new|whats new|changelog|change log|"
        r"update notes|localized notes|release copy|store copy)\b",
        re.I,
    ),
    "hotfix_rollback": re.compile(
        r"\b(?:hotfix|hot fix|rollback|roll back|expedited review|emergency patch|"
        r"previous build|revert release|kill switch|feature flag rollback|disable remotely)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceMobileAppReleaseRequirement:
    """One source-backed mobile app release requirement."""

    category: MobileAppReleaseCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: MobileAppReleaseConfidence = "medium"
    suggested_owner: str | None = None
    planning_note: str | None = None

    @property
    def requirement_category(self) -> MobileAppReleaseCategory:
        """Compatibility view for extractors that expose category naming."""
        return self.category

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "suggested_owner": self.suggested_owner,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceMobileAppReleaseRequirementsReport:
    """Source-level mobile app release requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceMobileAppReleaseRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceMobileAppReleaseRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceMobileAppReleaseRequirement, ...]:
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
        """Return mobile app release requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Mobile App Release Requirements Report"
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
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source mobile app release requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Confidence | Source Field | Matched Terms | Suggested Owner | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell(', '.join(requirement.matched_terms))} | "
                f"{_markdown_cell(requirement.suggested_owner or '')} | "
                f"{_markdown_cell(requirement.planning_note or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_mobile_app_release_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceMobileAppReleaseRequirementsReport:
    """Build a mobile app release requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceMobileAppReleaseRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_mobile_app_release_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceMobileAppReleaseRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted mobile app release requirements."""
    if isinstance(source, SourceMobileAppReleaseRequirementsReport):
        return dict(source.summary)
    return build_source_mobile_app_release_requirements(source).summary


def derive_source_mobile_app_release_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceMobileAppReleaseRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_mobile_app_release_requirements(source)


def generate_source_mobile_app_release_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceMobileAppReleaseRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_mobile_app_release_requirements(source)


def extract_source_mobile_app_release_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceMobileAppReleaseRequirement, ...]:
    """Return mobile app release requirement records from brief-shaped input."""
    return build_source_mobile_app_release_requirements(source).requirements


def source_mobile_app_release_requirements_to_dict(
    report: SourceMobileAppReleaseRequirementsReport,
) -> dict[str, Any]:
    """Serialize a mobile app release requirements report to a plain dictionary."""
    return report.to_dict()


source_mobile_app_release_requirements_to_dict.__test__ = False


def source_mobile_app_release_requirements_to_dicts(
    requirements: (
        tuple[SourceMobileAppReleaseRequirement, ...]
        | list[SourceMobileAppReleaseRequirement]
        | SourceMobileAppReleaseRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize mobile app release requirement records to dictionaries."""
    if isinstance(requirements, SourceMobileAppReleaseRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_mobile_app_release_requirements_to_dicts.__test__ = False


def source_mobile_app_release_requirements_to_markdown(
    report: SourceMobileAppReleaseRequirementsReport,
) -> str:
    """Render a mobile app release requirements report as Markdown."""
    return report.to_markdown()


source_mobile_app_release_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: MobileAppReleaseCategory
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: MobileAppReleaseConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _brief_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _brief_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _brief_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    if _brief_out_of_scope(payload):
        return []
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        if not _is_requirement(segment):
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [
            category
            for category in _CATEGORY_ORDER
            if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        for category in _dedupe(categories):
            terms = _matched_terms(_CATEGORY_PATTERNS[category], segment.text)
            candidates.append(
                _Candidate(
                    category=category,
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    matched_terms=tuple(terms),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceMobileAppReleaseRequirement]:
    grouped: dict[MobileAppReleaseCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceMobileAppReleaseRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(
                    _CONFIDENCE_ORDER[item.confidence]
                    for item in items
                    if item.source_field == field
                ),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourceMobileAppReleaseRequirement(
                category=category,
                source_field=source_field,
                evidence=tuple(
                    sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
                )[:5],
                matched_terms=tuple(
                    _dedupe(
                        term
                        for item in sorted(
                            items, key=lambda candidate: candidate.source_field.casefold()
                        )
                        for term in item.matched_terms
                    )
                )[:8],
                confidence=confidence,
                suggested_owner=_SUGGESTED_OWNERS[category],
                planning_note=_PLANNING_NOTES[category],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    global_context = _brief_mobile_context(payload)
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], global_context)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(segments, str(key), payload[key], global_context)
    return segments


def _append_value(
    segments: list[_Segment],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _MOBILE_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _CATEGORY_PATTERNS.values())
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if value is not None and not isinstance(value, (bytes, bytearray)):
        raw_text = str(value).strip()
        if not raw_text:
            return
        for segment_text, segment_context in _segments(raw_text, field_context):
            segments.append(_Segment(source_field, segment_text, segment_context))


def _segments(value: str, inherited_context: bool) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    section_context = inherited_context
    for raw_line in value.splitlines() or [value]:
        line = raw_line.strip()
        if not line:
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            title = _clean_text(heading.group("title"))
            section_context = inherited_context or bool(
                _MOBILE_CONTEXT_RE.search(title)
                or _STRUCTURED_FIELD_RE.search(title)
                or any(pattern.search(title) for pattern in _CATEGORY_PATTERNS.values())
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            clauses = (
                [part]
                if _NEGATED_SCOPE_RE.search(part) and _MOBILE_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    has_context = bool(
        _MOBILE_CONTEXT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
    )
    if not has_context:
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return True
    return bool(_VALUE_RE.search(searchable))


def _matched_terms(pattern: re.Pattern[str], text: str) -> list[str]:
    return _dedupe(_clean_text(match.group(0)).casefold() for match in pattern.finditer(text))


def _confidence(segment: _Segment) -> MobileAppReleaseConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_value = bool(_VALUE_RE.search(searchable))
    has_specific_context = bool(
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "mobile",
                "release",
                "ios",
                "android",
            )
        )
    )
    if _REQUIREMENT_RE.search(segment.text) and has_specific_context and has_value:
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and has_specific_context:
        return "high"
    if _REQUIREMENT_RE.search(segment.text) or has_specific_context or has_value:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceMobileAppReleaseRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "categories": [requirement.category for requirement in requirements],
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "status": (
            "ready_for_mobile_release_planning"
            if requirements
            else "no_mobile_app_release_language"
        ),
    }


def _brief_out_of_scope(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in (
            "title",
            "summary",
            "scope",
            "non_goals",
            "constraints",
            "source_payload",
        )
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_NEGATED_SCOPE_RE.search(scoped_text))


def _brief_mobile_context(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "domain", "summary", "workflow_context", "product_surface")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(
        _MOBILE_CONTEXT_RE.search(scoped_text) and not _NEGATED_SCOPE_RE.search(scoped_text)
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
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
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "mobile",
        "mobile_app",
        "mobile_release",
        "app_release",
        "release",
        "ios",
        "android",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _strings(value: Any) -> list[str]:
    if value is None or isinstance(value, (bytes, bytearray)):
        return []
    if isinstance(value, str):
        text = _clean_text(value)
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
    text = _clean_text(value)
    return [text] if text else []


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(value)
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


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
    "MobileAppReleaseCategory",
    "MobileAppReleaseConfidence",
    "SourceMobileAppReleaseRequirement",
    "SourceMobileAppReleaseRequirementsReport",
    "build_source_mobile_app_release_requirements",
    "derive_source_mobile_app_release_requirements",
    "extract_source_mobile_app_release_requirements",
    "generate_source_mobile_app_release_requirements",
    "summarize_source_mobile_app_release_requirements",
    "source_mobile_app_release_requirements_to_dict",
    "source_mobile_app_release_requirements_to_dicts",
    "source_mobile_app_release_requirements_to_markdown",
]
