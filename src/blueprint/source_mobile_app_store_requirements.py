"""Extract source-level mobile app store release requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


MobileAppStoreRequirementType = Literal[
    "ios_app_store",
    "android_play_store",
    "review_submission",
    "phased_release",
    "minimum_os_version",
    "app_signing",
    "screenshots_metadata",
    "privacy_nutrition_labels",
    "rollback_constraint",
]
MobileAppStoreConfidence = Literal["high", "medium", "low"]
MobileAppStoreMissingDetail = Literal[
    "missing_platform",
    "missing_review_owner",
    "missing_release_track",
    "missing_rollout_timing",
    "missing_rollback_path",
    "missing_store_compliance_evidence",
]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[MobileAppStoreRequirementType, ...] = (
    "ios_app_store",
    "android_play_store",
    "review_submission",
    "phased_release",
    "minimum_os_version",
    "app_signing",
    "screenshots_metadata",
    "privacy_nutrition_labels",
    "rollback_constraint",
)
_CONFIDENCE_ORDER: dict[MobileAppStoreConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_PLAN_IMPACTS: dict[MobileAppStoreRequirementType, tuple[str, ...]] = {
    "ios_app_store": ("Add App Store Connect packaging, review, metadata, and release coordination tasks.",),
    "android_play_store": ("Add Play Console packaging, review, track, metadata, and release coordination tasks.",),
    "review_submission": ("Assign store review ownership and schedule submission, review response, and approval checks.",),
    "phased_release": ("Plan staged rollout controls, monitoring gates, pause criteria, and rollout timing.",),
    "minimum_os_version": ("Confirm minimum supported OS versions, device compatibility, and store listing constraints.",),
    "app_signing": ("Validate signing certificates, provisioning profiles, bundle identifiers, and Play App Signing setup.",),
    "screenshots_metadata": ("Prepare screenshots, release notes, localized metadata, store copy, and listing assets.",),
    "privacy_nutrition_labels": ("Collect privacy nutrition label and data safety evidence before store submission.",),
    "rollback_constraint": ("Define rollback, pause, hotfix, version re-submission, and customer mitigation paths.",),
}
_TYPE_PATTERNS: dict[MobileAppStoreRequirementType, re.Pattern[str]] = {
    "ios_app_store": re.compile(r"\b(?:ios|iphone|ipad|app store|app store connect|testflight)\b", re.I),
    "android_play_store": re.compile(r"\b(?:android|google play|play store|play console|aab|apk)\b", re.I),
    "review_submission": re.compile(
        r"\b(?:review submission|submit(?:ted)? for review|store review|app review|review approval|"
        r"app store review|play review|submission owner|review owner)\b",
        re.I,
    ),
    "phased_release": re.compile(
        r"\b(?:phased release|staged rollout|gradual rollout|rollout percentage|percent rollout|"
        r"\d+\s*%|production track|beta track|internal track|closed testing|open testing|release track)\b",
        re.I,
    ),
    "minimum_os_version": re.compile(
        r"\b(?:minimum os|min(?:imum)? ios|min(?:imum)? android|ios\s*\d+(?:\.\d+)?\+?|"
        r"android\s*\d+(?:\.\d+)?\+?|sdk\s*\d+|min sdk|target sdk|deployment target|supported os)\b",
        re.I,
    ),
    "app_signing": re.compile(
        r"\b(?:app signing|play app signing|signing certificate|certificate|provisioning profile|"
        r"bundle id|bundle identifier|keystore|upload key|entitlements?|notariz|code signing)\b",
        re.I,
    ),
    "screenshots_metadata": re.compile(
        r"\b(?:screenshots?|store metadata|app metadata|metadata|release notes?|store listing|"
        r"app listing|listing assets?|localized copy|app description|keywords?|promotional text)\b",
        re.I,
    ),
    "privacy_nutrition_labels": re.compile(
        r"\b(?:privacy nutrition labels?|privacy label|app privacy|data safety|privacy questionnaire|"
        r"privacy manifest|tracking disclosure|data collection disclosure|store compliance evidence)\b",
        re.I,
    ),
    "rollback_constraint": re.compile(
        r"\b(?:rollback|roll back|revert|hotfix|pause rollout|halt rollout|stop rollout|expedite review|"
        r"resubmit|version rollback|cannot rollback|no rollback|rollback path)\b",
        re.I,
    ),
}
_MOBILE_CONTEXT_RE = re.compile(
    r"\b(?:mobile|native app|ios|iphone|ipad|android|app store|app store connect|testflight|"
    r"google play|play store|play console|aab|apk|store review|app review|phased release|"
    r"staged rollout|app signing|privacy nutrition|data safety|minimum os)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:mobile|native|ios|android|app[_ -]?store|play[_ -]?store|play[_ -]?console|"
    r"store[_ -]?release|store[_ -]?review|review|submission|release[_ -]?track|rollout|"
    r"phased|minimum[_ -]?os|min[_ -]?sdk|signing|certificate|metadata|screenshots?|"
    r"privacy|nutrition|data[_ -]?safety|rollback|hotfix|compliance|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|define|"
    r"document|include|provide|submit|approve|review|release|rollout|roll out|phase|"
    r"stage|sign|upload|prepare|before launch|before release|acceptance|constraint|policy)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,120}"
    r"\b(?:mobile|native app|ios|android|app store|play store|store release)\b"
    r".{0,120}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:mobile|native app|ios|android|app store|play store|store release)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|no work|non-goal|non goal)\b",
    re.I,
)
_PLATFORM_RE = re.compile(r"\b(?:ios|iphone|ipad|app store|app store connect|android|google play|play store|play console)\b", re.I)
_IOS_RE = re.compile(r"\b(?:ios|iphone|ipad|app store|app store connect|testflight)\b", re.I)
_ANDROID_RE = re.compile(r"\b(?:android|google play|play store|play console|aab|apk)\b", re.I)
_TRACK_RE = re.compile(
    r"\b(?:testflight|production track|production|beta track|beta|internal track|internal testing|"
    r"closed testing|open testing|app store phased release|phased release|staged rollout)\b",
    re.I,
)
_TIMING_RE = re.compile(
    r"\b(?:within\s+\d+\s*(?:minutes?|hours?|days?|weeks?)|after\s+\d+\s*(?:hours?|days?|weeks?)|"
    r"before\s+(?:launch|release|submission)|on\s+\d{4}-\d{2}-\d{2}|by\s+\d{4}-\d{2}-\d{2}|"
    r"\d+\s*%\s*(?:per\s+day|daily|then|for)|over\s+\d+\s*(?:days?|weeks?)|"
    r"day\s+\d+|week\s+\d+|24\s*hours?|48\s*hours?)\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:review owner|submission owner|store owner|dri|owner|owned by|responsible team|assigned to)\b[:\s-]*(?P<tail>[^.;\n]+)?",
    re.I,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback path|rollback|roll back|pause rollout|halt rollout|stop rollout|hotfix|"
    r"expedite review|resubmit|disable via feature flag|kill switch)\b(?:[:\s-]*(?P<tail>[^.;\n]+))?",
    re.I,
)
_COMPLIANCE_RE = re.compile(
    r"\b(?:privacy nutrition labels?|privacy label|data safety|privacy questionnaire|"
    r"store compliance evidence|review evidence|screenshots?|metadata|release notes?|"
    r"signing certificate|provisioning profile|keystore|upload key|app privacy)\b",
    re.I,
)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SPACE_RE = re.compile(r"\s+")
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
    "acceptance",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "release",
    "release_plan",
    "mobile",
    "app_store",
    "play_store",
    "store_release",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
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
    "status",
}


@dataclass(frozen=True, slots=True)
class SourceMobileAppStoreRequirement:
    """One source-backed mobile app store release requirement."""

    source_brief_id: str | None
    requirement_type: MobileAppStoreRequirementType
    requirement_text: str
    platforms: tuple[str, ...] = field(default_factory=tuple)
    review_owner: str | None = None
    release_track: str | None = None
    rollout_timing: str | None = None
    rollback_path: str | None = None
    store_compliance_evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: MobileAppStoreConfidence = "medium"
    missing_detail_flags: tuple[MobileAppStoreMissingDetail, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def category(self) -> MobileAppStoreRequirementType:
        """Compatibility view for extractors that expose category naming."""
        return self.requirement_type

    @property
    def requirement_category(self) -> MobileAppStoreRequirementType:
        """Compatibility view for extractors that expose requirement_category naming."""
        return self.requirement_type

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting planning notes."""
        return self.suggested_plan_impacts

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "requirement_text": self.requirement_text,
            "platforms": list(self.platforms),
            "review_owner": self.review_owner,
            "release_track": self.release_track,
            "rollout_timing": self.rollout_timing,
            "rollback_path": self.rollback_path,
            "store_compliance_evidence": list(self.store_compliance_evidence),
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "missing_detail_flags": list(self.missing_detail_flags),
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourceMobileAppStoreRequirementsReport:
    """Source-level mobile app store release requirements report."""

    source_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceMobileAppStoreRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceMobileAppStoreRequirement, ...]:
        """Compatibility view matching reports that name extracted items records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceMobileAppStoreRequirement, ...]:
        """Compatibility view matching reports that name extracted items findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return mobile app store requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Mobile App Store Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("requirement_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        missing_counts = self.summary.get("missing_detail_flag_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement type counts: "
            + ", ".join(f"{item} {type_counts.get(item, 0)}" for item in _TYPE_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{item} {confidence_counts.get(item, 0)}" for item in _CONFIDENCE_ORDER),
            "- Missing detail counts: "
            + ", ".join(
                f"{item} {missing_counts.get(item, 0)}"
                for item in (
                    "missing_platform",
                    "missing_review_owner",
                    "missing_release_track",
                    "missing_rollout_timing",
                    "missing_rollback_path",
                    "missing_store_compliance_evidence",
                )
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No mobile app store release requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Requirement Type | Requirement | Platforms | Owner | Track | Timing | Rollback | Compliance Evidence | Source Field | Confidence | Missing Details | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.requirement_type)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(', '.join(requirement.platforms))} | "
                f"{_markdown_cell(requirement.review_owner or '')} | "
                f"{_markdown_cell(requirement.release_track or '')} | "
                f"{_markdown_cell(requirement.rollout_timing or '')} | "
                f"{_markdown_cell(requirement.rollback_path or '')} | "
                f"{_markdown_cell(', '.join(requirement.store_compliance_evidence))} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(', '.join(requirement.missing_detail_flags))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_mobile_app_store_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceMobileAppStoreRequirementsReport:
    """Extract source-level mobile app store release requirements from brief-shaped input."""
    source_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(source_id, payload)))
    return SourceMobileAppStoreRequirementsReport(
        source_id=source_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_mobile_app_store_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceMobileAppStoreRequirementsReport:
    """Compatibility alias for building a mobile app store requirements report."""
    return build_source_mobile_app_store_requirements(source)


def generate_source_mobile_app_store_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceMobileAppStoreRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_mobile_app_store_requirements(source)


def derive_source_mobile_app_store_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceMobileAppStoreRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_mobile_app_store_requirements(source)


def summarize_source_mobile_app_store_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceMobileAppStoreRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted mobile app store requirements."""
    if isinstance(source_or_result, SourceMobileAppStoreRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_mobile_app_store_requirements(source_or_result).summary


def source_mobile_app_store_requirements_to_dict(
    report: SourceMobileAppStoreRequirementsReport,
) -> dict[str, Any]:
    """Serialize a mobile app store requirements report to a plain dictionary."""
    return report.to_dict()


source_mobile_app_store_requirements_to_dict.__test__ = False


def source_mobile_app_store_requirements_to_dicts(
    requirements: (
        tuple[SourceMobileAppStoreRequirement, ...]
        | list[SourceMobileAppStoreRequirement]
        | SourceMobileAppStoreRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize mobile app store requirement records to dictionaries."""
    if isinstance(requirements, SourceMobileAppStoreRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_mobile_app_store_requirements_to_dicts.__test__ = False


def source_mobile_app_store_requirements_to_markdown(
    report: SourceMobileAppStoreRequirementsReport,
) -> str:
    """Render a mobile app store requirements report as Markdown."""
    return report.to_markdown()


source_mobile_app_store_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: MobileAppStoreRequirementType
    source_field: str
    requirement_text: str
    evidence: str
    matched_terms: tuple[str, ...]
    platforms: tuple[str, ...]
    review_owner: str | None
    release_track: str | None
    rollout_timing: str | None
    rollback_path: str | None
    store_compliance_evidence: tuple[str, ...]
    confidence: MobileAppStoreConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_id(payload), payload
    return None, {}


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(source_id: str | None, payload: Mapping[str, Any]) -> list[_Candidate]:
    if _brief_out_of_scope(payload):
        return []
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        if not _is_requirement(segment):
            continue
        requirement_types = [
            requirement_type
            for requirement_type in _TYPE_ORDER
            if _TYPE_PATTERNS[requirement_type].search(searchable)
        ]
        for requirement_type in _dedupe(requirement_types):
            candidates.append(
                _Candidate(
                    source_brief_id=source_id,
                    requirement_type=requirement_type,
                    source_field=segment.source_field,
                    requirement_text=_requirement_text(segment.text),
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    matched_terms=tuple(_matched_terms(_TYPE_PATTERNS[requirement_type], segment.text)),
                    platforms=_candidate_platforms(requirement_type, searchable),
                    review_owner=_owner_detail(segment.text),
                    release_track=_track_detail(segment.text),
                    rollout_timing=_match_detail(_TIMING_RE, segment.text),
                    rollback_path=_rollback_detail(segment.text),
                    store_compliance_evidence=_compliance_evidence(segment.text),
                    confidence=_confidence(segment, requirement_type),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceMobileAppStoreRequirement]:
    grouped: dict[MobileAppStoreRequirementType, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.requirement_type, []).append(candidate)

    requirements: list[SourceMobileAppStoreRequirement] = []
    for requirement_type in _TYPE_ORDER:
        items = grouped.get(requirement_type, [])
        if not items:
            continue
        best = max(items, key=_candidate_score)
        platforms = tuple(_dedupe(platform for item in items for platform in item.platforms))
        review_owner = _first_detail(item.review_owner for item in items)
        release_track = _best_track(item.release_track for item in items)
        rollout_timing = _first_detail(item.rollout_timing for item in items)
        rollback_path = _first_detail(item.rollback_path for item in items)
        compliance = tuple(
            _dedupe(detail for item in items for detail in item.store_compliance_evidence)
        )[:8]
        requirements.append(
            SourceMobileAppStoreRequirement(
                source_brief_id=best.source_brief_id,
                requirement_type=requirement_type,
                requirement_text=best.requirement_text,
                platforms=platforms,
                review_owner=review_owner,
                release_track=release_track,
                rollout_timing=rollout_timing,
                rollback_path=rollback_path,
                store_compliance_evidence=compliance,
                source_field=best.source_field,
                evidence=tuple(
                    sorted(
                        _dedupe_evidence(
                            sorted(
                                (item.evidence for item in items),
                                key=lambda value: (
                                    len(value.partition(": ")[2] or value),
                                    value.casefold(),
                                ),
                            )
                        ),
                        key=str.casefold,
                    )
                )[:5],
                matched_terms=tuple(
                    _dedupe(
                        term
                        for item in sorted(items, key=lambda candidate: candidate.source_field.casefold())
                        for term in item.matched_terms
                    )
                )[:8],
                confidence=min(
                    (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
                ),
                missing_detail_flags=_missing_detail_flags(
                    requirement_type=requirement_type,
                    platforms=platforms,
                    review_owner=review_owner,
                    release_track=release_track,
                    rollout_timing=rollout_timing,
                    rollback_path=rollback_path,
                    store_compliance_evidence=compliance,
                ),
                suggested_plan_impacts=_PLAN_IMPACTS[requirement_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _TYPE_ORDER.index(requirement.requirement_type),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field or "",
            requirement.requirement_text.casefold(),
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
                or any(pattern.search(key_text) for pattern in _TYPE_PATTERNS.values())
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text, segment_context in _segments(text, field_context):
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
                or any(pattern.search(title) for pattern in _TYPE_PATTERNS.values())
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
            clauses = [part] if _ROLLBACK_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
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
    has_type = any(pattern.search(searchable) for pattern in _TYPE_PATTERNS.values())
    if not has_type:
        return False
    if _REQUIREMENT_RE.search(searchable):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return True
    return bool(_PLATFORM_RE.search(searchable) and (_TRACK_RE.search(searchable) or _COMPLIANCE_RE.search(searchable)))


def _confidence(segment: _Segment, requirement_type: MobileAppStoreRequirementType) -> MobileAppStoreConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_requirement = bool(_REQUIREMENT_RE.search(searchable))
    has_structured_context = bool(
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "release",
                "mobile",
                "app_store",
                "play_store",
                "metadata",
                "source_payload",
            )
        )
    )
    has_detail = any(
        (
            _platforms(searchable),
            _owner_detail(segment.text),
            _track_detail(segment.text),
            _match_detail(_TIMING_RE, segment.text),
            _rollback_detail(segment.text),
            _compliance_evidence(segment.text),
        )
    )
    if _TYPE_PATTERNS[requirement_type].search(searchable) and has_requirement and (has_structured_context or has_detail):
        return "high"
    if has_requirement or has_structured_context or has_detail:
        return "medium"
    return "low"


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, str]:
    detail_count = sum(
        bool(value)
        for value in (
            candidate.platforms,
            candidate.review_owner,
            candidate.release_track,
            candidate.rollout_timing,
            candidate.rollback_path,
            candidate.store_compliance_evidence,
        )
    )
    return (
        detail_count,
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        candidate.evidence,
    )


def _summary(requirements: tuple[SourceMobileAppStoreRequirement, ...]) -> dict[str, Any]:
    missing_flags = (
        "missing_platform",
        "missing_review_owner",
        "missing_release_track",
        "missing_rollout_timing",
        "missing_rollback_path",
        "missing_store_compliance_evidence",
    )
    return {
        "requirement_count": len(requirements),
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "requirement_type_counts": {
            requirement_type: sum(
                1 for requirement in requirements if requirement.requirement_type == requirement_type
            )
            for requirement_type in _TYPE_ORDER
        },
        "platform_counts": {
            platform: sum(1 for requirement in requirements if platform in requirement.platforms)
            for platform in ("ios", "android")
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "missing_detail_flag_counts": {
            flag: sum(1 for requirement in requirements if flag in requirement.missing_detail_flags)
            for flag in missing_flags
        },
        "status": (
            "ready_for_mobile_app_store_planning"
            if requirements
            else "no_mobile_app_store_language"
        ),
    }


def _missing_detail_flags(
    *,
    requirement_type: MobileAppStoreRequirementType,
    platforms: tuple[str, ...],
    review_owner: str | None,
    release_track: str | None,
    rollout_timing: str | None,
    rollback_path: str | None,
    store_compliance_evidence: tuple[str, ...],
) -> tuple[MobileAppStoreMissingDetail, ...]:
    flags: list[MobileAppStoreMissingDetail] = []
    if requirement_type not in {"ios_app_store", "android_play_store"} and not platforms:
        flags.append("missing_platform")
    if requirement_type in {"review_submission", "ios_app_store", "android_play_store"} and not review_owner:
        flags.append("missing_review_owner")
    if requirement_type in {"phased_release", "review_submission", "android_play_store"} and not release_track:
        flags.append("missing_release_track")
    if requirement_type in {"phased_release", "review_submission"} and not rollout_timing:
        flags.append("missing_rollout_timing")
    if requirement_type in {"rollback_constraint", "phased_release"} and not rollback_path:
        flags.append("missing_rollback_path")
    if requirement_type in {
        "review_submission",
        "screenshots_metadata",
        "privacy_nutrition_labels",
        "app_signing",
    } and not store_compliance_evidence:
        flags.append("missing_store_compliance_evidence")
    return tuple(flags)


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
    return bool(_MOBILE_CONTEXT_RE.search(scoped_text) and not _NEGATED_SCOPE_RE.search(scoped_text))


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
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
        "product_surface",
        "requirements",
        "constraints",
        "scope",
        "non_goals",
        "assumptions",
        "success_criteria",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "release",
        "release_plan",
        "mobile",
        "app_store",
        "play_store",
        "store_release",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _platforms(text: str) -> tuple[str, ...]:
    platforms: list[str] = []
    if _IOS_RE.search(text):
        platforms.append("ios")
    if _ANDROID_RE.search(text):
        platforms.append("android")
    return tuple(_dedupe(platforms))


def _candidate_platforms(
    requirement_type: MobileAppStoreRequirementType, text: str
) -> tuple[str, ...]:
    if requirement_type == "ios_app_store":
        return ("ios",)
    if requirement_type == "android_play_store":
        return ("android",)
    return _platforms(text)


def _owner_detail(text: str) -> str | None:
    if match := re.search(r"\bsubmitted by\s+(?P<tail>[^.;\n]+?\s+owner)\b", text, re.I):
        return _clean_text(match.group("tail"))[:100]
    if match := _OWNER_RE.search(text):
        tail = _clean_text(match.groupdict().get("tail") or "")
        if tail:
            return tail[:100]
        return _detail(match.group(0))
    return None


def _rollback_detail(text: str) -> str | None:
    for pattern in (
        r"\bpause rollout\b",
        r"\bhalt rollout\b",
        r"\bstop rollout\b",
        r"\bdisable via feature flag\b",
        r"\bkill switch\b",
        r"\bship hotfix\b",
        r"\bhotfix\b",
        r"\bexpedite review\b",
        r"\bresubmit\b",
        r"\brollback path\b[:\s-]*(?P<tail>[^.;\n]+)?",
        r"\brollback\b[:\s-]*(?P<rollback_tail>[^.;\n]+)?",
        r"\broll back\b[:\s-]*(?P<roll_back_tail>[^.;\n]+)?",
    ):
        if match := re.search(pattern, text, re.I):
            tail = _clean_text(
                next((value for value in match.groupdict().values() if value), "")
            )
            return (tail or _detail(match.group(0)))[:120]
    return None


def _compliance_evidence(text: str) -> tuple[str, ...]:
    return tuple(
        _dedupe(_detail(match.group(0)).casefold() for match in _COMPLIANCE_RE.finditer(text))
    )


def _track_detail(text: str) -> str | None:
    return _best_track(_detail(match.group(0)) for match in _TRACK_RE.finditer(text))


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    if match := pattern.search(text):
        return _detail(match.group(0))
    return None


def _matched_terms(pattern: re.Pattern[str], text: str) -> list[str]:
    return _dedupe(_clean_text(match.group(0)).casefold() for match in pattern.finditer(text))


def _first_detail(values: Iterable[str | None]) -> str | None:
    details = sorted(
        _dedupe(value for value in values if value),
        key=lambda value: (len(value), value.casefold()),
    )
    return details[0] if details else None


def _best_track(values: Iterable[str | None]) -> str | None:
    priority = {
        "production track": 0,
        "closed testing": 1,
        "open testing": 2,
        "internal track": 3,
        "internal testing": 4,
        "beta track": 5,
        "testflight": 6,
        "production": 7,
        "beta": 8,
        "staged rollout": 9,
        "phased release": 10,
    }
    details = sorted(
        _dedupe(value for value in values if value),
        key=lambda value: (priority.get(value.casefold(), 20), len(value), value.casefold()),
    )
    return details[0] if details else None


def _detail(value: str) -> str:
    return _clean_text(value).strip(" :.-")


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


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
        tail_key = key.rsplit(" - ", 1)[-1]
        if key in seen or tail_key in seen:
            continue
        deduped.append(value)
        seen.add(key)
        seen.add(tail_key)
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
    "MobileAppStoreRequirementType",
    "MobileAppStoreConfidence",
    "MobileAppStoreMissingDetail",
    "SourceMobileAppStoreRequirement",
    "SourceMobileAppStoreRequirementsReport",
    "build_source_mobile_app_store_requirements",
    "derive_source_mobile_app_store_requirements",
    "extract_source_mobile_app_store_requirements",
    "generate_source_mobile_app_store_requirements",
    "summarize_source_mobile_app_store_requirements",
    "source_mobile_app_store_requirements_to_dict",
    "source_mobile_app_store_requirements_to_dicts",
    "source_mobile_app_store_requirements_to_markdown",
]
