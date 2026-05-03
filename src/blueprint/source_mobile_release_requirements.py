"""Extract source-level mobile release requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


MobileReleasePlatform = Literal["ios", "android", "cross_platform", "unspecified"]
MobileReleaseCategory = Literal[
    "ios_app_store",
    "android_play_store",
    "signing",
    "provisioning_profile",
    "store_review",
    "phased_rollout",
    "minimum_os_version",
    "push_notification_entitlement",
    "test_distribution",
    "deep_links",
    "app_versioning",
    "rollback_update",
]
MobileReleaseConfidence = Literal["high", "medium", "low"]
MobileReleaseReadiness = Literal["ready", "needs_details", "not_ready"]
MobileReleaseMissingDetail = Literal[
    "missing_platform",
    "missing_signing_materials",
    "missing_review_owner",
    "missing_release_track",
    "missing_rollout_timing",
    "missing_minimum_os_version",
    "missing_test_distribution_track",
    "missing_versioning_scheme",
    "missing_rollback_path",
]
_T = TypeVar("_T")

_PLATFORM_ORDER: tuple[MobileReleasePlatform, ...] = (
    "ios",
    "android",
    "cross_platform",
    "unspecified",
)
_CATEGORY_ORDER: tuple[MobileReleaseCategory, ...] = (
    "ios_app_store",
    "android_play_store",
    "signing",
    "provisioning_profile",
    "store_review",
    "phased_rollout",
    "minimum_os_version",
    "push_notification_entitlement",
    "test_distribution",
    "deep_links",
    "app_versioning",
    "rollback_update",
)
_CONFIDENCE_ORDER: dict[MobileReleaseConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_READINESS_ORDER: dict[MobileReleaseReadiness, int] = {
    "ready": 0,
    "needs_details": 1,
    "not_ready": 2,
}
_MISSING_DETAIL_FLAGS: tuple[MobileReleaseMissingDetail, ...] = (
    "missing_platform",
    "missing_signing_materials",
    "missing_review_owner",
    "missing_release_track",
    "missing_rollout_timing",
    "missing_minimum_os_version",
    "missing_test_distribution_track",
    "missing_versioning_scheme",
    "missing_rollback_path",
)
_OWNER_BY_CATEGORY: dict[MobileReleaseCategory, str] = {
    "ios_app_store": "mobile_release",
    "android_play_store": "mobile_release",
    "signing": "mobile_engineering",
    "provisioning_profile": "mobile_engineering",
    "store_review": "release_engineering",
    "phased_rollout": "release_engineering",
    "minimum_os_version": "mobile_engineering",
    "push_notification_entitlement": "mobile_engineering",
    "test_distribution": "qa",
    "deep_links": "mobile_engineering",
    "app_versioning": "release_engineering",
    "rollback_update": "incident_response",
}
_PLANNING_NOTES: dict[MobileReleaseCategory, str] = {
    "ios_app_store": "Confirm App Store Connect metadata, review gates, release controls, and iOS store ownership.",
    "android_play_store": "Confirm Play Console track, Play App Signing, review gates, and Android release ownership.",
    "signing": "Validate signing certificates, keystores, upload keys, bundle identifiers, and CI signing access.",
    "provisioning_profile": "Confirm provisioning profiles, entitlements, bundle IDs, device scope, and expiration dates.",
    "store_review": "Assign review owner, reviewer access, submission notes, metadata evidence, and response timing.",
    "phased_rollout": "Plan rollout percentages, track timing, monitoring gates, pause criteria, and widening decisions.",
    "minimum_os_version": "Document minimum iOS, Android, SDK, deployment target, and device compatibility constraints.",
    "push_notification_entitlement": "Validate APNs, FCM, notification entitlement, device token, and permission requirements.",
    "test_distribution": "Confirm TestFlight, internal testing, closed testing, beta tracks, testers, and feedback gates.",
    "deep_links": "Validate universal links, Android App Links, URL schemes, domains, and store handoff behavior.",
    "app_versioning": "Define version name, build number, version code, SemVer, branch, and upgrade rules.",
    "rollback_update": "Define rollback, pause rollout, hotfix, expedited review, kill switch, and forced-update paths.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_MOBILE_CONTEXT_RE = re.compile(
    r"\b(?:mobile|native app|mobile app|app release|ios|iphone|ipad|android|"
    r"app store|app store connect|testflight|google play|play store|play console|"
    r"apk|aab|ipa|store review|app review|phased rollout|staged rollout|"
    r"provisioning profile|app signing|keystore|push notification|apns|fcm|"
    r"deep link|universal link|app links?|version code|build number|minimum os)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:mobile|native|ios|android|app[_ -]?store|play[_ -]?store|release|review|"
    r"signing|certificate|keystore|provisioning|profile|entitlement|push|notification|"
    r"testflight|internal[_ -]?testing|closed[_ -]?testing|beta|rollout|phased|"
    r"version|build|minimum[_ -]?os|min[_ -]?sdk|deep[_ -]?link|rollback|hotfix|"
    r"requirements?|acceptance|criteria|definition[_ -]?of[_ -]?done|metadata|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"define|document|include|provide|submit|approve|review|release|rollout|roll out|"
    r"phase|stage|sign|upload|validate|support|target|minimum|min|before launch|"
    r"before release|acceptance|done when|cannot ship|constraint|policy|gate|ready)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:mobile|native app|ios|android|app store|play store|mobile release|app release)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|planned|changes?|work)\b|"
    r"\b(?:mobile|native app|ios|android|app store|play store|mobile release|app release)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|non[- ]?goal)\b",
    re.I,
)
_IOS_RE = re.compile(r"\b(?:ios|iphone|ipad|ipados|app store|app store connect|testflight|ipa)\b", re.I)
_ANDROID_RE = re.compile(r"\b(?:android|google play|play store|play console|apk|aab)\b", re.I)
_TRACK_RE = re.compile(
    r"\b(?:testflight|internal testing|internal track|closed testing|open testing|beta track|"
    r"production track|production|beta|staged rollout|phased rollout|phased release)\b",
    re.I,
)
_TIMING_RE = re.compile(
    r"\b(?:\d+\s*%\s*(?:daily|per day|then|for)?|over\s+\d+\s*(?:days?|weeks?)|"
    r"within\s+\d+\s*(?:hours?|days?|weeks?)|after\s+\d+\s*(?:hours?|days?|weeks?)|"
    r"24\s*hours?|48\s*hours?|day\s+\d+|week\s+\d+)\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:review owner|submission owner|store owner|release owner|dri|owner|owned by|"
    r"responsible team|assigned to)\b[:\s-]*(?P<tail>[^.;\n]+)?",
    re.I,
)
_SIGNING_MATERIAL_RE = re.compile(
    r"\b(?:signing certificate|certificate|keystore|upload key|play app signing|"
    r"provisioning profile|bundle id|bundle identifier|entitlements?|apns|fcm)\b",
    re.I,
)
_OS_VERSION_RE = re.compile(
    r"\b(?:(?:ios|ipados|android)\s*\d+(?:\.\d+)?\+?|(?:minimum|min|target)\s+"
    r"(?:ios|android|os|sdk)\s*(?:version)?\s*\d+(?:\.\d+)?|deployment target\s*\d+(?:\.\d+)?)\b",
    re.I,
)
_VERSIONING_RE = re.compile(
    r"\b(?:version(?: name| code|ing)?|build number|cfbundleversion|"
    r"cfbundleshortversionstring|semver|semantic version|release version|upgrade path)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback path|rollback|roll back|pause rollout|halt rollout|stop rollout|hotfix|"
    r"expedited review|expedite review|resubmit|kill switch|forced update|force update|"
    r"minimum supported version|disable via feature flag)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[MobileReleaseCategory, re.Pattern[str]] = {
    "ios_app_store": re.compile(r"\b(?:ios|iphone|ipad|app store|app store connect|testflight|ipa)\b", re.I),
    "android_play_store": re.compile(r"\b(?:android|google play|play store|play console|apk|aab)\b", re.I),
    "signing": re.compile(
        r"\b(?:app signing|play app signing|code signing|signing certificate|certificate|"
        r"keystore|upload key|sign(?:ed|ing)? build|bundle id|bundle identifier)\b",
        re.I,
    ),
    "provisioning_profile": re.compile(
        r"\b(?:provisioning profile|mobileprovision|profiles?|apple developer profile|"
        r"distribution profile|ad hoc profile|entitlements?)\b",
        re.I,
    ),
    "store_review": re.compile(
        r"\b(?:store review|app review|review submission|submit(?:ted)? for review|"
        r"review approval|reviewer access|review notes|app store review|play review)\b",
        re.I,
    ),
    "phased_rollout": re.compile(
        r"\b(?:phased rollout|phased release|staged rollout|gradual rollout|canary rollout|"
        r"rollout percentage|rollout percent|\d+\s*%\s*rollout|release to \d+\s*%|"
        r"pause rollout|widen rollout)\b",
        re.I,
    ),
    "minimum_os_version": _OS_VERSION_RE,
    "push_notification_entitlement": re.compile(
        r"\b(?:push notifications?|remote notifications?|notification entitlement|apns|"
        r"apple push notification|fcm|firebase cloud messaging|device tokens?|notification permission)\b",
        re.I,
    ),
    "test_distribution": re.compile(
        r"\b(?:testflight|internal testing|internal track|closed testing|open testing|"
        r"beta track|beta testers?|firebase app distribution|app center|test distribution)\b",
        re.I,
    ),
    "deep_links": re.compile(
        r"\b(?:deep[-_ /]?links?|universal links?|android app links?|app links?|"
        r"custom url scheme|url scheme|deferred deep link|assetlinks\.json|apple-app-site-association)\b",
        re.I,
    ),
    "app_versioning": _VERSIONING_RE,
    "rollback_update": _ROLLBACK_RE,
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
    "mobile_release",
    "app_release",
    "app_store",
    "play_store",
    "ios",
    "android",
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
class SourceMobileReleaseRequirement:
    """One source-backed mobile release requirement."""

    source_brief_id: str | None
    platform: MobileReleasePlatform
    category: MobileReleaseCategory
    requirement_text: str
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    missing_details: tuple[MobileReleaseMissingDetail, ...] = field(default_factory=tuple)
    confidence: MobileReleaseConfidence = "medium"
    readiness: MobileReleaseReadiness = "needs_details"
    owner_suggestion: str = ""
    planning_note: str = ""

    @property
    def requirement_category(self) -> MobileReleaseCategory:
        """Compatibility view for category-oriented reports."""
        return self.category

    @property
    def platforms(self) -> tuple[MobileReleasePlatform, ...]:
        """Compatibility view for reports that expose a platform tuple."""
        return () if self.platform == "unspecified" else (self.platform,)

    @property
    def missing_detail_flags(self) -> tuple[MobileReleaseMissingDetail, ...]:
        """Compatibility alias for missing detail flags."""
        return self.missing_details

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural planning notes."""
        return (self.planning_note,)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "platform": self.platform,
            "category": self.category,
            "requirement_text": self.requirement_text,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "missing_details": list(self.missing_details),
            "confidence": self.confidence,
            "readiness": self.readiness,
            "owner_suggestion": self.owner_suggestion,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceMobileReleaseRequirementsReport:
    """Source-level mobile release requirements report."""

    source_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceMobileReleaseRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceMobileReleaseRequirement, ...]:
        """Compatibility view matching reports that name extracted rows records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceMobileReleaseRequirement, ...]:
        """Compatibility view matching reports that name extracted rows findings."""
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
        """Return mobile release requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Mobile Release Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        platform_counts = self.summary.get("platform_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Platform counts: "
            + ", ".join(
                f"{platform} {platform_counts.get(platform, 0)}" for platform in _PLATFORM_ORDER
            ),
            "- Category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{confidence} {confidence_counts.get(confidence, 0)}"
                for confidence in _CONFIDENCE_ORDER
            ),
            "- Readiness counts: "
            + ", ".join(
                f"{readiness} {readiness_counts.get(readiness, 0)}"
                for readiness in _READINESS_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No mobile release requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Platform | Category | Requirement | Source Field | Confidence | Readiness | Missing Details | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.platform} | "
                f"{requirement.category} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{requirement.confidence} | "
                f"{requirement.readiness} | "
                f"{_markdown_cell(', '.join(requirement.missing_details))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_mobile_release_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMobileReleaseRequirementsReport:
    """Extract source-level mobile release requirements from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    titles = _dedupe(_optional_text(payload.get("title")) for _, payload in brief_payloads)
    return SourceMobileReleaseRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        title=titles[0] if len(titles) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_mobile_release_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMobileReleaseRequirementsReport:
    """Compatibility alias for building a mobile release requirements report."""
    return build_source_mobile_release_requirements(source)


def generate_source_mobile_release_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMobileReleaseRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_mobile_release_requirements(source)


def derive_source_mobile_release_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMobileReleaseRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_mobile_release_requirements(source)


def summarize_source_mobile_release_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceMobileReleaseRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted mobile release requirements."""
    if isinstance(source_or_result, SourceMobileReleaseRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_mobile_release_requirements(source_or_result).summary


def source_mobile_release_requirements_to_dict(
    report: SourceMobileReleaseRequirementsReport,
) -> dict[str, Any]:
    """Serialize a mobile release requirements report to a plain dictionary."""
    return report.to_dict()


source_mobile_release_requirements_to_dict.__test__ = False


def source_mobile_release_requirements_to_dicts(
    requirements: (
        tuple[SourceMobileReleaseRequirement, ...]
        | list[SourceMobileReleaseRequirement]
        | SourceMobileReleaseRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize mobile release requirement records to dictionaries."""
    if isinstance(requirements, SourceMobileReleaseRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_mobile_release_requirements_to_dicts.__test__ = False


def source_mobile_release_requirements_to_markdown(
    report: SourceMobileReleaseRequirementsReport,
) -> str:
    """Render a mobile release requirements report as Markdown."""
    return report.to_markdown()


source_mobile_release_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    platform: MobileReleasePlatform
    category: MobileReleaseCategory
    source_field: str
    requirement_text: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: MobileReleaseConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


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


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        if _brief_out_of_scope(payload):
            continue
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
                platform = _platform_for_category(category, searchable)
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        platform=platform,
                        category=category,
                        source_field=segment.source_field,
                        requirement_text=_requirement_text(segment.text),
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=tuple(_matched_terms(_CATEGORY_PATTERNS[category], segment.text)),
                        confidence=_confidence(segment, category),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceMobileReleaseRequirement]:
    grouped: dict[tuple[str | None, MobileReleasePlatform, MobileReleaseCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.platform, candidate.category), []).append(candidate)

    requirements: list[SourceMobileReleaseRequirement] = []
    for (source_brief_id, platform, category), items in grouped.items():
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(_CONFIDENCE_ORDER[item.confidence] for item in items if item.source_field == field),
                field.casefold(),
            ),
        )[0]
        evidence = tuple(_dedupe_evidence(item.evidence for item in items))[:5]
        requirement_text = sorted(
            _dedupe(item.requirement_text for item in items),
            key=lambda value: (len(value), value.casefold()),
        )[0]
        matched_terms = tuple(
            _dedupe(
                term
                for item in sorted(items, key=lambda candidate: candidate.source_field.casefold())
                for term in item.matched_terms
            )
        )[:8]
        missing_details = _missing_details(category, platform, " ".join(item.requirement_text for item in items))
        requirements.append(
            SourceMobileReleaseRequirement(
                source_brief_id=source_brief_id,
                platform=platform,
                category=category,
                requirement_text=requirement_text,
                source_field=source_field,
                evidence=evidence,
                matched_terms=matched_terms,
                missing_details=missing_details,
                confidence=confidence,
                readiness=_readiness(confidence, missing_details),
                owner_suggestion=_OWNER_BY_CATEGORY[category],
                planning_note=_PLANNING_NOTES[category],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CATEGORY_ORDER.index(requirement.category),
            _PLATFORM_ORDER.index(requirement.platform),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field or "",
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
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
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
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _MOBILE_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _CATEGORY_PATTERNS.values())
            )
            _append_value(segments, f"{source_field}.{key}", value[key], child_context)
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
                or any(pattern.search(title) for pattern in _CATEGORY_PATTERNS.values())
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_SCOPE_RE.search(cleaned):
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            clauses = (
                [part]
                if (
                    _ROLLBACK_RE.search(part)
                    or _VERSIONING_RE.search(part)
                    or _OS_VERSION_RE.search(part)
                    or _SIGNING_MATERIAL_RE.search(part)
                )
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text and not _NEGATED_SCOPE_RE.search(text):
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
    return bool(
        _REQUIREMENT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
    )


def _confidence(segment: _Segment, category: MobileReleaseCategory) -> MobileReleaseConfidence:
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
                "source_payload",
                "metadata",
            )
        )
    )
    has_detail = bool(
        _SIGNING_MATERIAL_RE.search(searchable)
        or _OS_VERSION_RE.search(searchable)
        or _VERSIONING_RE.search(searchable)
        or _TRACK_RE.search(searchable)
        or _TIMING_RE.search(searchable)
        or _ROLLBACK_RE.search(searchable)
        or _OWNER_RE.search(searchable)
    )
    if _CATEGORY_PATTERNS[category].search(searchable) and has_requirement and (has_structured_context or has_detail):
        return "high"
    if has_requirement or has_structured_context or has_detail:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceMobileReleaseRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "platforms": [requirement.platform for requirement in requirements],
        "categories": [requirement.category for requirement in requirements],
        "platform_counts": {
            platform: sum(1 for requirement in requirements if requirement.platform == platform)
            for platform in _PLATFORM_ORDER
        },
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "readiness_counts": {
            readiness: sum(1 for requirement in requirements if requirement.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "missing_detail_counts": {
            detail: sum(1 for requirement in requirements if detail in requirement.missing_details)
            for detail in _MISSING_DETAIL_FLAGS
        },
        "status": _status(requirements),
    }


def _status(requirements: tuple[SourceMobileReleaseRequirement, ...]) -> str:
    if not requirements:
        return "no_mobile_release_language"
    if any(requirement.readiness == "not_ready" for requirement in requirements):
        return "needs_mobile_release_details"
    if any(requirement.readiness == "needs_details" for requirement in requirements):
        return "needs_mobile_release_details"
    return "ready_for_mobile_release_planning"


def _missing_details(
    category: MobileReleaseCategory,
    platform: MobileReleasePlatform,
    text: str,
) -> tuple[MobileReleaseMissingDetail, ...]:
    flags: list[MobileReleaseMissingDetail] = []
    if category not in {"ios_app_store", "android_play_store"} and platform == "unspecified":
        flags.append("missing_platform")
    if category in {"signing", "provisioning_profile", "push_notification_entitlement"} and not _SIGNING_MATERIAL_RE.search(text):
        flags.append("missing_signing_materials")
    if category == "store_review" and not _OWNER_RE.search(text):
        flags.append("missing_review_owner")
    if category in {"android_play_store", "phased_rollout", "test_distribution"} and not _TRACK_RE.search(text):
        flags.append("missing_release_track")
    if category == "phased_rollout" and not _TIMING_RE.search(text):
        flags.append("missing_rollout_timing")
    if category == "minimum_os_version" and not _OS_VERSION_RE.search(text):
        flags.append("missing_minimum_os_version")
    if category == "test_distribution" and not _TRACK_RE.search(text):
        flags.append("missing_test_distribution_track")
    if category == "app_versioning" and not re.search(r"\b(?:\d+(?:\.\d+){1,3}|version code|build number|semver|semantic version)\b", text, re.I):
        flags.append("missing_versioning_scheme")
    if category == "rollback_update" and not _ROLLBACK_RE.search(text):
        flags.append("missing_rollback_path")
    return tuple(_dedupe(flags))


def _readiness(
    confidence: MobileReleaseConfidence,
    missing_details: tuple[MobileReleaseMissingDetail, ...],
) -> MobileReleaseReadiness:
    if confidence == "low":
        return "not_ready"
    if missing_details:
        return "needs_details"
    return "ready"


def _platform_for_category(
    category: MobileReleaseCategory,
    text: str,
) -> MobileReleasePlatform:
    if category == "ios_app_store":
        return "ios"
    if category == "android_play_store":
        return "android"
    has_ios = bool(_IOS_RE.search(text))
    has_android = bool(_ANDROID_RE.search(text))
    if has_ios and has_android:
        return "cross_platform"
    if has_ios:
        return "ios"
    if has_android:
        return "android"
    if _MOBILE_CONTEXT_RE.search(text):
        return "cross_platform"
    return "unspecified"


def _brief_out_of_scope(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "summary", "scope", "non_goals", "constraints", "source_payload")
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
        "mobile_release",
        "app_release",
        "app_store",
        "play_store",
        "ios",
        "android",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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


def _matched_terms(pattern: re.Pattern[str], text: str) -> list[str]:
    return _dedupe(_clean_text(match.group(0)).casefold() for match in pattern.finditer(text))


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


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


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
    "MobileReleaseCategory",
    "MobileReleaseConfidence",
    "MobileReleaseMissingDetail",
    "MobileReleasePlatform",
    "MobileReleaseReadiness",
    "SourceMobileReleaseRequirement",
    "SourceMobileReleaseRequirementsReport",
    "build_source_mobile_release_requirements",
    "derive_source_mobile_release_requirements",
    "extract_source_mobile_release_requirements",
    "generate_source_mobile_release_requirements",
    "source_mobile_release_requirements_to_dict",
    "source_mobile_release_requirements_to_dicts",
    "source_mobile_release_requirements_to_markdown",
    "summarize_source_mobile_release_requirements",
]
