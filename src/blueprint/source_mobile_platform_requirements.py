"""Extract mobile platform requirements from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


MobilePlatform = Literal["ios", "android", "tablet", "cross_platform", "unspecified"]
MobileRequirementType = Literal[
    "platform_support",
    "app_store",
    "push_notification",
    "deep_link",
    "offline_mode",
    "biometric",
    "os_version",
    "native_permission",
]
MobileRequirementConfidence = Literal["high", "medium", "low"]

_PLATFORM_ORDER: tuple[MobilePlatform, ...] = (
    "ios",
    "android",
    "tablet",
    "cross_platform",
    "unspecified",
)
_TYPE_ORDER: tuple[MobileRequirementType, ...] = (
    "platform_support",
    "app_store",
    "push_notification",
    "deep_link",
    "offline_mode",
    "biometric",
    "os_version",
    "native_permission",
)
_CONFIDENCE_ORDER: dict[MobileRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|cannot ship|support|compatible|target|minimum)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:mobile|ios|android|tablet|app store|push|deep link|"
    r"native app|offline|biometric|native permission).*?\b(?:in scope|required|requirements?|needed|changes?)\b",
    re.I,
)
_MOBILE_CONTEXT_RE = re.compile(
    r"\b(?:mobile|native app|mobile app|ios|iphone|ipad|android|tablet|app store|"
    r"play store|push notifications?|deep links?|offline|biometric|face id|touch id|"
    r"fingerprint|camera permission|location permission|photo library|contacts permission|"
    r"microphone permission|bluetooth permission|os version|sdk)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:mobile|platform|ios|iphone|ipad|android|tablet|app[-_ ]?store|play[-_ ]?store|"
    r"push|notification|deep[-_ ]?link|offline|biometric|permission|os[-_ ]?version|"
    r"native|capabilit(?:y|ies)|acceptance|requirements?|constraints?)",
    re.I,
)
_PLATFORM_PATTERNS: dict[MobilePlatform, re.Pattern[str]] = {
    "ios": re.compile(r"\b(?:ios|iphone|ipad(?:os)?|apple mobile|iPhone|iPad)\b", re.I),
    "android": re.compile(
        r"\b(?:android|google play|play store|android phones?|android tablets?)\b", re.I
    ),
    "tablet": re.compile(r"\b(?:tablet|tablets|ipad|ipad(?:os)?|android tablets?)\b", re.I),
    "cross_platform": re.compile(
        r"\b(?:mobile apps?|native apps?|ios and android|android and ios|both platforms|cross[- ]platform)\b",
        re.I,
    ),
    "unspecified": re.compile(r"\b(?:mobile|native app|mobile app)\b", re.I),
}
_TYPE_PATTERNS: dict[MobileRequirementType, re.Pattern[str]] = {
    "platform_support": re.compile(
        r"\b(?:support (?:ios|android|iphone|ipad|tablet|mobile)|(?:ios|android|tablet) support|"
        r"support (?:ios and android|android and ios)|"
        r"mobile platform|native apps?|mobile apps?|compatible with (?:ios|android)|"
        r"target (?:ios|android|tablet)|tablet layout)\b",
        re.I,
    ),
    "app_store": re.compile(
        r"\b(?:app store|apple app store|google play|play store|store review|store submission|"
        r"app review|review guidelines|testflight|app signing|privacy manifest|store listing)\b",
        re.I,
    ),
    "push_notification": re.compile(
        r"\b(?:push notifications?|push alerts?|remote notifications?|apns|fcm|device tokens?|"
        r"notification permission)\b",
        re.I,
    ),
    "deep_link": re.compile(
        r"\b(?:deep[-_ /]?links?|universal links?|app links?|custom URL scheme|url scheme|"
        r"open links? in app|deferred deep link)\b",
        re.I,
    ),
    "offline_mode": re.compile(
        r"\b(?:offline mode|offline access|works? offline|offline[- ]first|sync when online|"
        r"background sync|local cache|local storage for offline)\b",
        re.I,
    ),
    "biometric": re.compile(
        r"\b(?:biometric|biometrics|face id|touch id|fingerprint|local authentication|"
        r"device authentication)\b",
        re.I,
    ),
    "os_version": re.compile(
        r"\b(?:(?:minimum|min|target|supported)\s+(?:ios|android|ipados|os)\s*(?:version)?\s*\d+|"
        r"(?:ios|android|ipados)\s*\d+(?:\.\d+)?\+?|android api\s*\d+|sdk\s*\d+|"
        r"os version|minimum os|supported os)(?=$|\W)",
        re.I,
    ),
    "native_permission": re.compile(
        r"\b(?:(?:camera|location|photos?|photo library|contacts|microphone|bluetooth|calendar|"
        r"motion|health|files?|storage|notification)\s+permissions?|native permissions?|"
        r"runtime permissions?|notification permission|permission prompt|permission rationale)\b",
        re.I,
    ),
}
_ACTOR_RE = re.compile(
    r"\b(?:for|by|to|when)\s+(?:the\s+)?(?P<actor>field technicians?|drivers?|patients?|clinicians?|"
    r"admins?|support agents?|customers?|users?|members?|operators?)\b",
    re.I,
)
_CAPABILITY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"\b(?:enable|support|allow|handle|open|send|receive|sync|unlock|authenticate)\s+(?P<capability>[a-z][a-z0-9 -]{2,60})",
        re.I,
    ),
    re.compile(
        r"\b(?P<capability>check[- ]in|login|sign[- ]in|checkout|document upload|photo capture|location tracking|account recovery)\b",
        re.I,
    ),
)
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "integration_points",
    "risks",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
    "files",
    "file_paths",
    "paths",
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


@dataclass(frozen=True, slots=True)
class SourceMobilePlatformRequirement:
    """One source-backed mobile platform requirement."""

    source_brief_id: str | None
    platform: MobilePlatform
    requirement_type: MobileRequirementType
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: MobileRequirementConfidence = "medium"
    actor: str | None = None
    capability: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "platform": self.platform,
            "requirement_type": self.requirement_type,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "actor": self.actor,
            "capability": self.capability,
        }


@dataclass(frozen=True, slots=True)
class SourceMobilePlatformRequirementsReport:
    """Source-level mobile platform requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceMobilePlatformRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceMobilePlatformRequirement, ...]:
        """Compatibility view matching extractors that name findings records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return mobile platform requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Mobile Platform Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        platform_counts = self.summary.get("platform_counts", {})
        type_counts = self.summary.get("requirement_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
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
            "- Requirement type counts: "
            + ", ".join(f"{kind} {type_counts.get(kind, 0)}" for kind in _TYPE_ORDER),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No mobile platform requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Platform | Requirement Type | Confidence | Actor | Capability | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.platform} | "
                f"{requirement.requirement_type} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.actor or '')} | "
                f"{_markdown_cell(requirement.capability or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_mobile_platform_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMobilePlatformRequirementsReport:
    """Extract mobile platform requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _platform_index(requirement.platform),
                _type_index(requirement.requirement_type),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.actor or "",
                requirement.capability or "",
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceMobilePlatformRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_mobile_platform_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMobilePlatformRequirementsReport:
    """Compatibility alias for building a mobile platform requirements report."""
    return build_source_mobile_platform_requirements(source)


def generate_source_mobile_platform_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMobilePlatformRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_mobile_platform_requirements(source)


def derive_source_mobile_platform_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMobilePlatformRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_mobile_platform_requirements(source)


def summarize_source_mobile_platform_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceMobilePlatformRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted mobile platform requirements."""
    if isinstance(source_or_result, SourceMobilePlatformRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_mobile_platform_requirements(source_or_result).summary


def source_mobile_platform_requirements_to_dict(
    report: SourceMobilePlatformRequirementsReport,
) -> dict[str, Any]:
    """Serialize a mobile platform requirements report to a plain dictionary."""
    return report.to_dict()


source_mobile_platform_requirements_to_dict.__test__ = False


def source_mobile_platform_requirements_to_dicts(
    requirements: (
        tuple[SourceMobilePlatformRequirement, ...]
        | list[SourceMobilePlatformRequirement]
        | SourceMobilePlatformRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize mobile platform requirement records to dictionaries."""
    if isinstance(requirements, SourceMobilePlatformRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_mobile_platform_requirements_to_dicts.__test__ = False


def source_mobile_platform_requirements_to_markdown(
    report: SourceMobilePlatformRequirementsReport,
) -> str:
    """Render a mobile platform requirements report as Markdown."""
    return report.to_markdown()


source_mobile_platform_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    platform: MobilePlatform
    requirement_type: MobileRequirementType
    evidence: str
    confidence: MobileRequirementConfidence
    actor: str | None
    capability: str | None


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
    if isinstance(
        source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)
    ) or hasattr(source, "model_dump"):
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
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                value = model.model_validate(source).model_dump(mode="python")
                payload = dict(value)
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    return None, _object_payload(source)


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
        for source_field, segment in _candidate_segments(payload):
            if _NEGATED_SCOPE_RE.search(segment):
                continue
            requirement_types = _requirement_types(segment, source_field)
            if not requirement_types:
                continue
            platforms = _platforms(segment, source_field)
            evidence = _evidence_snippet(source_field, segment)
            for requirement_type in requirement_types:
                for platform in platforms:
                    candidates.append(
                        _Candidate(
                            source_brief_id=source_brief_id,
                            platform=platform,
                            requirement_type=requirement_type,
                            evidence=evidence,
                            confidence=_confidence(requirement_type, segment, source_field),
                            actor=_actor(segment),
                            capability=_capability(requirement_type, segment),
                        )
                    )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceMobilePlatformRequirement]:
    grouped: dict[tuple[str | None, MobilePlatform, MobileRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.source_brief_id, candidate.platform, candidate.requirement_type),
            [],
        ).append(candidate)

    requirements: list[SourceMobilePlatformRequirement] = []
    for (source_brief_id, platform, requirement_type), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        requirements.append(
            SourceMobilePlatformRequirement(
                source_brief_id=source_brief_id,
                platform=platform,
                requirement_type=requirement_type,
                evidence=tuple(
                    sorted(
                        _dedupe(item.evidence for item in items),
                        key=lambda item: item.casefold(),
                    )
                )[:5],
                confidence=confidence,
                actor=next((item.actor for item in items if item.actor), None),
                capability=next((item.capability for item in items if item.capability), None),
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            if _any_signal(key_text) and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    values.append((child_field, _clean_text(f"{key_text}: {text}")))
                continue
            _append_value(values, child_field, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        values.extend((source_field, segment) for segment in _segments(text))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in parts:
            segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _requirement_types(text: str, source_field: str) -> tuple[MobileRequirementType, ...]:
    searchable = _searchable_text(source_field, text)
    requirement_types = [kind for kind in _TYPE_ORDER if _TYPE_PATTERNS[kind].search(searchable)]
    if source_field == "title" and requirement_types and not _REQUIRED_RE.search(text):
        return ()
    if requirement_types and not (_mobile_context(text, source_field) or _REQUIRED_RE.search(text)):
        return ()
    return tuple(_dedupe(requirement_types))


def _platforms(text: str, source_field: str) -> tuple[MobilePlatform, ...]:
    searchable = _searchable_text(source_field, text)
    platforms = [
        platform
        for platform in _PLATFORM_ORDER
        if platform != "unspecified" and _PLATFORM_PATTERNS[platform].search(searchable)
    ]
    if re.search(r"\b(?:app store|apple app store|testflight)\b", searchable, re.I):
        platforms.append("ios")
    if re.search(r"\b(?:google play|play store)\b", searchable, re.I):
        platforms.append("android")
    if "ios" in platforms and "android" in platforms and "cross_platform" not in platforms:
        platforms.append("cross_platform")
    if "ios" in platforms and "tablet" in platforms and "ipad" not in searchable.casefold():
        platforms.remove("tablet")
    if not platforms and (
        _MOBILE_CONTEXT_RE.search(searchable)
        or _STRUCTURED_FIELD_RE.search(_field_words(source_field))
    ):
        platforms.append("unspecified")
    return tuple(_dedupe(platforms or ["unspecified"]))


def _confidence(
    requirement_type: MobileRequirementType, text: str, source_field: str
) -> MobileRequirementConfidence:
    field_text = source_field.replace("-", "_").casefold()
    if _REQUIRED_RE.search(text) or any(
        marker in field_text
        for marker in (
            "requirements",
            "acceptance_criteria",
            "success_criteria",
            "definition_of_done",
            "constraints",
            "scope",
        )
    ):
        return "high"
    if _TYPE_PATTERNS[requirement_type].search(_field_words(source_field)):
        return "high"
    if _mobile_context(text, source_field):
        return "medium"
    return "low"


def _actor(text: str) -> str | None:
    if match := _ACTOR_RE.search(text):
        return _clean_scope(match.group("actor"))
    return None


def _capability(requirement_type: MobileRequirementType, text: str) -> str | None:
    specific: dict[MobileRequirementType, tuple[re.Pattern[str], ...]] = {
        "push_notification": (
            re.compile(
                r"\b(?:push notifications?|push alerts?)\s+(?:for|about)\s+([a-z][a-z0-9 -]{2,50})",
                re.I,
            ),
        ),
        "deep_link": (
            re.compile(
                r"\b(?:deep links?|universal links?|app links?)\s+(?:to|into|for)\s+([a-z][a-z0-9 -]{2,50})",
                re.I,
            ),
        ),
        "offline_mode": (
            re.compile(r"\boffline (?:mode|access)\s+(?:for|to)\s+([a-z][a-z0-9 -]{2,50})", re.I),
        ),
        "biometric": (
            re.compile(
                r"\b(?:biometric|face id|touch id|fingerprint)\s+(?:login|sign[- ]in|unlock|authentication)\b",
                re.I,
            ),
        ),
        "native_permission": (
            re.compile(
                r"\b((?:camera|location|photo library|contacts|microphone|bluetooth|calendar|motion|health|storage)\s+permission)",
                re.I,
            ),
        ),
        "os_version": (
            re.compile(
                r"\b((?:minimum|min|target|supported)\s+(?:ios|android|ipados|os)\s*(?:version)?\s*\d+(?:\.\d+)?\+?)",
                re.I,
            ),
        ),
        "app_store": (
            re.compile(
                r"\b((?:app store|google play|play store|store review|testflight|store listing)[a-z0-9 -]{0,35})",
                re.I,
            ),
        ),
        "platform_support": (),
    }
    for pattern in specific[requirement_type]:
        if match := pattern.search(text):
            return _trim_capability(
                _clean_scope(match.group(1) if match.lastindex else match.group(0))
            )
    for pattern in _CAPABILITY_PATTERNS:
        if match := pattern.search(text):
            capability = _clean_scope(match.group("capability"))
            return _trim_capability(capability)
    return None


def _summary(
    requirements: tuple[SourceMobilePlatformRequirement, ...], source_count: int
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "platform_counts": {
            platform: sum(1 for requirement in requirements if requirement.platform == platform)
            for platform in _PLATFORM_ORDER
        },
        "requirement_type_counts": {
            kind: sum(1 for requirement in requirements if requirement.requirement_type == kind)
            for kind in _TYPE_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "platforms": [
            platform
            for platform in _PLATFORM_ORDER
            if any(requirement.platform == platform for requirement in requirements)
        ],
        "requirement_types": [
            kind
            for kind in _TYPE_ORDER
            if any(requirement.requirement_type == kind for requirement in requirements)
        ],
    }


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
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "integration_points",
        "risks",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
        "files",
        "file_paths",
        "paths",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _mobile_context(text: str, source_field: str) -> bool:
    field_text = _field_words(source_field)
    return (
        _MOBILE_CONTEXT_RE.search(text) is not None
        or _STRUCTURED_FIELD_RE.search(field_text) is not None
    )


def _any_signal(text: str) -> bool:
    return _MOBILE_CONTEXT_RE.search(text) is not None or any(
        pattern.search(text) for pattern in (*_PLATFORM_PATTERNS.values(), *_TYPE_PATTERNS.values())
    )


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    text = _CHECKBOX_RE.sub("", _BULLET_RE.sub("", value.strip()))
    return _SPACE_RE.sub(" ", text).strip()


def _clean_scope(value: str) -> str:
    return _clean_text(value).casefold().strip(" .,:;")


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _searchable_text(source_field: str, text: str) -> str:
    value = f"{_field_words(source_field)} {text}"
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    value = re.sub(r"\bi\s+(OS|Pad|Phone)\b", r"i\1", value, flags=re.I)
    return value.replace("/", " ").replace("_", " ").replace("-", " ")


def _trim_capability(value: str) -> str:
    return re.split(
        r"\b(?:with|using|on|for|when|before|after|and|or)\b", value, maxsplit=1, flags=re.I
    )[0].strip()


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _platform_index(platform: MobilePlatform) -> int:
    return _PLATFORM_ORDER.index(platform)


def _type_index(requirement_type: MobileRequirementType) -> int:
    return _TYPE_ORDER.index(requirement_type)


def _dedupe(values: Iterable[Any]) -> list[Any]:
    seen: set[str] = set()
    result: list[Any] = []
    for value in values:
        key = _dedupe_key(value)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _dedupe_key(value: Any) -> str:
    return _clean_text(str(value)).casefold() if value is not None else ""


__all__ = [
    "MobilePlatform",
    "MobileRequirementType",
    "SourceMobilePlatformRequirement",
    "SourceMobilePlatformRequirementsReport",
    "build_source_mobile_platform_requirements",
    "derive_source_mobile_platform_requirements",
    "extract_source_mobile_platform_requirements",
    "generate_source_mobile_platform_requirements",
    "source_mobile_platform_requirements_to_dict",
    "source_mobile_platform_requirements_to_dicts",
    "source_mobile_platform_requirements_to_markdown",
    "summarize_source_mobile_platform_requirements",
]
