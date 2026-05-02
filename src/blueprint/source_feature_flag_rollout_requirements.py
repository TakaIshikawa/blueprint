"""Extract source-level feature flag and rollout requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


FeatureFlagRolloutRequirementType = Literal[
    "feature_flag",
    "staged_rollout",
    "percentage_rollout",
    "beta_access",
    "cohort_targeting",
    "kill_switch",
    "gradual_release",
]
FeatureFlagRolloutConfidence = Literal["high", "medium", "low"]

_REQUIREMENT_ORDER: tuple[FeatureFlagRolloutRequirementType, ...] = (
    "feature_flag",
    "staged_rollout",
    "percentage_rollout",
    "beta_access",
    "cohort_targeting",
    "kill_switch",
    "gradual_release",
)
_CONFIDENCE_ORDER: dict[FeatureFlagRolloutConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)]|\[[ xX]\])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIREMENT_LANGUAGE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|acceptance|done when|before launch|cannot ship|gate|gated|"
    r"enable|disable|target|roll(?:\s|-)?out|release|launch|ramp)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:feature[- ]?flags?|feature[- ]?toggles?|"
    r"staged rollout|percentage rollout|beta|cohort|kill switch|gradual release)\b"
    r".{0,80}\b(?:scope|required|needed|changes?)\b",
    re.I,
)
_ROLLOUT_SIGNAL_RE = re.compile(
    r"\b(?:feature[- ]?flags?|feature[- ]?toggles?|flag gate|flag key|flag config|"
    r"launchdarkly|split\.io|flipper|unleash|kill switch|killswitch|emergency off|"
    r"circuit breaker|enable flag|disable flag|staged rollout|phased rollout|phased launch|staged enablement|"
    r"gradual enablement|gradual release|gradual rollout|progressive delivery|"
    r"canary|ramp(?:ing)?|percentage rollout|percent rollout|"
    r"beta access|private beta|public beta|beta users?|early access|preview users?|"
    r"allowlist|whitelist|cohorts?|segments?|audiences?|target users?|tenant targeting|"
    r"account targeting|customer targeting|rollout rules?)\b|"
    r"\b\d{1,3}\s*%\s*(?:of\s+)?(?:users|traffic|accounts|tenants|customers|requests)?\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?P<value>(?:\d{1,3}\s*%\s*(?:of\s+)?(?:users|traffic|accounts|tenants|customers|requests)?|"
    r"(?:wave|phase)\s+\d+|"
    r"(?:internal|staff|employee|dogfood|beta|pilot|canary)\s+(?:users?|customers?|accounts?|cohorts?)|"
    r"(?:enterprise|paid|free|pro|premium|business|trial)\s+(?:customers?|accounts?|tenants?|plans?)|"
    r"(?:EU|US|UK|APAC|EMEA|LATAM|Japan|Canada|Australia)\s+(?:users?|customers?|accounts?|tenants?)))\b",
    re.I,
)
_AUDIENCE_RE = re.compile(
    r"\b(?:for|to|target(?!ing\b)|targeting(?!\s+required\b)|enable\s+for|available\s+to|allowlist(?:ed)?|"
    r"whitelist(?:ed)?|cohort(?:s)?(?:\s+of)?|audience(?:s)?(?:\s+of)?)\s+"
    r"(?P<audience>[A-Za-z0-9][A-Za-z0-9 &/_.+-]{1,90})",
    re.I,
)
_AUDIENCE_STOP_RE = re.compile(
    r"\b(?:first|initially|only|before|after|during|when|with|without|while|unless|"
    r"using|via|who|that|where|because|from|until|and then|at|by|in)\b",
    re.I,
)
_REQUIREMENT_PATTERNS: dict[FeatureFlagRolloutRequirementType, re.Pattern[str]] = {
    "feature_flag": re.compile(
        r"\b(?:feature[- ]?flags?|feature[- ]?toggles?|flag gate|flag key|flag config|"
        r"launchdarkly|split\.io|flipper|unleash|enable flag|disable flag|"
        r"roll out behind a flag|behind a flag)\b",
        re.I,
    ),
    "staged_rollout": re.compile(
        r"\b(?:staged rollout|staged enablement|phased rollout|phased launch|"
        r"phase\s+\d+|wave\s+\d+|waves?|internal first|staff first|dogfood|canary)\b",
        re.I,
    ),
    "percentage_rollout": re.compile(
        r"\b(?:\d{1,3}\s*%\s*(?:of\s+)?(?:users|traffic|accounts|tenants|customers|requests)?|"
        r"percentage rollout|percent rollout|percentage ramp|flag ramp|ramp(?:ing)?\s+"
        r"(?:from|to|up|users|traffic|accounts|tenants|customers))\b",
        re.I,
    ),
    "beta_access": re.compile(
        r"\b(?:beta access|private beta|public beta|beta users?|beta customers?|"
        r"early access|preview users?|limited preview)\b",
        re.I,
    ),
    "cohort_targeting": re.compile(
        r"\b(?:cohorts?|segments?|audiences?|target users?|targeted users?|"
        r"tenant targeting|account targeting|customer targeting|rollout rules?|"
        r"allowlist|allowlisted|whitelist|whitelisted)\b",
        re.I,
    ),
    "kill_switch": re.compile(
        r"\b(?:kill switch|killswitch|emergency off|circuit breaker|disable quickly|"
        r"instant(?:ly)? disable|disable flag|turn off the flag|backout switch|"
        r"abort rollout|pause rollout)\b",
        re.I,
    ),
    "gradual_release": re.compile(
        r"\b(?:gradual release|gradual rollout|gradual enablement|progressive delivery|"
        r"progressive rollout|slow rollout|limited rollout|ramp(?:ing)? rollout)\b",
        re.I,
    ),
}
_PLANNING_NOTES: dict[FeatureFlagRolloutRequirementType, str] = {
    "feature_flag": "Define flag key, defaults, ownership, evaluation path, and cleanup criteria.",
    "staged_rollout": "Document rollout stages, promotion gates, owners, and stop conditions.",
    "percentage_rollout": "Record ramp percentages, timing, guardrails, and metrics required before expansion.",
    "beta_access": "Confirm beta eligibility, invite or removal criteria, support path, and feedback loop.",
    "cohort_targeting": "Specify targeting attributes, allowlist ownership, exclusions, and validation cohorts.",
    "kill_switch": "Define emergency disable behavior, authority, propagation time, and verification steps.",
    "gradual_release": "Plan progressive exposure, monitoring checkpoints, and criteria for widening release.",
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "release",
    "rollout",
    "risks",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_IGNORED_FIELDS = {
    "id",
    "source_brief_id",
    "source_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
    "source_links",
}


@dataclass(frozen=True, slots=True)
class SourceFeatureFlagRolloutRequirement:
    """One source-backed feature flag or rollout requirement."""

    source_brief_id: str | None
    requirement_type: FeatureFlagRolloutRequirementType
    rollout_value: str | None = None
    target_audience: str | None = None
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: FeatureFlagRolloutConfidence = "medium"
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "rollout_value": self.rollout_value,
            "target_audience": self.target_audience,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceFeatureFlagRolloutRequirementsReport:
    """Source-level feature flag and rollout requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceFeatureFlagRolloutRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceFeatureFlagRolloutRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceFeatureFlagRolloutRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return feature flag rollout requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Feature Flag Rollout Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("requirement_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Requirement type counts: "
            + ", ".join(
                f"{requirement_type} {type_counts.get(requirement_type, 0)}"
                for requirement_type in _REQUIREMENT_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source feature flag rollout requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Type | Value | Audience | Confidence | Source Field | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell(requirement.rollout_value or '')} | "
                f"{_markdown_cell(requirement.target_audience or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_feature_flag_rollout_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFeatureFlagRolloutRequirementsReport:
    """Extract source-level feature flag rollout requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceFeatureFlagRolloutRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_feature_flag_rollout_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFeatureFlagRolloutRequirementsReport:
    """Compatibility alias for building a feature flag rollout requirements report."""
    return build_source_feature_flag_rollout_requirements(source)


def generate_source_feature_flag_rollout_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFeatureFlagRolloutRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_feature_flag_rollout_requirements(source)


def derive_source_feature_flag_rollout_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFeatureFlagRolloutRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_feature_flag_rollout_requirements(source)


def summarize_source_feature_flag_rollout_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceFeatureFlagRolloutRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted feature flag rollout requirements."""
    if isinstance(source_or_result, SourceFeatureFlagRolloutRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_feature_flag_rollout_requirements(source_or_result).summary


def source_feature_flag_rollout_requirements_to_dict(
    report: SourceFeatureFlagRolloutRequirementsReport,
) -> dict[str, Any]:
    """Serialize a feature flag rollout requirements report to a plain dictionary."""
    return report.to_dict()


source_feature_flag_rollout_requirements_to_dict.__test__ = False


def source_feature_flag_rollout_requirements_to_dicts(
    requirements: (
        tuple[SourceFeatureFlagRolloutRequirement, ...]
        | list[SourceFeatureFlagRolloutRequirement]
        | SourceFeatureFlagRolloutRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize feature flag rollout requirement records to dictionaries."""
    if isinstance(requirements, SourceFeatureFlagRolloutRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_feature_flag_rollout_requirements_to_dicts.__test__ = False


def source_feature_flag_rollout_requirements_to_markdown(
    report: SourceFeatureFlagRolloutRequirementsReport,
) -> str:
    """Render a feature flag rollout requirements report as Markdown."""
    return report.to_markdown()


source_feature_flag_rollout_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: FeatureFlagRolloutRequirementType
    rollout_value: str | None
    target_audience: str | None
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: FeatureFlagRolloutConfidence


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


def _requirement_candidates(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for source_field, segment in _candidate_segments(payload):
            if not _is_rollout_requirement_signal(source_field, segment):
                continue
            requirement_types = _requirement_types(segment)
            if not requirement_types:
                continue
            value = _rollout_value(segment)
            audience = _target_audience(segment)
            confidence = _confidence(source_field, segment, value, audience)
            for requirement_type in requirement_types:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        requirement_type=requirement_type,
                        rollout_value=value,
                        target_audience=audience,
                        source_field=source_field,
                        evidence=_evidence_snippet(source_field, segment),
                        matched_terms=_matched_terms(requirement_type, segment),
                        confidence=confidence,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceFeatureFlagRolloutRequirement]:
    grouped: dict[tuple[str | None, FeatureFlagRolloutRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.requirement_type), []).append(
            candidate
        )

    requirements: list[SourceFeatureFlagRolloutRequirement] = []
    for (source_brief_id, requirement_type), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda item: _CONFIDENCE_ORDER[item]
        )
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda item: item.casefold(),
        )[0]
        requirements.append(
            SourceFeatureFlagRolloutRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                rollout_value=_joined_details(item.rollout_value for item in items),
                target_audience=_joined_details(item.target_audience for item in items),
                source_field=source_field,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                matched_terms=tuple(
                    _dedupe(
                        term
                        for item in items
                        for term in item.matched_terms
                        if term
                    )
                ),
                confidence=confidence,
                planning_note=_PLANNING_NOTES[requirement_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _REQUIREMENT_ORDER.index(requirement.requirement_type),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and key not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _ROLLOUT_SIGNAL_RE.search(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            values.append((source_field, segment))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for sentence in _SENTENCE_SPLIT_RE.split(value):
        segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _is_rollout_requirement_signal(source_field: str, text: str) -> bool:
    if _NEGATED_SCOPE_RE.search(text):
        return False
    if not _ROLLOUT_SIGNAL_RE.search(text):
        return False
    if source_field == "title" and not re.search(
        r"\b(?:must|shall|required|requires?|need(?:s)? to|should|ensure|"
        r"enable|disable|target|allowlist|whitelist|\d{1,3}\s*%)\b",
        text,
        re.I,
    ):
        return False
    return True


def _requirement_types(text: str) -> tuple[FeatureFlagRolloutRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _REQUIREMENT_ORDER
        if _REQUIREMENT_PATTERNS[requirement_type].search(text)
    )


def _rollout_value(text: str) -> str | None:
    if match := _VALUE_RE.search(text):
        return _clean_text(match.group("value"))
    return None


def _target_audience(text: str) -> str | None:
    if match := _AUDIENCE_RE.search(text):
        audience = _clean_text(match.group("audience"))
        stop = _AUDIENCE_STOP_RE.search(audience)
        if stop:
            audience = audience[: stop.start()].strip()
        audience = re.sub(r"[.;:]+$", "", audience).strip()
        return audience or None
    return None


def _matched_terms(
    requirement_type: FeatureFlagRolloutRequirementType, text: str
) -> tuple[str, ...]:
    terms: list[str] = []
    for match in _REQUIREMENT_PATTERNS[requirement_type].finditer(text):
        if match.group(0):
            terms.append(_clean_text(match.group(0)).casefold())
    return tuple(_dedupe(terms))


def _confidence(
    source_field: str,
    text: str,
    value: str | None,
    audience: str | None,
) -> FeatureFlagRolloutConfidence:
    normalized_field = source_field.replace("-", "_").casefold()
    if _REQUIREMENT_LANGUAGE_RE.search(text) and (
        value
        or audience
        or re.search(r"\b(?:kill switch|killswitch|emergency off|disable flag)\b", text, re.I)
        or any(
            marker in normalized_field
            for marker in (
                "acceptance",
                "success_criteria",
                "definition_of_done",
                "requirement",
                "constraint",
                "rollout",
            )
        )
    ):
        return "high"
    if (
        value
        or audience
        or re.search(r"\b(?:must|shall|required|kill switch|feature[- ]?flag)\b", text, re.I)
    ):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceFeatureFlagRolloutRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "requirement_type_counts": {
            requirement_type: sum(
                1
                for requirement in requirements
                if requirement.requirement_type == requirement_type
            )
            for requirement_type in _REQUIREMENT_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "target_audiences": sorted(
            {
                requirement.target_audience
                for requirement in requirements
                if requirement.target_audience
            },
            key=str.casefold,
        ),
        "status": "ready_for_planning" if requirements else "no_feature_flag_rollout_language",
    }


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
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "release",
        "rollout",
        "risks",
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


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 200:
        value = f"{value[:197].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _joined_details(values: Iterable[str | None]) -> str | None:
    details = _dedupe(value for value in values if value)
    return ", ".join(details) if details else None


def _dedupe(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = _clean_text(value)
        key = cleaned.casefold()
        if not cleaned or key in seen:
            continue
        deduped.append(cleaned)
        seen.add(key)
    return deduped


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
    return sorted(deduped, key=lambda item: item.casefold())


__all__ = [
    "FeatureFlagRolloutConfidence",
    "FeatureFlagRolloutRequirementType",
    "SourceFeatureFlagRolloutRequirement",
    "SourceFeatureFlagRolloutRequirementsReport",
    "build_source_feature_flag_rollout_requirements",
    "derive_source_feature_flag_rollout_requirements",
    "extract_source_feature_flag_rollout_requirements",
    "generate_source_feature_flag_rollout_requirements",
    "summarize_source_feature_flag_rollout_requirements",
    "source_feature_flag_rollout_requirements_to_dict",
    "source_feature_flag_rollout_requirements_to_dicts",
    "source_feature_flag_rollout_requirements_to_markdown",
]
