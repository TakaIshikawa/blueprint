"""Extract source-level feature flag requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


FeatureFlagRequirementCategory = Literal[
    "rollout_gate",
    "cohort_targeting",
    "kill_switch",
    "experiment_toggle",
    "config_flag",
    "permission_gate",
    "cleanup_policy",
    "owner_approval",
]
FeatureFlagRequirementConfidence = Literal["high", "medium", "low"]
FeatureFlagMissingDetail = Literal[
    "missing_flag_owner",
    "missing_rollout_scope",
    "missing_disable_or_rollback_behavior",
    "missing_cleanup_plan",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[FeatureFlagRequirementCategory, ...] = (
    "rollout_gate",
    "cohort_targeting",
    "kill_switch",
    "experiment_toggle",
    "config_flag",
    "permission_gate",
    "cleanup_policy",
    "owner_approval",
)
_MISSING_DETAIL_ORDER: tuple[FeatureFlagMissingDetail, ...] = (
    "missing_flag_owner",
    "missing_rollout_scope",
    "missing_disable_or_rollback_behavior",
    "missing_cleanup_plan",
)
_CONFIDENCE_ORDER: dict[FeatureFlagRequirementConfidence, int] = {
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
    r"acceptance|done when|before launch|cannot ship|support|allow|provide|"
    r"roll(?:out| back)|gate|flag|toggle|disable|enable|experiment|cohort|"
    r"permission|approval|cleanup|remove)\b",
    re.I,
)
_FEATURE_FLAG_CONTEXT_RE = re.compile(
    r"\b(?:feature flags?|flags?|feature toggles?|toggles?|rollout gates?|gated rollout|"
    r"dark launch|progressive rollout|canary|percentage rollout|cohorts?|segments?|"
    r"beta users?|allowlist|denylist|kill switch(?:es)?|circuit breakers?|emergency off|"
    r"disable switch|experiments?|a/b tests?|ab tests?|variant|control group|"
    r"remote config|configuration flags?|config flags?|runtime config|permission gates?|"
    r"entitlements?|rbac|roles?|admin approval|owner approval|cleanup|sunset|remove flag|"
    r"retire flag|flag debt|launchdarkly|split\.io|statsig|unleash)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:feature[_ -]?flags?|flags?|toggles?|rollout|cohorts?|experiments?|"
    r"permissions?|entitlements?|cleanup|approval|requirements?|acceptance|criteria|"
    r"definition_of_done|constraints?|risks?|metadata|source_payload|launch)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:feature flags?|flags?|toggles?|rollout gates?|"
    r"experiments?|permission gates?)\b.{0,80}\b(?:scope|required|needed|changes?)\b",
    re.I,
)
_GENERIC_FEATURE_FLAG_RE = re.compile(
    r"^(?:general\s+)?(?:feature flags?|feature flagging|rollout|release controls?)\s+"
    r"(?:implementation\s+)?"
    r"(?:requirements?|behavior)\.?$|^validate feature flag behavior\.?$",
    re.I,
)
_FEATURE_RE = re.compile(
    r"\b(?P<feature>(?:checkout|billing|pricing|search|admin|customer|merchant|operator|"
    r"support|onboarding|dashboard|reporting|export|import|notification|messaging|"
    r"profile|settings|workspace|team|experiment|beta|release|feature)\s+"
    r"(?:feature|flow|experience|page|screen|workflow|release|rollout|experiment|"
    r"permission|gate|flag|toggle))\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:owner|owned by|approver|approval from|approved by|product owner|engineering owner|"
    r"release owner|ops owner|security approval|admin approval|sign[- ]off)\b",
    re.I,
)
_ROLLOUT_SCOPE_RE = re.compile(
    r"\b(?:percentage|percent|%|cohort|segment|tenant|workspace|team|region|locale|"
    r"beta users?|allowlist|denylist|canary|internal users?|staff|role|entitlement|"
    r"admin only|gradual|progressive|phased|ramp)\b",
    re.I,
)
_DISABLE_RE = re.compile(
    r"\b(?:kill switch|circuit breaker|emergency off|disable|turn off|rollback|roll back|"
    r"revert|fallback|fail closed|fail open|stop rollout|pause rollout)\b",
    re.I,
)
_CLEANUP_RE = re.compile(
    r"\b(?:cleanup|clean up|remove|delete|retire|sunset|decommission|expiry|expires?|"
    r"expiration|flag debt|post[- ]launch|after launch|migration complete)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[FeatureFlagRequirementCategory, re.Pattern[str]] = {
    "rollout_gate": re.compile(
        r"\b(?:rollout gate|gated rollout|progressive rollout|phased rollout|"
        r"percentage rollout|canary|dark launch|launch gate|release gate|flagged rollout)\b",
        re.I,
    ),
    "cohort_targeting": re.compile(
        r"\b(?:cohorts?|segments?|targeting|targeted users?|beta users?|allowlist|denylist|"
        r"tenant|tenant allowlist|workspace|workspace allowlist|internal users?|staff only|region|locale|"
        r"percentage|percent|%)\b",
        re.I,
    ),
    "kill_switch": re.compile(
        r"\b(?:kill switch|circuit breaker|emergency off|disable switch|turn off|"
        r"instant disable|rollback flag|pause rollout|stop rollout)\b",
        re.I,
    ),
    "experiment_toggle": re.compile(
        r"\b(?:experiment|experimentation|a/b test|ab test|split test|variant|control group|"
        r"treatment group|statsig|optimizely)\b",
        re.I,
    ),
    "config_flag": re.compile(
        r"\b(?:config flag|configuration flag|remote config|runtime config|feature flag|"
        r"feature toggle|toggle|flag value|launchdarkly|split\.io|unleash)\b",
        re.I,
    ),
    "permission_gate": re.compile(
        r"\b(?:permission gate|permissions? gate|entitlement|rbac|role based|role-based|"
        r"admin only|operator only|access gate|gated by role|requires? permission)\b",
        re.I,
    ),
    "cleanup_policy": _CLEANUP_RE,
    "owner_approval": _OWNER_RE,
}
_PLANNING_NOTES: dict[FeatureFlagRequirementCategory, str] = {
    "rollout_gate": "Define rollout phases, default state, monitoring gates, and rollback criteria.",
    "cohort_targeting": "Specify targeting dimensions, cohort ownership, eligibility rules, and auditability.",
    "kill_switch": "Document disable path, expected blast radius, fallback behavior, and operator access.",
    "experiment_toggle": "Define variants, assignment rules, metrics, guardrails, and experiment end state.",
    "config_flag": "Specify flag storage, defaults, environments, runtime behavior, and test coverage.",
    "permission_gate": "Define roles, entitlements, denial behavior, and permission test cases.",
    "cleanup_policy": "Set the flag removal trigger, owner, target date, and stale-code cleanup tasks.",
    "owner_approval": "Record the approving owner, approval workflow, and launch sign-off evidence.",
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
    "data_requirements",
    "architecture_notes",
    "feature_flags",
    "flags",
    "rollout",
    "permissions",
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
class SourceFeatureFlagRequirement:
    """One source-backed feature flag requirement."""

    source_brief_id: str | None
    feature_area: str
    requirement_category: FeatureFlagRequirementCategory
    missing_detail_flags: tuple[FeatureFlagMissingDetail, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: FeatureFlagRequirementConfidence = "medium"
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "feature_area": self.feature_area,
            "requirement_category": self.requirement_category,
            "missing_detail_flags": list(self.missing_detail_flags),
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceFeatureFlagRequirementsReport:
    """Source-level feature flag requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceFeatureFlagRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceFeatureFlagRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceFeatureFlagRequirement, ...]:
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
        """Return feature flag requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Feature Flag Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("requirement_category_counts", {})
        missing_counts = self.summary.get("missing_detail_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Requirement category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}"
                for category in _CATEGORY_ORDER
            ),
            "- Missing detail counts: "
            + ", ".join(
                f"{flag} {missing_counts.get(flag, 0)}" for flag in _MISSING_DETAIL_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source feature flag requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Feature Area | Category | Confidence | Missing Details | Source Field Paths | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.feature_area)} | "
                f"{requirement.requirement_category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(', '.join(requirement.missing_detail_flags))} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_feature_flag_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFeatureFlagRequirementsReport:
    """Extract source-level feature flag requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceFeatureFlagRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_feature_flag_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFeatureFlagRequirementsReport:
    """Compatibility alias for building a feature flag requirements report."""
    return build_source_feature_flag_requirements(source)


def generate_source_feature_flag_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFeatureFlagRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_feature_flag_requirements(source)


def derive_source_feature_flag_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFeatureFlagRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_feature_flag_requirements(source)


def summarize_source_feature_flag_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceFeatureFlagRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted feature flag requirements."""
    if isinstance(source_or_result, SourceFeatureFlagRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_feature_flag_requirements(source_or_result).summary


def recommend_source_feature_flag_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceFeatureFlagRequirementsReport:
    """Compatibility helper for callers that use recommend_* naming."""
    return build_source_feature_flag_requirements(source)


def source_feature_flag_requirements_to_dict(
    report: SourceFeatureFlagRequirementsReport,
) -> dict[str, Any]:
    """Serialize a feature flag requirements report to a plain dictionary."""
    return report.to_dict()


source_feature_flag_requirements_to_dict.__test__ = False


def source_feature_flag_requirements_to_dicts(
    requirements: (
        tuple[SourceFeatureFlagRequirement, ...]
        | list[SourceFeatureFlagRequirement]
        | SourceFeatureFlagRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize feature flag requirement records to dictionaries."""
    if isinstance(requirements, SourceFeatureFlagRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_feature_flag_requirements_to_dicts.__test__ = False


def source_feature_flag_requirements_to_markdown(
    report: SourceFeatureFlagRequirementsReport,
) -> str:
    """Render a feature flag requirements report as Markdown."""
    return report.to_markdown()


source_feature_flag_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    feature_area: str
    requirement_category: FeatureFlagRequirementCategory
    missing_detail_flags: tuple[FeatureFlagMissingDetail, ...]
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]
    confidence: FeatureFlagRequirementConfidence


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
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        feature_area=_feature_area(segment.source_field, segment.text),
                        requirement_category=category,
                        missing_detail_flags=_missing_detail_flags(searchable, category),
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        source_field_path=segment.source_field,
                        matched_terms=_matched_terms(category, searchable),
                        confidence=_confidence(segment),
                    )
                )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceFeatureFlagRequirement]:
    grouped: dict[
        tuple[str | None, str, FeatureFlagRequirementCategory], list[_Candidate]
    ] = {}
    for candidate in candidates:
        key = (
            candidate.source_brief_id,
            candidate.feature_area.casefold(),
            candidate.requirement_category,
        )
        grouped.setdefault(key, []).append(candidate)

    requirements: list[SourceFeatureFlagRequirement] = []
    for (_source_brief_id, _area_key, category), items in grouped.items():
        source_brief_id = items[0].source_brief_id
        feature_area = sorted({item.feature_area for item in items}, key=str.casefold)[0]
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in items), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(
                _dedupe(term for item in items for term in item.matched_terms),
                key=str.casefold,
            )
        )
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        present_flags = {
            flag for item in items for flag in item.missing_detail_flags if flag
        }
        missing_detail_flags = tuple(
            flag for flag in _MISSING_DETAIL_ORDER if flag in present_flags
        )
        requirements.append(
            SourceFeatureFlagRequirement(
                source_brief_id=source_brief_id,
                feature_area=feature_area,
                requirement_category=category,
                missing_detail_flags=missing_detail_flags,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                confidence=confidence,
                planning_note=_PLANNING_NOTES[category],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CATEGORY_ORDER.index(requirement.requirement_category),
            requirement.feature_area.casefold(),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
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
                or _FEATURE_FLAG_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text in _segments(text):
            segments.append(_Segment(source_field, segment_text, field_context))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for raw_line in value.splitlines() or [value]:
        cleaned = _clean_text(raw_line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(raw_line) or _CHECKBOX_RE.match(raw_line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _is_requirement(segment: _Segment) -> bool:
    if _NEGATED_SCOPE_RE.search(segment.text):
        return False
    if _GENERIC_FEATURE_FLAG_RE.match(segment.text):
        return False
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if not _FEATURE_FLAG_CONTEXT_RE.search(searchable):
        return False
    if segment.source_field == "title" and not re.search(
        r"\b(?:must|shall|required|requires?|need(?:s)? to|should|ensure|support|"
        r"gate|flag|toggle|rollout|cohort|experiment|permission|approval|cleanup)\b",
        segment.text,
        re.I,
    ):
        return False
    if segment.section_context:
        return True
    return bool(_REQUIRED_RE.search(segment.text))


def _feature_area(source_field: str, text: str) -> str:
    searchable = f"{_field_words(source_field)} {text}"
    if match := _FEATURE_RE.search(searchable):
        area = _clean_text(match.group("feature")).casefold()
        area = re.sub(
            r"\s+(?:feature|flow|experience|page|screen|workflow|release|rollout|"
            r"experiment|permission|gate|flag|toggle)$",
            " feature",
            area,
        )
        return area
    return "unspecified feature area"


def _missing_detail_flags(
    searchable: str,
    category: FeatureFlagRequirementCategory,
) -> tuple[FeatureFlagMissingDetail, ...]:
    flags: list[FeatureFlagMissingDetail] = []
    if not _OWNER_RE.search(searchable):
        flags.append("missing_flag_owner")
    if category in {
        "rollout_gate",
        "cohort_targeting",
        "experiment_toggle",
        "permission_gate",
    } and not _ROLLOUT_SCOPE_RE.search(searchable):
        flags.append("missing_rollout_scope")
    if category in {"rollout_gate", "kill_switch", "config_flag"} and not _DISABLE_RE.search(searchable):
        flags.append("missing_disable_or_rollback_behavior")
    if category != "cleanup_policy" and not _CLEANUP_RE.search(searchable):
        flags.append("missing_cleanup_plan")
    return tuple(flags)


def _matched_terms(
    category: FeatureFlagRequirementCategory,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)).casefold()
            for match in _CATEGORY_PATTERNS[category].finditer(text)
        )
    )


def _confidence(segment: _Segment) -> FeatureFlagRequirementConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    detail_count = sum(
        1
        for pattern in (_OWNER_RE, _ROLLOUT_SCOPE_RE, _DISABLE_RE, _CLEANUP_RE)
        if pattern.search(searchable)
    )
    if _REQUIRED_RE.search(segment.text) and (detail_count or segment.section_context):
        return "high"
    if segment.section_context or _REQUIRED_RE.search(segment.text) or detail_count:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceFeatureFlagRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_categories": [
            requirement.requirement_category for requirement in requirements
        ],
        "requirement_category_counts": {
            category: sum(
                1
                for requirement in requirements
                if requirement.requirement_category == category
            )
            for category in _CATEGORY_ORDER
        },
        "missing_detail_counts": {
            flag: sum(
                1
                for requirement in requirements
                if flag in requirement.missing_detail_flags
            )
            for flag in _MISSING_DETAIL_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "feature_areas": sorted(
            {requirement.feature_area for requirement in requirements},
            key=str.casefold,
        ),
        "status": "ready_for_planning" if requirements else "no_feature_flag_language",
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
        "data_requirements",
        "architecture_notes",
        "feature_flags",
        "flags",
        "rollout",
        "permissions",
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
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
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
    "FeatureFlagMissingDetail",
    "FeatureFlagRequirementCategory",
    "FeatureFlagRequirementConfidence",
    "SourceFeatureFlagRequirement",
    "SourceFeatureFlagRequirementsReport",
    "build_source_feature_flag_requirements",
    "derive_source_feature_flag_requirements",
    "extract_source_feature_flag_requirements",
    "generate_source_feature_flag_requirements",
    "recommend_source_feature_flag_requirements",
    "source_feature_flag_requirements_to_dict",
    "source_feature_flag_requirements_to_dicts",
    "source_feature_flag_requirements_to_markdown",
    "summarize_source_feature_flag_requirements",
]
