"""Extract source-level feature flag rollout requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


FeatureFlagRolloutCategory = Literal[
    "flag_ownership",
    "audience_targeting",
    "staged_rollout_percentages",
    "kill_switch_behavior",
    "experiment_variant_tracking",
    "observability",
    "rollback_criteria",
    "cleanup_deprecation",
]
FeatureFlagRolloutGapCategory = Literal["missing_flag_owner", "missing_rollback_criteria"]
FeatureFlagRolloutConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[FeatureFlagRolloutCategory, ...] = (
    "flag_ownership",
    "audience_targeting",
    "staged_rollout_percentages",
    "kill_switch_behavior",
    "experiment_variant_tracking",
    "observability",
    "rollback_criteria",
    "cleanup_deprecation",
)
_GAP_ORDER: tuple[FeatureFlagRolloutGapCategory, ...] = (
    "missing_flag_owner",
    "missing_rollback_criteria",
)
_CONFIDENCE_ORDER: dict[FeatureFlagRolloutConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_OWNER_BY_CATEGORY: dict[FeatureFlagRolloutCategory, tuple[str, ...]] = {
    "flag_ownership": ("product", "engineering"),
    "audience_targeting": ("product", "data"),
    "staged_rollout_percentages": ("product", "engineering"),
    "kill_switch_behavior": ("engineering", "sre"),
    "experiment_variant_tracking": ("product", "analytics"),
    "observability": ("sre", "analytics"),
    "rollback_criteria": ("engineering", "sre"),
    "cleanup_deprecation": ("engineering", "product"),
}
_PLANNING_NOTES: dict[FeatureFlagRolloutCategory, tuple[str, ...]] = {
    "flag_ownership": ("Assign flag owner, approving team, decision authority, and lifecycle accountability.",),
    "audience_targeting": ("Define targeted cohorts, segments, tenants, environments, allowlists, and exclusions.",),
    "staged_rollout_percentages": ("Specify rollout stages, percentage ramps, hold periods, and promotion gates.",),
    "kill_switch_behavior": ("Define kill switch defaults, disable path, propagation expectations, and emergency access.",),
    "experiment_variant_tracking": ("Track variants, exposure events, experiment assignment, and analytics dimensions.",),
    "observability": ("Instrument metrics, alerts, dashboards, logs, and health checks for rollout monitoring.",),
    "rollback_criteria": ("Document rollback triggers, thresholds, decision makers, and rollback execution steps.",),
    "cleanup_deprecation": ("Schedule flag cleanup, deprecation criteria, stale flag removal, and code path deletion.",),
}
_GAP_MESSAGES: dict[FeatureFlagRolloutGapCategory, str] = {
    "missing_flag_owner": "Specify the flag owner or accountable team before rollout planning.",
    "missing_rollback_criteria": "Specify rollback criteria, thresholds, or revert triggers before rollout planning.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_FEATURE_FLAG_CONTEXT_RE = re.compile(
    r"\b(?:feature flags?|feature toggles?|release flags?|launch flags?|rollout flags?|"
    r"flag rollout|staged rollout|gradual rollout|progressive rollout|percentage rollout|"
    r"canary rollout|dark launch|targeted rollout|cohort rollout|experiment rollout|"
    r"variants?|a/?b tests?|experiments?|exposure events?|kill switch|circuit breaker|"
    r"roll back|rollback|revert|cleanup|deprecat|stale flags?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:feature[_ -]?flags?|feature[_ -]?toggles?|release[_ -]?flags?|rollout|staged|"
    r"percentage|target|audience|cohort|segment|allowlist|variant|experiment|ab[_ -]?test|"
    r"exposure|kill[_ -]?switch|rollback|revert|observability|monitor|metrics?|alerts?|"
    r"cleanup|deprecat|stale|owner|requirements?|acceptance|criteria|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"support|allow|provide|enable|define|assign|own|owner|approve|target|segment|"
    r"rollout|roll out|ramp|percentage|percent|enable|disable|kill switch|rollback|"
    r"revert|monitor|alert|metric|dashboard|log|track|experiment|variant|exposure|"
    r"cleanup|deprecat|remove|delete|acceptance|done when|before launch|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:feature flags?|feature toggles?|release flags?|staged rollout|rollout controls?|"
    r"kill switch|rollback|experiments?|variants?|flag cleanup|flag owner)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:feature flags?|feature toggles?|release flags?|staged rollout|rollout controls?|"
    r"kill switch|rollback|experiments?|variants?|flag cleanup|flag owner)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_FEATURE_FLAGS_RE = re.compile(
    r"\b(?:no feature flags?|feature flags? are out of scope|feature toggles? are out of scope|"
    r"no staged rollout|rollout controls? are out of scope|no flag rollout work)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:flag icon|flag emoji|country flag|red flag copy|feature list|rollout mat|"
    r"stage lighting|percentage discount|variant sku|cleanup copy)\b",
    re.I,
)
_VALUE_PATTERNS: dict[FeatureFlagRolloutCategory, re.Pattern[str]] = {
    "flag_ownership": re.compile(
        r"\b(?:owner|owned by|accountable team|approver|approval|product owner|engineering owner|"
        r"release manager|on[- ]?call|sre|product|engineering|data|analytics)\b",
        re.I,
    ),
    "audience_targeting": re.compile(
        r"\b(?:target|targeting|targeted|cohort|segment|audience|tenants?|workspace|organization|customer|beta users?|"
        r"allowlist|blocklist|region|plan|environment|internal users?|staff|percentage of users?)\b",
        re.I,
    ),
    "staged_rollout_percentages": re.compile(
        r"(?:\b\d+\s?%|\b\d+\s*percent|\b(?:percentage rollout|ramp|ramp up|staged rollout|"
        r"gradual rollout|canary|phase|hold period|promotion gate)\b)",
        re.I,
    ),
    "kill_switch_behavior": re.compile(
        r"\b(?:kill switch|circuit breaker|disable flag|turn off|shut off|emergency off|"
        r"fail closed|fail open|default off|global disable|instant disable)\b",
        re.I,
    ),
    "experiment_variant_tracking": re.compile(
        r"\b(?:experiment|variant|a/?b test|split test|control group|treatment group|"
        r"exposure event|assignment|conversion|analytics dimension)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|monitor|monitoring|metric|metrics|alert|alerting|dashboard|"
        r"logs?|health checks?|error rate|latency|conversion|slo|datadog|grafana)\b",
        re.I,
    ),
    "rollback_criteria": re.compile(
        r"\b(?:rollback|roll back|revert|backout|abort|stop rollout|rollback criteria|"
        r"rollback trigger|error threshold|latency threshold|conversion drop|decision maker)\b",
        re.I,
    ),
    "cleanup_deprecation": re.compile(
        r"\b(?:cleanup|clean up|deprecat(?:e|ion)|remove flag|delete flag|stale flag|"
        r"flag debt|sunset|retire|code path removal|dead code)\b",
        re.I,
    ),
}
_OWNER_DETAIL_RE = re.compile(
    r"\b(?:owner|owned by|accountable team|approver|approval|product owner|engineering owner|"
    r"release manager|decision maker|on[- ]?call)\b",
    re.I,
)
_ROLLBACK_DETAIL_RE = re.compile(
    r"\b(?:rollback|roll back|revert|backout|abort|stop rollout|trigger|threshold|criteria|"
    r"error rate|latency|conversion drop|slo breach)\b",
    re.I,
)
_ROLLOUT_CONTROL_RE = re.compile(
    r"\b(?:feature flags?|feature toggles?|staged rollout|gradual rollout|progressive rollout|"
    r"percentage rollout|ramp|kill switch|target(?:ing)?|cohort|variant|experiment)\b",
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
    "release",
    "rollout",
    "feature_flags",
    "feature_toggles",
    "experimentation",
    "analytics",
    "observability",
    "monitoring",
    "operations",
    "support",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS = _VALUE_PATTERNS


@dataclass(frozen=True, slots=True)
class SourceFeatureFlagRolloutRequirement:
    """One source-backed feature flag rollout requirement."""

    category: FeatureFlagRolloutCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: FeatureFlagRolloutConfidence = "medium"
    value: str = ""
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> FeatureFlagRolloutCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> FeatureFlagRolloutCategory:
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
        }


@dataclass(frozen=True, slots=True)
class SourceFeatureFlagRolloutEvidenceGap:
    """One missing rollout detail that should be resolved before planning."""

    category: FeatureFlagRolloutGapCategory
    message: str
    confidence: FeatureFlagRolloutConfidence = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "message": self.message,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourceFeatureFlagRolloutRequirementsReport:
    """Source-level feature flag rollout requirements report."""

    source_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceFeatureFlagRolloutRequirement, ...] = field(default_factory=tuple)
    evidence_gaps: tuple[SourceFeatureFlagRolloutEvidenceGap, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceFeatureFlagRolloutRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceFeatureFlagRolloutRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    @property
    def gaps(self) -> tuple[SourceFeatureFlagRolloutEvidenceGap, ...]:
        """Compatibility alias for evidence gaps."""
        return self.evidence_gaps

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "title": self.title,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "evidence_gaps": [gap.to_dict() for gap in self.evidence_gaps],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
            "gaps": [gap.to_dict() for gap in self.gaps],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return feature flag rollout requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Feature Flag Rollout Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Evidence gaps: {self.summary.get('evidence_gap_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if self.requirements:
            lines.extend(
                [
                    "",
                    "## Requirements",
                    "",
                    "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes |",
                    "| --- | --- | --- | --- | --- | --- | --- |",
                ]
            )
            for requirement in self.requirements:
                lines.append(
                    "| "
                    f"{requirement.category} | "
                    f"{_markdown_cell(requirement.value)} | "
                    f"{requirement.confidence} | "
                    f"{_markdown_cell(requirement.source_field)} | "
                    f"{_markdown_cell(', '.join(requirement.suggested_owners))} | "
                    f"{_markdown_cell('; '.join(requirement.evidence))} | "
                    f"{_markdown_cell('; '.join(requirement.planning_notes))} |"
                )
        else:
            lines.extend(["", "No feature flag rollout requirements were found in the source brief."])
        if self.evidence_gaps:
            lines.extend(["", "## Evidence Gaps", "", "| Gap | Confidence | Message |", "| --- | --- | --- |"])
            for gap in self.evidence_gaps:
                lines.append(f"| {gap.category} | {gap.confidence} | {_markdown_cell(gap.message)} |")
        return "\n".join(lines)


def build_source_feature_flag_rollout_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceFeatureFlagRolloutRequirementsReport:
    """Build a feature flag rollout requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    requirements = tuple(_merge_candidates(candidates))
    gaps = tuple(_evidence_gaps(requirements, candidates))
    return SourceFeatureFlagRolloutRequirementsReport(
        source_id=source_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        evidence_gaps=gaps,
        summary=_summary(requirements, gaps),
    )


def generate_source_feature_flag_rollout_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceFeatureFlagRolloutRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_feature_flag_rollout_requirements(source)


def derive_source_feature_flag_rollout_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceFeatureFlagRolloutRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_feature_flag_rollout_requirements(source)


def extract_source_feature_flag_rollout_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceFeatureFlagRolloutRequirement, ...]:
    """Return feature flag rollout requirement records extracted from brief-shaped input."""
    return build_source_feature_flag_rollout_requirements(source).requirements


def summarize_source_feature_flag_rollout_requirements(
    source_or_result: (
        str
        | Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceFeatureFlagRolloutRequirementsReport
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic feature flag rollout requirements summary."""
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
    requirements: tuple[SourceFeatureFlagRolloutRequirement, ...]
    | list[SourceFeatureFlagRolloutRequirement]
    | SourceFeatureFlagRolloutRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize source feature flag rollout requirement records to dictionaries."""
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
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: FeatureFlagRolloutCategory
    confidence: FeatureFlagRolloutConfidence
    evidence: str
    source_field: str
    value: str


def _source_payload(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (bytes, bytearray)):
        return None, {}
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
    payload = _object_payload(source)
    return _brief_id(payload), payload


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        if not _is_requirement(segment):
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
        for category in _dedupe(categories):
            candidates.append(
                _Candidate(
                    category=category,
                    confidence=_confidence(segment),
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    source_field=segment.source_field,
                    value=_extract_value(category, segment.text),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceFeatureFlagRolloutRequirement]:
    by_category: dict[FeatureFlagRolloutCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceFeatureFlagRolloutRequirement] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        best = min(
            items,
            key=lambda item: (
                _CONFIDENCE_ORDER[item.confidence],
                _field_category_rank(category, item.source_field),
                item.source_field.casefold(),
            ),
        )
        requirements.append(
            SourceFeatureFlagRolloutRequirement(
                category=category,
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                confidence=best.confidence,
                value=_merge_values(item.value for item in items),
                suggested_owners=_OWNER_BY_CATEGORY[category],
                planning_notes=_PLANNING_NOTES[category],
            )
        )
    return requirements


def _evidence_gaps(
    requirements: tuple[SourceFeatureFlagRolloutRequirement, ...],
    candidates: list[_Candidate],
) -> list[SourceFeatureFlagRolloutEvidenceGap]:
    if not candidates:
        return []
    text = " ".join(candidate.evidence for candidate in candidates)
    if not _ROLLOUT_CONTROL_RE.search(text):
        return []
    present = {requirement.category for requirement in requirements}
    gaps: list[SourceFeatureFlagRolloutEvidenceGap] = []
    if "flag_ownership" not in present and not _OWNER_DETAIL_RE.search(text):
        gaps.append(
            SourceFeatureFlagRolloutEvidenceGap(
                category="missing_flag_owner",
                message=_GAP_MESSAGES["missing_flag_owner"],
                confidence="medium",
            )
        )
    if "rollback_criteria" not in present and not _ROLLBACK_DETAIL_RE.search(text):
        gaps.append(
            SourceFeatureFlagRolloutEvidenceGap(
                category="missing_rollback_criteria",
                message=_GAP_MESSAGES["missing_rollback_criteria"],
                confidence="medium",
            )
        )
    return [gap for category in _GAP_ORDER for gap in gaps if gap.category == category]


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
            if str(key) in _IGNORED_FIELDS:
                continue
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _FEATURE_FLAG_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        raw_text = str(value) if isinstance(value, str) else text
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
                _FEATURE_FLAG_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = (
                [part]
                if _NEGATED_SCOPE_RE.search(part) and _FEATURE_FLAG_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _NO_FEATURE_FLAGS_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _FEATURE_FLAG_CONTEXT_RE.search(searchable):
        return False
    if not (_FEATURE_FLAG_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    if not any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values()):
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _FEATURE_FLAG_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:owned|targeted|ramped|enabled|disabled|rolled back|reverted|monitored|tracked|removed)\b",
            segment.text,
            re.I,
        )
    )


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        root_field = segment.source_field.split("[", 1)[0].split(".", 1)[0]
        if root_field not in {"title", "summary", "body", "description", "scope", "non_goals", "constraints", "source_payload"}:
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        if _NO_FEATURE_FLAGS_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _confidence(segment: _Segment) -> FeatureFlagRolloutConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    score = 0
    if _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        score += 1
    if segment.section_context or _FEATURE_FLAG_CONTEXT_RE.search(searchable):
        score += 1
    if _REQUIREMENT_RE.search(segment.text):
        score += 1
    if any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values()):
        score += 1
    return "high" if score >= 4 else "medium" if score >= 2 else "low"


def _extract_value(category: FeatureFlagRolloutCategory, text: str) -> str:
    if category == "staged_rollout_percentages":
        percentages = _dedupe(_clean_text(match.group(0)) for match in re.finditer(r"\b\d+\s?%|\b\d+\s*percent", text, re.I))
        if percentages:
            return ", ".join(percentages[:3])
    values = _dedupe(_clean_text(match.group(0)) for match in _VALUE_PATTERNS[category].finditer(text))
    return ", ".join(values[:3])


def _summary(
    requirements: tuple[SourceFeatureFlagRolloutRequirement, ...],
    gaps: tuple[SourceFeatureFlagRolloutEvidenceGap, ...],
) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [requirement.category for requirement in requirements],
        "evidence_gap_count": len(gaps),
        "evidence_gaps": [gap.category for gap in gaps],
        "status": _status(requirements, gaps),
    }


def _status(
    requirements: tuple[SourceFeatureFlagRolloutRequirement, ...],
    gaps: tuple[SourceFeatureFlagRolloutEvidenceGap, ...],
) -> str:
    if not requirements:
        return "no_feature_flag_rollout_requirements_found"
    if gaps:
        return "needs_feature_flag_rollout_detail"
    return "ready_for_planning"


def _object_payload(value: object) -> dict[str, Any]:
    fields = ("id", "source_brief_id", "source_id", *_SCANNED_FIELDS)
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: FeatureFlagRolloutCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[FeatureFlagRolloutCategory, tuple[str, ...]] = {
        "flag_ownership": ("owner", "approval", "accountable"),
        "audience_targeting": ("target", "audience", "cohort", "segment"),
        "staged_rollout_percentages": ("percentage", "stage", "rollout", "ramp"),
        "kill_switch_behavior": ("kill", "disable", "emergency", "switch"),
        "experiment_variant_tracking": ("experiment", "variant", "exposure", "analytics"),
        "observability": ("observability", "monitor", "metric", "alert", "dashboard"),
        "rollback_criteria": ("rollback", "revert", "threshold", "criteria"),
        "cleanup_deprecation": ("cleanup", "deprecat", "stale", "remove"),
    }
    return 0 if any(marker in field_words for marker in markers[category]) else 1


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


def _merge_values(values: Iterable[str]) -> str:
    parts: list[str] = []
    for value in values:
        parts.extend(item.strip() for item in value.split(",") if item.strip())
    return ", ".join(_dedupe(parts)[:3])


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
    "FeatureFlagRolloutCategory",
    "FeatureFlagRolloutConfidence",
    "FeatureFlagRolloutGapCategory",
    "SourceFeatureFlagRolloutRequirement",
    "SourceFeatureFlagRolloutEvidenceGap",
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
