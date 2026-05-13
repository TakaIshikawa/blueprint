"""Extract source-level cache policy requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal

from blueprint._source_requirement_utils import dedupe, evidence_snippet, markdown_cell, segments, source_payloads

CachePolicyRequirementType = Literal["cacheable_resources", "ttl", "cache_scope", "invalidation_trigger", "staleness_tolerance", "privacy_constraint", "ownership"]
CachePolicyConfidence = Literal["high", "medium", "low"]
CachePolicyReadiness = Literal["ready", "needs_detail"]
_TYPE_ORDER: tuple[CachePolicyRequirementType, ...] = ("cacheable_resources", "ttl", "cache_scope", "invalidation_trigger", "staleness_tolerance", "privacy_constraint", "ownership")
_CONFIDENCE_ORDER = {"high": 0, "medium": 1, "low": 2}
_READINESS_ORDER = ("ready", "needs_detail")
_LABELS = {"cacheable_resources": "Cacheable resources", "ttl": "TTL", "cache_scope": "Cache scope", "invalidation_trigger": "Invalidation trigger", "staleness_tolerance": "Staleness tolerance", "privacy_constraint": "Privacy constraint", "ownership": "Ownership"}
_MISSING = {
    "cacheable_resources": ("resource", "cache_layer", "vary_key"),
    "ttl": ("duration", "layer", "override"),
    "cache_scope": ("scope", "keying", "sharing_boundary"),
    "invalidation_trigger": ("trigger", "target", "propagation"),
    "staleness_tolerance": ("max_staleness", "fallback_behavior", "user_impact"),
    "privacy_constraint": ("data_class", "restriction", "enforcement"),
    "ownership": ("owner", "review_cadence", "escalation"),
}
_PATTERNS: dict[CachePolicyRequirementType, re.Pattern[str]] = {
    "cacheable_resources": re.compile(r"\b(?:cacheable resources?|cache (?:api responses?|assets?|pages?|queries?|objects?|resources?)|cached resources?)\b", re.I),
    "ttl": re.compile(r"\b(?:ttl|time[- ]?to[- ]?live|cache lifetime|max[- ]age|s-maxage|expires|expiry|\d+\s*(?:seconds?|minutes?|hours?|days?))\b", re.I),
    "cache_scope": re.compile(r"\b(?:cache scope|per[- ]?tenant|per[- ]?user|shared cache|private cache|browser cache|cdn|edge cache|service worker|vary)\b", re.I),
    "invalidation_trigger": re.compile(r"\b(?:invalidate|invalidation|purge|bust cache|refresh on|evict|revalidate|webhook trigger|content update)\b", re.I),
    "staleness_tolerance": re.compile(r"\b(?:stale[- ]?while[- ]?revalidate|staleness|stale for|serve stale|max stale|eventual consistency|fallback)\b", re.I),
    "privacy_constraint": re.compile(r"\b(?:privacy|pii|personal data|private|sensitive|auth(?:enticated)?|authorization|cookie|no-store|user-specific)\b", re.I),
    "ownership": re.compile(r"\b(?:cache owner|owner|owned by|platform|frontend|api team|sre|review cadence|escalation)\b", re.I),
}
_DETAILS = {
    "resource": re.compile(r"\b(?:api responses?|assets?|pages?|queries?|objects?|resources?|profile|catalog|dashboard|report)\b", re.I),
    "cache_layer": re.compile(r"\b(?:cdn|edge|browser|redis|memcached|service worker|gateway|api cache)\b", re.I),
    "vary_key": re.compile(r"\b(?:vary|cache key|locale|tenant|user|authorization|accept-language|query params?)\b", re.I),
    "duration": re.compile(r"\b(?:\d+\s*(?:seconds?|minutes?|hours?|days?)|ttl|max[- ]age|s-maxage|lifetime)\b", re.I),
    "layer": re.compile(r"\b(?:cdn|edge|browser|redis|memcached|service worker|gateway|api)\b", re.I),
    "override": re.compile(r"\b(?:override|default|exception|per route|per resource|configurable)\b", re.I),
    "scope": re.compile(r"\b(?:per[- ]?tenant|per[- ]?user|shared|private|public|global|environment)\b", re.I),
    "keying": re.compile(r"\b(?:cache key|keyed by|vary|tenant id|user id|locale|query params?)\b", re.I),
    "sharing_boundary": re.compile(r"\b(?:tenant|user|account|workspace|public|private|authorization boundary)\b", re.I),
    "trigger": re.compile(r"\b(?:update|publish|deploy|webhook|mutation|write|purge|manual|event)\b", re.I),
    "target": re.compile(r"\b(?:url|route|resource|tag|surrogate key|cdn|browser|redis|edge)\b", re.I),
    "propagation": re.compile(r"\b(?:within \d+|seconds?|minutes?|propagat|immediate|eventual)\b", re.I),
    "max_staleness": re.compile(r"\b(?:\d+\s*(?:seconds?|minutes?|hours?)|max stale|stale for|staleness)\b", re.I),
    "fallback_behavior": re.compile(r"\b(?:fallback|serve stale|revalidate|bypass|error|refresh)\b", re.I),
    "user_impact": re.compile(r"\b(?:user impact|customer impact|visible|acceptable|tolerance|banner)\b", re.I),
    "data_class": re.compile(r"\b(?:pii|personal data|sensitive|private|auth|user-specific|profile)\b", re.I),
    "restriction": re.compile(r"\b(?:no-store|private|do not cache|bypass|must not cache|restricted)\b", re.I),
    "enforcement": re.compile(r"\b(?:header|cache-control|authorization|cookie|test|enforce|guard)\b", re.I),
    "owner": re.compile(r"\b(?:owner|owned by|platform|frontend|api team|sre)\b", re.I),
    "review_cadence": re.compile(r"\b(?:review cadence|monthly|quarterly|before launch|each release|review)\b", re.I),
    "escalation": re.compile(r"\b(?:escalation|pager|slack|incident|support|on-call)\b", re.I),
}
_REQ_RE = re.compile(r"\b(?:must|shall|required|requires?|need(?:ed|s)?|should|define|support|ensure|cache|invalidate|purge|revalidate|own|review)\b", re.I)
_NEGATED_RE = re.compile(r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}\b(?:cache|caching|ttl|cdn|browser cache|invalidation|stale)\b.{0,140}\b(?:required|needed|planned|in scope|work|support)\b|\b(?:cache|caching|ttl|cdn|browser cache|invalidation|stale)\b.{0,140}\b(?:out of scope|not required|not needed|no work|no changes?)\b", re.I)
_SCANNED_FIELDS = ("title", "summary", "body", "description", "requirements", "constraints", "scope", "acceptance_criteria", "definition_of_done", "performance", "api", "frontend", "infrastructure", "security", "privacy", "metadata", "source_payload")


@dataclass(frozen=True, slots=True)
class SourceCachePolicyRequirement:
    source_brief_id: str | None
    requirement_type: CachePolicyRequirementType
    requirement_text: str
    label: str
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: CachePolicyConfidence = "medium"
    readiness: CachePolicyReadiness = "needs_detail"

    @property
    def category(self) -> CachePolicyRequirementType:
        return self.requirement_type

    @property
    def requirement_category(self) -> CachePolicyRequirementType:
        return self.requirement_type

    @property
    def missing_detail_guidance(self) -> str | None:
        return "; ".join(self.missing_details) if self.missing_details else None

    def to_dict(self) -> dict[str, Any]:
        return {"source_brief_id": self.source_brief_id, "requirement_type": self.requirement_type, "requirement_category": self.requirement_category, "requirement_text": self.requirement_text, "label": self.label, "source_field": self.source_field, "evidence": list(self.evidence), "missing_details": list(self.missing_details), "missing_detail_guidance": self.missing_detail_guidance, "confidence": self.confidence, "readiness": self.readiness}


@dataclass(frozen=True, slots=True)
class SourceCachePolicyRequirementsReport:
    source_id: str | None = None
    requirements: tuple[SourceCachePolicyRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceCachePolicyRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceCachePolicyRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {"source_id": self.source_id, "requirements": [item.to_dict() for item in self.requirements], "summary": dict(self.summary), "records": [item.to_dict() for item in self.records], "findings": [item.to_dict() for item in self.findings]}

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        lines = [f"# Source Cache Policy Requirements{': ' + self.source_id if self.source_id else ''}", "", f"Requirements found: {self.summary.get('requirement_count', 0)}"]
        if not self.requirements:
            return "\n".join([*lines, "", "No cache policy requirements were inferred."])
        lines.extend(["", "| Type | Requirement | Missing Details | Readiness | Evidence |", "| --- | --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(f"| {markdown_cell(item.requirement_type)} | {markdown_cell(item.requirement_text)} | {markdown_cell('; '.join(item.missing_details))} | {item.readiness} | {markdown_cell('; '.join(item.evidence))} |")
        return "\n".join(lines)


def build_source_cache_policy_requirements(source: Any) -> SourceCachePolicyRequirementsReport:
    payloads = source_payloads(source)
    records = tuple(_merge(_candidates(payloads)))
    ids = dedupe(source_id for source_id, _ in payloads if source_id)
    return SourceCachePolicyRequirementsReport(ids[0] if len(ids) == 1 else None, records, _summary(records, len(payloads)))


extract_source_cache_policy_requirements = build_source_cache_policy_requirements
generate_source_cache_policy_requirements = build_source_cache_policy_requirements
derive_source_cache_policy_requirements = build_source_cache_policy_requirements


def summarize_source_cache_policy_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceCachePolicyRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_cache_policy_requirements(source_or_report).summary


def source_cache_policy_requirements_to_dict(report: SourceCachePolicyRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_cache_policy_requirements_to_dict.__test__ = False


def source_cache_policy_requirements_to_dicts(items: SourceCachePolicyRequirementsReport | Iterable[SourceCachePolicyRequirement]) -> list[dict[str, Any]]:
    if isinstance(items, SourceCachePolicyRequirementsReport):
        return items.to_dicts()
    return [item.to_dict() for item in items]


source_cache_policy_requirements_to_dicts.__test__ = False


def source_cache_policy_requirements_to_markdown(report: SourceCachePolicyRequirementsReport) -> str:
    return report.to_markdown()


source_cache_policy_requirements_to_markdown.__test__ = False


def _candidates(payloads: Iterable[tuple[str | None, dict[str, Any]]]) -> list[SourceCachePolicyRequirement]:
    out: list[SourceCachePolicyRequirement] = []
    for source_id, payload in payloads:
        for field_name, text in segments(payload, _SCANNED_FIELDS):
            searchable = f"{field_name} {text}"
            if _NEGATED_RE.search(searchable) or not _REQ_RE.search(text):
                continue
            for requirement_type, pattern in _PATTERNS.items():
                if pattern.search(searchable):
                    missing = tuple(detail for detail in _MISSING[requirement_type] if not _DETAILS[detail].search(searchable))
                    readiness: CachePolicyReadiness = "ready" if not missing else "needs_detail"
                    out.append(SourceCachePolicyRequirement(source_id, requirement_type, text, _LABELS[requirement_type], field_name, (evidence_snippet(field_name, text),), missing, "high", readiness))
    return out


def _merge(candidates: Iterable[SourceCachePolicyRequirement]) -> list[SourceCachePolicyRequirement]:
    grouped: dict[CachePolicyRequirementType, list[SourceCachePolicyRequirement]] = {}
    for item in candidates:
        grouped.setdefault(item.requirement_type, []).append(item)
    records: list[SourceCachePolicyRequirement] = []
    for requirement_type in _TYPE_ORDER:
        items = grouped.get(requirement_type, [])
        if not items:
            continue
        best = min(items, key=lambda item: (len(item.missing_details), item.source_field or ""))
        missing = tuple(detail for detail in _MISSING[requirement_type] if all(detail in item.missing_details for item in items))
        readiness: CachePolicyReadiness = "ready" if not missing else "needs_detail"
        records.append(SourceCachePolicyRequirement(best.source_brief_id, requirement_type, best.requirement_text, best.label, best.source_field, tuple(dedupe(ev for item in items for ev in item.evidence))[:5], missing, "high", readiness))
    return records


def _summary(records: tuple[SourceCachePolicyRequirement, ...], source_count: int) -> dict[str, Any]:
    counts = {item: sum(1 for record in records if record.requirement_type == item) for item in _TYPE_ORDER}
    return {"source_count": source_count, "requirement_count": len(records), "requirement_type_counts": counts, "category_counts": counts, "readiness_counts": {item: sum(1 for record in records if record.readiness == item) for item in _READINESS_ORDER}, "confidence_counts": {level: sum(1 for record in records if record.confidence == level) for level in _CONFIDENCE_ORDER}, "missing_detail_count": sum(len(record.missing_details) for record in records), "requirement_types": [item for item in _TYPE_ORDER if counts[item]]}


__all__ = [name for name in globals() if name.startswith(("SourceCachePolicy", "build_source_cache", "extract_source_cache", "generate_source_cache", "derive_source_cache", "summarize_source_cache", "source_cache"))] + ["CachePolicyRequirementType", "CachePolicyConfidence", "CachePolicyReadiness"]
