"""Extract analytics requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


AnalyticsRequirementType = Literal[
    "event_tracking",
    "funnel_analysis",
    "cohort_analysis",
    "ab_testing",
    "custom_dimensions",
    "pii_handling",
    "data_governance",
    "retention_policies",
    "cross_device_tracking",
    "consent_management",
]

_TYPE_ORDER: tuple[AnalyticsRequirementType, ...] = (
    "event_tracking",
    "funnel_analysis",
    "cohort_analysis",
    "ab_testing",
    "custom_dimensions",
    "pii_handling",
    "data_governance",
    "retention_policies",
    "cross_device_tracking",
    "consent_management",
)

_SPACE_RE = re.compile(r"\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")

_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "integration_points",
    "integrations",
    "constraints",
    "metadata",
)

_TYPE_PATTERNS: dict[AnalyticsRequirementType, re.Pattern[str]] = {
    "event_tracking": re.compile(
        r"\b(?:event[_\s-]*track(?:ing)?|track[_\s-]*event[s]?|analytics[_\s-]*event[s]?|"
        r"log[_\s-]*event[s]?|capture[_\s-]*event[s]?|instrumentation[_\s-]*event[s]?|"
        r"event[_\s-]*(?:name|taxonomy|schema|payload)|user[_\s-]*action[s]?|"
        r"page[_\s-]*view[s]?|click[_\s-]*(?:event|tracking)|custom[_\s-]*event[s]?)\b",
        re.I,
    ),
    "funnel_analysis": re.compile(
        r"\b(?:funnel[_\s-]*analysis|conversion[_\s-]*funnel[s]?|user[_\s-]*funnel[s]?|"
        r"funnel[_\s-]*(?:step[s]?|stage[s]?|metric[s]?)|drop[_\s-]*off[_\s-]*analysis|"
        r"funnel[_\s-]*optimization|conversion[_\s-]*(?:rate|metric[s]?)|"
        r"funnel[_\s-]*tracking|multi[_\s-]*step[_\s-]*(?:flow|funnel[s]?))\b",
        re.I,
    ),
    "cohort_analysis": re.compile(
        r"\b(?:cohort[_\s-]*analysis|user[s]?[_\s-]*cohort[s]?|cohort[_\s-]*(?:segment|group)[s]?|"
        r"cohort[_\s-]*retention|cohort[_\s-]*behavior|cohort[_\s-]*comparison|"
        r"cohort[_\s-]*metric[s]?|cohort[_\s-]*definition|"
        r"segment[_\s-]*(?:user[s]?[_\s-]*)?(?:into[_\s-]*)?cohort[s]?)\b",
        re.I,
    ),
    "ab_testing": re.compile(
        r"\b(?:a/?b[_\s-]*test(?:ing)?|experiment[_\s-]*(?:tracking|analytics)|"
        r"feature[_\s-]*flag[_\s-]*analytics|split[_\s-]*test(?:ing)?|"
        r"variant[_\s-]*(?:tracking|analytics)|control[_\s-]*(?:group|variant)|"
        r"treatment[_\s-]*(?:group|variant)|experiment[_\s-]*(?:assignment|exposure))\b",
        re.I,
    ),
    "custom_dimensions": re.compile(
        r"\b(?:custom[_\s-]*dimension[s]?|custom[_\s-]*(?:propert(?:y|ies)|attribute[s]?)|"
        r"user[_\s-]*propert(?:y|ies)|event[_\s-]*propert(?:y|ies)|"
        r"custom[_\s-]*metadata|dimensional[_\s-]*data|context[_\s-]*data|"
        r"additional[_\s-]*(?:dimension[s]?|attribute[s]?))\b",
        re.I,
    ),
    "pii_handling": re.compile(
        r"\b(?:pii|personally[_\s-]*identifiable[_\s-]*information|"
        r"sensitive[_\s-]*(?:data|information)|personal[_\s-]*data|"
        r"pii[_\s-]*(?:scrubbing|redaction|filtering|masking|removal)|"
        r"(?:mask|redact|anonymize|pseudonymize)[_\s-]*(?:pii|data)|"
        r"data[_\s-]*privacy|privacy[_\s-]*(?:protection|safeguards))\b",
        re.I,
    ),
    "data_governance": re.compile(
        r"\b(?:data[_\s-]*governance|data[_\s-]*(?:quality|compliance|lineage)|"
        r"governance[_\s-]*polic(?:y|ies)|data[_\s-]*catalog|metadata[_\s-]*management|"
        r"data[_\s-]*classification|data[_\s-]*stewardship|compliance[_\s-]*(?:requirement[s]?|rule[s]?))\b",
        re.I,
    ),
    "retention_policies": re.compile(
        r"\b(?:retention[_\s-]*polic(?:y|ies)|data[_\s-]*retention|"
        r"retention[_\s-]*(?:period|window|schedule)|data[_\s-]*expiration|"
        r"expir(?:e|ation)[_\s-]*data|delete[_\s-]*(?:after|old[_\s-]*data)|"
        r"ttl|time[_\s-]*to[_\s-]*live|data[_\s-]*lifecycle)\b",
        re.I,
    ),
    "cross_device_tracking": re.compile(
        r"\b(?:cross[_\s-]*device[_\s-]*track(?:ing)?|multi[_\s-]*device[_\s-]*track(?:ing)?|"
        r"device[_\s-]*(?:stitching|resolution|unification|reconciliation)|"
        r"cross[_\s-]*platform[_\s-]*(?:track(?:ing)?|identity)|"
        r"unified[_\s-]*(?:user[_\s-]*)?profile|device[_\s-]*(?:graph|fingerprint))\b",
        re.I,
    ),
    "consent_management": re.compile(
        r"\b(?:consent[_\s-]*management|user[_\s-]*consent|consent[_\s-]*(?:banner|modal|form)|"
        r"(?:gdpr|ccpa)[_\s-]*consent|cookie[_\s-]*consent|opt[_\s-]*(?:in|out)|"
        r"consent[_\s-]*(?:tracking|validation|enforcement)|privacy[_\s-]*preferences|"
        r"consent[_\s-]*(?:withdrawal|revocation))\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[AnalyticsRequirementType, tuple[str, ...]] = {
    "event_tracking": (
        "What is the event naming convention and taxonomy?",
        "Which user actions should be tracked as events?",
    ),
    "funnel_analysis": (
        "What are the key conversion funnels to track?",
        "How should funnel drop-off points be identified?",
    ),
    "cohort_analysis": (
        "How should user cohorts be defined and segmented?",
        "What retention metrics should be tracked per cohort?",
    ),
    "ab_testing": (
        "How should experiment assignments be tracked?",
        "What conversion metrics should be measured for experiments?",
    ),
    "custom_dimensions": (
        "Which custom dimensions and properties should be captured?",
        "How should custom dimensions be validated and structured?",
    ),
    "pii_handling": (
        "Which data fields contain PII that must be scrubbed?",
        "How should PII be redacted or anonymized in analytics?",
    ),
    "data_governance": (
        "What compliance requirements must analytics data meet?",
        "How should data lineage and quality be maintained?",
    ),
    "retention_policies": (
        "What is the data retention period for analytics events?",
        "How should expired data be purged or archived?",
    ),
    "cross_device_tracking": (
        "How should users be identified across devices?",
        "What device stitching strategy should be used?",
    ),
    "consent_management": (
        "How should user consent be captured and stored?",
        "What happens to analytics data when consent is withdrawn?",
    ),
}


@dataclass(frozen=True, slots=True)
class AnalyticsRequirement:
    """One source-backed analytics requirement."""

    requirement_type: AnalyticsRequirementType
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class AnalyticsRequirementsReport:
    """Source-level analytics requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[AnalyticsRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[AnalyticsRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirements": [req.to_dict() for req in self.requirements],
            "summary": dict(self.summary),
            "records": [rec.to_dict() for rec in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return analytics requirement records as plain dictionaries."""
        return [req.to_dict() for req in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Analytics Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Source count: {self.summary.get('source_count', 1)}",
            f"- Event taxonomy coverage: {self.summary.get('event_coverage', 0)}%",
            f"- Privacy compliance coverage: {self.summary.get('privacy_coverage', 0)}%",
            f"- Implementation clarity: {self.summary.get('implementation_clarity', 0)}%",
            "- Requirement type counts: "
            + ", ".join(f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No analytics requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Source Field Paths | Evidence | Follow-up Questions |",
                "| --- | --- | --- | --- |",
            ]
        )
        for req in self.requirements:
            lines.append(
                "| "
                f"{req.requirement_type} | "
                f"{_markdown_cell('; '.join(req.source_field_paths))} | "
                f"{_markdown_cell('; '.join(req.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(req.follow_up_questions) or 'none')} |"
            )
        return "\n".join(lines)


def extract_analytics_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[AnalyticsRequirement, ...]:
    """Extract analytics requirement records from brief-shaped input."""
    return build_analytics_requirements_report(source).requirements


def build_analytics_requirements_report(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> AnalyticsRequirementsReport:
    """Extract analytics requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped = _group_requirements(payload)
    requirements = _merge_requirements(grouped)
    return AnalyticsRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_compute_summary(requirements),
    )


# Compatibility aliases
generate_analytics_requirements = extract_analytics_requirements
analyze_analytics_requirements = extract_analytics_requirements
derive_analytics_requirements = extract_analytics_requirements
summarize_analytics_requirements = lambda source: build_analytics_requirements_report(source).summary


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: AnalyticsRequirementType
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_brief_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            return _source_brief_id(value), dict(value)
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
            return _source_brief_id(payload), payload
        except (TypeError, ValueError, ValidationError):
            return _source_brief_id(source), dict(source)
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_brief_id(payload), payload
    return None, {}


def _source_brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _object_payload(obj: object) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for attr in dir(obj):
        if attr.startswith("_"):
            continue
        try:
            value = getattr(obj, attr)
            if not callable(value):
                payload[attr] = value
        except AttributeError:
            pass
    return payload


def _group_requirements(payload: Mapping[str, Any]) -> dict[AnalyticsRequirementType, list[_Candidate]]:
    grouped: dict[AnalyticsRequirementType, list[_Candidate]] = {}
    for source_field, text in _candidate_texts(payload):
        for segment in _segments(text):
            for req_type in _matched_requirement_types(segment):
                candidate = _Candidate(
                    requirement_type=req_type,
                    evidence=_evidence_snippet(source_field, segment),
                    source_field_path=source_field,
                    matched_terms=_matched_terms(req_type, segment),
                )
                grouped.setdefault(req_type, []).append(candidate)
    return grouped


def _merge_requirements(
    grouped: dict[AnalyticsRequirementType, list[_Candidate]],
) -> tuple[AnalyticsRequirement, ...]:
    requirements: list[AnalyticsRequirement] = []
    for req_type in _TYPE_ORDER:
        candidates = grouped.get(req_type, [])
        if not candidates:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in candidates))[:5]
        source_field_paths = tuple(sorted(_dedupe(item.source_field_path for item in candidates), key=str.casefold))
        matched_terms = tuple(
            sorted(_dedupe(term for item in candidates for term in item.matched_terms), key=str.casefold)
        )
        questions = tuple(_BASE_QUESTIONS[req_type])
        requirements.append(
            AnalyticsRequirement(
                requirement_type=req_type,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                follow_up_questions=questions,
            )
        )
    return tuple(requirements)


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in _SCANNED_FIELDS:
        value = payload.get(field_name)
        if field_name == "metadata":
            texts.extend(_nested_texts(value, field_name))
            continue
        for index, text in enumerate(_strings(value)):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))

    if isinstance(payload.get("source_payload"), Mapping):
        for field_name in _SCANNED_FIELDS:
            if field_name in payload["source_payload"]:
                texts.extend(_nested_texts(payload["source_payload"][field_name], f"source_payload.{field_name}"))
    return texts


def _nested_texts(value: Any, prefix: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
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
    text = _optional_text(value)
    return [text] if text else []


def _segments(text: str) -> list[str]:
    segments: list[str] = []
    for raw_segment in _SENTENCE_SPLIT_RE.split(text):
        segment = _clean_text(raw_segment)
        if segment:
            segments.append(segment)
    return segments


def _matched_requirement_types(text: str) -> tuple[AnalyticsRequirementType, ...]:
    return tuple(req_type for req_type in _TYPE_ORDER if _TYPE_PATTERNS[req_type].search(text))


def _matched_terms(req_type: AnalyticsRequirementType, text: str) -> tuple[str, ...]:
    return tuple(_dedupe(_clean_text(match.group(0)) for match in _TYPE_PATTERNS[req_type].finditer(text)))


def _evidence_snippet(source_field: str, text: str, max_chars: int = 150) -> str:
    _ = source_field  # Reserved for future use in evidence formatting
    clean = _clean_text(text)
    if len(clean) <= max_chars:
        return clean
    return clean[:max_chars].rsplit(" ", 1)[0] + "..."


def _clean_text(text: str) -> str:
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        text = _clean_text(value)
        return text if text else None
    return _clean_text(str(value)) if value else None


def _dedupe(items: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item.lower() not in seen:
            seen.add(item.lower())
            result.append(item)
    return tuple(result)


def _dedupe_evidence(items: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        clean = _clean_text(item)
        normalized = clean.lower()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(clean)
    return tuple(result)


def _compute_summary(requirements: tuple[AnalyticsRequirement, ...]) -> dict[str, Any]:
    type_counts = {req_type: 0 for req_type in _TYPE_ORDER}
    for req in requirements:
        type_counts[req.requirement_type] += 1

    # Event taxonomy coverage
    event_types = {"event_tracking", "funnel_analysis", "cohort_analysis", "ab_testing", "custom_dimensions"}
    event_coverage = sum(1 for req_type in event_types if type_counts[req_type] > 0)
    event_coverage_pct = int((event_coverage / len(event_types)) * 100)

    # Privacy compliance coverage
    privacy_types = {"pii_handling", "data_governance", "retention_policies", "consent_management"}
    privacy_coverage = sum(1 for req_type in privacy_types if type_counts[req_type] > 0)
    privacy_coverage_pct = int((privacy_coverage / len(privacy_types)) * 100)

    # Implementation clarity (overall coverage)
    total_types = len(_TYPE_ORDER)
    implementation_clarity = sum(1 for count in type_counts.values() if count > 0)
    implementation_clarity_pct = int((implementation_clarity / total_types) * 100)

    return {
        "requirement_count": len(requirements),
        "source_count": 1,
        "type_counts": type_counts,
        "event_coverage": event_coverage_pct,
        "privacy_coverage": privacy_coverage_pct,
        "implementation_clarity": implementation_clarity_pct,
    }


def _markdown_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "AnalyticsRequirement",
    "AnalyticsRequirementsReport",
    "AnalyticsRequirementType",
    "extract_analytics_requirements",
    "build_analytics_requirements_report",
    "generate_analytics_requirements",
    "analyze_analytics_requirements",
    "derive_analytics_requirements",
    "summarize_analytics_requirements",
]
