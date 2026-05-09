"""Extract reporting and dashboard requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


ReportingRequirementType = Literal[
    "report_types",
    "data_sources",
    "aggregation_levels",
    "visualization_types",
    "refresh_frequency",
    "data_freshness",
    "query_performance",
    "drill_down_capabilities",
    "export_formats",
    "access_controls",
]

_TYPE_ORDER: tuple[ReportingRequirementType, ...] = (
    "report_types",
    "data_sources",
    "aggregation_levels",
    "visualization_types",
    "refresh_frequency",
    "data_freshness",
    "query_performance",
    "drill_down_capabilities",
    "export_formats",
    "access_controls",
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

_TYPE_PATTERNS: dict[ReportingRequirementType, re.Pattern[str]] = {
    "report_types": re.compile(
        r"\b(?:(?:scheduled|ad[_\s-]*hoc|recurring|on[_\s-]*demand)[_\s-]+report[s]?|"
        r"dashboard[s]?|report[_\s-]+(?:type[s]?|format[s]?|schedule[s]?)|"
        r"(?:daily|weekly|monthly|quarterly)[_\s-]+report[s]?|"
        r"(?:executive|operational|analytical)[_\s-]+(?:report[s]?|dashboard[s]?)|"
        r"real[_\s-]*time[_\s-]+dashboard[s]?|interactive[_\s-]+report[s]?)\b",
        re.I,
    ),
    "data_sources": re.compile(
        r"\b(?:data[_\s-]+(?:source[s]?|from[_\s-]+(?:postgres|mysql|mongodb|oracle|api))|"
        r"(?:database|db)[_\s-]+(?:query|connection|source)|"
        r"(?:using[_\s-]+)?(?:data[_\s-]+from[_\s-]+)?(?:postgres(?:ql)?|mysql|mongodb|oracle)|"
        r"api[_\s-]+(?:data[_\s-]+)?(?:source[s]?|endpoint[s]?)|(?:and[_\s-]+)?apis?|"
        r"aggregate[_\s-]+data[_\s-]+from|data[_\s-]+aggregation[s]?|"
        r"(?:join|union|combine)[_\s-]+(?:data|table[s]?|source[s]?)|"
        r"data[_\s-]+(?:feed[s]?|pipeline[s]?|integration[s]?)|"
        r"(?:sql|nosql)[_\s-]+(?:query|source|database)|rest[_\s-]+api|"
        r"data[_\s-]+(?:warehouse|lake|mart)|execute[_\s-]+sql)\b",
        re.I,
    ),
    "aggregation_levels": re.compile(
        r"\b(?:aggregation[_\s-]+level[s]?|(?:daily|weekly|monthly|quarterly|yearly)[_\s-]+aggregation[s]?|"
        r"(?:user|customer|account|org(?:anization)?)[_\s-]*(?:level|level[_\s-]+)?(?:data|metric[s]?|aggregation|granularity)|"
        r"group[_\s-]+by[_\s-]+(?:date|time|user|period)|roll[_\s-]*up[_\s-]+(?:data|metric[s]?)|"
        r"(?:summarize|aggregate)[_\s-]+(?:data[_\s-]+)?(?:by|at)[_\s-]+(?:day|daily|week|month|monthly|user|level)|"
        r"granularity[_\s-]+(?:level|setting[s]?)|(?:at|and)[_\s-]+(?:user|customer)[_\s-]*level)\b",
        re.I,
    ),
    "visualization_types": re.compile(
        r"\b(?:visualization[_\s-]+type[s]?|"
        r"(?:bar|line|pie|scatter|area|bubble)[_\s-]+(?:chart[s]?|graph[s]?|plot[s]?)|"
        r"(?:heat|tree)map[s]?|data[_\s-]+(?:table[s]?|grid[s]?)|"
        r"(?:time[_\s-]*series|histogram|funnel|gauge|sparkline)[s]?|"
        r"visual(?:ization)?[_\s-]+(?:component[s]?|widget[s]?)|"
        r"(?:display|show|present|use|create)[_\s-]+(?:data[_\s-]+in[_\s-]+|interactive[_\s-]+)?(?:bar[_\s-]+|scatter[_\s-]+)?(?:chart[s]?|plot[s]?|graph[s]?|table[s]?))\b",
        re.I,
    ),
    "refresh_frequency": re.compile(
        r"\b(?:refresh[_\s-]+(?:frequency|data|hourly|cache)|(?:real[_\s-]*time|live|streaming)[_\s-]+(?:data|update[s]?|dashboard|refresh)|"
        r"(?:hourly|daily|weekly)[_\s-]+(?:refresh|update[s]?)|"
        r"auto[_\s-]*refresh|refresh[_\s-]+(?:rate|interval|schedule|on)|"
        r"update[_\s-]+(?:frequency|interval|schedule)|cache[_\s-]+(?:refresh|invalidation)|"
        r"invalidate[_\s-]+cache|(?:continuous|incremental)[_\s-]+update[s]?)\b",
        re.I,
    ),
    "data_freshness": re.compile(
        r"\b(?:data[_\s-]+freshness|(?:data|report)[_\s-]+latency|"
        r"(?:stale|outdated)[_\s-]+data|freshness[_\s-]+(?:requirement[s]?|threshold)|"
        r"data[_\s-]+(?:staleness|recency|timeliness)|time[_\s-]+to[_\s-]+(?:data|availability)|"
        r"(?:near[_\s-]*)?real[_\s-]*time[_\s-]+data|data[_\s-]+lag|"
        r"(?:synchronization|sync)[_\s-]+(?:delay|latency))\b",
        re.I,
    ),
    "query_performance": re.compile(
        r"\b(?:query[_\s-]+performance|(?:optimize|optimization)[_\s-]+(?:query|queries)|"
        r"query[_\s-]+(?:speed|latency|execution[_\s-]+time)|"
        r"(?:fast|slow)[_\s-]+(?:query|queries)|performance[_\s-]+tuning|"
        r"(?:index|indexing)[_\s-]+(?:strategy|optimization)|"
        r"query[_\s-]+(?:cache|caching)|materialized[_\s-]+view[s]?|"
        r"query[_\s-]+(?:timeout|limit[s]?))\b",
        re.I,
    ),
    "drill_down_capabilities": re.compile(
        r"\b(?:drill[_\s-]*down[_\s-]+capabilit(?:y|ies)|drill[_\s-]*(?:down|through|up)|"
        r"interactive[_\s-]+(?:report[s]?|dashboard[s]?|exploration)|"
        r"click[_\s-]*through[_\s-]+(?:navigation|detail[s]?)|"
        r"(?:detail|details)[_\s-]+(?:view|level|navigation)|"
        r"hierarchical[_\s-]+(?:navigation|drill[_\s-]*down)|"
        r"expand[_\s-]+(?:detail[s]?|row[s]?)|filter[_\s-]+(?:down|interaction))\b",
        re.I,
    ),
    "export_formats": re.compile(
        r"\b(?:export[_\s-]+(?:format[s]?|reports?[_\s-]+to[_\s-]+(?:pdf|csv|excel|xlsx|json|xml))|"
        r"(?:download|export)[_\s-]+(?:as|to)[_\s-]+(?:pdf|csv|excel|xlsx|json|xml)|"
        r"(?:pdf|csv|excel|xlsx|json|xml)[_\s-]+(?:export|download|format[s]?|file)|"
        r"(?:download|export)[_\s-]+(?:data|report[s]?)[_\s-]+(?:as|to)[_\s-]+(?:csv|pdf|excel)|"
        r"file[_\s-]+(?:export|download)|report[_\s-]+(?:export|download)|"
        r"data[_\s-]+export[_\s-]+(?:option[s]?|format[s]?))\b",
        re.I,
    ),
    "access_controls": re.compile(
        r"\b(?:access[_\s-]+control[s]?|(?:user|role)[_\s-]+permission[s]?|"
        r"(?:row[_\s-]*level|field[_\s-]*level|data[_\s-]*level)[_\s-]+security|"
        r"rbac|role[_\s-]*based[_\s-]+access[_\s-]+control|"
        r"authorization[_\s-]+(?:rule[s]?|polic(?:y|ies))|"
        r"permission[_\s-]+(?:model|system|check[s]?)|"
        r"restrict[_\s-]+(?:access|view|data)|(?:view|data)[_\s-]+restriction[s]?|"
        r"security[_\s-]+(?:filter[s]?|rule[s]?|polic(?:y|ies)))\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[ReportingRequirementType, tuple[str, ...]] = {
    "report_types": (
        "What types of reports are needed (scheduled, ad-hoc, dashboard, etc.)?",
        "What is the scheduling frequency for each report type?",
    ),
    "data_sources": (
        "Which databases, APIs, or data sources should be integrated?",
        "How should data be aggregated or joined across sources?",
    ),
    "aggregation_levels": (
        "What aggregation levels are required (daily, monthly, user-level, etc.)?",
        "How should data be grouped and summarized?",
    ),
    "visualization_types": (
        "Which visualization types are needed (charts, tables, graphs)?",
        "What metrics should each visualization display?",
    ),
    "refresh_frequency": (
        "How often should data be refreshed (real-time, hourly, daily)?",
        "What are the auto-refresh and caching requirements?",
    ),
    "data_freshness": (
        "What are the data freshness requirements and acceptable latency?",
        "How should stale or outdated data be handled?",
    ),
    "query_performance": (
        "What are the target query execution times?",
        "What optimization strategies are needed (indexes, caching, materialized views)?",
    ),
    "drill_down_capabilities": (
        "What drill-down and interactive features are required?",
        "How should users navigate between summary and detail views?",
    ),
    "export_formats": (
        "Which export formats should be supported (PDF, CSV, Excel)?",
        "What export options and customizations are needed?",
    ),
    "access_controls": (
        "What access control and permission rules are needed?",
        "Should row-level or field-level security be implemented?",
    ),
}


@dataclass(frozen=True, slots=True)
class ReportingRequirement:
    """One source-backed reporting requirement."""

    requirement_type: ReportingRequirementType
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
class ReportingRequirementsReport:
    """Source-level reporting requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[ReportingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[ReportingRequirement, ...]:
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
        """Return reporting requirement records as plain dictionaries."""
        return [req.to_dict() for req in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Reporting Requirements Report"
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
            f"- Report design coverage: {self.summary.get('report_design_coverage', 0)}%",
            f"- Data architecture coverage: {self.summary.get('data_architecture_coverage', 0)}%",
            f"- User experience coverage: {self.summary.get('ux_coverage', 0)}%",
            "- Requirement type counts: "
            + ", ".join(f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No reporting requirements were inferred."])
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


def extract_reporting_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[ReportingRequirement, ...]:
    """Extract reporting requirement records from brief-shaped input."""
    return build_reporting_requirements_report(source).requirements


def build_reporting_requirements_report(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> ReportingRequirementsReport:
    """Extract reporting requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped = _group_requirements(payload)
    requirements = _merge_requirements(grouped)
    return ReportingRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_compute_summary(requirements),
    )


# Compatibility aliases
generate_reporting_requirements = extract_reporting_requirements
analyze_reporting_requirements = extract_reporting_requirements
derive_reporting_requirements = extract_reporting_requirements
summarize_reporting_requirements = lambda source: build_reporting_requirements_report(source).summary


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: ReportingRequirementType
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


def _group_requirements(payload: Mapping[str, Any]) -> dict[ReportingRequirementType, list[_Candidate]]:
    grouped: dict[ReportingRequirementType, list[_Candidate]] = {}
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
    grouped: dict[ReportingRequirementType, list[_Candidate]],
) -> tuple[ReportingRequirement, ...]:
    requirements: list[ReportingRequirement] = []
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
            ReportingRequirement(
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


def _matched_requirement_types(text: str) -> tuple[ReportingRequirementType, ...]:
    return tuple(req_type for req_type in _TYPE_ORDER if _TYPE_PATTERNS[req_type].search(text))


def _matched_terms(req_type: ReportingRequirementType, text: str) -> tuple[str, ...]:
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


def _compute_summary(requirements: tuple[ReportingRequirement, ...]) -> dict[str, Any]:
    type_counts = {req_type: 0 for req_type in _TYPE_ORDER}
    for req in requirements:
        type_counts[req.requirement_type] += 1

    # Report design coverage
    report_design_types = {"report_types", "visualization_types", "export_formats"}
    report_design_coverage = sum(1 for req_type in report_design_types if type_counts[req_type] > 0)
    report_design_coverage_pct = int((report_design_coverage / len(report_design_types)) * 100)

    # Data architecture coverage
    data_architecture_types = {"data_sources", "aggregation_levels", "refresh_frequency", "data_freshness", "query_performance"}
    data_architecture_coverage = sum(1 for req_type in data_architecture_types if type_counts[req_type] > 0)
    data_architecture_coverage_pct = int((data_architecture_coverage / len(data_architecture_types)) * 100)

    # UX coverage
    ux_types = {"drill_down_capabilities", "access_controls"}
    ux_coverage = sum(1 for req_type in ux_types if type_counts[req_type] > 0)
    ux_coverage_pct = int((ux_coverage / len(ux_types)) * 100)

    return {
        "requirement_count": len(requirements),
        "source_count": 1,
        "type_counts": type_counts,
        "report_design_coverage": report_design_coverage_pct,
        "data_architecture_coverage": data_architecture_coverage_pct,
        "ux_coverage": ux_coverage_pct,
    }


def _markdown_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "ReportingRequirement",
    "ReportingRequirementsReport",
    "ReportingRequirementType",
    "extract_reporting_requirements",
    "build_reporting_requirements_report",
    "generate_reporting_requirements",
    "analyze_reporting_requirements",
    "derive_reporting_requirements",
    "summarize_reporting_requirements",
]
