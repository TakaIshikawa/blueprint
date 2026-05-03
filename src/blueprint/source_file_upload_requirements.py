"""Extract source-level file upload and multipart form requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceFileUploadRequirementCategory = Literal[
    "multipart_form",
    "file_size_limits",
    "mime_type_validation",
    "virus_scanning",
    "upload_progress",
    "resumable_upload",
    "direct_upload",
    "chunked_upload",
    "temp_file_cleanup",
]
SourceFileUploadConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SourceFileUploadRequirementCategory, ...] = (
    "multipart_form",
    "file_size_limits",
    "mime_type_validation",
    "virus_scanning",
    "upload_progress",
    "resumable_upload",
    "direct_upload",
    "chunked_upload",
    "temp_file_cleanup",
)
_CONFIDENCE_ORDER: dict[SourceFileUploadConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_PLANNING_NOTES: dict[SourceFileUploadRequirementCategory, str] = {
    "multipart_form": "Implement multipart/form-data handling for file uploads, configure parsers, and handle form fields.",
    "file_size_limits": "Configure maximum file size limits, validate upload sizes, and return appropriate error messages.",
    "mime_type_validation": "Validate MIME types and file extensions, configure allowed file types, and reject invalid uploads.",
    "virus_scanning": "Integrate virus scanning service (ClamAV, VirusTotal), scan uploaded files, and quarantine threats.",
    "upload_progress": "Implement upload progress tracking, report upload status to clients, and handle progress callbacks.",
    "resumable_upload": "Support resumable uploads with chunk tracking, resume tokens, and partial upload recovery.",
    "direct_upload": "Configure direct-to-cloud uploads (S3, GCS, Azure), generate presigned URLs, and handle upload callbacks.",
    "chunked_upload": "Implement chunked upload strategies, handle chunk assembly, validate chunk ordering, and manage partial uploads.",
    "temp_file_cleanup": "Configure temporary file cleanup, implement cleanup schedules, and prevent disk space exhaustion.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_FILE_UPLOAD_CONTEXT_RE = re.compile(
    r"\b(?:file upload(?:s)?|upload(?:s)?|multipart|form[- ]?data|attachment(?:s)?|"
    r"file size|max[- ]?size|mime[- ]?type(?:s)?|file[- ]?type(?:s)?|content[- ]?type(?:s)?|"
    r"virus[- ]?scan(?:ning)?|malware[- ]?scan(?:ning)?|clamav|virustotal|"
    r"upload[- ]?progress|resumable|chunked[- ]?upload|direct[- ]?upload|"
    r"presigned[- ]?url|s3[- ]?upload|cloud[- ]?upload|temp[- ]?file(?:s)?|"
    r"file[- ]?cleanup|validate.{0,20}(?:file(?:s)?|mime|type(?:s)?)|"
    r"reject.{0,20}file|allowed.{0,20}(?:file(?:s)?|type(?:s)?|mime)|"
    r"integrate.{0,20}(?:virus|malware|scan))\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:upload(?:s)?|file(?:s)?|attachment(?:s)?|multipart|form|storage|"
    r"api|requirements?|constraints?|acceptance|metadata|source[_ -]?payload|implementation[_ -]?notes)",
    re.I,
)
_DIRECTIVE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|enable|configure|provide|document|define|implement|enforce|validate|"
    r"cannot ship|before launch|done when|acceptance)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|outside scope|non[- ]?goal|defer|deferred)\b"
    r".{0,160}\b(?:file upload(?:s)?|upload(?:s)?|multipart|form[- ]?data|attachment(?:s)?|"
    r"file size|mime[- ]?type|virus[- ]?scan|resumable|chunked|direct[- ]?upload)\b"
    r".{0,160}\b(?:required|needed|in scope|supported|support|work|changes?|planned|"
    r"requirements?)?\b|"
    r"\b(?:file upload(?:s)?|upload(?:s)?|multipart|form[- ]?data|attachment(?:s)?|"
    r"file size|mime[- ]?type|virus[- ]?scan|resumable|chunked|direct[- ]?upload)\b"
    r".{0,160}\b(?:out of scope|outside scope|not required|not needed|no support|"
    r"unsupported|no work|non[- ]?goal|deferred)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[SourceFileUploadRequirementCategory, re.Pattern[str]] = {
    "multipart_form": re.compile(
        r"\b(?:multipart[/\s-]form[- ]?data|multipart upload(?:s)?|form[- ]?data|"
        r"file upload(?:s)?|upload form|form field(?:s)?|multipart parser|"
        r"multipart[/\s-]mixed|enctype|application[/\s]multipart)\b",
        re.I,
    ),
    "file_size_limits": re.compile(
        r"\b(?:file size limit(?:s)?|max(?:imum)? file size|size limit(?:s)?|"
        r"upload size limit(?:s)?|max[- ]?size|size restriction(?:s)?|"
        r"file too large|(?:limit(?:ed)?|restrict|validate)\s+(?:file\s+)?size(?:s)?|"
        r"\d+\s*(?:mb|gb|kb|bytes?)|size constraint(?:s)?)\b",
        re.I,
    ),
    "mime_type_validation": re.compile(
        r"\b(?:mime[- ]?type(?:s)?|file[- ]?type(?:s)?|content[- ]?type(?:s)?|"
        r"allowed (?:file(?:s)?|mime|type(?:s)?)|file extension(?:s)?|validate (?:file[- ]?type(?:s)?|mime)|"
        r"mime validation|file format(?:s)?|accept(?:ed)? type(?:s)?|reject.{0,20}file[- ]?type(?:s)?|"
        r"whitelist(?:ed)? type(?:s)?|image[/\s]jpeg|application[/\s]pdf|text[/\s]plain|"
        r"unsupported file[- ]?type(?:s)?)\b",
        re.I,
    ),
    "virus_scanning": re.compile(
        r"\b(?:virus scan(?:ning)?|malware scan(?:ning)?|antivirus|anti[- ]?virus|"
        r"clamav|virustotal|virus[- ]?total|virus check|malware detection|"
        r"scan(?:s)?(?:\s+(?:for|uploaded))?\s+(?:virus(?:es)?|malware|file(?:s)?)|security scan(?:ning)?|"
        r"file scan(?:ning)?|threat detection|integrate.{0,20}(?:virus|malware)|"
        r"(?:virus|malware)\s+(?:scanning|detection|integration))\b",
        re.I,
    ),
    "upload_progress": re.compile(
        r"\b(?:upload progress|progress (?:tracking|indicator|bar|callback(?:s)?)|"
        r"track(?:ing)? upload(?:s)?|upload status|progress event(?:s)?|"
        r"progress report(?:ing)?|upload percent(?:age)?|progress update(?:s)?|"
        r"(?:show|display|report)\s+progress)\b",
        re.I,
    ),
    "resumable_upload": re.compile(
        r"\b(?:resumable upload(?:s)?|resume upload(?:s)?|upload resume|"
        r"partial upload(?:s)?|resume token(?:s)?|upload recovery|"
        r"interrupted upload(?:s)?|continue upload(?:s)?|upload checkpoint(?:s)?|"
        r"tus[- ]?protocol|tus upload(?:s)?)\b",
        re.I,
    ),
    "direct_upload": re.compile(
        r"\b(?:direct[- ]?upload(?:s)?|direct[- ]?to[- ]?(?:s3|cloud|storage)|"
        r"presigned[- ]?url(?:s)?|pre[- ]?signed[- ]?url(?:s)?|s3[- ]?upload(?:s)?|"
        r"cloud[- ]?upload(?:s)?|gcs[- ]?upload(?:s)?|azure[- ]?upload(?:s)?|"
        r"client[- ]?side[- ]?upload(?:s)?|browser[- ]?upload(?:s)?)\b",
        re.I,
    ),
    "chunked_upload": re.compile(
        r"\b(?:chunked[- ]?upload(?:s)?|chunk(?:ed)? transfer|upload chunk(?:s)?|"
        r"chunk[- ]?size|chunk assembly|split upload(?:s)?|multi[- ]?part upload(?:s)?|"
        r"chunk(?:s)? (?:upload|strategy|processing)|partial chunk(?:s)?)\b",
        re.I,
    ),
    "temp_file_cleanup": re.compile(
        r"\b(?:temp(?:orary)? file(?:s)?|temp[- ]?file cleanup|cleanup temp(?:orary)? file(?:s)?|"
        r"cleanup schedule|disk[- ]?space|storage cleanup|delete temp(?:orary)? file(?:s)?|"
        r"tmp[- ]?(?:file(?:s)?|dir|directory)|cleanup upload(?:s)?|orphaned file(?:s)?|"
        r"file retention|garbage collection)\b",
        re.I,
    ),
}
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "security",
    "api",
    "upload",
    "uploads",
    "file",
    "files",
    "storage",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)


@dataclass(frozen=True, slots=True)
class SourceFileUploadRequirement:
    """One source-backed file upload requirement."""

    category: SourceFileUploadRequirementCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceFileUploadConfidence = "medium"
    planning_note: str = ""
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> SourceFileUploadRequirementCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
            "unresolved_questions": list(self.unresolved_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceFileUploadRequirementsReport:
    """Source-level file upload requirements report."""

    source_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceFileUploadRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceFileUploadRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceFileUploadRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "title": self.title,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return file upload requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source File Upload Requirements Report"
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
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source file upload requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Confidence | Source | Evidence | Planning Note | Unresolved Questions |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.unresolved_questions))} |"
            )
        return "\n".join(lines)


def build_source_file_upload_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceFileUploadRequirementsReport:
    """Build a file upload requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    requirements = tuple(_merge_candidates(candidates))
    return SourceFileUploadRequirementsReport(
        source_id=source_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def build_source_file_upload_requirements_report(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceFileUploadRequirementsReport:
    """Compatibility helper for callers that use explicit report naming."""
    return build_source_file_upload_requirements(source)


def generate_source_file_upload_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceFileUploadRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_file_upload_requirements(source)


def derive_source_file_upload_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceFileUploadRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_file_upload_requirements(source)


def extract_source_file_upload_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceFileUploadRequirement, ...]:
    """Return file upload requirement records extracted from brief-shaped input."""
    return build_source_file_upload_requirements(source).requirements


def summarize_source_file_upload_requirements(
    source_or_result: (
        str
        | Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceFileUploadRequirementsReport
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic file upload requirements summary."""
    if isinstance(source_or_result, SourceFileUploadRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_file_upload_requirements(source_or_result).summary


def source_file_upload_requirements_to_dict(
    report: SourceFileUploadRequirementsReport,
) -> dict[str, Any]:
    """Serialize a file upload requirements report to a plain dictionary."""
    return report.to_dict()


source_file_upload_requirements_to_dict.__test__ = False


def source_file_upload_requirements_to_dicts(
    requirements: tuple[SourceFileUploadRequirement, ...]
    | list[SourceFileUploadRequirement]
    | SourceFileUploadRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize source file upload requirement records to dictionaries."""
    if isinstance(requirements, SourceFileUploadRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_file_upload_requirements_to_dicts.__test__ = False


def source_file_upload_requirements_to_markdown(
    report: SourceFileUploadRequirementsReport,
) -> str:
    """Render a file upload requirements report as Markdown."""
    return report.to_markdown()


source_file_upload_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SourceFileUploadRequirementCategory
    source_field: str
    evidence: str
    confidence: SourceFileUploadConfidence


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


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        if _is_out_of_scope(segment):
            continue
        searchable = _searchable_text(segment.source_field, segment.text)
        categories: list[SourceFileUploadRequirementCategory] = [
            category
            for category in _CATEGORY_ORDER
            if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        if not categories or not _is_requirement(segment, categories):
            continue
        evidence = _evidence_snippet(segment.source_field, segment.text)
        for category in _dedupe(categories):
            candidates.append(
                _Candidate(
                    category=category,
                    source_field=segment.source_field,
                    evidence=evidence,
                    confidence=_confidence(category, segment),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceFileUploadRequirement]:
    grouped: dict[SourceFileUploadRequirementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceFileUploadRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        best = min(items, key=_candidate_sort_key)
        evidence = tuple(_dedupe_evidence(item.evidence for item in sorted(items, key=_candidate_sort_key)))[:6]
        requirements.append(
            SourceFileUploadRequirement(
                category=category,
                source_field=best.source_field,
                evidence=evidence,
                confidence=best.confidence,
                planning_note=_PLANNING_NOTES[category],
                unresolved_questions=tuple(_unresolved_questions(category, items)),
            )
        )
    return requirements


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
                _STRUCTURED_FIELD_RE.search(key_text) or _FILE_UPLOAD_CONTEXT_RE.search(key_text)
            )
            child = value[key]
            if child_context and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    for segment_text, segment_context in _segments(f"{key_text}: {text}", child_context):
                        segments.append(_Segment(child_field, segment_text, segment_context))
                continue
            _append_value(segments, child_field, child, child_context)
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
            section_context = inherited_context or bool(_FILE_UPLOAD_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if _NEGATED_SCOPE_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(
    segment: _Segment,
    categories: Iterable[SourceFileUploadRequirementCategory],
) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    if not (_FILE_UPLOAD_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    if _DIRECTIVE_RE.search(segment.text) or segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(categories)


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        root_field = segment.source_field.split("[", 1)[0].split(".", 1)[0]
        if root_field not in {"title", "summary", "body", "description", "scope", "non_goals", "constraints", "source_payload"}:
            continue
        if _is_out_of_scope(segment):
            return True
    return False


def _is_out_of_scope(segment: _Segment) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    return bool(_NEGATED_SCOPE_RE.search(searchable))


def _confidence(
    category: SourceFileUploadRequirementCategory,
    segment: _Segment,
) -> SourceFileUploadConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    score = 0
    if _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        score += 1
    if segment.section_context or _FILE_UPLOAD_CONTEXT_RE.search(searchable):
        score += 1
    if _DIRECTIVE_RE.search(segment.text):
        score += 1
    if _CATEGORY_PATTERNS[category].search(searchable):
        score += 1
    return "high" if score >= 3 else "medium" if score >= 2 else "low"


def _unresolved_questions(
    category: SourceFileUploadRequirementCategory,
    items: Iterable[_Candidate],
) -> list[str]:
    item_list = list(items)
    questions: list[str] = []
    if category == "multipart_form" and not any(re.search(r"\b(?:parser|library|framework)\b", item.evidence, re.I) for item in item_list):
        questions.append("What multipart form parser library or framework should be used?")
    if category == "file_size_limits" and not any(re.search(r"\b(?:\d+\s*(?:mb|gb|kb)|limit|max)\b", item.evidence, re.I) for item in item_list):
        questions.append("What specific file size limits should be enforced (e.g., 10MB, 100MB)?")
    if category == "mime_type_validation" and not any(re.search(r"\b(?:allowed|whitelist|accept|type(?:s)?)\b", item.evidence, re.I) for item in item_list):
        questions.append("What specific MIME types and file extensions should be allowed?")
    if category == "virus_scanning" and not any(re.search(r"\b(?:clamav|virustotal|service|integration)\b", item.evidence, re.I) for item in item_list):
        questions.append("What virus scanning service should be integrated (ClamAV, VirusTotal, etc.)?")
    if category == "direct_upload" and not any(re.search(r"\b(?:s3|gcs|azure|cloud|provider)\b", item.evidence, re.I) for item in item_list):
        questions.append("What cloud storage provider should be used for direct uploads (S3, GCS, Azure)?")
    if category == "chunked_upload" and not any(re.search(r"\b(?:chunk[- ]?size|\d+\s*(?:mb|kb))\b", item.evidence, re.I) for item in item_list):
        questions.append("What chunk size should be used for chunked uploads?")
    if category == "temp_file_cleanup" and not any(re.search(r"\b(?:schedule|retention|policy|hours?|days?)\b", item.evidence, re.I) for item in item_list):
        questions.append("What cleanup schedule and retention policy should be used for temporary files?")
    return questions[:3]


def _summary(requirements: tuple[SourceFileUploadRequirement, ...]) -> dict[str, Any]:
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
        "status": "ready_for_planning" if requirements else "no_file_upload_requirements_found",
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = ("id", "source_brief_id", "source_id", *_SCANNED_FIELDS)
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _searchable_text(source_field: str, text: str) -> str:
    return f"{_field_words(source_field)} {text}".replace("_", " ").replace("-", " ")


def _candidate_sort_key(item: _Candidate) -> tuple[int, int, str, str]:
    return (
        _CONFIDENCE_ORDER[item.confidence],
        _field_category_rank(item.category, item.source_field),
        item.source_field.casefold(),
        item.evidence.casefold(),
    )


def _field_category_rank(category: SourceFileUploadRequirementCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[SourceFileUploadRequirementCategory, tuple[str, ...]] = {
        "multipart_form": ("multipart", "form", "upload"),
        "file_size_limits": ("size", "limit", "max"),
        "mime_type_validation": ("mime", "type", "validation", "file"),
        "virus_scanning": ("virus", "scan", "malware", "security"),
        "upload_progress": ("progress", "tracking", "status"),
        "resumable_upload": ("resumable", "resume", "recovery"),
        "direct_upload": ("direct", "s3", "cloud", "presigned"),
        "chunked_upload": ("chunk", "chunked", "split"),
        "temp_file_cleanup": ("temp", "cleanup", "retention"),
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
    seen: dict[str, int] = {}
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            index = seen[key]
            if _evidence_priority(value) < _evidence_priority(deduped[index]):
                deduped[index] = value
            continue
        deduped.append(value)
        seen[key] = len(deduped) - 1
    return deduped


def _evidence_priority(value: str) -> int:
    source_field, _, _ = value.partition(": ")
    if ".requirements" in source_field or ".constraints" in source_field or ".acceptance" in source_field:
        return 0
    if ".metadata" in source_field or ".brief_metadata" in source_field:
        return 2
    return 1


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = _clean_text(str(value)).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "SourceFileUploadConfidence",
    "SourceFileUploadRequirement",
    "SourceFileUploadRequirementCategory",
    "SourceFileUploadRequirementsReport",
    "build_source_file_upload_requirements",
    "build_source_file_upload_requirements_report",
    "derive_source_file_upload_requirements",
    "extract_source_file_upload_requirements",
    "generate_source_file_upload_requirements",
    "source_file_upload_requirements_to_dict",
    "source_file_upload_requirements_to_dicts",
    "source_file_upload_requirements_to_markdown",
    "summarize_source_file_upload_requirements",
]
