"""Extract source-level file attachment and upload requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceFileAttachmentRequirementType = Literal[
    "allowed_file_type",
    "max_file_size",
    "attachment_count",
    "virus_scanning",
    "preview",
    "download",
    "retention",
]
SourceFileAttachmentSurface = Literal[
    "file_upload",
    "image_upload",
    "document_upload",
    "attachment",
]
SourceFileAttachmentConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[SourceFileAttachmentRequirementType, ...] = (
    "allowed_file_type",
    "max_file_size",
    "attachment_count",
    "virus_scanning",
    "preview",
    "download",
    "retention",
)
_SURFACE_ORDER: tuple[SourceFileAttachmentSurface, ...] = (
    "file_upload",
    "image_upload",
    "document_upload",
    "attachment",
)
_CONFIDENCE_ORDER: dict[SourceFileAttachmentConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_DIRECTIVE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|accept|reject|block|limit|maximum|max|minimum|min|only|cannot|"
    r"before launch|done when|acceptance|validate|retain|delete|expire)\b",
    re.I,
)
_ALLOWED_TYPE_DIRECTIVE_RE = re.compile(
    r"\b(?:allowed|accepted|supported|permitted|only|restrict(?:ed)? to|limited to|"
    r"file types?|mime types?|extensions?|support)\b",
    re.I,
)
_ATTACHMENT_CONTEXT_RE = re.compile(
    r"\b(?:attachments?|attached files?|file uploads?|uploads?|uploaded files?|documents?|"
    r"images?|photos?|pdfs?|spreadsheets?|csvs?|docs?|previews?|downloads?|virus|"
    r"malware|av scan|retention|storage expiry|delete uploaded|remove uploaded)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:attachments?|file[-_ ]?uploads?|uploads?|files?|documents?|images?|media|"
    r"preview|download|scan|virus|malware|retention|storage|requirements?|constraints?|"
    r"acceptance|criteria|source_payload|metadata|security)",
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
}
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "workflow_context",
    "problem_statement",
    "mvp_goal",
    "goals",
    "requirements",
    "constraints",
    "implementation_constraints",
    "acceptance",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "risks",
    "scope",
    "non_goals",
    "assumptions",
    "integration_points",
    "security",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_SURFACE_PATTERNS: dict[SourceFileAttachmentSurface, re.Pattern[str]] = {
    "image_upload": re.compile(r"\b(?:image|images|photo|photos|avatar|png|jpe?g|gif|webp|heic)\b", re.I),
    "document_upload": re.compile(
        r"\b(?:documents?|pdfs?|docx?|xlsx?|spreadsheets?|csvs?|presentations?|slides?)\b",
        re.I,
    ),
    "file_upload": re.compile(r"\b(?:file uploads?|upload files?|uploaded files?|uploads?)\b", re.I),
    "attachment": re.compile(r"\b(?:attachments?|attached files?)\b", re.I),
}
_TYPE_PATTERNS: dict[SourceFileAttachmentRequirementType, re.Pattern[str]] = {
    "allowed_file_type": re.compile(
        r"\b(?:allowed|accepted|supported|permitted|only|restrict(?:ed)? to|file types?|"
        r"mime types?|extensions?|png|jpe?g|gif|webp|pdf|docx?|xlsx?|csv|txt|zip|image files?|documents?)\b",
        re.I,
    ),
    "max_file_size": re.compile(
        r"\b(?:(?:max(?:imum)?|limit|up to|no larger than|under|less than)\s+(?:file\s+)?size|"
        r"(?:\d+(?:\.\d+)?\s*(?:kb|mb|gb|kib|mib|gib))\b)",
        re.I,
    ),
    "attachment_count": re.compile(
        r"\b(?:(?:max(?:imum)?|limit|up to|no more than|at most)\s+\d+\s+(?:files?|attachments?|images?|documents?)|"
        r"\d+\s+(?:files?|attachments?|images?|documents?)\s+(?:maximum|max|limit)|"
        r"(?:single|one)\s+(?:file|attachment|image|document)\b)",
        re.I,
    ),
    "virus_scanning": re.compile(
        r"\b(?:virus scan|virus scanning|malware scan|malware scanning|av scan|antivirus|"
        r"scan(?:ned)? for (?:viruses|malware)|quarantine)\b",
        re.I,
    ),
    "preview": re.compile(r"\b(?:previews?|thumbnails?|inline view|render(?:ed)? preview|image preview|pdf preview)\b", re.I),
    "download": re.compile(r"\b(?:download|downloads|downloadable|export original|save locally)\b", re.I),
    "retention": re.compile(
        r"\b(?:retention|retain|retained|delete(?:d)? after|expire(?:s|d)? after|purge(?:d)? after|"
        r"remove(?:d)? after|storage expiry|keep for \d+)\b",
        re.I,
    ),
}
_FILE_VALUE_RE = re.compile(
    r"(?:(?:\.[a-z0-9]{2,5})\b|\b(?:png|jpe?g|gif|webp|heic|pdf|docx?|xlsx?|csv|txt|zip|"
    r"image files?|documents?|spreadsheets?)\b|[a-z0-9.+-]+/[a-z0-9.+-]+)",
    re.I,
)
_SIZE_VALUE_RE = re.compile(r"\b\d+(?:\.\d+)?\s*(?:kb|mb|gb|kib|mib|gib)\b", re.I)
_COUNT_VALUE_RE = re.compile(
    r"\b(?:(?:max(?:imum)?|limit|up to|no more than|at most)\s+)?(?P<count>\d+|one|single)\s+"
    r"(?:files?|attachments?|images?|documents?)\b",
    re.I,
)
_RETENTION_VALUE_RE = re.compile(
    r"\b(?:delete(?:d)? after|expire(?:s|d)? after|purge(?:d)? after|remove(?:d)? after|retain(?:ed)? for|keep for)\s+"
    r"(?P<value>\d+\s+(?:hours?|days?|weeks?|months?|years?))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SourceFileAttachmentRequirement:
    """One source-backed file attachment requirement."""

    source_id: str | None
    requirement_type: SourceFileAttachmentRequirementType
    attachment_surface: SourceFileAttachmentSurface
    value: str | None = None
    evidence: str = ""
    confidence: SourceFileAttachmentConfidence = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirement_type": self.requirement_type,
            "attachment_surface": self.attachment_surface,
            "value": self.value,
            "evidence": self.evidence,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourceFileAttachmentRequirementsReport:
    """Source-level file attachment requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceFileAttachmentRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceFileAttachmentRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def recommendations(self) -> tuple[SourceFileAttachmentRequirement, ...]:
        """Compatibility view for consumers that expect recommendation-like rows."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "recommendations": [recommendation.to_dict() for recommendation in self.recommendations],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return file attachment requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source File Attachment Requirements"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
            "- Requirement type counts: "
            + ", ".join(f"{key} {type_counts.get(key, 0)}" for key in _TYPE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source file attachment requirements were found."])
            return "\n".join(lines)
        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source | Type | Surface | Value | Confidence | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_id or '')} | "
                f"{requirement.requirement_type} | "
                f"{requirement.attachment_surface} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.evidence)} |"
            )
        return "\n".join(lines)


def build_source_file_attachment_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> SourceFileAttachmentRequirementsReport:
    """Build a source file attachment requirements report from brief-shaped input."""
    payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_payloads(payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_id) or "",
                _CONFIDENCE_ORDER[requirement.confidence],
                _TYPE_ORDER.index(requirement.requirement_type),
                _SURFACE_ORDER.index(requirement.attachment_surface),
                requirement.value or "",
                requirement.evidence.casefold(),
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in payloads if source_id)
    return SourceFileAttachmentRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(payloads)),
    )


def build_source_file_attachment_requirements_report(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> SourceFileAttachmentRequirementsReport:
    """Compatibility helper for callers that use explicit report naming."""
    return build_source_file_attachment_requirements(source)


def summarize_source_file_attachment_requirements(
    source_or_report: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceFileAttachmentRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for source file attachment requirements."""
    if isinstance(source_or_report, SourceFileAttachmentRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_file_attachment_requirements(source_or_report).summary


def derive_source_file_attachment_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> SourceFileAttachmentRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_file_attachment_requirements(source)


def generate_source_file_attachment_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> SourceFileAttachmentRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_file_attachment_requirements(source)


def extract_source_file_attachment_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> tuple[SourceFileAttachmentRequirement, ...]:
    """Return file attachment requirement records extracted from brief-shaped input."""
    return build_source_file_attachment_requirements(source).requirements


def source_file_attachment_requirements_to_dict(
    report: SourceFileAttachmentRequirementsReport,
) -> dict[str, Any]:
    """Serialize a source file attachment requirements report to a plain dictionary."""
    return report.to_dict()


source_file_attachment_requirements_to_dict.__test__ = False


def source_file_attachment_requirements_to_dicts(
    requirements: (
        tuple[SourceFileAttachmentRequirement, ...]
        | list[SourceFileAttachmentRequirement]
        | SourceFileAttachmentRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source file attachment requirement records to dictionaries."""
    if isinstance(requirements, SourceFileAttachmentRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_file_attachment_requirements_to_dicts.__test__ = False


def source_file_attachment_requirements_to_markdown(
    report: SourceFileAttachmentRequirementsReport,
) -> str:
    """Render a source file attachment requirements report as Markdown."""
    return report.to_markdown()


source_file_attachment_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_id: str | None
    requirement_type: SourceFileAttachmentRequirementType
    attachment_surface: SourceFileAttachmentSurface
    value: str | None
    evidence: str
    confidence: SourceFileAttachmentConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(
        source, "model_dump"
    ):
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


def _candidates_for_payloads(
    payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_id, payload in payloads:
        if not payload:
            continue
        for segment in _candidate_segments(payload):
            requirement_types = _requirement_types(segment)
            if not requirement_types:
                continue
            surface = _surface(segment)
            evidence = _evidence_snippet(segment.source_field, segment.text)
            confidence = _confidence(segment)
            for requirement_type in requirement_types:
                candidates.append(
                    _Candidate(
                        source_id=source_id,
                        requirement_type=requirement_type,
                        attachment_surface=surface,
                        value=_value(requirement_type, segment.text),
                        evidence=evidence,
                        confidence=confidence,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceFileAttachmentRequirement]:
    grouped: dict[
        tuple[
            str | None,
            SourceFileAttachmentRequirementType,
            SourceFileAttachmentSurface | None,
            str | None,
            str,
        ],
        list[_Candidate],
    ] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_id,
                candidate.requirement_type,
                _surface_key(candidate),
                candidate.value,
                _dedupe_key(candidate.evidence, candidate.requirement_type, candidate.value),
            ),
            [],
        ).append(candidate)

    requirements: list[SourceFileAttachmentRequirement] = []
    for (source_id, requirement_type, _, value, _), items in grouped.items():
        best = min(items, key=lambda item: _CONFIDENCE_ORDER[item.confidence])
        requirements.append(
            SourceFileAttachmentRequirement(
                source_id=source_id,
                requirement_type=requirement_type,
                attachment_surface=best.attachment_surface,
                value=value,
                evidence=best.evidence,
                confidence=best.confidence,
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


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text))
            if _any_signal(key_text) and not isinstance(value[key], (Mapping, list, tuple, set)):
                if text := _optional_text(value[key]):
                    _append_text(segments, child_field, f"{key_text}: {text}", child_context)
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if _optional_text(value):
        _append_text(segments, source_field, str(value), field_context)


def _append_text(segments: list[_Segment], source_field: str, text: str, field_context: bool) -> None:
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
                _ATTACHMENT_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _requirement_types(segment: _Segment) -> tuple[SourceFileAttachmentRequirementType, ...]:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    types = [
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(searchable)
    ]
    if not types or not _is_requirement(segment, types):
        return ()
    if "allowed_file_type" in types and not _allowed_file_type_signal(segment):
        types.remove("allowed_file_type")
    return tuple(_dedupe(types))


def _is_requirement(
    segment: _Segment,
    requirement_types: Iterable[SourceFileAttachmentRequirementType],
) -> bool:
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    if field_context or segment.section_context:
        return True
    if _ATTACHMENT_CONTEXT_RE.search(segment.text) and _DIRECTIVE_RE.search(segment.text):
        return True
    if any(requirement_type in {"virus_scanning", "preview", "download", "retention"} for requirement_type in requirement_types):
        return bool(_ATTACHMENT_CONTEXT_RE.search(segment.text) or _DIRECTIVE_RE.search(segment.text))
    if any(requirement_type in {"max_file_size", "attachment_count"} for requirement_type in requirement_types):
        return bool(_ATTACHMENT_CONTEXT_RE.search(segment.text) or _DIRECTIVE_RE.search(segment.text))
    if "allowed_file_type" in requirement_types:
        return bool(_ATTACHMENT_CONTEXT_RE.search(segment.text) or _DIRECTIVE_RE.search(segment.text))
    return False


def _surface(segment: _Segment) -> SourceFileAttachmentSurface:
    for surface in _SURFACE_ORDER:
        if _SURFACE_PATTERNS[surface].search(segment.text):
            return surface
    searchable = _field_words(segment.source_field)
    for surface in _SURFACE_ORDER:
        if _SURFACE_PATTERNS[surface].search(searchable):
            return surface
    return "attachment"


def _value(requirement_type: SourceFileAttachmentRequirementType, text: str) -> str | None:
    if requirement_type == "allowed_file_type":
        values = _file_values(text)
        return ", ".join(values) if values else None
    if requirement_type == "max_file_size":
        match = _SIZE_VALUE_RE.search(text)
        return _clean_text(match.group(0)).lower() if match else None
    if requirement_type == "attachment_count":
        match = _COUNT_VALUE_RE.search(text)
        if not match:
            return None
        count = match.group("count").casefold()
        return "1" if count in {"one", "single"} else count
    if requirement_type == "retention":
        match = _RETENTION_VALUE_RE.search(text)
        return _clean_text(match.group("value")).lower() if match else None
    return None


def _allowed_file_type_signal(segment: _Segment) -> bool:
    field_words = _field_words(segment.source_field)
    values = _file_values(segment.text)
    has_specific_type = any(
        value.startswith(".")
        or "/" in value
        or value
        in {
            "png",
            "jpeg",
            "gif",
            "webp",
            "heic",
            "pdf",
            "doc",
            "docx",
            "xls",
            "xlsx",
            "csv",
            "txt",
            "zip",
        }
        for value in values
    )
    if has_specific_type:
        return True
    return bool(
        _ALLOWED_TYPE_DIRECTIVE_RE.search(segment.text)
        or _ALLOWED_TYPE_DIRECTIVE_RE.search(field_words)
    ) and bool(values or _surface(segment) in {"image_upload", "document_upload"})


def _surface_key(candidate: _Candidate) -> SourceFileAttachmentSurface | None:
    if candidate.requirement_type in {"max_file_size", "attachment_count", "retention"} and candidate.value:
        return None
    return candidate.attachment_surface


def _file_values(text: str) -> list[str]:
    values: list[str] = []
    for match in _FILE_VALUE_RE.finditer(text):
        token = match.group(0).casefold()
        token = token.replace("jpg", "jpeg")
        if token in {"file", "files", "upload", "uploads"}:
            continue
        values.append(token)
    return sorted(_dedupe(values), key=str.casefold)


def _confidence(segment: _Segment) -> SourceFileAttachmentConfidence:
    field_text = _field_words(segment.source_field).casefold()
    if _DIRECTIVE_RE.search(segment.text) or any(
        marker in field_text for marker in ("acceptance", "criteria", "constraint", "requirement", "definition of done")
    ):
        return "high"
    if segment.section_context or _ATTACHMENT_CONTEXT_RE.search(segment.text):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceFileAttachmentRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "attachment_surfaces": [requirement.attachment_surface for requirement in requirements],
        "type_counts": {
            requirement_type: sum(1 for requirement in requirements if requirement.requirement_type == requirement_type)
            for requirement_type in _TYPE_ORDER
        },
        "surface_counts": {
            surface: sum(1 for requirement in requirements if requirement.attachment_surface == surface)
            for surface in _SURFACE_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "source_ids": sorted(_dedupe(requirement.source_id for requirement in requirements if requirement.source_id)),
    }


def _object_payload(value: object) -> dict[str, Any]:
    if isinstance(value, (bytes, bytearray)):
        return {}
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "goals",
        "requirements",
        "constraints",
        "implementation_constraints",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "security",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _any_signal(text: str) -> bool:
    return bool(_ATTACHMENT_CONTEXT_RE.search(text) or _STRUCTURED_FIELD_RE.search(text)) or any(
        pattern.search(text) for pattern in _TYPE_PATTERNS.values()
    )


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _dedupe_key(
    value: str,
    requirement_type: SourceFileAttachmentRequirementType | None = None,
    requirement_value: str | None = None,
) -> str:
    if requirement_type in {"max_file_size", "attachment_count", "retention"} and requirement_value:
        return f"{requirement_type}:{requirement_value.casefold()}"
    _, _, statement = value.partition(": ")
    text = statement or value
    text = re.sub(r"\b(?:must|shall|should|is required to|are required to|requires?)\b", "", text, flags=re.I)
    text = re.sub(r"[^a-z0-9]+", " ", text.casefold())
    return _SPACE_RE.sub(" ", text).strip()


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[Any] = set()
    for value in values:
        key = value.casefold() if isinstance(value, str) else value
        if not value or key in seen:
            continue
        result.append(value)
        seen.add(key)
    return result


__all__ = [
    "SourceFileAttachmentConfidence",
    "SourceFileAttachmentRequirement",
    "SourceFileAttachmentRequirementType",
    "SourceFileAttachmentRequirementsReport",
    "SourceFileAttachmentSurface",
    "build_source_file_attachment_requirements",
    "build_source_file_attachment_requirements_report",
    "derive_source_file_attachment_requirements",
    "extract_source_file_attachment_requirements",
    "generate_source_file_attachment_requirements",
    "source_file_attachment_requirements_to_dict",
    "source_file_attachment_requirements_to_dicts",
    "source_file_attachment_requirements_to_markdown",
    "summarize_source_file_attachment_requirements",
]
