"""Extract file handling requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


FileHandlingRequirementType = Literal[
    "upload",
    "download",
    "attachment",
    "import",
    "export",
    "signed_url",
    "file_size",
    "file_type",
    "malware_scanning",
    "storage_access",
]
FileHandlingRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_REQUIREMENT_ORDER: tuple[FileHandlingRequirementType, ...] = (
    "upload",
    "download",
    "attachment",
    "import",
    "export",
    "signed_url",
    "file_size",
    "file_type",
    "malware_scanning",
    "storage_access",
)
_CONFIDENCE_ORDER: dict[FileHandlingRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|support|allow|ensure|handle|"
    r"validate|acceptance|definition of done|before launch)\b",
    re.I,
)

_SIGNAL_PATTERNS: dict[FileHandlingRequirementType, re.Pattern[str]] = {
    "upload": re.compile(
        r"\b(?:uploads?|uploading|uploader|file picker|drag[- ]and[- ]drop|dropzone|"
        r"submit files?|send files?|add files?)\b",
        re.I,
    ),
    "download": re.compile(
        r"\b(?:downloads?|downloading|downloadable|save as|bulk download|download link|"
        r"retrieve files?)\b",
        re.I,
    ),
    "attachment": re.compile(
        r"\b(?:attachments?|attached files?|attach files?|supporting documents?|"
        r"document attachments?|message attachments?)\b",
        re.I,
    ),
    "import": re.compile(
        r"\b(?:imports?|importing|csv import|spreadsheet import|bulk import|"
        r"data import|ingest files?|file ingestion)\b",
        re.I,
    ),
    "export": re.compile(
        r"\b(?:exports?|exporting|csv export|pdf export|spreadsheet export|"
        r"bulk export|data export|evidence export)\b",
        re.I,
    ),
    "signed_url": re.compile(
        r"\b(?:signed urls?|signed links?|pre[- ]signed urls?|presigned urls?|"
        r"temporary urls?|temporary download links?|expiring links?|time[- ]limited urls?)\b",
        re.I,
    ),
    "file_size": re.compile(
        r"\b(?:file size|size limits?|max(?:imum)? upload size|max(?:imum)? file size|"
        r"larger than \d+\s*(?:kb|mb|gb)|up to \d+\s*(?:kb|mb|gb)|"
        r"\d+\s*(?:kb|mb|gb)\s*(?:limit|max|maximum))\b",
        re.I,
    ),
    "file_type": re.compile(
        r"\b(?:file types?|mime types?|content types?|allowed extensions?|extension allowlist|"
        r"pdfs?|csvs?|xlsx?|docx?|pngs?|jpe?gs?|images?|spreadsheets?)\b|"
        r"\.(?:pdf|csv|xlsx?|docx?|png|jpe?g)\b",
        re.I,
    ),
    "malware_scanning": re.compile(
        r"\b(?:malware scan(?:ning)?|virus scan(?:ning)?|antivirus|anti-virus|"
        r"quarantine files?|infected files?|clamav|safe browsing scan)\b",
        re.I,
    ),
    "storage_access": re.compile(
        r"\b(?:storage access|object storage|s3 bucket|gcs bucket|blob storage|"
        r"bucket permissions?|storage permissions?|private bucket|storage acl|file acl|"
        r"access control|role[- ]based file access|authorized users? only)\b",
        re.I,
    ),
}
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:file|upload|download|attachment|import|export|signed|url|size|mime|type|"
    r"malware|virus|storage|bucket|acl|access)",
    re.I,
)
_PLANNING_IMPLICATIONS: dict[FileHandlingRequirementType, str] = {
    "upload": "Plan upload endpoints, client validation, resumability needs, and storage ownership.",
    "download": "Plan download authorization, cache behavior, content disposition, and audit needs.",
    "attachment": "Plan attachment lifecycle, association rules, previews, deletion, and retention behavior.",
    "import": "Plan import parsing, validation, error reporting, idempotency, and rollback behavior.",
    "export": "Plan export generation, authorization, format guarantees, retention, and delivery path.",
    "signed_url": "Plan signed URL expiry, scope, revocation, and leakage controls.",
    "file_size": "Plan size validation, streaming limits, timeout behavior, and user-facing error states.",
    "file_type": "Plan MIME sniffing, extension allowlists, parser coverage, and unsafe type rejection.",
    "malware_scanning": "Plan malware scanning, quarantine workflow, scan status visibility, and failure handling.",
    "storage_access": "Plan bucket permissions, least-privilege access, tenant isolation, and audit controls.",
}


@dataclass(frozen=True, slots=True)
class SourceFileHandlingRequirement:
    """One file handling requirement found in source evidence."""

    source_brief_id: str | None
    requirement_type: FileHandlingRequirementType
    detected_signals: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: FileHandlingRequirementConfidence = "medium"
    planning_implications: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "detected_signals": list(self.detected_signals),
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_implications": list(self.planning_implications),
        }


@dataclass(frozen=True, slots=True)
class SourceFileHandlingRequirementsReport:
    """Source-level file handling requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceFileHandlingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceFileHandlingRequirement, ...]:
        """Compatibility view matching reports that expose rows as records."""
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
        """Return requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source File Handling Requirements Report"
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
            "- Confidence counts: "
            f"high {confidence_counts.get('high', 0)}, "
            f"medium {confidence_counts.get('medium', 0)}, "
            f"low {confidence_counts.get('low', 0)}",
            "- Requirement type counts: "
            + (", ".join(f"{key} {type_counts[key]}" for key in sorted(type_counts)) or "none"),
        ]
        if not self.requirements:
            lines.extend(["", "No file handling requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Type | Confidence | Signals | Evidence | Planning Implications |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.requirement_type} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.detected_signals))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.planning_implications))} |"
            )
        return "\n".join(lines)


def build_source_file_handling_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceFileHandlingRequirementsReport:
    """Extract file handling requirement records from source briefs."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _requirement_index(requirement.requirement_type),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceFileHandlingRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def generate_source_file_handling_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceFileHandlingRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_file_handling_requirements(source)


def extract_source_file_handling_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> tuple[SourceFileHandlingRequirement, ...]:
    """Return file handling requirement records from brief-shaped input."""
    return build_source_file_handling_requirements(source).requirements


def source_file_handling_requirements_to_dict(
    report: SourceFileHandlingRequirementsReport,
) -> dict[str, Any]:
    """Serialize a file handling requirements report to a plain dictionary."""
    return report.to_dict()


source_file_handling_requirements_to_dict.__test__ = False


def source_file_handling_requirements_to_dicts(
    requirements: tuple[SourceFileHandlingRequirement, ...] | list[SourceFileHandlingRequirement],
) -> list[dict[str, Any]]:
    """Serialize file handling requirement records to dictionaries."""
    return [requirement.to_dict() for requirement in requirements]


source_file_handling_requirements_to_dicts.__test__ = False


def source_file_handling_requirements_to_markdown(
    report: SourceFileHandlingRequirementsReport,
) -> str:
    """Render a file handling requirements report as Markdown."""
    return report.to_markdown()


source_file_handling_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: FileHandlingRequirementType
    detected_signal: str
    evidence: str
    confidence: FileHandlingRequirementConfidence


def _source_payloads(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief)) or hasattr(
        source, "model_dump"
    ):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        try:
            value = SourceBrief.model_validate(source).model_dump(mode="python")
            payload = dict(value)
        except (TypeError, ValueError, ValidationError):
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
            requirement_types = _requirement_types(segment)
            if not requirement_types:
                continue
            confidence = _confidence(segment, source_field)
            evidence = _evidence_snippet(source_field, segment)
            for requirement_type in requirement_types:
                for signal in _detected_signals(requirement_type, segment):
                    candidates.append(
                        _Candidate(
                            source_brief_id=source_brief_id,
                            requirement_type=requirement_type,
                            detected_signal=signal,
                            evidence=evidence,
                            confidence=confidence,
                        )
                    )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceFileHandlingRequirement]:
    grouped: dict[tuple[str | None, FileHandlingRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.requirement_type), []).append(
            candidate
        )

    requirements: list[SourceFileHandlingRequirement] = []
    for (source_brief_id, requirement_type), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        evidence = tuple(
            sorted(_dedupe(item.evidence for item in items), key=lambda item: item.casefold())
        )
        requirements.append(
            SourceFileHandlingRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                detected_signals=tuple(
                    sorted(
                        _dedupe(item.detected_signal for item in items),
                        key=lambda item: item.casefold(),
                    )
                ),
                evidence=evidence,
                confidence=confidence,
                planning_implications=(_PLANNING_IMPLICATIONS[requirement_type],),
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "requirements",
        "constraints",
        "risks",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "data_requirements",
        "architecture_notes",
        "implementation_notes",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _any_signal(key_text):
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


def _requirement_types(text: str) -> tuple[FileHandlingRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _REQUIREMENT_ORDER
        if _SIGNAL_PATTERNS[requirement_type].search(text)
    )


def _detected_signals(
    requirement_type: FileHandlingRequirementType, text: str
) -> tuple[str, ...]:
    pattern = _SIGNAL_PATTERNS[requirement_type]
    return tuple(_dedupe(match.group(0).casefold() for match in pattern.finditer(text)))


def _confidence(text: str, source_field: str) -> FileHandlingRequirementConfidence:
    structured_field = bool(_STRUCTURED_FIELD_RE.search(source_field))
    requirement_count = len(_requirement_types(text))
    if _REQUIRED_RE.search(text) or structured_field or requirement_count > 1:
        return "high"
    if requirement_count == 1:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceFileHandlingRequirement, ...], source_count: int
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
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
    }


def _requirement_index(requirement_type: FileHandlingRequirementType) -> int:
    return _REQUIREMENT_ORDER.index(requirement_type)


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
        "requirements",
        "constraints",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
        "source_links",
        "acceptance_criteria",
        "implementation_notes",
        "validation_plan",
        "data_requirements",
        "architecture_notes",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _SIGNAL_PATTERNS.values())


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


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
    "FileHandlingRequirementConfidence",
    "FileHandlingRequirementType",
    "SourceFileHandlingRequirement",
    "SourceFileHandlingRequirementsReport",
    "build_source_file_handling_requirements",
    "extract_source_file_handling_requirements",
    "generate_source_file_handling_requirements",
    "source_file_handling_requirements_to_dict",
    "source_file_handling_requirements_to_dicts",
    "source_file_handling_requirements_to_markdown",
]
