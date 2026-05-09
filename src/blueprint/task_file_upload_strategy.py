"""Analyze file upload strategy for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for file upload concepts
_FILE_SIZE_LIMIT_RE = re.compile(
    r"\b(?:file[_\s]+size[_\s]+(?:limit(?:s)?|restriction|constraint|maximum|max)|"
    r"max(?:imum)?[_\s]+file[_\s]+size|"
    r"upload[_\s]+size[_\s]+limit|size[_\s]+constraint|"
    r"(?:set|configure|define)[_\s]+file[_\s]+size[_\s]+limit)\b",
    re.I,
)
_SUPPORTED_FORMATS_RE = re.compile(
    r"\b(?:supported[_\s]+(?:file[_\s]+)?(?:format(?:s)?|type(?:s)?|extension(?:s)?)|"
    r"(?:support|allow(?:ed)?|accept(?:ed)?)[_\s]+(?:multiple[_\s]+)?(?:\w+[_\s]+(?:and[_\s]+)?\w+[_\s]+)?(?:file[_\s]+)?(?:format(?:s)?|type(?:s)?|extension(?:s)?)|"
    r"file[_\s]+(?:format|type|extension)(?:s)?[_\s]+(?:support|allowed|permitted|whitelist|validation)|"
    r"mime[_\s]+type(?:s)?(?:[_\s]+(?:allowed|supported|validation))?|"
    r"validate[_\s]+(?:file[_\s]+)?(?:mime[_\s]+)?type(?:s)?|"
    r"file[_\s]+validation|validate[_\s]+file[_\s]+(?:type|format))\b",
    re.I,
)
_CHUNKING_STRATEGY_RE = re.compile(
    r"\b(?:chunk(?:ing)?[_\s]+(?:strategy|upload|size)|"
    r"chunked[_\s]+upload|upload[_\s]+in[_\s]+chunks?|"
    r"multi[_\s-]*part[_\s]+upload|multipart[_\s]+upload|"
    r"split[_\s]+file[_\s]+(?:into[_\s]+)?chunk(?:s)?|"
    r"chunk[_\s]+size[_\s]+configuration)\b",
    re.I,
)
_RESUMABLE_UPLOAD_RE = re.compile(
    r"\b(?:resumable[_\s]+upload(?:s)?|resume[_\s]+upload|"
    r"upload[_\s]+resume|pause[_\s]+(?:and[_\s]+)?resume|"
    r"(?:support|enable)[_\s]+resumable[_\s]+upload|"
    r"restart[_\s]+upload|continue[_\s]+upload)\b",
    re.I,
)
_VIRUS_SCANNING_RE = re.compile(
    r"\b(?:virus[_\s]+scan(?:ning)?|scan[_\s]+(?:for[_\s]+)?(?:virus(?:es)?|malware)|"
    r"malware[_\s]+(?:scan|detection|check)|"
    r"antivirus[_\s]+(?:scan|check)|av[_\s]+scan|"
    r"security[_\s]+scan|file[_\s]+scan(?:ning)?|"
    r"(?:check|validate)[_\s]+(?:for[_\s]+)?malware)\b",
    re.I,
)
_STORAGE_BACKEND_RE = re.compile(
    r"\b(?:storage[_\s]+(?:backend|service|provider|system)|"
    r"upload[_\s]+to[_\s]+(?:s3|gcs|azure[_\s]+blob|blob[_\s]+storage|cloud[_\s]+storage)|"
    r"s3[_\s]+(?:bucket|storage|upload)|"
    r"object[_\s]+storage|blob[_\s]+storage|"
    r"file[_\s]+storage[_\s]+(?:backend|service))\b",
    re.I,
)
_CDN_INTEGRATION_RE = re.compile(
    r"\b(?:cdn[_\s]+(?:integration|delivery|distribution)|"
    r"cloudfront|cloudflare|fastly|"
    r"content[_\s]+delivery[_\s]+network|"
    r"edge[_\s]+caching|cache[_\s]+distribution)\b",
    re.I,
)
_PRESIGNED_URL_RE = re.compile(
    r"\b(?:presigned[_\s]+url(?:s)?|pre[_\s-]*signed[_\s]+url|"
    r"signed[_\s]+url(?:s)?|temporary[_\s]+upload[_\s]+url|"
    r"upload[_\s]+url[_\s]+generation|"
    r"generate[_\s]+upload[_\s]+url)\b",
    re.I,
)
_METADATA_EXTRACTION_RE = re.compile(
    r"\b(?:metadata[_\s]+extraction|extract[_\s]+metadata|"
    r"file[_\s]+metadata|exif[_\s]+(?:data|extraction)|"
    r"(?:extract|parse|read)[_\s]+(?:image|file)[_\s]+metadata|"
    r"content[_\s]+type[_\s]+detection)\b",
    re.I,
)
_THUMBNAIL_GENERATION_RE = re.compile(
    r"\b(?:thumbnail[_\s]+generation|generate[_\s]+(?:thumbnail|preview)(?:s)?|"
    r"create[_\s]+thumbnail(?:s)?|image[_\s]+thumbnail|"
    r"preview[_\s]+(?:image(?:s)?|generation)|"
    r"image[_\s]+preview|resize[_\s]+image)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class FileUploadStrategy:
    """File upload strategy analysis for a task."""

    file_size_limits_defined: bool = False
    supported_formats_specified: bool = False
    chunking_strategy_configured: bool = False
    resumable_upload_enabled: bool = False
    virus_scanning_implemented: bool = False
    storage_backend_specified: bool = False
    cdn_integration_planned: bool = False
    presigned_urls_used: bool = False
    metadata_extraction_configured: bool = False
    thumbnail_generation_planned: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.file_size_limits_defined,
            self.supported_formats_specified,
            self.chunking_strategy_configured,
            self.resumable_upload_enabled,
            self.virus_scanning_implemented,
            self.storage_backend_specified,
            self.cdn_integration_planned,
            self.presigned_urls_used,
            self.metadata_extraction_configured,
            self.thumbnail_generation_planned,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "file_size_limits_defined": self.file_size_limits_defined,
            "supported_formats_specified": self.supported_formats_specified,
            "chunking_strategy_configured": self.chunking_strategy_configured,
            "resumable_upload_enabled": self.resumable_upload_enabled,
            "virus_scanning_implemented": self.virus_scanning_implemented,
            "storage_backend_specified": self.storage_backend_specified,
            "cdn_integration_planned": self.cdn_integration_planned,
            "presigned_urls_used": self.presigned_urls_used,
            "metadata_extraction_configured": self.metadata_extraction_configured,
            "thumbnail_generation_planned": self.thumbnail_generation_planned,
            "readiness_score": self.readiness_score,
        }


def analyze_file_upload_strategy(task_data: Mapping[str, Any]) -> FileUploadStrategy:
    """
    Analyze file upload strategy from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        FileUploadStrategy with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return FileUploadStrategy()

    searchable_text = _extract_searchable_text(task_data)

    return FileUploadStrategy(
        file_size_limits_defined=bool(_FILE_SIZE_LIMIT_RE.search(searchable_text)),
        supported_formats_specified=bool(_SUPPORTED_FORMATS_RE.search(searchable_text)),
        chunking_strategy_configured=bool(_CHUNKING_STRATEGY_RE.search(searchable_text)),
        resumable_upload_enabled=bool(_RESUMABLE_UPLOAD_RE.search(searchable_text)),
        virus_scanning_implemented=bool(_VIRUS_SCANNING_RE.search(searchable_text)),
        storage_backend_specified=bool(_STORAGE_BACKEND_RE.search(searchable_text)),
        cdn_integration_planned=bool(_CDN_INTEGRATION_RE.search(searchable_text)),
        presigned_urls_used=bool(_PRESIGNED_URL_RE.search(searchable_text)),
        metadata_extraction_configured=bool(_METADATA_EXTRACTION_RE.search(searchable_text)),
        thumbnail_generation_planned=bool(_THUMBNAIL_GENERATION_RE.search(searchable_text)),
    )


def _extract_searchable_text(task_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the task data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = task_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = task_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = task_data.get("validation_command") or task_data.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "FileUploadStrategy",
    "analyze_file_upload_strategy",
]
