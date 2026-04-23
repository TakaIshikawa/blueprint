"""Rendered export diffing utilities."""

from __future__ import annotations

import json
import re
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass, field
from difflib import unified_diff
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from blueprint.exporters.export_validation import create_exporter


VOLATILE_JSON_KEYS = {
    "created_at",
    "updated_at",
    "generated_at",
    "exported_at",
    "timestamp",
}

MARKDOWN_BOILERPLATE_PATTERNS = (
    re.compile(r"^(?:[-*]\s*)?(?:created|updated|generated|exported at|timestamp):\s*.+$", re.I),
)


@dataclass(frozen=True, slots=True)
class ExportDiffFileChange:
    """One file-level change inside a rendered export diff."""

    path: str
    status: str
    left: str | None = None
    right: str | None = None
    diff: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the change for JSON output."""
        payload: dict[str, Any] = {
            "path": self.path,
            "status": self.status,
        }
        if self.left is not None:
            payload["left"] = self.left
        if self.right is not None:
            payload["right"] = self.right
        if self.diff:
            payload["diff"] = self.diff
        return payload


@dataclass(frozen=True, slots=True)
class ExportDiffResult:
    """Structured comparison between two rendered exports."""

    target: str
    left_plan_id: str
    right_plan_id: str
    artifact_type: str
    left_file_count: int
    right_file_count: int
    added_files: list[ExportDiffFileChange] = field(default_factory=list)
    removed_files: list[ExportDiffFileChange] = field(default_factory=list)
    changed_files: list[ExportDiffFileChange] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Return True when the rendered exports differ."""
        return any((self.added_files, self.removed_files, self.changed_files))

    def to_dict(self) -> dict[str, Any]:
        """Serialize the comparison result for CLI JSON output."""
        return {
            "target": self.target,
            "left_plan_id": self.left_plan_id,
            "right_plan_id": self.right_plan_id,
            "artifact_type": self.artifact_type,
            "summary": {
                "left_files": self.left_file_count,
                "right_files": self.right_file_count,
                "added_files": len(self.added_files),
                "removed_files": len(self.removed_files),
                "changed_files": len(self.changed_files),
                "unchanged_files": max(
                    0,
                    min(self.left_file_count, self.right_file_count)
                    - len(self.changed_files),
                ),
            },
            "files": {
                "added": [change.to_dict() for change in self.added_files],
                "removed": [change.to_dict() for change in self.removed_files],
                "changed": [change.to_dict() for change in self.changed_files],
            },
        }


def compare_rendered_exports(
    left_plan: dict[str, Any],
    left_brief: dict[str, Any],
    right_plan: dict[str, Any],
    right_brief: dict[str, Any],
    target: str,
) -> ExportDiffResult:
    """Render two plan/brief combinations and compare their normalized artifacts."""
    exporter = create_exporter(target)
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        left_path = _render_temporary_artifact(
            exporter,
            left_plan,
            left_brief,
            temp_root / "left",
        )
        right_path = _render_temporary_artifact(
            exporter,
            right_plan,
            right_brief,
            temp_root / "right",
        )

        left_artifact = _normalize_rendered_artifact(left_path)
        right_artifact = _normalize_rendered_artifact(right_path)

    return _diff_normalized_artifacts(
        target=target,
        left_plan_id=str(left_plan.get("id") or ""),
        right_plan_id=str(right_plan.get("id") or ""),
        left_artifact=left_artifact,
        right_artifact=right_artifact,
    )


@dataclass(frozen=True, slots=True)
class _NormalizedArtifact:
    artifact_type: str
    files: dict[str, str]

    @property
    def file_count(self) -> int:
        return len(self.files)


def _diff_normalized_artifacts(
    *,
    target: str,
    left_plan_id: str,
    right_plan_id: str,
    left_artifact: _NormalizedArtifact,
    right_artifact: _NormalizedArtifact,
) -> ExportDiffResult:
    """Compare two normalized artifacts as file maps."""
    left_paths = set(left_artifact.files)
    right_paths = set(right_artifact.files)

    added_paths = sorted(right_paths - left_paths)
    removed_paths = sorted(left_paths - right_paths)
    shared_paths = sorted(left_paths & right_paths)

    added_files = [
        ExportDiffFileChange(path=path, status="added", right=right_artifact.files[path])
        for path in added_paths
    ]
    removed_files = [
        ExportDiffFileChange(path=path, status="removed", left=left_artifact.files[path])
        for path in removed_paths
    ]
    changed_files = [
        _diff_file(path, left_artifact.files[path], right_artifact.files[path])
        for path in shared_paths
        if left_artifact.files[path] != right_artifact.files[path]
    ]

    artifact_type = (
        left_artifact.artifact_type
        if left_artifact.artifact_type == right_artifact.artifact_type
        else "mixed"
    )

    return ExportDiffResult(
        target=target,
        left_plan_id=left_plan_id,
        right_plan_id=right_plan_id,
        artifact_type=artifact_type,
        left_file_count=left_artifact.file_count,
        right_file_count=right_artifact.file_count,
        added_files=added_files,
        removed_files=removed_files,
        changed_files=changed_files,
    )


def _diff_file(path: str, left: str, right: str) -> ExportDiffFileChange:
    """Build a line-oriented diff for one normalized file."""
    diff = list(
        unified_diff(
            left.splitlines(),
            right.splitlines(),
            fromfile=f"{path} (left)",
            tofile=f"{path} (right)",
            lineterm="",
        )
    )
    return ExportDiffFileChange(
        path=path,
        status="changed",
        left=left,
        right=right,
        diff=diff,
    )


def _render_temporary_artifact(
    exporter,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
    output_root: Path,
) -> Path:
    """Write a temporary rendered artifact using the existing exporter."""
    extension = exporter.get_extension()
    if extension:
        output_path = output_root.with_suffix(extension)
    else:
        output_path = output_root

    return Path(exporter.export(execution_plan, implementation_brief, str(output_path)))


def _normalize_rendered_artifact(path: Path) -> _NormalizedArtifact:
    """Normalize a rendered artifact into a comparable file map."""
    if path.is_dir():
        files = {
            candidate.relative_to(path).as_posix(): _normalize_file(candidate)
            for candidate in sorted(
                (candidate for candidate in path.rglob("*") if candidate.is_file()),
                key=lambda candidate: candidate.relative_to(path).as_posix(),
            )
        }
        return _NormalizedArtifact(artifact_type="directory", files=files)

    return _NormalizedArtifact(artifact_type="file", files={"export": _normalize_file(path)})


def _normalize_file(path: Path) -> str:
    """Normalize one rendered file based on its file type."""
    suffix = path.suffix.lower()
    if suffix == ".json":
        return _normalize_json_text(path.read_text())
    if suffix == ".xml":
        return _normalize_xml_text(path.read_text())
    if suffix in {".md", ".markdown", ".mmd", ".txt"}:
        return _normalize_markdown_text(path.read_text())
    return _normalize_text(path.read_text())


def _normalize_json_text(text: str) -> str:
    """Canonicalize JSON by sorting keys and stripping volatile metadata."""
    payload = json.loads(text)
    payload = _strip_volatile_json_values(payload)
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _strip_volatile_json_values(value: Any) -> Any:
    """Remove volatile JSON keys recursively before comparison."""
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if key in VOLATILE_JSON_KEYS or key.endswith("_at"):
                continue
            normalized[key] = _strip_volatile_json_values(item)
        return normalized
    if isinstance(value, list):
        return [_strip_volatile_json_values(item) for item in value]
    return value


def _normalize_xml_text(text: str) -> str:
    """Canonicalize XML by sorting attributes and stripping ignorable whitespace."""
    root = ElementTree.fromstring(text)
    _normalize_xml_element(root)
    return ElementTree.tostring(root, encoding="unicode")


def _normalize_xml_element(element: ElementTree.Element) -> None:
    """Normalize one XML element and its descendants."""
    element.attrib = dict(sorted(element.attrib.items()))
    if element.text is not None:
        stripped_text = element.text.strip()
        element.text = stripped_text or None
    if element.tail is not None:
        stripped_tail = element.tail.strip()
        element.tail = stripped_tail or None
    for child in element:
        _normalize_xml_element(child)


def _normalize_markdown_text(text: str) -> str:
    """Normalize Markdown and Markdown-like text for stable comparisons."""
    lines = []
    for raw_line in _normalize_text(text).splitlines():
        stripped = raw_line.strip()
        if not stripped:
            lines.append("")
            continue
        if any(pattern.match(stripped) for pattern in MARKDOWN_BOILERPLATE_PATTERNS):
            continue
        lines.append(raw_line.rstrip())

    return _collapse_blank_lines(lines) + "\n"


def _normalize_text(text: str) -> str:
    """Normalize generic text by removing line-ending and trailing whitespace noise."""
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    return _collapse_blank_lines(lines).strip("\n")


def _collapse_blank_lines(lines: Iterable[str]) -> str:
    """Collapse consecutive blank lines and preserve a trailing newline-free string."""
    collapsed: list[str] = []
    previous_blank = False
    for line in lines:
        is_blank = not line.strip()
        if is_blank:
            if previous_blank:
                continue
            collapsed.append("")
        else:
            collapsed.append(line)
        previous_blank = is_blank
    return "\n".join(collapsed).strip("\n")
