"""JSON Lines source brief bulk importer."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from blueprint.domain import SourceBrief
from blueprint.store import Store


COMPARE_FIELDS = (
    "title",
    "domain",
    "summary",
    "source_payload",
    "source_links",
)


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True)
class SourceJsonlImportError:
    """One failed JSONL line with an actionable message."""

    line_number: int
    message: str


@dataclass(frozen=True)
class SourceJsonlImportRecord:
    """One successfully processed JSONL source brief."""

    line_number: int
    source_brief_id: str
    source_id: str
    title: str
    status: str


@dataclass
class SourceJsonlImportResult:
    """Summary of a source brief JSONL import."""

    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[SourceJsonlImportError] = field(default_factory=list)
    records: list[SourceJsonlImportRecord] = field(default_factory=list)
    total_lines: int = 0

    @property
    def error_count(self) -> int:
        """Return the number of failed lines."""
        return len(self.errors)

    @property
    def valid_count(self) -> int:
        """Return the number of valid source brief records."""
        return self.inserted + self.updated + self.skipped


class SourceJsonlImporter:
    """Import newline-delimited SourceBrief records into a store."""

    def import_file(
        self,
        file_path: str,
        store: Store,
        *,
        dry_run: bool = False,
        continue_on_error: bool = False,
        regenerate_missing_ids: bool = False,
    ) -> SourceJsonlImportResult:
        """Validate and import SourceBrief objects from a JSONL file."""
        path = Path(file_path).expanduser()
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError as exc:
            raise ImportError(f"Source brief JSONL file not found: {path}") from exc
        except OSError as exc:
            raise ImportError(f"Could not read source brief JSONL file: {path}") from exc

        result = SourceJsonlImportResult(total_lines=len(lines))
        validated: list[tuple[int, dict[str, Any]]] = []

        for line_number, line in enumerate(lines, start=1):
            source_brief, error = self._parse_line(
                line,
                line_number=line_number,
                regenerate_missing_ids=regenerate_missing_ids,
            )
            if error is not None:
                result.errors.append(error)
                if not continue_on_error:
                    break
                continue
            validated.append((line_number, source_brief))

        if result.errors and not continue_on_error:
            return result

        for line_number, source_brief in validated:
            existing_source_brief = store.get_source_brief_by_source(
                source_project=source_brief["source_project"],
                source_entity_type=source_brief["source_entity_type"],
                source_id=source_brief["source_id"],
            )

            if existing_source_brief is None:
                status = "inserted"
                source_brief_id = source_brief["id"]
                result.inserted += 1
                if not dry_run:
                    source_brief_id = store.upsert_source_brief(source_brief)
            elif self._matches_existing(source_brief, existing_source_brief):
                status = "skipped"
                source_brief_id = existing_source_brief["id"]
                result.skipped += 1
            else:
                status = "updated"
                source_brief_id = existing_source_brief["id"]
                result.updated += 1
                if not dry_run:
                    source_brief_id = store.upsert_source_brief(source_brief, replace=True)

            result.records.append(
                SourceJsonlImportRecord(
                    line_number=line_number,
                    source_brief_id=source_brief_id,
                    source_id=source_brief["source_id"],
                    title=source_brief["title"],
                    status=status,
                )
            )

        return result

    def _parse_line(
        self,
        line: str,
        *,
        line_number: int,
        regenerate_missing_ids: bool,
    ) -> tuple[dict[str, Any], None] | tuple[None, SourceJsonlImportError]:
        if not line.strip():
            return None, SourceJsonlImportError(
                line_number=line_number,
                message="empty line is not a SourceBrief JSON object",
            )

        try:
            payload = json.loads(line)
        except JSONDecodeError as exc:
            return None, SourceJsonlImportError(
                line_number=line_number,
                message=f"invalid JSON: {exc.msg}",
            )

        if not isinstance(payload, dict):
            return None, SourceJsonlImportError(
                line_number=line_number,
                message="expected a JSON object",
            )

        if regenerate_missing_ids and not payload.get("id"):
            payload = {**payload, "id": generate_source_brief_id()}

        try:
            source_brief = SourceBrief.model_validate(payload)
        except ValidationError as exc:
            return None, SourceJsonlImportError(
                line_number=line_number,
                message=f"validation failed: {_format_validation_error(exc)}",
            )

        return source_brief.model_dump(mode="python", exclude_none=True), None

    def _matches_existing(
        self,
        source_brief: dict[str, Any],
        existing_source_brief: dict[str, Any],
    ) -> bool:
        return all(
            source_brief.get(field) == existing_source_brief.get(field) for field in COMPARE_FIELDS
        )


def _format_validation_error(exc: ValidationError) -> str:
    messages: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"])
        messages.append(f"{location}: {error['msg']}")
    return "; ".join(messages)
