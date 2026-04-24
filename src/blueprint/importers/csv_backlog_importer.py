"""CSV backlog importer."""

from __future__ import annotations

import csv
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from blueprint.importers.base import SourceImporter


DEFAULT_TITLE_COLUMN = "title"
DEFAULT_SUMMARY_COLUMN = "summary"
DEFAULT_DOMAIN_COLUMN = "domain"
DEFAULT_ID_COLUMN = "source_id"
DEFAULT_LINKS_COLUMN = "links"
DEFAULT_TAGS_COLUMN = "tags"


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True)
class CsvBacklogColumns:
    """Column names used to normalize backlog CSV rows."""

    title: str = DEFAULT_TITLE_COLUMN
    summary: str = DEFAULT_SUMMARY_COLUMN
    domain: str = DEFAULT_DOMAIN_COLUMN
    source_id: str = DEFAULT_ID_COLUMN
    links: str = DEFAULT_LINKS_COLUMN
    tags: str = DEFAULT_TAGS_COLUMN


class CsvBacklogImporter(SourceImporter):
    """Import backlog rows from CSV files."""

    def __init__(
        self,
        *,
        title_column: str = DEFAULT_TITLE_COLUMN,
        summary_column: str = DEFAULT_SUMMARY_COLUMN,
        domain_column: str = DEFAULT_DOMAIN_COLUMN,
        id_column: str = DEFAULT_ID_COLUMN,
        links_column: str = DEFAULT_LINKS_COLUMN,
        tags_column: str = DEFAULT_TAGS_COLUMN,
    ) -> None:
        self.columns = CsvBacklogColumns(
            title=title_column,
            summary=summary_column,
            domain=domain_column,
            source_id=id_column,
            links=links_column,
            tags=tags_column,
        )

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Read a CSV backlog file and return the first normalized SourceBrief."""
        source_briefs = self.import_file(source_id)
        if not source_briefs:
            raise ImportError(f"CSV backlog has no importable rows: {source_id}")
        return source_briefs[0]

    def import_file(self, source_id: str) -> list[dict[str, Any]]:
        """Read and normalize all backlog rows from a CSV file."""
        path = Path(source_id).expanduser()
        try:
            with path.open(newline="", encoding="utf-8-sig") as csv_file:
                reader = csv.DictReader(csv_file)
                _validate_required_columns(reader.fieldnames, self.columns)
                rows = list(reader)
        except FileNotFoundError as exc:
            raise ImportError(f"CSV backlog file not found: {path}") from exc
        except OSError as exc:
            raise ImportError(f"Could not read CSV backlog file: {path}") from exc
        except csv.Error as exc:
            raise ImportError(f"CSV backlog file is invalid CSV: {path}") from exc

        resolved_path = str(path.resolve())
        source_briefs: list[dict[str, Any]] = []
        for index, row in enumerate(rows, start=2):
            if _is_empty_row(row):
                continue
            source_briefs.append(
                parse_csv_backlog_row(
                    row,
                    columns=self.columns,
                    file_path=resolved_path,
                    row_number=index,
                )
            )
        return source_briefs

    def validate_source(self, source_id: str) -> bool:
        """Check whether a CSV backlog file is readable and valid."""
        try:
            self.import_file(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """CSV backlog imports are file-based and do not support discovery."""
        return []


def parse_csv_backlog_row(
    row: dict[str, Any],
    *,
    columns: CsvBacklogColumns | None = None,
    file_path: str | None = None,
    row_number: int | None = None,
) -> dict[str, Any]:
    """Normalize a CSV backlog row into a SourceBrief dictionary."""
    columns = columns or CsvBacklogColumns()
    title = _required_cell(row, columns.title, row_number=row_number)
    summary = _required_cell(row, columns.summary, row_number=row_number)
    source_id = _required_cell(row, columns.source_id, row_number=row_number)
    domain = _optional_cell(row, columns.domain)
    links = _split_multi_value(_optional_cell(row, columns.links))
    tags = _split_multi_value(_optional_cell(row, columns.tags))

    source_links: dict[str, Any] = {}
    if file_path:
        source_links["file_path"] = file_path
    if row_number is not None:
        source_links["row_number"] = row_number
    if links:
        source_links["links"] = links

    now = datetime.utcnow()
    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "csv-backlog",
        "source_entity_type": "backlog_row",
        "source_id": source_id,
        "source_payload": {
            "row": dict(row),
            "columns": {
                "title": columns.title,
                "summary": columns.summary,
                "domain": columns.domain,
                "source_id": columns.source_id,
                "links": columns.links,
                "tags": columns.tags,
            },
            "normalized": {
                "title": title,
                "summary": summary,
                "domain": domain,
                "source_id": source_id,
                "links": links,
                "tags": tags,
            },
        },
        "source_links": source_links,
        "created_at": now,
        "updated_at": now,
    }


def _validate_required_columns(
    fieldnames: list[str] | None,
    columns: CsvBacklogColumns,
) -> None:
    if not fieldnames:
        raise ImportError("CSV backlog file must include a header row")

    available = set(fieldnames)
    required = (columns.title, columns.summary, columns.source_id)
    missing = [column for column in required if column not in available]
    if missing:
        raise ImportError(f"Missing required CSV columns: {', '.join(missing)}")


def _required_cell(row: dict[str, Any], column: str, *, row_number: int | None) -> str:
    value = _optional_cell(row, column)
    if value:
        return value

    prefix = f"Row {row_number}: " if row_number is not None else ""
    raise ValueError(f"{prefix}CSV column '{column}' must be a non-empty value")


def _optional_cell(row: dict[str, Any], column: str) -> str | None:
    value = row.get(column)
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _split_multi_value(value: str | None) -> list[str]:
    if not value:
        return []

    normalized = value.replace("\n", ",").replace(";", ",")
    return [part.strip() for part in normalized.split(",") if part.strip()]


def _is_empty_row(row: dict[str, Any]) -> bool:
    return all(str(value or "").strip() == "" for value in row.values())
