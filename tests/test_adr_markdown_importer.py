from blueprint.domain import SourceBrief
from blueprint.importers import AdrMarkdownImporter
from blueprint.importers.adr_markdown_importer import parse_adr_markdown


def test_parse_adr_markdown_preserves_sections_frontmatter_links_and_normalized_payload():
    source_brief = parse_adr_markdown(_adr_markdown(), file_path="docs/adr/0001-api.md")

    assert SourceBrief.model_validate(source_brief)
    assert source_brief["id"] == "sb-adr-d8c51e3ed733"
    assert source_brief["title"] == "Use JSONL execution events"
    assert source_brief["domain"] == "execution"
    assert source_brief["summary"] == "Write one event per line so imports can stream safely."
    assert source_brief["source_project"] == "adr"
    assert source_brief["source_entity_type"] == "architecture_decision_record"
    assert source_brief["source_id"] == "adr/0001-api"
    assert source_brief["source_links"]["file_path"].endswith("docs/adr/0001-api.md")
    assert source_brief["source_links"]["spec"] == "https://example.test/spec"
    assert source_brief["source_links"]["links"] == [
        "https://example.test/design",
        "../0000-record-template.md",
    ]

    payload = source_brief["source_payload"]
    assert payload["frontmatter"]["owner"] == "platform"
    assert payload["frontmatter"]["tags"] == ["execution", "events"]
    assert payload["status"] == "Accepted"
    assert payload["context"] == "Execution events need to be append-only and easy to inspect."
    assert payload["decision"] == "Write one event per line so imports can stream safely."
    assert payload["consequences"] == (
        "- Recovery can resume from the last valid line.\n"
        "- Consumers must tolerate partial files."
    )
    assert payload["alternatives"] == "- Store one large JSON document.\n- Emit SQLite rows directly."
    assert payload["normalized"]["status"] == "Accepted"
    assert payload["normalized"]["source_links"] == source_brief["source_links"]


def test_adr_markdown_importer_imports_directory_in_filename_order(tmp_path):
    directory = tmp_path / "adr"
    directory.mkdir()
    (directory / "002-second.md").write_text(_adr_markdown(title="Second ADR"), encoding="utf-8")
    (directory / "001-first.md").write_text(_adr_markdown(title="First ADR"), encoding="utf-8")
    (directory / "ignored.txt").write_text(_adr_markdown(title="Ignored"), encoding="utf-8")

    source_briefs = AdrMarkdownImporter().import_path(directory)

    assert [brief["title"] for brief in source_briefs] == ["First ADR", "Second ADR"]
    assert [brief["source_id"] for brief in source_briefs] == [
        "adr/001-first",
        "adr/002-second",
    ]
    assert AdrMarkdownImporter(directory).list_available() == [
        {
            "id": "adr/001-first",
            "title": "First ADR",
            "metadata": {
                "status": "Accepted",
                "file_path": str((directory / "001-first.md").resolve()),
            },
        },
        {
            "id": "adr/002-second",
            "title": "Second ADR",
            "metadata": {
                "status": "Accepted",
                "file_path": str((directory / "002-second.md").resolve()),
            },
        },
    ]


def test_adr_markdown_importer_imports_single_file_and_validates_source(tmp_path):
    adr_path = tmp_path / "0003-store-decisions.md"
    adr_path.write_text(_adr_markdown(title="Store Decisions"), encoding="utf-8")
    importer = AdrMarkdownImporter()

    source_brief = importer.import_from_source(str(adr_path))

    assert source_brief["title"] == "Store Decisions"
    assert source_brief["source_id"] == "adr/0003-store-decisions"
    assert importer.validate_source(str(adr_path)) is True
    assert importer.validate_source(str(tmp_path / "missing.md")) is False


def test_blueprint_importers_lazy_export_exposes_adr_markdown_importer():
    assert AdrMarkdownImporter.__name__ == "AdrMarkdownImporter"


def _adr_markdown(*, title: str = "Use JSONL execution events") -> str:
    return f"""---
title: {title}
domain: execution
owner: platform
tags:
  - execution
  - events
source_links:
  spec: https://example.test/spec
links:
  - https://example.test/design
---
# {title}

## Status
Accepted

## Context
Execution events need to be append-only and easy to inspect.

## Decision
Write one event per line so imports can stream safely.

## Consequences
- Recovery can resume from the last valid line.
- Consumers must tolerate partial files.

## Alternatives
- Store one large JSON document.
- Emit SQLite rows directly.

## Links
- [Template](../0000-record-template.md)
"""
