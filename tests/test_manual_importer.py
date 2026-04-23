from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.importers.manual_importer import (
    ManualBriefImporter,
    parse_manual_brief_markdown,
)
from blueprint.store import Store, init_db


def test_parse_manual_brief_markdown_extracts_sections_and_metadata():
    source_brief = parse_manual_brief_markdown(_manual_markdown(), file_path="briefs/manual.md")

    assert source_brief["title"] == "Manual Import Brief"
    assert source_brief["domain"] == "operations"
    assert source_brief["summary"] == "Manual imports need a reliable path into Blueprint."
    assert source_brief["source_project"] == "manual"
    assert source_brief["source_entity_type"] == "markdown_brief"
    assert source_brief["source_id"].endswith("briefs/manual.md")
    assert source_brief["source_links"]["file_path"].endswith("briefs/manual.md")
    assert source_brief["source_payload"]["raw_markdown"].startswith("---\n")
    assert source_brief["source_payload"]["normalized"]["mvp_goal"] == (
        "Import a markdown brief into the source brief store."
    )
    assert source_brief["source_payload"]["normalized"]["scope"] == [
        "Read markdown from disk",
        "Parse optional YAML front matter",
        "Normalize the brief sections",
    ]
    assert source_brief["source_payload"]["normalized"]["non_goals"] == [
        "Generate an implementation brief",
    ]
    assert source_brief["source_payload"]["normalized"]["assumptions"] == [
        "The file is UTF-8 encoded",
    ]
    assert source_brief["source_payload"]["normalized"]["validation_plan"] == (
        "Validate with pytest and inspect the imported record."
    )
    assert source_brief["source_payload"]["normalized"]["definition_of_done"] == [
        "The brief is stored in Blueprint",
        "The raw markdown is preserved",
    ]
    assert source_brief["source_payload"]["normalized"]["product_surface"] == "CLI"
    assert (
        source_brief["source_payload"]["normalized"]["source_metadata"]["frontmatter"]["owner"]
        == "design-systems"
    )


def test_manual_importer_duplicate_handling_reuses_or_replaces_existing_brief(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    brief_path = tmp_path / "manual.md"
    brief_path.write_text(_manual_markdown())
    importer = ManualBriefImporter()

    first_id = store.upsert_source_brief(importer.import_from_source(str(brief_path)))

    brief_path.write_text(_manual_markdown(title="Updated Manual Import Brief"))
    skipped_id = store.upsert_source_brief(
        importer.import_from_source(str(brief_path)),
        skip_existing=True,
    )
    replaced_id = store.upsert_source_brief(
        importer.import_from_source(str(brief_path)),
        replace=True,
    )

    assert skipped_id == first_id
    assert replaced_id == first_id
    briefs = store.list_source_briefs(source_project="manual")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Updated Manual Import Brief"


def test_cli_import_manual_reports_import_skip_and_replace(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    brief_path = tmp_path / "manual.md"
    brief_path.write_text(_manual_markdown())

    runner = CliRunner()

    first = runner.invoke(cli, ["import", "manual", str(brief_path)])
    assert first.exit_code == 0, first.output
    assert "Imported source brief" in first.output
    assert "from manual brief" in first.output

    skipped = runner.invoke(
        cli,
        ["import", "manual", str(brief_path), "--skip-existing"],
    )
    assert skipped.exit_code == 0, skipped.output
    assert "Skipped existing source brief" in skipped.output

    brief_path.write_text(_manual_markdown(title="Updated Manual Import Brief"))
    replaced = runner.invoke(
        cli,
        ["import", "manual", str(brief_path), "--replace"],
    )
    assert replaced.exit_code == 0, replaced.output
    assert "Replaced source brief" in replaced.output

    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="manual")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Updated Manual Import Brief"


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path}
"""
    )
    blueprint_config.reload_config()


def _manual_markdown(*, title: str = "Manual Import Brief") -> str:
    return f"""---
title: {title}
domain: operations
owner: design-systems
product_surface: CLI
---
# {title}

## Problem Statement
Manual imports need a reliable path into Blueprint.

## MVP Goal
Import a markdown brief into the source brief store.

## Scope
- Read markdown from disk
- Parse optional YAML front matter
- Normalize the brief sections

## Non-Goals
- Generate an implementation brief

## Assumptions
- The file is UTF-8 encoded

## Validation Plan
Validate with pytest and inspect the imported record.

## Definition of Done
- The brief is stored in Blueprint
- The raw markdown is preserved
"""
