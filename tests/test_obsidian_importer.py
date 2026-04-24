import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.importers.obsidian_importer import ObsidianImporter, parse_obsidian_note
from blueprint.store import Store, init_db


def test_parse_obsidian_note_preserves_frontmatter_body_path_tags_aliases_and_links():
    source_brief = parse_obsidian_note(_obsidian_note(), file_path="vault/product/import.md")

    assert source_brief["title"] == "Obsidian Import Brief"
    assert source_brief["domain"] == "knowledge-management"
    assert source_brief["summary"] == "Import notes from an Obsidian vault."
    assert source_brief["source_project"] == "obsidian"
    assert source_brief["source_entity_type"] == "markdown_note"
    assert source_brief["source_id"].endswith("vault/product/import.md")
    assert source_brief["source_links"]["file_path"].endswith("vault/product/import.md")
    assert source_brief["source_links"]["source"] == "obsidian://open?vault=Team&file=import"
    assert source_brief["source_links"]["spec"] == "https://example.test/spec"
    payload = source_brief["source_payload"]
    assert payload["file_path"].endswith("vault/product/import.md")
    assert payload["frontmatter"]["title"] == "Obsidian Import Brief"
    assert payload["tags"] == ["blueprint", "import"]
    assert payload["aliases"] == ["Vault import", "Note import"]
    assert payload["body"].startswith("# Heading That Should Not Override")


def test_parse_obsidian_note_title_and_summary_fallbacks():
    source_brief = parse_obsidian_note(
        """---
tags: fallback
---
# Fallback Title

First paragraph becomes the summary.

## Details
More context.
""",
        file_path="vault/fallback-note.md",
    )

    assert source_brief["title"] == "Fallback Title"
    assert source_brief["summary"] == "First paragraph becomes the summary."
    assert source_brief["source_payload"]["tags"] == ["fallback"]


def test_cli_import_obsidian_note_reports_import_skip_and_replace(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    note_path = tmp_path / "vault" / "note.md"
    note_path.parent.mkdir()
    note_path.write_text(_obsidian_note())
    runner = CliRunner()

    first = runner.invoke(cli, ["import", "obsidian-note", str(note_path)])
    assert first.exit_code == 0, first.output
    assert "Imported source brief" in first.output
    assert "from Obsidian note" in first.output

    skipped = runner.invoke(
        cli,
        ["import", "obsidian-note", str(note_path), "--skip-existing"],
    )
    assert skipped.exit_code == 0, skipped.output
    assert "Skipped existing source brief" in skipped.output

    note_path.write_text(_obsidian_note(title="Updated Obsidian Import Brief"))
    replaced = runner.invoke(
        cli,
        ["import", "obsidian-note", str(note_path), "--replace"],
    )
    assert replaced.exit_code == 0, replaced.output
    assert "Replaced source brief" in replaced.output

    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="obsidian")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Updated Obsidian Import Brief"


def test_import_obsidian_dir_imports_matching_notes_and_reports_counts(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    vault = tmp_path / "vault"
    nested = vault / "nested"
    nested.mkdir(parents=True)
    (vault / "alpha.md").write_text(_obsidian_note(title="Alpha Note"))
    (vault / "ignored.txt").write_text(_obsidian_note(title="Ignored Note"))
    (nested / "beta.md").write_text(_obsidian_note(title="Beta Note"))

    result = CliRunner().invoke(cli, ["import", "obsidian-dir", str(vault), "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["counts"] == {
        "failed": 0,
        "imported": 1,
        "replaced": 0,
        "skipped": 0,
        "total": 1,
    }
    assert [record["relative_path"] for record in payload["files"]] == ["alpha.md"]
    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="obsidian")
    assert [brief["title"] for brief in briefs] == ["Alpha Note"]


def test_import_obsidian_dir_duplicate_handling_reports_skipped_and_replaced(
    tmp_path,
    monkeypatch,
):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    vault = tmp_path / "vault"
    vault.mkdir()
    note_path = vault / "note.md"
    note_path.write_text(_obsidian_note(title="Original Note"))
    runner = CliRunner()

    first = runner.invoke(cli, ["import", "obsidian-dir", str(vault), "--json"])
    skipped = runner.invoke(
        cli,
        ["import", "obsidian-dir", str(vault), "--skip-existing", "--json"],
    )
    note_path.write_text(_obsidian_note(title="Replacement Note"))
    replaced = runner.invoke(
        cli,
        ["import", "obsidian-dir", str(vault), "--replace", "--json"],
    )

    assert first.exit_code == 0, first.output
    assert skipped.exit_code == 0, skipped.output
    skipped_payload = json.loads(skipped.output)
    assert skipped_payload["counts"] == {
        "failed": 0,
        "imported": 0,
        "replaced": 0,
        "skipped": 1,
        "total": 1,
    }
    assert skipped_payload["files"][0]["status"] == "skipped"

    assert replaced.exit_code == 0, replaced.output
    replaced_payload = json.loads(replaced.output)
    assert replaced_payload["counts"] == {
        "failed": 0,
        "imported": 0,
        "replaced": 1,
        "skipped": 0,
        "total": 1,
    }
    assert replaced_payload["files"][0]["status"] == "replaced"
    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="obsidian")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Replacement Note"


def test_obsidian_importer_duplicate_handling_reuses_or_replaces_existing_brief(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    note_path = tmp_path / "note.md"
    note_path.write_text(_obsidian_note(title="Original Note"))
    importer = ObsidianImporter()

    first_id = store.upsert_source_brief(importer.import_from_source(str(note_path)))
    note_path.write_text(_obsidian_note(title="Updated Note"))
    skipped_id = store.upsert_source_brief(
        importer.import_from_source(str(note_path)),
        skip_existing=True,
    )
    replaced_id = store.upsert_source_brief(
        importer.import_from_source(str(note_path)),
        replace=True,
    )

    assert skipped_id == first_id
    assert replaced_id == first_id
    briefs = store.list_source_briefs(source_project="obsidian")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Updated Note"


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


def _obsidian_note(*, title: str = "Obsidian Import Brief") -> str:
    return f"""---
title: {title}
summary: Import notes from an Obsidian vault.
domain: knowledge-management
tags:
  - blueprint
  - import
aliases:
  - Vault import
  - Note import
source: obsidian://open?vault=Team&file=import
source_links:
  spec: https://example.test/spec
---
# Heading That Should Not Override

This body text is preserved for downstream brief generation.
"""
