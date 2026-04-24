import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import Store, init_db


def test_import_manual_dir_imports_matching_markdown_files_only(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    briefs_dir = tmp_path / "briefs"
    nested_dir = briefs_dir / "nested"
    nested_dir.mkdir(parents=True)
    (briefs_dir / "alpha.md").write_text(_manual_markdown("Alpha Brief"))
    (briefs_dir / "beta.txt").write_text(_manual_markdown("Ignored Brief"))
    (nested_dir / "nested.md").write_text(_manual_markdown("Nested Brief"))

    result = CliRunner().invoke(cli, ["import", "manual-dir", str(briefs_dir)])

    assert result.exit_code == 0, result.output
    assert "Imported: 1" in result.output
    assert "Skipped: 0" in result.output
    assert "Failed: 0" in result.output
    assert "Total: 1" in result.output
    assert "alpha.md" in result.output
    assert "nested.md" not in result.output

    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="manual")
    assert [brief["title"] for brief in briefs] == ["Alpha Brief"]


def test_import_manual_dir_recursive_json_reports_deterministic_per_file_statuses(
    tmp_path,
    monkeypatch,
):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    briefs_dir = tmp_path / "briefs"
    nested_dir = briefs_dir / "nested"
    nested_dir.mkdir(parents=True)
    (briefs_dir / "zeta.md").write_text(_manual_markdown("Zeta Brief"))
    (briefs_dir / "alpha.md").write_text(_manual_markdown("Alpha Brief"))
    (nested_dir / "beta.md").write_text(_manual_markdown("Beta Brief"))

    result = CliRunner().invoke(
        cli,
        ["import", "manual-dir", str(briefs_dir), "--recursive", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["counts"] == {
        "failed": 0,
        "imported": 3,
        "skipped": 0,
        "total": 3,
    }
    assert [record["relative_path"] for record in payload["files"]] == [
        "alpha.md",
        "nested/beta.md",
        "zeta.md",
    ]
    assert [record["status"] for record in payload["files"]] == [
        "imported",
        "imported",
        "imported",
    ]
    assert all(record["source_brief_id"] for record in payload["files"])


def test_import_manual_dir_continues_after_file_failure(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    briefs_dir = tmp_path / "briefs"
    briefs_dir.mkdir()
    (briefs_dir / "bad.md").write_bytes(b"\xff\xfe\xfd")
    (briefs_dir / "good.md").write_text(_manual_markdown("Good Brief"))

    result = CliRunner().invoke(
        cli,
        ["import", "manual-dir", str(briefs_dir), "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["counts"] == {
        "failed": 1,
        "imported": 1,
        "skipped": 0,
        "total": 2,
    }
    assert [record["relative_path"] for record in payload["files"]] == [
        "bad.md",
        "good.md",
    ]
    assert payload["files"][0]["status"] == "failed"
    assert payload["files"][0]["source_brief_id"] is None
    assert payload["files"][0]["error"]
    assert payload["files"][1]["status"] == "imported"
    assert payload["files"][1]["source_brief_id"]


def test_import_manual_dir_skip_existing_reports_skipped_status(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    briefs_dir = tmp_path / "briefs"
    briefs_dir.mkdir()
    brief_path = briefs_dir / "manual.md"
    brief_path.write_text(_manual_markdown("Original Brief"))
    runner = CliRunner()

    first = runner.invoke(cli, ["import", "manual-dir", str(briefs_dir)])
    second = runner.invoke(
        cli,
        ["import", "manual-dir", str(briefs_dir), "--skip-existing", "--json"],
    )

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    payload = json.loads(second.output)
    assert payload["counts"] == {
        "failed": 0,
        "imported": 0,
        "skipped": 1,
        "total": 1,
    }
    assert payload["files"][0]["status"] == "skipped"
    assert payload["files"][0]["source_brief_id"]


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


def _manual_markdown(title: str) -> str:
    return f"""---
title: {title}
domain: operations
---
# {title}

## Problem Statement
{title} needs a reliable path into Blueprint.

## MVP Goal
Import this markdown brief into the source brief store.

## Scope
- Read markdown from disk
- Normalize the brief sections

## Validation Plan
Run pytest.
"""
