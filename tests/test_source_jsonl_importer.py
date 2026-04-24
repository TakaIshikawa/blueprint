import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.importers.source_jsonl_importer import SourceJsonlImporter
from blueprint.store import Store, init_db


def test_source_jsonl_imports_valid_multiline_records_idempotently(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    jsonl_path = tmp_path / "source_briefs.jsonl"
    _write_jsonl(jsonl_path, [_source_brief("sb-one", "src-1"), _source_brief("sb-two", "src-2")])

    first = SourceJsonlImporter().import_file(str(jsonl_path), store)
    second = SourceJsonlImporter().import_file(str(jsonl_path), store)

    assert first.inserted == 2
    assert first.updated == 0
    assert first.skipped == 0
    assert second.inserted == 0
    assert second.updated == 0
    assert second.skipped == 2
    assert len(store.list_source_briefs(source_project="portable")) == 2


def test_source_jsonl_reports_invalid_json_with_line_number(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    jsonl_path = tmp_path / "source_briefs.jsonl"
    jsonl_path.write_text(
        json.dumps(_source_brief("sb-one", "src-1")) + "\n" + '{"id": "broken"\n',
        encoding="utf-8",
    )

    result = SourceJsonlImporter().import_file(str(jsonl_path), store)

    assert result.inserted == 0
    assert result.error_count == 1
    assert result.errors[0].line_number == 2
    assert "invalid JSON" in result.errors[0].message
    assert store.list_source_briefs() == []


def test_source_jsonl_reports_validation_errors_with_line_number(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    invalid = _source_brief("sb-one", "src-1")
    invalid.pop("summary")
    jsonl_path = tmp_path / "source_briefs.jsonl"
    _write_jsonl(jsonl_path, [invalid])

    result = SourceJsonlImporter().import_file(str(jsonl_path), store)

    assert result.error_count == 1
    assert result.errors[0].line_number == 1
    assert "summary" in result.errors[0].message
    assert store.list_source_briefs() == []


def test_source_jsonl_dry_run_validates_without_mutating_database(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    jsonl_path = tmp_path / "source_briefs.jsonl"
    _write_jsonl(jsonl_path, [_source_brief("sb-one", "src-1")])

    result = SourceJsonlImporter().import_file(str(jsonl_path), store, dry_run=True)

    assert result.inserted == 1
    assert result.error_count == 0
    assert store.list_source_briefs() == []


def test_source_jsonl_updates_changed_existing_records(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief("sb-original", "src-1"))
    changed = _source_brief("sb-replacement", "src-1")
    changed["title"] = "Changed Portable Brief"
    jsonl_path = tmp_path / "source_briefs.jsonl"
    _write_jsonl(jsonl_path, [changed])

    result = SourceJsonlImporter().import_file(str(jsonl_path), store)

    assert result.inserted == 0
    assert result.updated == 1
    assert result.records[0].source_brief_id == "sb-original"
    assert store.get_source_brief("sb-original")["title"] == "Changed Portable Brief"
    assert store.get_source_brief("sb-replacement") is None


def test_source_jsonl_can_regenerate_missing_ids(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    missing_id = _source_brief("sb-one", "src-1")
    missing_id.pop("id")
    jsonl_path = tmp_path / "source_briefs.jsonl"
    _write_jsonl(jsonl_path, [missing_id])

    result = SourceJsonlImporter().import_file(
        str(jsonl_path),
        store,
        regenerate_missing_ids=True,
    )

    assert result.inserted == 1
    assert result.records[0].source_brief_id.startswith("sb-")
    assert len(store.list_source_briefs()) == 1


def test_source_jsonl_continue_on_error_imports_valid_records(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    invalid = _source_brief("sb-bad", "src-bad")
    invalid["title"] = ""
    jsonl_path = tmp_path / "source_briefs.jsonl"
    _write_jsonl(jsonl_path, [_source_brief("sb-one", "src-1"), invalid])

    result = SourceJsonlImporter().import_file(
        str(jsonl_path),
        store,
        continue_on_error=True,
    )

    assert result.inserted == 1
    assert result.error_count == 1
    assert result.errors[0].line_number == 2
    assert len(store.list_source_briefs(source_project="portable")) == 1


def test_import_source_jsonl_cli_prints_summary(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    jsonl_path = tmp_path / "source_briefs.jsonl"
    _write_jsonl(jsonl_path, [_source_brief("sb-one", "src-1")])

    result = CliRunner().invoke(cli, ["import", "source-jsonl", str(jsonl_path)])

    assert result.exit_code == 0, result.output
    assert "Importing source JSONL from:" in result.output
    assert "Inserted: 1" in result.output
    assert "Updated: 0" in result.output
    assert "Skipped: 0" in result.output
    assert "Errors: 0" in result.output
    assert "line 1: inserted src-1 [sb-one] Portable Brief" in result.output
    assert len(Store(str(tmp_path / "blueprint.db")).list_source_briefs()) == 1


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
""",
        encoding="utf-8",
    )
    blueprint_config.reload_config()


def _write_jsonl(path, records):
    path.write_text(
        "".join(json.dumps(record, default=str) + "\n" for record in records),
        encoding="utf-8",
    )


def _source_brief(brief_id, source_id):
    return {
        "id": brief_id,
        "title": "Portable Brief",
        "domain": "testing",
        "summary": "A portable source brief for JSONL import.",
        "source_project": "portable",
        "source_entity_type": "source_brief",
        "source_id": source_id,
        "source_payload": {"source_id": source_id},
        "source_links": {"href": f"https://example.test/{source_id}"},
    }
