import pytest
from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.importers.csv_backlog_importer import CsvBacklogImporter
from blueprint.store import Store, init_db


def test_csv_backlog_importer_imports_multiple_rows(tmp_path):
    csv_path = tmp_path / "backlog.csv"
    csv_path.write_text(
        "source_id,title,summary,domain,links,tags\n"
        "CSV-1,Import backlog,Normalize CSV rows,planning,https://example.com/1,import;csv\n"
        "CSV-2,Report counts,Show import summary,cli,https://example.com/2,cli\n"
    )

    source_briefs = CsvBacklogImporter().import_file(str(csv_path))

    assert len(source_briefs) == 2
    assert source_briefs[0]["title"] == "Import backlog"
    assert source_briefs[0]["summary"] == "Normalize CSV rows"
    assert source_briefs[0]["domain"] == "planning"
    assert source_briefs[0]["source_project"] == "csv-backlog"
    assert source_briefs[0]["source_entity_type"] == "backlog_row"
    assert source_briefs[0]["source_id"] == "CSV-1"
    assert source_briefs[0]["source_links"]["links"] == ["https://example.com/1"]
    assert source_briefs[0]["source_payload"]["normalized"]["tags"] == ["import", "csv"]
    assert source_briefs[1]["source_id"] == "CSV-2"


def test_csv_backlog_importer_accepts_custom_columns(tmp_path):
    csv_path = tmp_path / "variant.csv"
    csv_path.write_text(
        "Key,Name,Description,Area,URL,Labels\n"
        "PLAN-7,Custom backlog,Imported through custom headers,ops,"
        'https://example.com/spec,"ops,backlog"\n'
    )

    source_brief = CsvBacklogImporter(
        id_column="Key",
        title_column="Name",
        summary_column="Description",
        domain_column="Area",
        links_column="URL",
        tags_column="Labels",
    ).import_from_source(str(csv_path))

    assert source_brief["source_id"] == "PLAN-7"
    assert source_brief["title"] == "Custom backlog"
    assert source_brief["summary"] == "Imported through custom headers"
    assert source_brief["domain"] == "ops"
    assert source_brief["source_payload"]["normalized"]["tags"] == ["ops", "backlog"]


def test_csv_backlog_importer_reports_missing_required_columns(tmp_path):
    csv_path = tmp_path / "missing.csv"
    csv_path.write_text("source_id,title,domain\nCSV-1,Missing summary,planning\n")

    with pytest.raises(ImportError, match="Missing required CSV columns: summary"):
        CsvBacklogImporter().import_file(str(csv_path))


def test_csv_backlog_duplicate_handling_reuses_or_replaces_existing_brief(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    csv_path = tmp_path / "backlog.csv"
    importer = CsvBacklogImporter()

    csv_path.write_text("source_id,title,summary\nCSV-1,Original,Original summary\n")
    first_id = store.upsert_source_brief(importer.import_from_source(str(csv_path)))

    csv_path.write_text("source_id,title,summary\nCSV-1,Updated,Updated summary\n")
    skipped_id = store.upsert_source_brief(
        importer.import_from_source(str(csv_path)),
        skip_existing=True,
    )
    replaced_id = store.upsert_source_brief(
        importer.import_from_source(str(csv_path)),
        replace=True,
    )

    assert skipped_id == first_id
    assert replaced_id == first_id
    briefs = store.list_source_briefs(source_project="csv-backlog")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Updated"
    assert briefs[0]["summary"] == "Updated summary"


def test_cli_import_csv_backlog_reports_import_skip_and_replace(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    csv_path = tmp_path / "backlog.csv"
    csv_path.write_text(
        "source_id,title,summary\n"
        "CSV-1,First row,First summary\n"
        "CSV-1,Duplicate row,Duplicate summary\n"
        "CSV-2,Second row,Second summary\n"
    )

    runner = CliRunner()
    first = runner.invoke(cli, ["import", "csv-backlog", str(csv_path)])

    assert first.exit_code == 0, first.output
    assert "Imported: 2" in first.output
    assert "Skipped: 1" in first.output
    assert "Replaced: 0" in first.output

    csv_path.write_text(
        "source_id,title,summary\n"
        "CSV-1,First row updated,First summary updated\n"
        "CSV-2,Second row updated,Second summary updated\n"
    )
    replaced = runner.invoke(cli, ["import", "csv-backlog", str(csv_path), "--replace"])

    assert replaced.exit_code == 0, replaced.output
    assert "Imported: 0" in replaced.output
    assert "Skipped: 0" in replaced.output
    assert "Replaced: 2" in replaced.output

    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="csv-backlog")
    assert len(briefs) == 2
    assert {brief["title"] for brief in briefs} == {"First row updated", "Second row updated"}


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
