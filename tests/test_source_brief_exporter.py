import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.source_brief import SourceBriefExporter
from blueprint.store import init_db


def test_source_brief_exporter_renders_deterministic_markdown():
    rendered = SourceBriefExporter().render_markdown(_source_brief())

    assert rendered.startswith("# Source Brief: Patient Intake Dashboard\n")
    assert "## Metadata" in rendered
    assert "- Source Brief ID: `sb-export`" in rendered
    assert "- Domain: healthcare" in rendered
    assert "## Source Identity" in rendered
    assert "- Source Project: manual" in rendered
    assert "- Source Entity Type: note" in rendered
    assert "- Source ID: `note-1`" in rendered
    assert "## Summary\nGive nurses a queue for reviewing patient intake forms." in rendered
    assert '## Source Links\n- path: "notes/patient-intake.md"' in rendered
    assert '- labels: ["intake", "nursing"]' in rendered
    assert '- raw_markdown: "# Patient Intake"' in rendered
    assert rendered == SourceBriefExporter().render_markdown(_source_brief())


def test_source_brief_exporter_renders_deterministic_json():
    rendered = SourceBriefExporter().render_json(_source_brief())

    assert rendered == SourceBriefExporter().render_json(_source_brief())
    payload = json.loads(rendered)
    assert list(payload) == [
        "created_at",
        "domain",
        "id",
        "source_entity_type",
        "source_id",
        "source_links",
        "source_payload",
        "source_project",
        "summary",
        "title",
        "updated_at",
    ]
    assert payload == {
        "created_at": None,
        "domain": "healthcare",
        "id": "sb-export",
        "source_entity_type": "note",
        "source_id": "note-1",
        "source_links": {"path": "notes/patient-intake.md"},
        "source_payload": {
            "labels": ["intake", "nursing"],
            "raw_markdown": "# Patient Intake",
            "title": "Patient Intake Dashboard",
        },
        "source_project": "manual",
        "summary": "Give nurses a queue for reviewing patient intake forms.",
        "title": "Patient Intake Dashboard",
        "updated_at": None,
    }


def test_source_export_cli_writes_markdown_file(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    output_path = tmp_path / "source.md"

    result = CliRunner().invoke(
        cli,
        [
            "source",
            "export",
            "sb-export",
            "--format",
            "markdown",
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output == ""
    assert output_path.read_text().startswith("# Source Brief: Patient Intake Dashboard\n")
    assert "## Source Payload" in output_path.read_text()


def test_source_export_cli_writes_json_file(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    output_path = tmp_path / "source.json"

    result = CliRunner().invoke(
        cli,
        ["source", "export", "sb-export", "--format", "json", "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert result.output == ""
    assert json.loads(output_path.read_text())["id"] == "sb-export"
    assert output_path.read_text() == SourceBriefExporter().render_json(
        json.loads(output_path.read_text())
    )


def test_source_export_cli_prints_to_stdout_without_output_path(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())

    result = CliRunner().invoke(cli, ["source", "export", "sb-export", "--format", "json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["id"] == "sb-export"
    assert payload["source_payload"]["labels"] == ["intake", "nursing"]


def test_source_export_cli_missing_source_brief_fails_clearly(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["source", "export", "sb-missing"])

    assert result.exit_code != 0
    assert "Error: Source brief not found: sb-missing" in result.output


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
"""
    )
    blueprint_config.reload_config()


def _source_brief():
    return {
        "id": "sb-export",
        "title": "Patient Intake Dashboard",
        "domain": "healthcare",
        "summary": "Give nurses a queue for reviewing patient intake forms.",
        "source_project": "manual",
        "source_entity_type": "note",
        "source_id": "note-1",
        "source_payload": {
            "title": "Patient Intake Dashboard",
            "raw_markdown": "# Patient Intake",
            "labels": ["intake", "nursing"],
        },
        "source_links": {"path": "notes/patient-intake.md"},
    }
