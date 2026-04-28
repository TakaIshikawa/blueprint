import json

import yaml
from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.importers.manual_importer import (
    ManualBriefImporter,
    parse_manual_brief_structured,
)
from blueprint.store import Store, init_db


def test_parse_manual_brief_structured_normalizes_json_document():
    source_brief = parse_manual_brief_structured(
        {
            "id": "manual/structured-json",
            "title": "Structured JSON Brief",
            "domain": "automation",
            "summary": "Structured source briefs should import cleanly.",
            "mvp_goal": "Accept JSON briefs from planning systems.",
            "scope": ["Parse JSON", "Preserve the original document"],
            "non_goals": "Generate plans",
            "assumptions": ["Input files are UTF-8"],
            "validation_plan": "Run importer tests.",
            "definition_of_done": ["JSON imports pass", "Payload is preserved"],
            "product_surface": "CLI",
            "source_links": {"spec": "https://example.test/spec"},
            "links": ["https://example.test/discovery"],
        },
        file_path="briefs/structured.json",
    )

    assert source_brief["title"] == "Structured JSON Brief"
    assert source_brief["domain"] == "automation"
    assert source_brief["summary"] == "Structured source briefs should import cleanly."
    assert source_brief["source_project"] == "manual"
    assert source_brief["source_entity_type"] == "structured_brief"
    assert source_brief["source_id"] == "manual/structured-json"
    assert source_brief["source_links"]["spec"] == "https://example.test/spec"
    assert source_brief["source_links"]["links"] == ["https://example.test/discovery"]
    assert source_brief["source_payload"]["document"]["title"] == "Structured JSON Brief"
    assert source_brief["source_payload"]["normalized"]["scope"] == [
        "Parse JSON",
        "Preserve the original document",
    ]
    assert source_brief["source_payload"]["normalized"]["non_goals"] == ["Generate plans"]
    assert source_brief["source_payload"]["normalized"]["definition_of_done"] == [
        "JSON imports pass",
        "Payload is preserved",
    ]


def test_manual_importer_accepts_yaml_file(tmp_path):
    brief_path = tmp_path / "structured.yaml"
    brief_path.write_text(
        yaml.safe_dump(
            {
                "source_id": "manual/structured-yaml",
                "title": "Structured YAML Brief",
                "summary": "YAML source briefs should import cleanly.",
                "area": "integrations",
                "goal": "Accept YAML briefs from planning systems.",
            },
            sort_keys=False,
        )
    )

    source_brief = ManualBriefImporter().import_from_source(str(brief_path))

    assert source_brief["title"] == "Structured YAML Brief"
    assert source_brief["domain"] == "integrations"
    assert source_brief["source_id"] == "manual/structured-yaml"
    assert source_brief["source_payload"]["document"]["goal"] == (
        "Accept YAML briefs from planning systems."
    )
    assert source_brief["source_payload"]["normalized"]["mvp_goal"] == (
        "Accept YAML briefs from planning systems."
    )


def test_cli_import_manual_routes_json_file(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    brief_path = tmp_path / "structured.json"
    brief_path.write_text(
        json.dumps(
            {
                "source_id": "manual/cli-json",
                "title": "CLI JSON Brief",
                "summary": "The manual import command accepts JSON.",
                "domain": "cli",
            }
        )
    )

    result = CliRunner().invoke(cli, ["import", "manual", str(brief_path)])

    assert result.exit_code == 0, result.output
    assert "Imported source brief" in result.output
    assert "CLI JSON Brief" in result.output
    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="manual")
    assert len(briefs) == 1
    assert briefs[0]["source_entity_type"] == "structured_brief"
    assert briefs[0]["source_payload"]["document"]["source_id"] == "manual/cli-json"


def test_cli_import_manual_rejects_structured_file_missing_title_without_traceback(
    tmp_path,
    monkeypatch,
):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    brief_path = tmp_path / "missing_title.json"
    brief_path.write_text(json.dumps({"summary": "A title is required."}))

    result = CliRunner().invoke(cli, ["import", "manual", str(brief_path)])

    assert result.exit_code != 0
    assert "must include a non-empty title" in result.output
    assert "Traceback" not in result.output
    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="manual")
    assert briefs == []


def test_cli_import_manual_rejects_malformed_structured_field_without_traceback(
    tmp_path,
    monkeypatch,
):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    brief_path = tmp_path / "malformed.yaml"
    brief_path.write_text(
        yaml.safe_dump(
            {
                "title": "Malformed YAML Brief",
                "summary": "Scope must not be an object.",
                "scope": {"bad": "shape"},
            },
            sort_keys=False,
        )
    )

    result = CliRunner().invoke(cli, ["import", "manual", str(brief_path)])

    assert result.exit_code != 0
    assert "field 'scope' must be a string or list" in result.output
    assert "Traceback" not in result.output
    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(source_project="manual")
    assert briefs == []


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
