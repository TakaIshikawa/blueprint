import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli


def test_config_inspect_json_shows_merged_config_without_secret(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "secret-test-key")

    db_dir = tmp_path / "db"
    db_dir.mkdir()
    source_db = tmp_path / "max.db"
    source_db.touch()
    export_dir = tmp_path / "exports"
    export_dir.mkdir()

    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {db_dir / "blueprint.db"}
sources:
  max:
    db_path: {source_db}
llm:
  provider: anthropic
  default_model: claude-sonnet-4-5
exports:
  output_dir: {export_dir}
  formats:
    codex: json
"""
    )
    blueprint_config.reload_config()

    result = CliRunner().invoke(cli, ["config", "inspect", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["config_path"] == ".blueprint.yaml"
    assert payload["values"]["database"]["path"] == str(db_dir / "blueprint.db")
    assert payload["values"]["sources"]["max"]["db_path"] == str(source_db)
    assert payload["values"]["llm"]["default_model"] == "claude-sonnet-4-5"
    assert payload["values"]["exports"]["formats"]["codex"] == "json"
    assert payload["values"]["exports"]["formats"]["relay"] == "json"
    assert payload["values"]["exports"]["formats"]["csv-tasks"] == "csv"
    assert payload["environment"]["ANTHROPIC_API_KEY"]["present"] is True
    assert payload["warnings"] == []
    assert "secret-test-key" not in result.output


def test_config_inspect_text_reports_validation_warnings(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "missing-db-dir" / "blueprint.db"}
sources:
  max:
    db_path: {tmp_path / "missing-max.db"}
llm:
  provider: anthropic
  default_model: ""
exports:
  output_dir: {tmp_path / "missing-exports"}
"""
    )
    blueprint_config.reload_config()

    result = CliRunner().invoke(cli, ["config", "inspect"])

    assert result.exit_code == 0, result.output
    assert "Blueprint configuration" in result.output
    assert "database.path:" in result.output
    assert "ANTHROPIC_API_KEY: missing" in result.output
    assert "Database path parent directory does not exist:" in result.output
    assert "Configured source 'max' path does not exist:" in result.output
    assert "Export directory does not exist:" in result.output
    assert "llm.default_model must be a non-empty string" in result.output
    assert "ANTHROPIC_API_KEY environment variable is not set" in result.output
