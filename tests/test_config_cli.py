import json
import stat

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli


def test_config_inspect_redacts_secret_values(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "env-secret")

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
  github:
    token_env: CUSTOM_GITHUB_TOKEN
    api_key: yaml-secret
llm:
  provider: anthropic
  default_model: claude-sonnet-4-5
exports:
  output_dir: {export_dir}
"""
    )
    blueprint_config.reload_config()

    result = CliRunner().invoke(cli, ["config", "inspect", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["values"]["sources"]["github"]["token_env"] == "CUSTOM_GITHUB_TOKEN"
    assert payload["values"]["sources"]["github"]["api_key"] == "[redacted]"
    assert "yaml-secret" not in result.output
    assert "env-secret" not in result.output


def test_config_validate_json_reports_passes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "env-secret")

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
  graph:
    path: {tmp_path}
llm:
  provider: anthropic
  default_model: claude-sonnet-4-5
exports:
  output_dir: {export_dir}
"""
    )
    blueprint_config.reload_config()

    result = CliRunner().invoke(cli, ["config", "validate", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["valid"] is True
    assert {check["status"] for check in payload["checks"]} == {"pass"}
    assert any(check["name"] == "exports.output_dir" for check in payload["checks"])


def test_config_validate_exits_nonzero_for_invalid_paths(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "env-secret")

    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "missing-db-dir" / "blueprint.db"}
sources:
  max:
    db_path: {tmp_path / "missing-max.db"}
llm:
  provider: anthropic
  default_model: claude-sonnet-4-5
exports:
  output_dir: {tmp_path / "missing-exports"}
"""
    )
    blueprint_config.reload_config()

    result = CliRunner().invoke(cli, ["config", "validate"])

    assert result.exit_code == 1, result.output
    assert "[FAIL] database.path: Database path parent directory does not exist:" in result.output
    assert "[FAIL] sources.max: Configured source 'max' path does not exist:" in result.output
    assert "[FAIL] exports.output_dir: Export directory does not exist:" in result.output


def test_config_validate_checks_export_directory_writability(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "env-secret")

    db_dir = tmp_path / "db"
    db_dir.mkdir()
    source_db = tmp_path / "max.db"
    source_db.touch()
    export_dir = tmp_path / "exports"
    export_dir.mkdir()
    export_dir.chmod(stat.S_IREAD | stat.S_IEXEC)

    try:
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
"""
        )
        blueprint_config.reload_config()

        result = CliRunner().invoke(cli, ["config", "validate", "--json"])
    finally:
        export_dir.chmod(stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

    assert result.exit_code == 1, result.output
    payload = json.loads(result.output)
    export_check = next(
        check for check in payload["checks"] if check["name"] == "exports.output_dir"
    )
    assert export_check["status"] == "fail"
    assert "Export directory is not writable:" in export_check["message"]
