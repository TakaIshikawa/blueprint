import json

from click.testing import CliRunner

from blueprint.cli import cli
from blueprint.domain import AVAILABLE_SCHEMA_MODELS, get_all_model_json_schemas


def test_schema_export_execution_plan_emits_valid_json_schema():
    result = CliRunner().invoke(cli, ["schema", "export", "execution-plan"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["title"] == "ExecutionPlan"
    assert payload["type"] == "object"
    assert "properties" in payload
    assert "id" in payload["properties"]
    assert "tasks" in payload["properties"]


def test_schema_export_all_includes_every_exported_domain_model():
    result = CliRunner().invoke(cli, ["schema", "export", "all"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert set(payload) == set(AVAILABLE_SCHEMA_MODELS)
    assert payload["source-brief"]["title"] == "SourceBrief"
    assert payload["implementation-brief"]["title"] == "ImplementationBrief"
    assert payload["execution-plan"]["title"] == "ExecutionPlan"
    assert payload["execution-task"]["title"] == "ExecutionTask"
    assert payload["status-event"]["title"] == "StatusEvent"
    assert payload["export-record"]["title"] == "ExportRecord"


def test_schema_export_output_writes_file_and_creates_parents(tmp_path):
    output_path = tmp_path / "schemas" / "domain" / "all.json"

    result = CliRunner().invoke(
        cli,
        ["schema", "export", "all", "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert result.output == ""
    assert output_path.exists()
    assert json.loads(output_path.read_text()) == get_all_model_json_schemas()


def test_schema_export_rejects_unknown_model_with_clear_error():
    result = CliRunner().invoke(cli, ["schema", "export", "not-a-model"])

    assert result.exit_code != 0
    assert "Unknown schema model: not-a-model" in result.output
    assert "execution-plan" in result.output
    assert "all" in result.output
