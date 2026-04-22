import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.importers.graph_importer import GraphImporter, parse_graph_node_json
from blueprint.store import Store, init_db


def test_parse_graph_node_json_normalizes_source_brief():
    payload = _graph_node_payload()

    source_brief = parse_graph_node_json(payload, file_path="/tmp/node.json")

    assert source_brief["title"] == "Graph Importer"
    assert source_brief["domain"] == "developer-tools"
    assert source_brief["summary"] == "Import Graph node exports into Blueprint."
    assert source_brief["source_project"] == "graph"
    assert source_brief["source_entity_type"] == "node"
    assert source_brief["source_id"] == "node-123"
    assert source_brief["source_payload"]["node"] == payload
    assert source_brief["source_payload"]["normalized"]["tags"] == ["importer", "graph"]
    assert source_brief["source_payload"]["normalized"]["properties"] == {
        "priority": "high",
    }
    assert source_brief["source_links"]["file_path"] == "/tmp/node.json"
    assert source_brief["source_links"]["links"] == [
        {"url": "https://example.com/spec", "title": "Spec"},
    ]


def test_parse_graph_node_json_derives_summary_from_description_then_body():
    described = _graph_node_payload(summary=None, description="Description text")
    body_only = _graph_node_payload(summary=None, description=None)

    assert parse_graph_node_json(described)["summary"] == "Description text"
    assert parse_graph_node_json(body_only)["summary"] == "Full body content"


def test_graph_importer_validation_accepts_valid_json_and_rejects_invalid_sources(tmp_path):
    valid_path = _write_graph_node(tmp_path, _graph_node_payload())
    missing_id_path = _write_graph_node(tmp_path, {"title": "Missing ID"}, name="missing.json")
    invalid_path = tmp_path / "invalid.json"
    invalid_path.write_text("{not-json")

    importer = GraphImporter()

    assert importer.validate_source(str(valid_path)) is True
    assert importer.validate_source(str(missing_id_path)) is False
    assert importer.validate_source(str(invalid_path)) is False
    assert importer.validate_source(str(tmp_path / "missing.json")) is False


def test_graph_importer_duplicate_handling_reuses_or_replaces_existing_brief(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    first_path = _write_graph_node(tmp_path, _graph_node_payload(title="Original"))
    second_path = _write_graph_node(
        tmp_path,
        _graph_node_payload(title="Updated", summary="Updated summary"),
        name="updated.json",
    )
    importer = GraphImporter()

    first_id = store.upsert_source_brief(importer.import_from_source(str(first_path)))
    skipped_id = store.upsert_source_brief(
        importer.import_from_source(str(second_path)),
        skip_existing=True,
    )
    replaced_id = store.upsert_source_brief(
        importer.import_from_source(str(second_path)),
        replace=True,
    )

    assert skipped_id == first_id
    assert replaced_id == first_id
    briefs = store.list_source_briefs(source_project="graph")
    assert len(briefs) == 1
    assert briefs[0]["title"] == "Updated"
    assert briefs[0]["summary"] == "Updated summary"


def test_cli_import_graph_node_stores_source_brief(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    node_path = _write_graph_node(tmp_path, _graph_node_payload())

    result = CliRunner().invoke(cli, ["import", "graph-node", str(node_path)])

    assert result.exit_code == 0, result.output
    assert "Imported source brief" in result.output
    assert "Graph node node-123" in result.output

    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(
        source_project="graph"
    )
    assert len(briefs) == 1
    assert briefs[0]["source_id"] == "node-123"
    assert briefs[0]["title"] == "Graph Importer"


def test_cli_import_graph_node_rejects_conflicting_duplicate_options(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["import", "graph-node", "node.json", "--replace", "--skip-existing"],
    )

    assert result.exit_code != 0
    assert "--replace and --skip-existing cannot be used together" in result.output


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
sources:
  graph:
    path: {tmp_path}
exports:
  output_dir: {tmp_path}
"""
    )
    blueprint_config.reload_config()


def _write_graph_node(tmp_path, payload, name="node.json"):
    node_path = tmp_path / name
    node_path.write_text(json.dumps(payload))
    return node_path


def _graph_node_payload(
    *,
    title="Graph Importer",
    summary="Import Graph node exports into Blueprint.",
    description="Fallback description",
):
    payload = {
        "id": "node-123",
        "title": title,
        "summary": summary,
        "description": description,
        "body": "Full body content",
        "domain": "developer-tools",
        "tags": ["importer", "graph"],
        "links": [{"url": "https://example.com/spec", "title": "Spec"}],
        "properties": {"priority": "high"},
    }
    return {key: value for key, value in payload.items() if value is not None}
