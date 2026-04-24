import json

import yaml
from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import Store, init_db


def test_brief_import_stores_valid_json_and_links_source(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    brief_path = tmp_path / "brief.json"
    brief_path.write_text(json.dumps(_implementation_brief(status="ready_for_planning")))

    result = CliRunner().invoke(
        cli,
        ["brief", "import", str(brief_path), "--source-id", "sb-test"],
    )

    assert result.exit_code == 0, result.output
    assert "Imported implementation brief ib-authored" in result.output

    imported = Store(str(tmp_path / "blueprint.db")).get_implementation_brief("ib-authored")
    assert imported["source_brief_id"] == "sb-test"
    assert imported["status"] == "ready_for_planning"
    assert imported["title"] == "Hand Authored Brief"


def test_brief_import_generates_id_and_defaults_invalid_status_to_draft(
    tmp_path,
    monkeypatch,
):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    brief_payload = _implementation_brief(status="not-a-status")
    brief_payload.pop("id")
    brief_path = tmp_path / "brief.json"
    brief_path.write_text(json.dumps(brief_payload))

    result = CliRunner().invoke(
        cli,
        ["brief", "import", str(brief_path), "--source-id", "sb-test"],
    )

    assert result.exit_code == 0, result.output
    briefs = Store(str(tmp_path / "blueprint.db")).list_implementation_briefs()
    assert len(briefs) == 1
    assert briefs[0]["id"].startswith("ib-")
    assert briefs[0]["status"] == "draft"


def test_brief_import_rejects_invalid_schema(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    brief_payload = _implementation_brief()
    brief_payload["scope"] = "not-a-list"
    brief_path = tmp_path / "brief.json"
    brief_path.write_text(json.dumps(brief_payload))

    result = CliRunner().invoke(
        cli,
        ["brief", "import", str(brief_path), "--source-id", "sb-test"],
    )

    assert result.exit_code != 0
    assert Store(str(tmp_path / "blueprint.db")).list_implementation_briefs() == []


def test_brief_import_requires_existing_source_brief(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    brief_path = tmp_path / "brief.json"
    brief_path.write_text(json.dumps(_implementation_brief()))

    result = CliRunner().invoke(
        cli,
        ["brief", "import", str(brief_path), "--source-id", "sb-missing"],
    )

    assert result.exit_code != 0
    assert "Source brief not found: sb-missing" in result.output


def test_brief_import_stores_valid_yaml_and_links_source(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    brief_path = tmp_path / "brief.yaml"
    brief_path.write_text(
        yaml.safe_dump(
            _implementation_brief(status="ready_for_planning"),
            sort_keys=False,
        )
    )

    result = CliRunner().invoke(
        cli,
        ["brief", "import", str(brief_path), "--source-id", "sb-test"],
    )

    assert result.exit_code == 0, result.output
    assert "Imported implementation brief ib-authored" in result.output

    imported = Store(str(tmp_path / "blueprint.db")).get_implementation_brief("ib-authored")
    assert imported["source_brief_id"] == "sb-test"
    assert imported["status"] == "ready_for_planning"
    assert imported["title"] == "Hand Authored Brief"


def test_brief_import_generates_id_and_defaults_invalid_status_from_yaml(
    tmp_path,
    monkeypatch,
):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    brief_payload = _implementation_brief(status="not-a-status")
    brief_payload.pop("id")
    brief_path = tmp_path / "brief.yml"
    brief_path.write_text(yaml.safe_dump(brief_payload, sort_keys=False))

    result = CliRunner().invoke(
        cli,
        ["brief", "import", str(brief_path), "--source-id", "sb-test"],
    )

    assert result.exit_code == 0, result.output
    briefs = Store(str(tmp_path / "blueprint.db")).list_implementation_briefs()
    assert len(briefs) == 1
    assert briefs[0]["id"].startswith("ib-")
    assert briefs[0]["source_brief_id"] == "sb-test"
    assert briefs[0]["status"] == "draft"


def test_brief_import_rejects_invalid_yaml_without_inserting(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    brief_path = tmp_path / "brief.yaml"
    brief_path.write_text("title: [unterminated")

    result = CliRunner().invoke(
        cli,
        ["brief", "import", str(brief_path), "--source-id", "sb-test"],
    )

    assert result.exit_code != 0
    assert "Invalid YAML" in result.output
    assert Store(str(tmp_path / "blueprint.db")).list_implementation_briefs() == []


def test_brief_import_rejects_unsupported_suffix(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    brief_path = tmp_path / "brief.txt"
    brief_path.write_text(json.dumps(_implementation_brief()))

    result = CliRunner().invoke(
        cli,
        ["brief", "import", str(brief_path), "--source-id", "sb-test"],
    )

    assert result.exit_code != 0
    assert "Unsupported implementation brief file suffix: .txt" in result.output
    assert Store(str(tmp_path / "blueprint.db")).list_implementation_briefs() == []


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


def _source_brief():
    return {
        "id": "sb-test",
        "title": "Source Brief",
        "domain": "testing",
        "summary": "A source brief for import tests",
        "source_project": "manual",
        "source_entity_type": "note",
        "source_id": "note-1",
        "source_payload": {"title": "Source Brief"},
        "source_links": {"path": "source.md"},
    }


def _implementation_brief(status="draft"):
    return {
        "id": "ib-authored",
        "source_brief_id": "sb-other",
        "title": "Hand Authored Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need to import authored implementation briefs",
        "mvp_goal": "Store a validated implementation brief from JSON",
        "product_surface": "CLI",
        "scope": ["Import implementation brief JSON"],
        "non_goals": ["Generate brief content"],
        "assumptions": ["Source brief already exists"],
        "architecture_notes": "Use domain validation before insertion",
        "data_requirements": "JSON implementation brief file",
        "integration_points": [],
        "risks": ["Invalid hand-authored JSON"],
        "validation_plan": "Run pytest",
        "definition_of_done": ["Brief is stored and linked to source"],
        "status": status,
    }
