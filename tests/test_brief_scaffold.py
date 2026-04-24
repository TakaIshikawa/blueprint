import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.generators.brief_scaffold import scaffold_implementation_brief
from blueprint.store import Store, init_db


def test_scaffold_extracts_manual_normalized_payload():
    brief = scaffold_implementation_brief(
        _source_brief(
            source_payload={
                "normalized": {
                    "title": "Manual Normalized Brief",
                    "domain": "operations",
                    "summary": "Operators need a reliable review queue.",
                    "mvp_goal": "Create the minimum review queue workflow.",
                    "scope": ["List queue items", "Update item status"],
                    "non_goals": ["Automated assignment"],
                    "assumptions": ["Queue data already exists"],
                    "validation_plan": "Run unit tests and review a manual fixture.",
                    "definition_of_done": ["Queue workflow is usable"],
                    "product_surface": "CLI",
                }
            },
        )
    )

    assert brief["title"] == "Manual Normalized Brief"
    assert brief["domain"] == "operations"
    assert brief["problem_statement"] == "Operators need a reliable review queue."
    assert brief["mvp_goal"] == "Create the minimum review queue workflow."
    assert brief["scope"] == ["List queue items", "Update item status"]
    assert brief["non_goals"] == ["Automated assignment"]
    assert brief["assumptions"] == ["Queue data already exists"]
    assert brief["validation_plan"] == "Run unit tests and review a manual fixture."
    assert brief["definition_of_done"] == ["Queue workflow is usable"]
    assert brief["product_surface"] == "CLI"
    assert brief["generation_model"] == "scaffold"
    assert brief["generation_tokens"] == 0


def test_scaffold_sparse_source_brief_uses_valid_defaults():
    brief = scaffold_implementation_brief(
        _source_brief(
            title="Sparse Source",
            domain=None,
            summary="Sparse source summary.",
            source_payload={},
        )
    )

    assert brief["title"] == "Sparse Source"
    assert brief["domain"] is None
    assert brief["problem_statement"] == "Sparse source summary."
    assert brief["mvp_goal"] == "Sparse source summary."
    assert brief["scope"] == ["Implement the core workflow described by source brief sb-test."]
    assert brief["non_goals"] == []
    assert brief["assumptions"] == []
    assert brief["risks"] == []
    assert brief["validation_plan"]
    assert brief["definition_of_done"]


def test_brief_scaffold_cli_json_output_creates_brief(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())

    result = CliRunner().invoke(
        cli,
        ["brief", "scaffold", "sb-test", "--status", "ready_for_planning", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["source_brief_id"] == "sb-test"
    assert payload["brief_id"].startswith("ib-")
    assert payload["status"] == "ready_for_planning"

    created = Store(str(tmp_path / "blueprint.db")).get_implementation_brief(
        payload["brief_id"]
    )
    assert created["source_brief_id"] == "sb-test"
    assert created["status"] == "ready_for_planning"
    assert created["generation_model"] == "scaffold"
    assert created["generation_tokens"] == 0
    assert created["generation_prompt"]


def test_brief_scaffold_cli_missing_source_fails_clearly(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["brief", "scaffold", "sb-missing"])

    assert result.exit_code != 0
    assert "Error: Source brief not found: sb-missing" in result.output


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


def _source_brief(
    *,
    title: str = "Source Brief",
    domain: str | None = "workflow",
    summary: str = "Source summary.",
    source_payload: dict | None = None,
) -> dict:
    return {
        "id": "sb-test",
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "manual",
        "source_entity_type": "markdown_brief",
        "source_id": "briefs/source.md",
        "source_payload": source_payload
        if source_payload is not None
        else {
            "normalized": {
                "summary": "Manual normalized summary.",
                "mvp_goal": "Manual normalized goal.",
                "scope": ["Manual scope"],
                "non_goals": ["Manual non-goal"],
                "assumptions": ["Manual assumption"],
                "validation_plan": "Manual validation.",
                "definition_of_done": ["Manual done"],
            }
        },
        "source_links": {},
    }
