import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.env_inventory import build_env_inventory
from blueprint.cli import cli
from blueprint.store import init_db


def test_env_inventory_extracts_env_vars_and_config_keys_with_merged_sources():
    result = build_env_inventory(_brief(), _plan())

    assert [item.name for item in result.items] == [
        "ANTHROPIC_API_KEY",
        "GITHUB_TOKEN",
        "database.path",
        "exports.output_dir",
        "llm.provider",
        "sources.github.token_env",
    ]

    anthropic = _item(result, "ANTHROPIC_API_KEY")
    assert anthropic.status == "required"
    assert anthropic.task_ids == ["task-configure"]
    assert anthropic.source_fields == [
        "data_requirements",
        "tasks.acceptance_criteria[0]",
        "tasks.description",
        "validation_plan",
    ]

    github_token = _item(result, "GITHUB_TOKEN")
    assert github_token.status == "optional"
    assert github_token.task_ids == ["task-configure"]
    assert github_token.source_fields == ["tasks.metadata.env.GITHUB_TOKEN"]

    llm_provider = _item(result, "llm.provider")
    assert llm_provider.status == "unknown"
    assert llm_provider.source_fields == [
        "architecture_notes",
        "tasks.metadata.config.llm.provider",
    ]


def test_env_inventory_cli_json_is_sorted_and_stable(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["plan", "env-inventory", "plan-env", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-env"
    assert payload["brief_id"] == "ib-env"
    assert [item["name"] for item in payload["items"]] == [
        "ANTHROPIC_API_KEY",
        "GITHUB_TOKEN",
        "database.path",
        "exports.output_dir",
        "llm.provider",
        "sources.github.token_env",
    ]
    assert payload["items"][0]["sources"] == [
        {
            "field": "data_requirements",
            "status": "required",
        },
        {
            "field": "tasks.acceptance_criteria[0]",
            "status": "required",
            "task_id": "task-configure",
        },
        {
            "field": "tasks.description",
            "status": "required",
            "task_id": "task-configure",
        },
        {
            "field": "validation_plan",
            "status": "required",
        },
    ]


def test_env_inventory_cli_human_output_groups_by_status(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch)

    result = CliRunner().invoke(cli, ["plan", "env-inventory", "plan-env"])

    assert result.exit_code == 0, result.output
    assert "Plan environment inventory: plan-env" in result.output
    assert "Required:" in result.output
    assert "  - ANTHROPIC_API_KEY (env_var)" in result.output
    assert "  - database.path (config_key)" in result.output
    assert "Optional:" in result.output
    assert "  - GITHUB_TOKEN (env_var)" in result.output
    assert "Unknown:" in result.output
    assert "  - llm.provider (config_key)" in result.output


def test_env_inventory_cli_writes_output_file(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch)
    output = tmp_path / "inventory.json"

    result = CliRunner().invoke(
        cli,
        ["plan", "env-inventory", "plan-env", "--json", "--output", str(output)],
    )

    assert result.exit_code == 0, result.output
    assert result.output == ""
    assert json.loads(output.read_text())["items"][0]["name"] == "ANTHROPIC_API_KEY"


def _item(result, name):
    return next(item for item in result.items if item.name == name)


def _seed_plan(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_brief())
    store.insert_execution_plan(_plan(), _plan()["tasks"])


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


def _brief():
    return {
        "id": "ib-env",
        "source_brief_id": "sb-env",
        "title": "Config handoff",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "Execution planning",
        "problem_statement": "Agents need explicit config inputs",
        "mvp_goal": "Inventory configuration requirements",
        "product_surface": "CLI",
        "scope": ["Extract config mentions"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": "Read llm.provider from Blueprint config.",
        "data_requirements": "Requires ANTHROPIC_API_KEY and database.path.",
        "integration_points": ["Optional exports.output_dir override"],
        "risks": ["Config may be missed"],
        "validation_plan": "Set ANTHROPIC_API_KEY before running pytest.",
        "definition_of_done": ["Inventory command reports grouped config"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _plan():
    return {
        "id": "plan-env",
        "implementation_brief_id": "ib-env",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Audit", "description": "Add env inventory"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": [
            {
                "id": "task-configure",
                "title": "Document config",
                "description": (
                    "Require ANTHROPIC_API_KEY and sources.github.token_env for handoff."
                ),
                "milestone": "Audit",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["README.md"],
                "acceptance_criteria": [
                    "ANTHROPIC_API_KEY is documented as required.",
                ],
                "estimated_complexity": "low",
                "status": "pending",
                "metadata": {
                    "env": {
                        "GITHUB_TOKEN": "Optional GITHUB_TOKEN token for GitHub imports.",
                    },
                    "config": {
                        "llm.provider": "anthropic",
                    },
                },
            },
            {
                "id": "task-validate",
                "title": "Validate output",
                "description": "Print unknown configuration mentions without a status.",
                "milestone": "Audit",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": ["task-configure"],
                "files_or_modules": ["tests/test_env_inventory.py"],
                "acceptance_criteria": [
                    "JSON output is sorted by variable name.",
                ],
                "estimated_complexity": "medium",
                "status": "pending",
            },
        ],
    }
