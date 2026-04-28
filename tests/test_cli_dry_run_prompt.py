from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint import cli as cli_module
from blueprint.generators.brief_generator import BriefGenerator
from blueprint.generators.plan_generator import PlanGenerator
from blueprint.store import init_db


def test_brief_create_dry_run_prompt_prints_prompt_without_insert(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    source_brief = _source_brief()
    store.insert_source_brief(source_brief)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(cli_module, "_create_llm_provider", _fail_if_llm_provider_is_created)

    result = CliRunner().invoke(
        cli_module.cli,
        ["brief", "create", source_brief["id"], "--dry-run-prompt"],
    )

    assert result.exit_code == 0, result.output
    assert result.output == BriefGenerator.build_prompt(source_brief)
    assert store.list_implementation_briefs(limit=10) == []


def test_plan_create_dry_run_prompt_prints_prompt_without_insert(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    implementation_brief = _implementation_brief()
    store.insert_implementation_brief(implementation_brief)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(cli_module, "_create_llm_provider", _fail_if_llm_provider_is_created)

    result = CliRunner().invoke(
        cli_module.cli,
        ["plan", "create", implementation_brief["id"], "--dry-run-prompt"],
    )

    assert result.exit_code == 0, result.output
    assert result.output == PlanGenerator.build_prompt(implementation_brief)
    assert store.list_execution_plans(limit=10) == []


def test_plan_create_dry_run_prompt_includes_configured_rules_files(tmp_path, monkeypatch):
    store = _setup_store(
        tmp_path,
        monkeypatch,
        rules_files=["AGENTS.md", ".cursorrules"],
    )
    (tmp_path / "AGENTS.md").write_text("Use pytest for validation.\n")
    (tmp_path / ".cursorrules").write_text("Keep CLI output deterministic.\n")
    implementation_brief = _implementation_brief()
    store.insert_implementation_brief(implementation_brief)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(cli_module, "_create_llm_provider", _fail_if_llm_provider_is_created)

    result = CliRunner().invoke(
        cli_module.cli,
        ["plan", "create", implementation_brief["id"], "--dry-run-prompt"],
    )

    assert result.exit_code == 0, result.output
    assert "## Repository Rules" in result.output
    assert "### AGENTS.md" in result.output
    assert "Use pytest for validation." in result.output
    assert "### .cursorrules" in result.output
    assert "Keep CLI output deterministic." in result.output
    assert store.list_execution_plans(limit=10) == []


def test_plan_create_dry_run_prompt_warns_for_missing_rules_file(tmp_path, monkeypatch):
    store = _setup_store(
        tmp_path,
        monkeypatch,
        rules_files=["AGENTS.md", "missing-rules.md"],
    )
    (tmp_path / "AGENTS.md").write_text("Follow repository conventions.\n")
    implementation_brief = _implementation_brief()
    store.insert_implementation_brief(implementation_brief)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setattr(cli_module, "_create_llm_provider", _fail_if_llm_provider_is_created)

    result = CliRunner().invoke(
        cli_module.cli,
        ["plan", "create", implementation_brief["id"], "--dry-run-prompt"],
    )

    assert result.exit_code == 0, result.output
    assert "Follow repository conventions." in result.output
    combined_output = result.output + getattr(result, "stderr", "")
    assert "Warning: planning rules file not found, skipping: missing-rules.md" in combined_output
    assert store.list_execution_plans(limit=10) == []


def _fail_if_llm_provider_is_created(config):
    raise AssertionError("dry-run prompt should not create an LLM provider")


def _setup_store(tmp_path, monkeypatch, rules_files=None):
    monkeypatch.chdir(tmp_path)
    planning_config = ""
    if rules_files is not None:
        rules_entries = "\n".join(f"    - {path}" for path in rules_files)
        planning_config = f"""
planning:
  rules_files:
{rules_entries}
"""
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path}
{planning_config}
"""
    )
    blueprint_config.reload_config()
    return init_db(str(tmp_path / "blueprint.db"))


def _source_brief():
    return {
        "id": "src-dry-run",
        "title": "Review Queue Assistant",
        "domain": "developer_tools",
        "summary": "Help maintainers triage pull request review queues.",
        "source_project": "manual",
        "source_entity_type": "design_brief",
        "source_id": "manual-dry-run",
        "source_payload": {},
        "source_links": {},
    }


def _implementation_brief():
    return {
        "id": "ib-dry-run",
        "source_brief_id": "src-dry-run",
        "title": "Review Queue Assistant",
        "domain": "developer_tools",
        "target_user": "Repository maintainers",
        "buyer": "Engineering managers",
        "workflow_context": "Daily pull request triage",
        "problem_statement": "Maintainers need a deterministic way to identify blocked reviews.",
        "mvp_goal": "Build a CLI that summarizes pending reviews and blockers.",
        "product_surface": "CLI",
        "scope": ["Load review queue data", "Render pending review summary"],
        "non_goals": ["Automated reviewer assignment", "Hosted dashboard"],
        "assumptions": ["Review data is available locally"],
        "architecture_notes": "Use a small command module backed by store reads.",
        "data_requirements": "Pull request IDs, statuses, reviewers, and blocker notes.",
        "integration_points": ["Local repository metadata"],
        "risks": ["Stale data may mislead users; show timestamps in output"],
        "validation_plan": "Run unit tests and manually inspect CLI output.",
        "definition_of_done": [
            "CLI prints queued reviews",
            "Tests cover empty and non-empty queues",
        ],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
