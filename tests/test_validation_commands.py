import json

from blueprint.exporters.claude_code import ClaudeCodeExporter
from blueprint.exporters.codex import CodexExporter
from blueprint.exporters.relay import RelayExporter
from blueprint.generators.plan_generator import PlanGenerator
from blueprint.repo_scanner import format_repository_context, scan_repository
from blueprint.validation_commands import flatten_validation_commands


def test_poetry_project_recommends_pytest_lint_and_format_checks(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text(
        """
[tool.poetry]
name = "demo"
version = "0.1.0"

[tool.poetry.group.dev.dependencies]
pytest = "^8.0.0"
ruff = "^0.1.0"
black = "^24.0.0"

[tool.ruff]
line-length = 100

[tool.black]
line-length = 100

[tool.pytest.ini_options]
testpaths = ["tests"]
"""
    )
    (repo / "poetry.lock").write_text("")
    (repo / "tests").mkdir()

    summary = scan_repository(repo)

    assert summary["python_tools"] == ["black", "pytest", "ruff"]
    assert summary["validation_commands"]["test"] == ["poetry run pytest"]
    assert summary["validation_commands"]["lint"] == ["poetry run ruff check"]
    assert summary["validation_commands"]["format"] == [
        "poetry run ruff format --check",
        "poetry run black --check .",
    ]
    assert "poetry build" in summary["validation_commands"]["build"]


def test_node_project_uses_lockfile_package_manager_for_scripts(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "package.json").write_text(
        json.dumps(
            {
                "scripts": {
                    "test": "vitest run",
                    "lint": "eslint .",
                    "format": "prettier --check .",
                    "typecheck": "tsc --noEmit",
                    "build": "vite build",
                }
            }
        )
    )
    (repo / "pnpm-lock.yaml").write_text("")
    (repo / "src").mkdir()
    (repo / "src" / "app.ts").write_text("export {}\n")

    summary = scan_repository(repo)

    assert summary["package_managers"] == ["pnpm"]
    assert summary["validation_commands"] == {
        "test": ["pnpm test"],
        "lint": ["pnpm lint"],
        "format": ["pnpm format"],
        "typecheck": ["pnpm typecheck"],
        "build": ["pnpm build"],
    }


def test_node_project_distinguishes_npm_and_yarn_lockfiles(tmp_path):
    for lockfile, expected in [
        ("package-lock.json", "npm run lint"),
        ("yarn.lock", "yarn lint"),
    ]:
        repo = tmp_path / lockfile
        repo.mkdir()
        (repo / "package.json").write_text('{"scripts": {"lint": "eslint ."}}')
        (repo / lockfile).write_text("")

        summary = scan_repository(repo)

        assert summary["validation_commands"]["lint"] == [expected]


def test_unknown_project_has_empty_validation_recommendations(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "README.md").write_text("# Notes\n")

    summary = scan_repository(repo)

    assert flatten_validation_commands(summary["validation_commands"]) == []


def test_plan_prompt_and_metadata_include_repository_validation_commands(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("[tool.poetry]\nname = 'demo'\n[tool.ruff]\n")
    (repo / "poetry.lock").write_text("")
    (repo / "tests").mkdir()
    repo_context = format_repository_context(scan_repository(repo))

    prompt = PlanGenerator.build_prompt(_implementation_brief(), repo_context=repo_context)

    assert "Recommended validation commands:" in prompt
    assert "`poetry run pytest`" in prompt
    assert "Prefer test_command values from the Repository Context" in prompt


def test_exports_include_recommended_validation_commands(tmp_path):
    plan = _execution_plan()
    brief = _implementation_brief()
    codex_path = tmp_path / "codex.md"
    claude_path = tmp_path / "claude.md"
    relay_path = tmp_path / "relay.json"

    CodexExporter().export(plan, brief, str(codex_path))
    ClaudeCodeExporter().export(plan, brief, str(claude_path))
    RelayExporter().export(plan, brief, str(relay_path))

    codex = codex_path.read_text()
    claude = claude_path.read_text()
    relay = json.loads(relay_path.read_text())

    assert "Recommended Validation Commands" in codex
    assert "`poetry run ruff check`" in codex
    assert "Recommended Commands" in claude
    assert "`poetry run black --check .`" in claude
    assert relay["validation"]["commands"][:3] == [
        "poetry run pytest",
        "poetry run ruff check",
        "poetry run ruff format --check",
    ]


def _implementation_brief():
    return {
        "id": "ib-validation",
        "source_brief_id": "sb-validation",
        "title": "Validation Commands",
        "domain": "developer_tools",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "Planning",
        "problem_statement": "Plans need executable validation guidance.",
        "mvp_goal": "Recommend commands from repository context.",
        "product_surface": "Python CLI",
        "scope": ["Recommend validation commands"],
        "non_goals": ["Run commands automatically"],
        "assumptions": [],
        "architecture_notes": "Use scanner output.",
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run focused validation command tests",
        "definition_of_done": ["Exports include validation commands"],
        "status": "draft",
    }


def _execution_plan():
    return {
        "id": "plan-validation",
        "implementation_brief_id": "ib-validation",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Add recommendations"}],
        "test_strategy": "Run recommended commands",
        "handoff_prompt": "Use repository validation guidance.",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "metadata": {
            "validation_commands": {
                "test": ["poetry run pytest"],
                "lint": ["poetry run ruff check"],
                "format": [
                    "poetry run ruff format --check",
                    "poetry run black --check .",
                ],
            }
        },
        "tasks": [
            {
                "id": "task-validation",
                "title": "Add recommender",
                "description": "Implement command recommendation.",
                "milestone": "Foundation",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/blueprint/validation_commands.py"],
                "acceptance_criteria": ["Commands are recommended"],
                "estimated_complexity": "medium",
                "estimated_hours": 2.0,
                "risk_level": "medium",
                "test_command": None,
                "status": "pending",
            }
        ],
    }
