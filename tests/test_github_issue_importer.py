import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.importers.github_issue_importer import (
    GitHubIssueImporter,
    parse_github_issue_json,
    parse_issue_ref,
)
from blueprint.store import Store, init_db


def test_parse_github_issue_json_normalizes_source_brief():
    source_brief = parse_github_issue_json(_issue_payload())

    assert source_brief["title"] == "Add GitHub imports"
    assert source_brief["domain"] == "github"
    assert source_brief["source_project"] == "github"
    assert source_brief["source_entity_type"] == "issue"
    assert source_brief["source_id"] == "acme/widgets#42"
    assert "GitHub issue acme/widgets#42" in source_brief["summary"]
    assert "Labels: importer, enhancement" in source_brief["summary"]
    assert "Normalize GitHub issues into SourceBriefs." in source_brief["summary"]
    assert source_brief["source_payload"]["normalized"]["author"] == "octocat"
    assert source_brief["source_payload"]["normalized"]["assignees"] == ["mona"]
    assert source_brief["source_links"]["html_url"] == (
        "https://github.com/acme/widgets/issues/42"
    )


def test_parse_issue_ref_uses_default_repository():
    issue_ref = parse_issue_ref("#42", default_owner="acme", default_repo="widgets")

    assert issue_ref.owner == "acme"
    assert issue_ref.repo == "widgets"
    assert issue_ref.number == 42
    assert issue_ref.source_id == "acme/widgets#42"


def test_importer_fetches_issue_with_mocked_response(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["authorization"] = request.headers.get("Authorization")
        seen["timeout"] = timeout
        return _Response(_issue_payload())

    monkeypatch.setenv("GH_TEST_TOKEN", "test-token")
    importer = GitHubIssueImporter(token_env="GH_TEST_TOKEN", http_open=fake_open)

    source_brief = importer.import_from_source("acme/widgets#42")

    assert seen["url"] == "https://api.github.com/repos/acme/widgets/issues/42"
    assert seen["authorization"] == "Bearer test-token"
    assert seen["timeout"] == 10
    assert source_brief["source_id"] == "acme/widgets#42"


def test_cli_import_github_issue_stores_source_brief(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    def fake_import_from_source(self, source_id):
        return parse_github_issue_json(_issue_payload(), owner="acme", repo="widgets")

    monkeypatch.setattr(
        GitHubIssueImporter,
        "import_from_source",
        fake_import_from_source,
    )

    result = CliRunner().invoke(cli, ["import", "github-issue", "acme/widgets#42"])

    assert result.exit_code == 0, result.output
    assert "Imported source brief" in result.output
    assert "GitHub issue acme/widgets#42" in result.output

    briefs = Store(str(tmp_path / "blueprint.db")).list_source_briefs(
        source_project="github"
    )
    assert len(briefs) == 1
    assert briefs[0]["source_id"] == "acme/widgets#42"
    assert briefs[0]["title"] == "Add GitHub imports"


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
sources:
  github:
    token_env: GH_TEST_TOKEN
    default_owner: acme
    default_repo: widgets
exports:
  output_dir: {tmp_path}
"""
    )
    blueprint_config.reload_config()


def _issue_payload():
    return {
        "url": "https://api.github.com/repos/acme/widgets/issues/42",
        "repository_url": "https://api.github.com/repos/acme/widgets",
        "html_url": "https://github.com/acme/widgets/issues/42",
        "number": 42,
        "state": "open",
        "title": "Add GitHub imports",
        "body": "Normalize GitHub issues into SourceBriefs.",
        "user": {"login": "octocat"},
        "labels": [{"name": "importer"}, {"name": "enhancement"}],
        "assignees": [{"login": "mona"}],
        "milestone": {"title": "MVP"},
        "created_at": "2026-04-20T00:00:00Z",
        "updated_at": "2026-04-21T00:00:00Z",
        "closed_at": None,
    }


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")
