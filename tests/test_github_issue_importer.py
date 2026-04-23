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


def test_list_available_filters_by_state_and_skips_pull_requests():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["timeout"] = timeout
        return _Response(_issues_payload())

    importer = GitHubIssueImporter(
        default_owner="acme",
        default_repo="widgets",
        http_open=fake_open,
    )

    issues = importer.list_available(limit=5, state="closed")

    assert seen["url"] == "https://api.github.com/repos/acme/widgets/issues?state=closed&per_page=5"
    assert seen["timeout"] == 10
    assert [issue["id"] for issue in issues] == [
        "acme/widgets#42",
        "acme/widgets#43",
    ]
    assert issues[0]["number"] == 42
    assert issues[0]["labels"] == ["importer", "enhancement"]
    assert issues[0]["assignees"] == ["mona"]
    assert issues[0]["html_url"] == "https://github.com/acme/widgets/issues/42"
    assert issues[0]["updated_at"] == "2026-04-21T00:00:00Z"


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


def test_cli_list_github_issues_text_output(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    seen = _install_fake_github_list_open(monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["import", "list-github-issues", "--state", "closed", "--limit", "5"],
    )

    assert result.exit_code == 0, result.output
    assert seen["url"] == "https://api.github.com/repos/acme/widgets/issues?state=closed&per_page=5"
    assert "acme/widgets#42" in result.output
    assert "Add GitHub imports" in result.output
    assert "importer, enhancement" in result.output
    assert "mona" in result.output
    assert "Pull request masquerading as issue" not in result.output
    assert "Total: 2 issues" in result.output


def test_cli_list_github_issues_json_output(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    _install_fake_github_list_open(monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["import", "list-github-issues", "--state", "all", "--json"],
    )

    assert result.exit_code == 0, result.output
    issues = json.loads(result.output)
    assert [issue["number"] for issue in issues] == [42, 43]
    assert issues[0]["labels"] == ["importer", "enhancement"]
    assert issues[0]["assignees"] == ["mona"]
    assert issues[0]["html_url"] == "https://github.com/acme/widgets/issues/42"


def test_cli_list_github_issues_requires_default_repo(tmp_path, monkeypatch):
    _write_config(
        tmp_path,
        monkeypatch,
        default_owner=None,
        default_repo=None,
    )
    _install_fake_github_list_open(monkeypatch)

    result = CliRunner().invoke(cli, ["import", "list-github-issues"])

    assert result.exit_code != 0
    assert "sources.github.default_owner and default_repo are required" in result.output


def _write_config(tmp_path, monkeypatch, default_owner="acme", default_repo="widgets"):
    monkeypatch.chdir(tmp_path)
    github_defaults = ""
    if default_owner is not None:
        github_defaults += f"    default_owner: {default_owner}\n"
    if default_repo is not None:
        github_defaults += f"    default_repo: {default_repo}\n"
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
sources:
  github:
    token_env: GH_TEST_TOKEN
{github_defaults.rstrip()}
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


def _issues_payload():
    second_issue = {
        **_issue_payload(),
        "number": 43,
        "html_url": "https://github.com/acme/widgets/issues/43",
        "title": "Document GitHub imports",
        "body": "Write importer docs.",
        "labels": [{"name": "docs"}],
        "assignees": [],
        "updated_at": "2026-04-22T00:00:00Z",
    }
    pull_request = {
        **_issue_payload(),
        "number": 44,
        "html_url": "https://github.com/acme/widgets/pull/44",
        "title": "Pull request masquerading as issue",
        "pull_request": {"url": "https://api.github.com/repos/acme/widgets/pulls/44"},
    }
    return [_issue_payload(), pull_request, second_issue]


def _install_fake_github_list_open(monkeypatch):
    seen = {}
    original_init = GitHubIssueImporter.__init__

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["timeout"] = timeout
        return _Response(_issues_payload())

    def fake_init(
        self,
        *,
        token_env="GITHUB_TOKEN",
        default_owner=None,
        default_repo=None,
        api_base="https://api.github.com",
        http_open=None,
    ):
        original_init(
            self,
            token_env=token_env,
            default_owner=default_owner,
            default_repo=default_repo,
            api_base=api_base,
            http_open=fake_open,
        )

    monkeypatch.setattr(GitHubIssueImporter, "__init__", fake_init)
    return seen


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")
