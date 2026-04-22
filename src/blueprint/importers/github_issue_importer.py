"""GitHub issue importer."""

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from blueprint.importers.base import SourceImporter


HttpOpen = Callable[..., Any]


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True)
class GitHubIssueRef:
    """Parsed GitHub issue reference."""

    owner: str
    repo: str
    number: int

    @property
    def source_id(self) -> str:
        """Return canonical source ID."""
        return f"{self.owner}/{self.repo}#{self.number}"


def parse_issue_ref(
    source_id: str,
    *,
    default_owner: str | None = None,
    default_repo: str | None = None,
) -> GitHubIssueRef:
    """Parse OWNER/REPO#NUMBER, optionally using configured repository defaults."""
    source_id = source_id.strip()
    owner = default_owner
    repo = default_repo
    issue_number = source_id

    if "#" in source_id:
        repo_part, issue_number = source_id.rsplit("#", 1)
        if repo_part:
            owner, repo = _parse_repo_slug(repo_part)
    elif "/" in source_id:
        repo_part, issue_number = source_id.rsplit("/", 1)
        owner, repo = _parse_repo_slug(repo_part.removesuffix("/issues"))

    if issue_number.startswith("#"):
        issue_number = issue_number[1:]

    if not owner or not repo:
        raise ValueError("GitHub issue reference must include OWNER/REPO or configure defaults")

    try:
        number = int(issue_number)
    except ValueError as exc:
        raise ValueError(f"Invalid GitHub issue number: {issue_number}") from exc

    if number < 1:
        raise ValueError(f"Invalid GitHub issue number: {number}")

    return GitHubIssueRef(owner=owner, repo=repo, number=number)


def parse_github_issue_json(
    issue: dict[str, Any],
    *,
    owner: str | None = None,
    repo: str | None = None,
) -> dict[str, Any]:
    """Normalize GitHub issue JSON into a SourceBrief dictionary."""
    if not isinstance(issue, dict):
        raise ValueError("GitHub issue payload must be a mapping")

    title = _required_string(issue, "title")
    number = _required_int(issue, "number")
    owner, repo = _resolve_repo(issue, owner=owner, repo=repo)
    source_id = f"{owner}/{repo}#{number}"

    now = datetime.utcnow()
    labels = _labels(issue.get("labels"))
    assignees = _users(issue.get("assignees"))
    author = _user_login(issue.get("user"))
    body = issue.get("body") or ""

    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": "github",
        "summary": _create_summary(issue, owner=owner, repo=repo),
        "source_project": "github",
        "source_entity_type": "issue",
        "source_id": source_id,
        "source_payload": {
            "issue": issue,
            "normalized": {
                "owner": owner,
                "repo": repo,
                "number": number,
                "title": title,
                "state": issue.get("state"),
                "author": author,
                "labels": labels,
                "assignees": assignees,
                "milestone": _milestone_title(issue.get("milestone")),
                "body": body,
            },
        },
        "source_links": {
            "html_url": issue.get("html_url"),
            "api_url": issue.get("url"),
            "repository_url": issue.get("repository_url"),
        },
        "created_at": now,
        "updated_at": now,
    }


class GitHubIssueImporter(SourceImporter):
    """Import GitHub issues through the REST API."""

    def __init__(
        self,
        *,
        token_env: str = "GITHUB_TOKEN",
        default_owner: str | None = None,
        default_repo: str | None = None,
        api_base: str = "https://api.github.com",
        http_open: HttpOpen = urlopen,
    ):
        """Initialize importer with optional GitHub REST API settings."""
        self.token_env = token_env
        self.default_owner = default_owner
        self.default_repo = default_repo
        self.api_base = api_base.rstrip("/")
        self.http_open = http_open

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Fetch and normalize a GitHub issue."""
        issue_ref = parse_issue_ref(
            source_id,
            default_owner=self.default_owner,
            default_repo=self.default_repo,
        )
        issue = self.fetch_issue(issue_ref)
        return parse_github_issue_json(issue, owner=issue_ref.owner, repo=issue_ref.repo)

    def validate_source(self, source_id: str) -> bool:
        """Check whether a GitHub issue is accessible."""
        try:
            issue_ref = parse_issue_ref(
                source_id,
                default_owner=self.default_owner,
                default_repo=self.default_repo,
            )
            self.fetch_issue(issue_ref)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List recent issues for the configured default repository."""
        if not self.default_owner or not self.default_repo:
            raise ValueError("sources.github.default_owner and default_repo are required")

        path = (
            f"/repos/{quote(self.default_owner)}/{quote(self.default_repo)}/issues"
            f"?state=all&per_page={limit}"
        )
        issues = self._request_json(path)
        if not isinstance(issues, list):
            raise ImportError("GitHub issues response was not a list")

        available = []
        for issue in issues:
            if "pull_request" in issue:
                continue
            available.append(
                {
                    "id": f"{self.default_owner}/{self.default_repo}#{issue['number']}",
                    "title": issue["title"],
                    "state": issue.get("state"),
                    "updated_at": issue.get("updated_at"),
                }
            )
        return available

    def fetch_issue(self, issue_ref: GitHubIssueRef) -> dict[str, Any]:
        """Fetch a single issue from GitHub."""
        owner = quote(issue_ref.owner)
        repo = quote(issue_ref.repo)
        issue = self._request_json(f"/repos/{owner}/{repo}/issues/{issue_ref.number}")
        if not isinstance(issue, dict):
            raise ImportError("GitHub issue response was not a mapping")
        if "pull_request" in issue:
            raise ImportError(
                f"GitHub reference is a pull request, not an issue: {issue_ref.source_id}"
            )
        return issue

    def _request_json(self, path: str) -> Any:
        """Request JSON from the GitHub REST API."""
        request = Request(
            f"{self.api_base}{path}",
            headers=self._headers(),
            method="GET",
        )

        try:
            with self.http_open(request, timeout=10) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(f"GitHub API request failed with HTTP {exc.code}") from exc
        except URLError as exc:
            raise ImportError(f"GitHub API request failed: {exc.reason}") from exc

        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ImportError("GitHub API returned invalid JSON") from exc

    def _headers(self) -> dict[str, str]:
        """Build GitHub REST API request headers."""
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "blueprint-github-issue-importer",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        token = os.getenv(self.token_env)
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers


def _parse_repo_slug(repo_part: str) -> tuple[str, str]:
    bits = [bit for bit in repo_part.strip("/").split("/") if bit]
    if len(bits) != 2:
        raise ValueError(f"Invalid GitHub repository reference: {repo_part}")
    return bits[0], bits[1]


def _required_string(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"GitHub issue field {key!r} must be a non-empty string")
    return value


def _required_int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    if not isinstance(value, int):
        raise ValueError(f"GitHub issue field {key!r} must be an integer")
    return value


def _resolve_repo(
    issue: dict[str, Any],
    *,
    owner: str | None,
    repo: str | None,
) -> tuple[str, str]:
    if owner and repo:
        return owner, repo

    repository_url = issue.get("repository_url")
    if isinstance(repository_url, str) and "/repos/" in repository_url:
        repo_slug = repository_url.rsplit("/repos/", 1)[1]
        return _parse_repo_slug(repo_slug)

    html_url = issue.get("html_url")
    if isinstance(html_url, str):
        parts = [part for part in html_url.split("/") if part]
        if len(parts) >= 5 and parts[-2] == "issues":
            return parts[-4], parts[-3]

    raise ValueError("GitHub issue payload does not identify its repository")


def _create_summary(issue: dict[str, Any], *, owner: str, repo: str) -> str:
    parts = [
        f"GitHub issue {owner}/{repo}#{issue['number']}",
        f"State: {issue.get('state') or 'unknown'}",
    ]

    author = _user_login(issue.get("user"))
    if author:
        parts.append(f"Author: {author}")

    labels = _labels(issue.get("labels"))
    if labels:
        parts.append(f"Labels: {', '.join(labels)}")

    assignees = _users(issue.get("assignees"))
    if assignees:
        parts.append(f"Assignees: {', '.join(assignees)}")

    milestone = _milestone_title(issue.get("milestone"))
    if milestone:
        parts.append(f"Milestone: {milestone}")

    body = (issue.get("body") or "").strip()
    if body:
        parts.append(f"\n{body}")
    else:
        parts.append(f"\n{issue['title']}")

    return "\n".join(parts)


def _labels(labels: Any) -> list[str]:
    if not isinstance(labels, list):
        return []
    names = []
    for label in labels:
        if isinstance(label, dict) and isinstance(label.get("name"), str):
            names.append(label["name"])
        elif isinstance(label, str):
            names.append(label)
    return names


def _users(users: Any) -> list[str]:
    if not isinstance(users, list):
        return []
    return [login for login in (_user_login(user) for user in users) if login]


def _user_login(user: Any) -> str | None:
    if isinstance(user, dict) and isinstance(user.get("login"), str):
        return user["login"]
    return None


def _milestone_title(milestone: Any) -> str | None:
    if isinstance(milestone, dict) and isinstance(milestone.get("title"), str):
        return milestone["title"]
    return None
