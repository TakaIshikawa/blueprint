"""GitLab importer using GitLab GraphQL API."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from blueprint.importers.base import SourceImporter

HttpOpen = Callable[..., Any]


def generate_source_brief_id() -> str:
    return f"sb-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# State mappings
# ---------------------------------------------------------------------------

DEFAULT_STATE_MAP: dict[str, str] = {
    "opened": "in_progress",
    "closed": "completed",
    "merged": "completed",
}

LABEL_STATE_OVERRIDES: dict[str, str] = {
    "in progress": "in_progress",
    "blocked": "blocked",
    "review": "in_progress",
    "done": "completed",
    "wontfix": "skipped",
}


# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

EPIC_QUERY = """
query($groupPath: ID!, $epicIid: ID!) {
  group(fullPath: $groupPath) {
    epic(iid: $epicIid) {
      id
      iid
      title
      description
      state
      webUrl
      author { username name }
      labels { nodes { title } }
      createdAt
      updatedAt
      closedAt
      startDate
      dueDate
      children { nodes { iid title state } }
      issues {
        nodes {
          iid
          title
          description
          state
          webUrl
          weight
          timeEstimate
          totalTimeSpent
          author { username name }
          assignees { nodes { username name } }
          labels { nodes { title } }
          milestone { title description startDate dueDate }
          createdAt
          updatedAt
          closedAt
          taskCompletionStatus { completedCount count }
          relatedIssues { nodes { iid title linkType } }
          blockedByIssues { nodes { iid title } }
        }
      }
    }
  }
}
"""

ISSUES_QUERY = """
query($projectPath: ID!, $after: String) {
  project(fullPath: $projectPath) {
    issues(after: $after, first: 50) {
      pageInfo { hasNextPage endCursor }
      nodes {
        iid
        title
        description
        state
        webUrl
        weight
        timeEstimate
        totalTimeSpent
        author { username name }
        assignees { nodes { username name } }
        labels { nodes { title } }
        milestone { title description startDate dueDate }
        createdAt
        updatedAt
        closedAt
        taskCompletionStatus { completedCount count }
        relatedIssues { nodes { iid title linkType } }
        blockedByIssues { nodes { iid title } }
      }
    }
  }
}
"""

MILESTONE_ISSUES_QUERY = """
query($projectPath: ID!, $milestoneTitle: String!) {
  project(fullPath: $projectPath) {
    milestone(title: $milestoneTitle) {
      title
      description
      startDate
      dueDate
      state
    }
    issues(milestoneTitle: $milestoneTitle, first: 100) {
      nodes {
        iid
        title
        description
        state
        webUrl
        weight
        timeEstimate
        totalTimeSpent
        author { username name }
        assignees { nodes { username name } }
        labels { nodes { title } }
        milestone { title description startDate dueDate }
        createdAt
        updatedAt
        closedAt
        taskCompletionStatus { completedCount count }
        relatedIssues { nodes { iid title linkType } }
        blockedByIssues { nodes { iid title } }
      }
    }
  }
}
"""

GROUP_QUERY = """
query($groupPath: ID!) {
  group(fullPath: $groupPath) {
    id
    name
    fullPath
    description
    webUrl
    projects { nodes { fullPath name description } }
    epics(first: 50) {
      nodes { iid title state webUrl }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

@dataclass
class GitLabClient:
    """Lightweight client for GitLab GraphQL API."""

    token: str
    instance_url: str = "https://gitlab.com"
    http_open: HttpOpen = urlopen

    @property
    def graphql_url(self) -> str:
        return f"{self.instance_url.rstrip('/')}/api/graphql"

    def query(self, graphql: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        body = json.dumps({"query": graphql, "variables": variables or {}}).encode("utf-8")
        request = Request(
            self.graphql_url,
            data=body,
            headers=self._headers(),
            method="POST",
        )

        try:
            with self.http_open(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(f"GitLab GraphQL request failed with HTTP {exc.code}") from exc
        except URLError as exc:
            raise ImportError(f"GitLab GraphQL request failed: {exc.reason}") from exc

        try:
            result = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ImportError("GitLab API returned invalid JSON") from exc

        if "errors" in result:
            messages = [e.get("message", str(e)) for e in result["errors"]]
            raise ImportError(f"GitLab GraphQL errors: {'; '.join(messages)}")

        return result.get("data") or {}

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "User-Agent": "blueprint-gitlab-importer",
        }


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class GitLabGroup:
    """Representation of a GitLab group."""

    id: str
    name: str
    full_path: str
    description: str
    web_url: str
    projects: list[dict[str, Any]] = field(default_factory=list)
    epics: list[dict[str, Any]] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class Dependency:
    """Normalized issue dependency."""

    source_iid: int
    target_iid: int
    link_type: str


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _extract_labels(labels_data: Any) -> list[str]:
    if not isinstance(labels_data, dict):
        return []
    nodes = labels_data.get("nodes") or []
    return [n["title"] for n in nodes if isinstance(n, dict) and "title" in n]


def _extract_assignees(assignees_data: Any) -> list[str]:
    if not isinstance(assignees_data, dict):
        return []
    nodes = assignees_data.get("nodes") or []
    return [n.get("username") or n.get("name", "") for n in nodes if isinstance(n, dict)]


def _extract_author(author_data: Any) -> str | None:
    if isinstance(author_data, dict):
        return author_data.get("username") or author_data.get("name")
    return None


def _resolve_state(state: str, labels: list[str]) -> str:
    """Resolve issue state using label overrides."""
    for label in labels:
        label_lower = label.lower()
        if label_lower in LABEL_STATE_OVERRIDES:
            return LABEL_STATE_OVERRIDES[label_lower]
    return DEFAULT_STATE_MAP.get(state, "pending")


def _parse_task_lists(description: str) -> list[dict[str, Any]]:
    """Parse markdown task lists (- [ ] / - [x]) into subtasks."""
    if not description:
        return []
    subtasks = []
    for line in description.split("\n"):
        stripped = line.strip()
        if stripped.startswith("- [ ] "):
            subtasks.append({"title": stripped[6:], "completed": False})
        elif stripped.startswith("- [x] ") or stripped.startswith("- [X] "):
            subtasks.append({"title": stripped[6:], "completed": True})
    return subtasks


def parse_issue_relations(issue: dict[str, Any]) -> list[Dependency]:
    """Extract dependencies from issue links and blocked-by relations."""
    iid = issue.get("iid")
    if iid is None:
        return []

    deps: list[Dependency] = []
    related = (issue.get("relatedIssues") or {}).get("nodes") or []
    for rel in related:
        if isinstance(rel, dict) and rel.get("iid") is not None:
            deps.append(Dependency(
                source_iid=iid,
                target_iid=rel["iid"],
                link_type=rel.get("linkType", "relates_to"),
            ))

    blocked_by = (issue.get("blockedByIssues") or {}).get("nodes") or []
    for blocker in blocked_by:
        if isinstance(blocker, dict) and blocker.get("iid") is not None:
            deps.append(Dependency(
                source_iid=iid,
                target_iid=blocker["iid"],
                link_type="is_blocked_by",
            ))

    return deps


def parse_gitlab_issue_json(
    issue: dict[str, Any],
    *,
    project_path: str,
    instance_url: str = "https://gitlab.com",
) -> dict[str, Any]:
    """Normalize a GitLab issue into a SourceBrief dictionary."""
    iid = issue.get("iid")
    if iid is None:
        raise ValueError("GitLab issue missing 'iid' field")

    title = issue.get("title") or ""
    if not title:
        raise ValueError("GitLab issue missing 'title' field")

    description = issue.get("description") or ""
    state = issue.get("state") or "opened"
    labels = _extract_labels(issue.get("labels"))
    assignees = _extract_assignees(issue.get("assignees"))
    author = _extract_author(issue.get("author"))
    weight = issue.get("weight")
    time_estimate = issue.get("timeEstimate")
    total_time_spent = issue.get("totalTimeSpent")
    milestone = issue.get("milestone")
    milestone_title = milestone.get("title") if isinstance(milestone, dict) else None

    mapped_state = _resolve_state(state, labels)
    subtasks = _parse_task_lists(description)
    relations = parse_issue_relations(issue)

    source_id = f"{project_path}#{iid}"
    web_url = issue.get("webUrl") or f"{instance_url}/{project_path}/-/issues/{iid}"

    now = datetime.utcnow()

    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": "gitlab",
        "summary": _create_issue_summary(
            iid=iid,
            title=title,
            state=state,
            author=author,
            labels=labels,
            assignees=assignees,
            milestone_title=milestone_title,
            description=description,
            project_path=project_path,
        ),
        "source_project": "gitlab",
        "source_entity_type": "issue",
        "source_id": source_id,
        "source_payload": {
            "issue": issue,
            "normalized": {
                "iid": iid,
                "title": title,
                "state": state,
                "mapped_state": mapped_state,
                "description": description,
                "author": author,
                "assignees": assignees,
                "labels": labels,
                "weight": weight,
                "time_estimate": time_estimate,
                "total_time_spent": total_time_spent,
                "milestone": milestone_title,
                "subtasks": subtasks,
                "relations": [
                    {"source_iid": d.source_iid, "target_iid": d.target_iid, "link_type": d.link_type}
                    for d in relations
                ],
            },
        },
        "source_links": {
            "html_url": web_url,
            "api_url": None,
        },
        "created_at": now,
        "updated_at": now,
    }


def map_gitlab_issue_to_task(
    issue: dict[str, Any],
    *,
    project_path: str,
    plan_id: str = "",
) -> dict[str, Any]:
    """Map a GitLab issue to an ExecutionTask dictionary."""
    iid = issue.get("iid")
    title = issue.get("title") or ""
    description = issue.get("description") or title
    state = issue.get("state") or "opened"
    labels = _extract_labels(issue.get("labels"))
    assignees = _extract_assignees(issue.get("assignees"))
    weight = issue.get("weight")
    time_estimate = issue.get("timeEstimate")
    total_time_spent = issue.get("totalTimeSpent")
    milestone = issue.get("milestone")
    milestone_title = milestone.get("title") if isinstance(milestone, dict) else None

    mapped_state = _resolve_state(state, labels)
    relations = parse_issue_relations(issue)

    depends_on = [
        str(d.target_iid) for d in relations
        if d.link_type in ("is_blocked_by", "blocks")
    ]

    estimated_hours: float | None = None
    if time_estimate is not None and time_estimate > 0:
        estimated_hours = time_estimate / 3600.0
    elif weight is not None:
        try:
            estimated_hours = float(weight) * 4
        except (TypeError, ValueError):
            pass

    complexity = "medium"
    if weight is not None:
        try:
            w = int(weight)
            if w >= 8:
                complexity = "high"
            elif w <= 2:
                complexity = "low"
        except (TypeError, ValueError):
            pass

    now = datetime.utcnow()

    return {
        "id": f"gl-{iid}",
        "execution_plan_id": plan_id or None,
        "title": title,
        "description": description,
        "milestone": milestone_title,
        "owner_type": assignees[0] if assignees else None,
        "suggested_engine": None,
        "depends_on": depends_on,
        "files_or_modules": None,
        "acceptance_criteria": [f"Complete: {title}"],
        "estimated_complexity": complexity,
        "estimated_hours": estimated_hours,
        "risk_level": None,
        "test_command": None,
        "status": mapped_state,
        "metadata": {
            "gitlab_iid": iid,
            "gitlab_project": project_path,
            "gitlab_state": state,
            "labels": labels,
            "weight": weight,
            "time_estimate": time_estimate,
            "total_time_spent": total_time_spent,
        },
        "blocked_reason": None,
        "created_at": now,
        "updated_at": now,
    }


def import_epic_to_plan(
    epic: dict[str, Any],
    *,
    group_path: str,
    plan_id: str | None = None,
    instance_url: str = "https://gitlab.com",
) -> dict[str, Any]:
    """Convert a GitLab epic with its issues into an ExecutionPlan dict."""
    effective_plan_id = plan_id or f"plan-gl-{uuid.uuid4().hex[:12]}"

    epic_title = epic.get("title") or "Untitled Epic"
    issues = (epic.get("issues") or {}).get("nodes") or []

    milestones: list[dict[str, Any]] = []
    seen_milestones: set[str] = set()
    tasks: list[dict[str, Any]] = []

    for issue in issues:
        ms = issue.get("milestone")
        if isinstance(ms, dict) and ms.get("title"):
            ms_title = ms["title"]
            if ms_title not in seen_milestones:
                seen_milestones.add(ms_title)
                milestones.append({
                    "name": ms_title,
                    "description": ms.get("description") or "",
                    "start_date": ms.get("startDate"),
                    "due_date": ms.get("dueDate"),
                })

        task = map_gitlab_issue_to_task(
            issue,
            project_path=group_path,
            plan_id=effective_plan_id,
        )
        tasks.append(task)

    now = datetime.utcnow()
    return {
        "id": effective_plan_id,
        "implementation_brief_id": f"ib-gl-{uuid.uuid4().hex[:12]}",
        "target_engine": None,
        "target_repo": group_path,
        "project_type": "gitlab",
        "milestones": milestones or [{"name": epic_title, "description": ""}],
        "test_strategy": None,
        "handoff_prompt": None,
        "status": "draft",
        "created_at": now,
        "updated_at": now,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
        "metadata": {
            "source": "gitlab",
            "group_path": group_path,
            "epic_iid": epic.get("iid"),
            "instance_url": instance_url,
        },
        "tasks": tasks,
    }


def import_milestone_to_plan(
    milestone: dict[str, Any],
    issues: list[dict[str, Any]],
    *,
    project_path: str,
    plan_id: str | None = None,
) -> dict[str, Any]:
    """Convert a GitLab milestone and its issues into an ExecutionPlan dict."""
    effective_plan_id = plan_id or f"plan-gl-{uuid.uuid4().hex[:12]}"
    ms_title = milestone.get("title") or "Untitled Milestone"

    tasks = [
        map_gitlab_issue_to_task(
            issue,
            project_path=project_path,
            plan_id=effective_plan_id,
        )
        for issue in issues
    ]

    now = datetime.utcnow()
    return {
        "id": effective_plan_id,
        "implementation_brief_id": f"ib-gl-{uuid.uuid4().hex[:12]}",
        "target_engine": None,
        "target_repo": project_path,
        "project_type": "gitlab",
        "milestones": [{
            "name": ms_title,
            "description": milestone.get("description") or "",
            "start_date": milestone.get("startDate"),
            "due_date": milestone.get("dueDate"),
        }],
        "test_strategy": None,
        "handoff_prompt": None,
        "status": "draft",
        "created_at": now,
        "updated_at": now,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
        "metadata": {
            "source": "gitlab",
            "project_path": project_path,
            "milestone": ms_title,
        },
        "tasks": tasks,
    }


# ---------------------------------------------------------------------------
# Main importer class
# ---------------------------------------------------------------------------

class GitLabImporter(SourceImporter):
    """Import GitLab issues and epics through the GraphQL API."""

    def __init__(
        self,
        *,
        token_env: str = "GITLAB_TOKEN",
        instance_url: str = "https://gitlab.com",
        default_project: str | None = None,
        default_group: str | None = None,
        http_open: HttpOpen = urlopen,
    ):
        self.token_env = token_env
        self.instance_url = instance_url.rstrip("/")
        self.default_project = default_project
        self.default_group = default_group
        self.http_open = http_open

    # -- SourceImporter interface ------------------------------------------

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Fetch and normalize a GitLab issue. source_id: 'project/path#iid'."""
        project_path, iid = self._parse_source_id(source_id)
        client = self._build_client()
        data = client.query(ISSUES_QUERY, {"projectPath": project_path})

        project_data = data.get("project") or {}
        issues = (project_data.get("issues") or {}).get("nodes") or []

        for issue in issues:
            if issue.get("iid") == iid:
                return parse_gitlab_issue_json(
                    issue,
                    project_path=project_path,
                    instance_url=self.instance_url,
                )

        raise ImportError(f"GitLab issue #{iid} not found in {project_path}")

    def validate_source(self, source_id: str) -> bool:
        try:
            self.import_from_source(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List recent issues for the configured default project."""
        project_path = self.default_project
        if not project_path:
            raise ValueError("sources.gitlab.default_project is required")

        client = self._build_client()
        data = client.query(ISSUES_QUERY, {"projectPath": project_path})
        project_data = data.get("project") or {}
        issues = (project_data.get("issues") or {}).get("nodes") or []

        available = []
        for issue in issues[:limit]:
            labels = _extract_labels(issue.get("labels"))
            assignees = _extract_assignees(issue.get("assignees"))
            available.append({
                "id": f"{project_path}#{issue.get('iid')}",
                "iid": issue.get("iid"),
                "title": issue.get("title", ""),
                "state": issue.get("state", ""),
                "labels": labels,
                "assignees": assignees,
                "html_url": issue.get("webUrl"),
                "updated_at": issue.get("updatedAt"),
            })

        return available

    # -- Extended methods --------------------------------------------------

    def authenticate(
        self,
        token: str,
        instance_url: str = "https://gitlab.com",
    ) -> GitLabClient:
        """Create an authenticated GitLab client."""
        client = GitLabClient(
            token=token,
            instance_url=instance_url,
            http_open=self.http_open,
        )
        # Validate with a simple query
        client.query("query { currentUser { username } }")
        return client

    def fetch_group(self, group_path: str) -> GitLabGroup:
        """Fetch GitLab group details."""
        client = self._build_client()
        data = client.query(GROUP_QUERY, {"groupPath": group_path})
        group = data.get("group")
        if not group:
            raise ImportError(f"GitLab group not found: {group_path}")

        return GitLabGroup(
            id=group.get("id", ""),
            name=group.get("name", ""),
            full_path=group.get("fullPath", group_path),
            description=group.get("description") or "",
            web_url=group.get("webUrl") or "",
            projects=(group.get("projects") or {}).get("nodes") or [],
            epics=(group.get("epics") or {}).get("nodes") or [],
            raw=group,
        )

    def import_epic(self, group_path: str, epic_iid: int) -> dict[str, Any]:
        """Import a GitLab epic and its issues as an ExecutionPlan."""
        client = self._build_client()
        data = client.query(EPIC_QUERY, {
            "groupPath": group_path,
            "epicIid": str(epic_iid),
        })

        group_data = data.get("group") or {}
        epic = group_data.get("epic")
        if not epic:
            raise ImportError(f"GitLab epic &{epic_iid} not found in {group_path}")

        return import_epic_to_plan(
            epic,
            group_path=group_path,
            instance_url=self.instance_url,
        )

    def import_milestone(
        self,
        project_path: str,
        milestone_title: str,
    ) -> dict[str, Any]:
        """Import a GitLab milestone and its issues as an ExecutionPlan."""
        client = self._build_client()
        data = client.query(MILESTONE_ISSUES_QUERY, {
            "projectPath": project_path,
            "milestoneTitle": milestone_title,
        })

        project_data = data.get("project") or {}
        milestone = project_data.get("milestone")
        if not milestone:
            raise ImportError(
                f"GitLab milestone '{milestone_title}' not found in {project_path}"
            )

        issues = (project_data.get("issues") or {}).get("nodes") or []
        return import_milestone_to_plan(
            milestone,
            issues,
            project_path=project_path,
        )

    # -- Private -----------------------------------------------------------

    def _parse_source_id(self, source_id: str) -> tuple[str, int]:
        """Parse 'group/project#iid' format."""
        source_id = source_id.strip()

        if "#" in source_id:
            path, iid_str = source_id.rsplit("#", 1)
        else:
            path = self.default_project or ""
            iid_str = source_id

        if not path:
            path = self.default_project or ""
        if not path:
            raise ValueError(
                "GitLab source ID must be 'project/path#iid' or configure default_project"
            )

        try:
            iid = int(iid_str)
        except ValueError as exc:
            raise ValueError(f"Invalid GitLab issue IID: {iid_str}") from exc

        return path, iid

    def _build_client(self) -> GitLabClient:
        token = os.getenv(self.token_env) or ""
        return GitLabClient(
            token=token,
            instance_url=self.instance_url,
            http_open=self.http_open,
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _create_issue_summary(
    *,
    iid: int,
    title: str,
    state: str,
    author: str | None,
    labels: list[str],
    assignees: list[str],
    milestone_title: str | None,
    description: str,
    project_path: str,
) -> str:
    parts = [
        f"GitLab issue {project_path}#{iid}",
        f"State: {state}",
    ]

    if author:
        parts.append(f"Author: {author}")

    if labels:
        parts.append(f"Labels: {', '.join(labels)}")

    if assignees:
        parts.append(f"Assignees: {', '.join(assignees)}")

    if milestone_title:
        parts.append(f"Milestone: {milestone_title}")

    body = description.strip() if description else title
    if body:
        parts.append(f"\n{body}")

    return "\n".join(parts)
