"""Linear GraphQL API importer for issues and projects."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from blueprint.importers.base import SourceImporter

HttpOpen = Callable[..., Any]


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Linear state → blueprint state mapping
# ---------------------------------------------------------------------------

DEFAULT_STATE_MAP: dict[str, str] = {
    "backlog": "pending",
    "unstarted": "pending",
    "triage": "pending",
    "started": "in_progress",
    "in_progress": "in_progress",
    "completed": "completed",
    "done": "completed",
    "cancelled": "skipped",
    "canceled": "skipped",
    "duplicate": "skipped",
}

# ---------------------------------------------------------------------------
# Linear issue label patterns → blueprint entity type mapping
# ---------------------------------------------------------------------------

DEFAULT_TYPE_MAP: dict[str, str] = {
    "feature": "task",
    "bug": "task",
    "improvement": "task",
    "task": "task",
    "epic": "phase",
    "project": "phase",
}

# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

ISSUE_QUERY = """
query IssueQuery($id: String!) {
  issue(id: $id) {
    id
    identifier
    title
    description
    priority
    priorityLabel
    estimate
    url
    createdAt
    updatedAt
    state {
      id
      name
      type
    }
    assignee {
      id
      name
      email
    }
    creator {
      id
      name
    }
    team {
      id
      name
      key
    }
    project {
      id
      name
    }
    cycle {
      id
      name
      number
    }
    parent {
      id
      identifier
      title
    }
    children {
      nodes {
        id
        identifier
        title
        state {
          name
          type
        }
      }
    }
    labels {
      nodes {
        id
        name
        color
      }
    }
    relations {
      nodes {
        id
        type
        relatedIssue {
          id
          identifier
          title
        }
      }
    }
    attachments {
      nodes {
        id
        title
        url
        metadata
      }
    }
  }
}
"""

ISSUES_BY_PROJECT_QUERY = """
query IssuesByProject($projectId: String!, $first: Int!) {
  issues(filter: { project: { id: { eq: $projectId } } }, first: $first) {
    nodes {
      id
      identifier
      title
      description
      priority
      priorityLabel
      estimate
      url
      createdAt
      updatedAt
      state {
        id
        name
        type
      }
      assignee {
        id
        name
        email
      }
      team {
        id
        name
        key
      }
      project {
        id
        name
      }
      cycle {
        id
        name
        number
      }
      parent {
        id
        identifier
        title
      }
      labels {
        nodes {
          id
          name
          color
        }
      }
      relations {
        nodes {
          id
          type
          relatedIssue {
            id
            identifier
            title
          }
        }
      }
    }
  }
}
"""

ISSUES_BY_TEAM_QUERY = """
query IssuesByTeam($teamId: String!, $first: Int!) {
  issues(filter: { team: { id: { eq: $teamId } } }, first: $first) {
    nodes {
      id
      identifier
      title
      description
      priority
      priorityLabel
      estimate
      url
      createdAt
      updatedAt
      state {
        id
        name
        type
      }
      assignee {
        id
        name
        email
      }
      team {
        id
        name
        key
      }
      project {
        id
        name
      }
      cycle {
        id
        name
        number
      }
      parent {
        id
        identifier
        title
      }
      labels {
        nodes {
          id
          name
          color
        }
      }
      relations {
        nodes {
          id
          type
          relatedIssue {
            id
            identifier
            title
          }
        }
      }
    }
  }
}
"""

ISSUES_BY_FILTER_QUERY = """
query IssuesByFilter($filter: IssueFilter!, $first: Int!) {
  issues(filter: $filter, first: $first) {
    nodes {
      id
      identifier
      title
      description
      priority
      priorityLabel
      estimate
      url
      createdAt
      updatedAt
      state {
        id
        name
        type
      }
      assignee {
        id
        name
        email
      }
      team {
        id
        name
        key
      }
      project {
        id
        name
      }
      cycle {
        id
        name
        number
      }
      parent {
        id
        identifier
        title
      }
      labels {
        nodes {
          id
          name
          color
        }
      }
      relations {
        nodes {
          id
          type
          relatedIssue {
            id
            identifier
            title
          }
        }
      }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Client dataclass
# ---------------------------------------------------------------------------

@dataclass
class LinearClient:
    """Lightweight client for Linear GraphQL API."""

    api_key: str
    api_url: str = "https://api.linear.app/graphql"
    http_open: HttpOpen = urlopen

    def execute(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query and return the data dict."""
        body: dict[str, Any] = {"query": query}
        if variables:
            body["variables"] = variables

        data = json.dumps(body).encode("utf-8")
        request = Request(
            self.api_url,
            data=data,
            headers=self._headers(),
            method="POST",
        )

        try:
            with self.http_open(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(
                f"Linear API request failed with HTTP {exc.code}"
            ) from exc
        except URLError as exc:
            raise ImportError(
                f"Linear API request failed: {exc.reason}"
            ) from exc

        try:
            result = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ImportError("Linear API returned invalid JSON") from exc

        if "errors" in result:
            errors = result["errors"]
            msg = errors[0].get("message", "Unknown error") if errors else "Unknown"
            raise ImportError(f"Linear GraphQL error: {msg}")

        return result.get("data", {})

    def _headers(self) -> dict[str, str]:
        """Build request headers with API key auth."""
        return {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
            "User-Agent": "blueprint-linear-importer",
        }


# ---------------------------------------------------------------------------
# Public parsing helpers
# ---------------------------------------------------------------------------

def parse_issue_json(
    issue: dict[str, Any],
    *,
    type_map: dict[str, str] | None = None,
    state_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Normalize a Linear issue into a SourceBrief dictionary."""
    if not isinstance(issue, dict):
        raise ValueError("Linear issue payload must be a mapping")

    identifier = issue.get("identifier")
    if not identifier:
        raise ValueError("Linear issue missing required 'identifier' field")

    title = issue.get("title") or ""
    if not title:
        raise ValueError("Linear issue missing required 'title' field")

    effective_type_map = {**DEFAULT_TYPE_MAP, **(type_map or {})}
    effective_state_map = {**DEFAULT_STATE_MAP, **(state_map or {})}

    # State
    state_obj = issue.get("state") or {}
    state_name = state_obj.get("name", "Backlog")
    state_type = state_obj.get("type", "backlog")
    mapped_state = effective_state_map.get(
        state_type, effective_state_map.get(state_name.lower(), "pending")
    )

    # Issue type from labels
    labels = _extract_labels(issue)
    label_names = [l["name"] for l in labels]
    issue_type = _infer_type_from_labels(label_names, effective_type_map)
    mapped_entity_type = effective_type_map.get(issue_type, "task")

    description = issue.get("description") or ""

    priority = issue.get("priority")
    priority_label = issue.get("priorityLabel")
    estimate = issue.get("estimate")

    assignee_obj = issue.get("assignee") or {}
    assignee = assignee_obj.get("name") if assignee_obj else None

    creator_obj = issue.get("creator") or {}
    creator = creator_obj.get("name") if creator_obj else None

    team_obj = issue.get("team") or {}
    team_name = team_obj.get("name") if team_obj else None
    team_key = team_obj.get("key") if team_obj else None

    project_obj = issue.get("project") or {}
    project_name = project_obj.get("name") if project_obj else None

    cycle_obj = issue.get("cycle") or {}
    cycle_name = cycle_obj.get("name") if cycle_obj else None

    parent_obj = issue.get("parent") or {}
    parent_id = parent_obj.get("identifier") if parent_obj else None

    children = _extract_children(issue)
    relations = _extract_relations(issue)

    url = issue.get("url") or ""
    html_url = url or f"https://linear.app/issue/{identifier}"

    now = datetime.utcnow()

    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": "linear",
        "summary": _create_summary(
            identifier=identifier,
            title=title,
            state_name=state_name,
            priority_label=priority_label,
            assignee=assignee,
            labels=label_names,
            team_name=team_name,
            project_name=project_name,
            cycle_name=cycle_name,
            description=description,
        ),
        "source_project": "linear",
        "source_entity_type": mapped_entity_type,
        "source_id": identifier,
        "source_payload": {
            "issue": issue,
            "normalized": {
                "identifier": identifier,
                "title": title,
                "state": state_name,
                "state_type": state_type,
                "mapped_state": mapped_state,
                "description": description,
                "priority": priority,
                "priority_label": priority_label,
                "estimate": estimate,
                "assignee": assignee,
                "creator": creator,
                "team": team_name,
                "team_key": team_key,
                "project": project_name,
                "cycle": cycle_name,
                "parent": parent_id,
                "labels": label_names,
                "children": children,
                "relations": relations,
            },
        },
        "source_links": {
            "html_url": html_url,
            "api_url": None,
        },
        "created_at": now,
        "updated_at": now,
    }


def map_issue_to_task(
    issue: dict[str, Any],
    *,
    plan_id: str = "",
    state_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Map a Linear issue to an ExecutionTask dictionary."""
    identifier = issue.get("identifier", "")
    title = issue.get("title") or identifier
    description = issue.get("description") or title

    state_obj = issue.get("state") or {}
    state_type = state_obj.get("type", "backlog")
    effective_state_map = {**DEFAULT_STATE_MAP, **(state_map or {})}
    mapped_status = effective_state_map.get(state_type, "pending")

    priority = issue.get("priority")
    priority_label = issue.get("priorityLabel")
    estimate = issue.get("estimate")

    assignee_obj = issue.get("assignee") or {}
    assignee = assignee_obj.get("name") if assignee_obj else None

    labels = _extract_labels(issue)
    label_names = [l["name"] for l in labels]

    cycle_obj = issue.get("cycle") or {}
    cycle_name = cycle_obj.get("name") if cycle_obj else None

    # Dependencies from relations
    relations = _extract_relations(issue)
    depends_on: list[str] = []
    for rel in relations:
        if rel.get("type") == "blocks":
            dep_id = rel.get("related_identifier")
            if dep_id:
                depends_on.append(dep_id)

    estimated_hours: float | None = None
    if estimate is not None:
        try:
            estimated_hours = float(estimate) * 4
        except (TypeError, ValueError):
            pass

    complexity = "medium"
    if estimate is not None:
        try:
            est = float(estimate)
            if est >= 8:
                complexity = "high"
            elif est <= 2:
                complexity = "low"
        except (TypeError, ValueError):
            pass

    risk_level = _priority_to_risk(priority)

    now = datetime.utcnow()

    return {
        "id": f"linear-{identifier}",
        "execution_plan_id": plan_id or None,
        "title": title,
        "description": description,
        "milestone": cycle_name,
        "owner_type": assignee,
        "suggested_engine": None,
        "depends_on": depends_on,
        "files_or_modules": None,
        "acceptance_criteria": [f"Complete: {title}"],
        "estimated_complexity": complexity,
        "estimated_hours": estimated_hours,
        "risk_level": risk_level,
        "test_command": None,
        "status": mapped_status,
        "metadata": {
            "linear_identifier": identifier,
            "linear_state": state_obj.get("name", ""),
            "linear_priority": priority_label,
            "linear_cycle": cycle_name,
            "labels": label_names,
        },
        "blocked_reason": None,
        "created_at": now,
        "updated_at": now,
    }


def import_project_to_plan(
    issues: list[dict[str, Any]],
    *,
    project_name: str,
    plan_id: str | None = None,
    type_map: dict[str, str] | None = None,
    state_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Convert a set of Linear issues into an ExecutionPlan dict."""
    effective_plan_id = plan_id or f"plan-linear-{uuid.uuid4().hex[:12]}"
    effective_type_map = {**DEFAULT_TYPE_MAP, **(type_map or {})}

    milestones: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []

    seen_cycles: set[str] = set()
    for issue in issues:
        labels = _extract_labels(issue)
        label_names = [l["name"] for l in labels]
        issue_type = _infer_type_from_labels(label_names, effective_type_map)
        mapped = effective_type_map.get(issue_type, "task")

        if mapped == "phase":
            milestones.append({
                "name": issue.get("title", f"Phase {issue.get('identifier')}"),
                "description": issue.get("description") or "",
                "linear_identifier": issue.get("identifier"),
            })
        else:
            task = map_issue_to_task(
                issue,
                plan_id=effective_plan_id,
                state_map=state_map,
            )
            tasks.append(task)
            cycle_obj = issue.get("cycle") or {}
            cycle_name = cycle_obj.get("name")
            if cycle_name and cycle_name not in seen_cycles:
                seen_cycles.add(cycle_name)

    if not milestones:
        for cycle in sorted(seen_cycles):
            milestones.append({"name": cycle, "description": ""})

    now = datetime.utcnow()
    return {
        "id": effective_plan_id,
        "implementation_brief_id": f"ib-linear-{uuid.uuid4().hex[:12]}",
        "target_engine": None,
        "target_repo": None,
        "project_type": "linear",
        "milestones": milestones,
        "test_strategy": None,
        "handoff_prompt": None,
        "status": "draft",
        "created_at": now,
        "updated_at": now,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
        "metadata": {
            "source": "linear",
            "project_name": project_name,
        },
        "tasks": tasks,
    }


# ---------------------------------------------------------------------------
# Main importer class
# ---------------------------------------------------------------------------

class LinearImporter(SourceImporter):
    """Import Linear issues through the GraphQL API."""

    def __init__(
        self,
        *,
        api_key_env: str = "LINEAR_API_KEY",
        api_url: str = "https://api.linear.app/graphql",
        type_map: dict[str, str] | None = None,
        state_map: dict[str, str] | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        http_open: HttpOpen = urlopen,
    ):
        self.api_key_env = api_key_env
        self.api_url = api_url
        self.type_map = type_map
        self.state_map = state_map
        self.team_id = team_id
        self.project_id = project_id
        self.http_open = http_open

    # -- SourceImporter interface ------------------------------------------

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Fetch and normalize a Linear issue by ID."""
        client = self._build_client()
        data = client.execute(ISSUE_QUERY, {"id": source_id.strip()})
        issue = data.get("issue")
        if not issue:
            raise ImportError(f"Linear issue not found: {source_id}")
        return parse_issue_json(
            issue,
            type_map=self.type_map,
            state_map=self._effective_state_map(),
        )

    def validate_source(self, source_id: str) -> bool:
        """Check whether a Linear issue is accessible."""
        try:
            client = self._build_client()
            data = client.execute(ISSUE_QUERY, {"id": source_id.strip()})
            return data.get("issue") is not None
        except Exception:
            return False

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List recent issues for the configured team or project."""
        client = self._build_client()

        if self.project_id:
            data = client.execute(
                ISSUES_BY_PROJECT_QUERY,
                {"projectId": self.project_id, "first": limit},
            )
        elif self.team_id:
            data = client.execute(
                ISSUES_BY_TEAM_QUERY,
                {"teamId": self.team_id, "first": limit},
            )
        else:
            raise ValueError(
                "team_id or project_id is required to list available issues"
            )

        issues = (data.get("issues") or {}).get("nodes", [])

        available: list[dict[str, Any]] = []
        for issue in issues:
            state_obj = issue.get("state") or {}
            assignee_obj = issue.get("assignee") or {}

            available.append({
                "id": issue.get("id", ""),
                "identifier": issue.get("identifier", ""),
                "title": issue.get("title", ""),
                "status": state_obj.get("name", ""),
                "assignee": assignee_obj.get("name") if assignee_obj else None,
                "priority": issue.get("priorityLabel"),
                "html_url": issue.get("url", ""),
                "updated_at": issue.get("updatedAt"),
            })

        return available

    # -- Extended methods --------------------------------------------------

    def authenticate(
        self,
        api_key: str,
    ) -> LinearClient:
        """Create an authenticated client and validate connectivity."""
        client = LinearClient(
            api_key=api_key,
            api_url=self.api_url,
            http_open=self.http_open,
        )
        # Validate by fetching viewer
        client.execute("query { viewer { id name } }")
        return client

    def fetch_by_project(
        self,
        project_id: str,
        *,
        max_results: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch issues for a project and return SourceBrief dicts."""
        client = self._build_client()
        data = client.execute(
            ISSUES_BY_PROJECT_QUERY,
            {"projectId": project_id, "first": max_results},
        )
        issues = (data.get("issues") or {}).get("nodes", [])

        return [
            parse_issue_json(
                issue,
                type_map=self.type_map,
                state_map=self._effective_state_map(),
            )
            for issue in issues
        ]

    def fetch_by_team(
        self,
        team_id: str,
        *,
        max_results: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch issues for a team and return SourceBrief dicts."""
        client = self._build_client()
        data = client.execute(
            ISSUES_BY_TEAM_QUERY,
            {"teamId": team_id, "first": max_results},
        )
        issues = (data.get("issues") or {}).get("nodes", [])

        return [
            parse_issue_json(
                issue,
                type_map=self.type_map,
                state_map=self._effective_state_map(),
            )
            for issue in issues
        ]

    def fetch_by_filter(
        self,
        graphql_filter: dict[str, Any],
        *,
        max_results: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch issues matching a GraphQL filter and return SourceBrief dicts."""
        client = self._build_client()
        data = client.execute(
            ISSUES_BY_FILTER_QUERY,
            {"filter": graphql_filter, "first": max_results},
        )
        issues = (data.get("issues") or {}).get("nodes", [])

        return [
            parse_issue_json(
                issue,
                type_map=self.type_map,
                state_map=self._effective_state_map(),
            )
            for issue in issues
        ]

    def import_project(
        self,
        project_id: str | None = None,
        project_name: str = "Linear Project",
        *,
        max_results: int = 200,
    ) -> dict[str, Any]:
        """Import all issues for a project as an ExecutionPlan."""
        pid = project_id or self.project_id
        if not pid:
            raise ValueError("project_id is required to import a project")

        client = self._build_client()
        data = client.execute(
            ISSUES_BY_PROJECT_QUERY,
            {"projectId": pid, "first": max_results},
        )
        issues = (data.get("issues") or {}).get("nodes", [])

        return import_project_to_plan(
            issues,
            project_name=project_name,
            type_map=self.type_map,
            state_map=self._effective_state_map(),
        )

    # -- Private helpers ---------------------------------------------------

    def _build_client(self) -> LinearClient:
        """Build an authenticated client from environment variables."""
        api_key = os.getenv(self.api_key_env) or ""
        return LinearClient(
            api_key=api_key,
            api_url=self.api_url,
            http_open=self.http_open,
        )

    def _effective_state_map(self) -> dict[str, str]:
        """Return the combined state map."""
        if self.state_map:
            return {**DEFAULT_STATE_MAP, **self.state_map}
        return DEFAULT_STATE_MAP


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_labels(issue: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract labels from a Linear issue."""
    labels_obj = issue.get("labels") or {}
    nodes = labels_obj.get("nodes", [])
    return [n for n in nodes if isinstance(n, dict)]


def _extract_children(issue: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract child issue summaries from a Linear issue."""
    children_obj = issue.get("children") or {}
    nodes = children_obj.get("nodes", [])
    result: list[dict[str, Any]] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        state_obj = node.get("state") or {}
        result.append({
            "identifier": node.get("identifier"),
            "title": node.get("title"),
            "state": state_obj.get("name"),
            "state_type": state_obj.get("type"),
        })
    return result


def _extract_relations(issue: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract relations from a Linear issue."""
    relations_obj = issue.get("relations") or {}
    nodes = relations_obj.get("nodes", [])
    result: list[dict[str, Any]] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        related = node.get("relatedIssue") or {}
        result.append({
            "type": node.get("type"),
            "related_id": related.get("id"),
            "related_identifier": related.get("identifier"),
            "related_title": related.get("title"),
        })
    return result


def _infer_type_from_labels(
    label_names: list[str],
    type_map: dict[str, str],
) -> str:
    """Infer issue type from label names using the type map."""
    for label in label_names:
        lower = label.lower()
        if lower in type_map:
            return lower
    return "task"


def _priority_to_risk(priority: int | None) -> str | None:
    """Map Linear priority number to a risk level.

    Linear priorities: 0=None, 1=Urgent, 2=High, 3=Medium, 4=Low.
    """
    if priority is None or priority == 0:
        return None
    if priority == 1:
        return "high"
    if priority == 2:
        return "high"
    if priority == 3:
        return "medium"
    return "low"


def _create_summary(
    *,
    identifier: str,
    title: str,
    state_name: str,
    priority_label: str | None,
    assignee: str | None,
    labels: list[str],
    team_name: str | None,
    project_name: str | None,
    cycle_name: str | None,
    description: str,
) -> str:
    """Create a summary string for the SourceBrief."""
    parts = [
        f"Linear Issue {identifier}",
        f"State: {state_name}",
    ]

    if priority_label:
        parts.append(f"Priority: {priority_label}")

    if assignee:
        parts.append(f"Assignee: {assignee}")

    if team_name:
        parts.append(f"Team: {team_name}")

    if project_name:
        parts.append(f"Project: {project_name}")

    if labels:
        parts.append(f"Labels: {', '.join(labels)}")

    if cycle_name:
        parts.append(f"Cycle: {cycle_name}")

    body = description.strip() if description else title
    if body:
        parts.append(f"\n{body}")

    return "\n".join(parts)
