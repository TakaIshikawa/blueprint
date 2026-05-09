"""Jira Cloud REST API importer for issues, stories, and epics."""

from __future__ import annotations

import base64
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


# ---------------------------------------------------------------------------
# Jira status → blueprint state mapping
# ---------------------------------------------------------------------------

DEFAULT_STATE_MAP: dict[str, str] = {
    "To Do": "pending",
    "Open": "pending",
    "Backlog": "pending",
    "Selected for Development": "pending",
    "In Progress": "in_progress",
    "In Review": "in_progress",
    "Done": "completed",
    "Closed": "completed",
    "Resolved": "completed",
}

# ---------------------------------------------------------------------------
# Jira issue type → blueprint entity type mapping
# ---------------------------------------------------------------------------

DEFAULT_TYPE_MAP: dict[str, str] = {
    "Epic": "phase",
    "Story": "task",
    "Task": "task",
    "Sub-task": "task",
    "Bug": "task",
    "Improvement": "task",
    "New Feature": "task",
}

# ---------------------------------------------------------------------------
# Jira link type → simplified name mapping
# ---------------------------------------------------------------------------

DEFAULT_LINK_TYPE_MAP: dict[str, str] = {
    "Blocks": "blocks",
    "is blocked by": "blocked_by",
    "Cloners": "clones",
    "is cloned by": "cloned_by",
    "Duplicate": "duplicates",
    "is duplicated by": "duplicated_by",
    "Relates": "relates_to",
    "Cause": "causes",
    "is caused by": "caused_by",
}


# ---------------------------------------------------------------------------
# Client dataclass
# ---------------------------------------------------------------------------

@dataclass
class JiraClient:
    """Lightweight client for Jira Cloud REST API v3."""

    base_url: str
    email: str
    api_token: str
    http_open: HttpOpen = urlopen

    def request_json(self, path: str) -> Any:
        """Execute a GET request and return parsed JSON."""
        url = f"{self.base_url}/rest/api/3/{path}"
        request = Request(url, headers=self._headers(), method="GET")

        try:
            with self.http_open(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(
                f"Jira API request failed with HTTP {exc.code}: {path}"
            ) from exc
        except URLError as exc:
            raise ImportError(
                f"Jira API request failed: {exc.reason}"
            ) from exc

        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ImportError("Jira API returned invalid JSON") from exc

    def search_issues(
        self,
        jql: str,
        *,
        max_results: int = 50,
        start_at: int = 0,
        fields: str | None = None,
    ) -> Any:
        """Search issues using JQL via the search endpoint."""
        params = (
            f"jql={quote(jql)}"
            f"&maxResults={max_results}"
            f"&startAt={start_at}"
        )
        if fields:
            params += f"&fields={quote(fields)}"
        return self.request_json(f"search?{params}")

    def _headers(self) -> dict[str, str]:
        """Build request headers with Basic auth (email:token)."""
        credentials = base64.b64encode(
            f"{self.email}:{self.api_token}".encode()
        ).decode()
        return {
            "Authorization": f"Basic {credentials}",
            "Accept": "application/json",
            "User-Agent": "blueprint-jira-importer",
        }


# ---------------------------------------------------------------------------
# Public parsing helpers
# ---------------------------------------------------------------------------

def parse_issue_json(
    issue: dict[str, Any],
    *,
    instance_url: str,
    type_map: dict[str, str] | None = None,
    state_map: dict[str, str] | None = None,
    custom_field_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Normalize a Jira issue into a SourceBrief dictionary."""
    if not isinstance(issue, dict):
        raise ValueError("Jira issue payload must be a mapping")

    issue_key = issue.get("key")
    if not issue_key:
        raise ValueError("Jira issue missing required 'key' field")

    fields = issue.get("fields") or {}

    summary = fields.get("summary") or ""
    if not summary:
        raise ValueError("Jira issue missing required 'summary' field")

    effective_type_map = {**DEFAULT_TYPE_MAP, **(type_map or {})}
    effective_state_map = {**DEFAULT_STATE_MAP, **(state_map or {})}

    # Extract standard fields
    issue_type_obj = fields.get("issuetype") or {}
    issue_type = issue_type_obj.get("name", "Task")

    status_obj = fields.get("status") or {}
    status_name = status_obj.get("name", "Open")
    mapped_state = effective_state_map.get(status_name, "pending")
    mapped_entity_type = effective_type_map.get(issue_type, "task")

    description = _extract_text_from_adf(fields.get("description"))

    priority_obj = fields.get("priority") or {}
    priority = priority_obj.get("name")

    assignee_obj = fields.get("assignee") or {}
    assignee = assignee_obj.get("displayName") if assignee_obj else None

    reporter_obj = fields.get("reporter") or {}
    reporter = reporter_obj.get("displayName") if reporter_obj else None

    labels = fields.get("labels") or []

    components = [
        c.get("name", "") for c in (fields.get("components") or [])
        if isinstance(c, dict)
    ]

    # Sprint extraction (commonly in customfield_10020)
    sprint = _extract_sprint(fields)

    # Epic link extraction (commonly in customfield_10014 or parent)
    epic_link = _extract_epic_link(fields)

    # Story points (commonly in customfield_10028 or story_points)
    story_points = _extract_story_points(fields)

    # Acceptance criteria (commonly in custom fields or description)
    acceptance_criteria = _extract_acceptance_criteria(fields, custom_field_map)

    # Linked issues
    links = _parse_issue_links(fields.get("issuelinks") or [])

    # Attachments
    attachments = _parse_attachments(fields.get("attachment") or [])

    # Custom field extraction
    custom_fields = _extract_custom_fields(fields, custom_field_map or {})

    # Resolution
    resolution_obj = fields.get("resolution") or {}
    resolution = resolution_obj.get("name") if resolution_obj else None

    project_obj = fields.get("project") or {}
    project_key = project_obj.get("key", "")

    source_id = issue_key
    html_url = f"{instance_url}/browse/{issue_key}"

    now = datetime.utcnow()

    return {
        "id": generate_source_brief_id(),
        "title": summary,
        "domain": "jira",
        "summary": _create_summary(
            issue_key=issue_key,
            issue_type=issue_type,
            summary=summary,
            status=status_name,
            priority=priority,
            assignee=assignee,
            labels=labels,
            components=components,
            sprint=sprint,
            description=description,
        ),
        "source_project": "jira",
        "source_entity_type": mapped_entity_type,
        "source_id": source_id,
        "source_payload": {
            "issue": issue,
            "normalized": {
                "key": issue_key,
                "issue_type": issue_type,
                "summary": summary,
                "status": status_name,
                "mapped_state": mapped_state,
                "description": description,
                "priority": priority,
                "assignee": assignee,
                "reporter": reporter,
                "labels": labels,
                "components": components,
                "sprint": sprint,
                "epic_link": epic_link,
                "story_points": story_points,
                "acceptance_criteria": acceptance_criteria,
                "resolution": resolution,
                "project_key": project_key,
                "links": links,
                "attachments": attachments,
                "custom_fields": custom_fields,
            },
        },
        "source_links": {
            "html_url": html_url,
            "api_url": issue.get("self"),
        },
        "created_at": now,
        "updated_at": now,
    }


def map_issue_to_task(
    issue: dict[str, Any],
    *,
    instance_url: str,  # noqa: ARG001 — kept for API consistency
    plan_id: str = "",
    state_map: dict[str, str] | None = None,
    custom_field_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Map a Jira issue to an ExecutionTask dictionary."""
    fields = issue.get("fields") or {}
    issue_key = issue.get("key", "")

    summary = fields.get("summary") or issue_key
    description = _extract_text_from_adf(fields.get("description")) or summary

    status_obj = fields.get("status") or {}
    status_name = status_obj.get("name", "Open")
    effective_state_map = {**DEFAULT_STATE_MAP, **(state_map or {})}
    mapped_status = effective_state_map.get(status_name, "pending")

    issue_type_obj = fields.get("issuetype") or {}
    issue_type = issue_type_obj.get("name", "Task")

    priority_obj = fields.get("priority") or {}
    priority_name = priority_obj.get("name")

    assignee_obj = fields.get("assignee") or {}
    assignee = assignee_obj.get("displayName") if assignee_obj else None

    labels = fields.get("labels") or []
    sprint = _extract_sprint(fields)
    story_points = _extract_story_points(fields)
    acceptance_criteria = _extract_acceptance_criteria(fields, custom_field_map)

    # Parse links for dependencies
    issue_links = fields.get("issuelinks") or []
    depends_on: list[str] = []
    for link in issue_links:
        if not isinstance(link, dict):
            continue
        link_type = link.get("type", {})
        inward_name = link_type.get("inward", "")
        if inward_name in ("is blocked by", "is caused by"):
            inward_issue = link.get("inwardIssue") or {}
            dep_key = inward_issue.get("key")
            if dep_key:
                depends_on.append(dep_key)

    estimated_hours: float | None = None
    if story_points is not None:
        try:
            estimated_hours = float(story_points) * 4
        except (TypeError, ValueError):
            pass

    complexity = "medium"
    if story_points is not None:
        try:
            sp = float(story_points)
            if sp >= 8:
                complexity = "high"
            elif sp <= 2:
                complexity = "low"
        except (TypeError, ValueError):
            pass

    risk_level = _priority_to_risk(priority_name)

    ac_list = [acceptance_criteria] if acceptance_criteria else [f"Complete: {summary}"]

    now = datetime.utcnow()

    return {
        "id": f"jira-{issue_key}",
        "execution_plan_id": plan_id or None,
        "title": summary,
        "description": description,
        "milestone": sprint,
        "owner_type": assignee,
        "suggested_engine": None,
        "depends_on": depends_on,
        "files_or_modules": None,
        "acceptance_criteria": ac_list,
        "estimated_complexity": complexity,
        "estimated_hours": estimated_hours,
        "risk_level": risk_level,
        "test_command": None,
        "status": mapped_status,
        "metadata": {
            "jira_key": issue_key,
            "jira_issue_type": issue_type,
            "jira_status": status_name,
            "jira_priority": priority_name,
            "jira_sprint": sprint,
            "labels": labels,
        },
        "blocked_reason": None,
        "created_at": now,
        "updated_at": now,
    }


def import_project_to_plan(
    issues: list[dict[str, Any]],
    *,
    instance_url: str,
    project_key: str,
    plan_id: str | None = None,
    type_map: dict[str, str] | None = None,
    state_map: dict[str, str] | None = None,
    custom_field_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Convert a set of Jira issues into an ExecutionPlan dict."""
    effective_plan_id = plan_id or f"plan-jira-{uuid.uuid4().hex[:12]}"
    effective_type_map = {**DEFAULT_TYPE_MAP, **(type_map or {})}

    milestones: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []

    seen_sprints: set[str] = set()
    for issue in issues:
        fields = issue.get("fields") or {}
        issue_type_obj = fields.get("issuetype") or {}
        issue_type = issue_type_obj.get("name", "Task")
        mapped = effective_type_map.get(issue_type, "task")

        if mapped == "phase":
            milestones.append({
                "name": fields.get("summary", f"Phase {issue.get('key')}"),
                "description": _extract_text_from_adf(fields.get("description")) or "",
                "jira_key": issue.get("key"),
                "jira_issue_type": issue_type,
            })
        else:
            task = map_issue_to_task(
                issue,
                instance_url=instance_url,
                plan_id=effective_plan_id,
                state_map=state_map,
                custom_field_map=custom_field_map,
            )
            tasks.append(task)
            sprint = _extract_sprint(fields)
            if sprint and sprint not in seen_sprints:
                seen_sprints.add(sprint)

    if not milestones:
        for sprint in sorted(seen_sprints):
            milestones.append({"name": sprint, "description": ""})

    now = datetime.utcnow()
    return {
        "id": effective_plan_id,
        "implementation_brief_id": f"ib-jira-{uuid.uuid4().hex[:12]}",
        "target_engine": None,
        "target_repo": None,
        "project_type": "jira",
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
            "source": "jira",
            "project_key": project_key,
            "instance_url": instance_url,
        },
        "tasks": tasks,
    }


# ---------------------------------------------------------------------------
# Main importer class
# ---------------------------------------------------------------------------

class JiraImporter(SourceImporter):
    """Import Jira issues through the Cloud REST API v3."""

    def __init__(
        self,
        *,
        instance_url: str | None = None,
        project_key: str | None = None,
        email_env: str = "JIRA_EMAIL",
        token_env: str = "JIRA_API_TOKEN",
        type_map: dict[str, str] | None = None,
        state_map: dict[str, str] | None = None,
        custom_field_map: dict[str, str] | None = None,
        http_open: HttpOpen = urlopen,
    ):
        self.instance_url = (instance_url or "").rstrip("/")
        self.project_key = project_key
        self.email_env = email_env
        self.token_env = token_env
        self.type_map = type_map
        self.state_map = state_map
        self.custom_field_map = custom_field_map
        self.http_open = http_open

    # -- SourceImporter interface ------------------------------------------

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Fetch and normalize a Jira issue by key (e.g. 'PROJ-123')."""
        issue_key = source_id.strip()
        client = self._build_client()
        issue = self._fetch_issue(client, issue_key)
        return parse_issue_json(
            issue,
            instance_url=self.instance_url,
            type_map=self.type_map,
            state_map=self._effective_state_map(),
            custom_field_map=self.custom_field_map,
        )

    def validate_source(self, source_id: str) -> bool:
        """Check whether a Jira issue is accessible."""
        try:
            issue_key = source_id.strip()
            client = self._build_client()
            self._fetch_issue(client, issue_key)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List recent issues for the configured project."""
        if not self.project_key:
            raise ValueError(
                "project_key is required to list available issues"
            )

        client = self._build_client()
        jql = f"project = {self.project_key} ORDER BY updated DESC"
        result = client.search_issues(jql, max_results=limit)

        available: list[dict[str, Any]] = []
        for issue in result.get("issues", []):
            fields = issue.get("fields") or {}
            issue_key = issue.get("key", "")
            status_obj = fields.get("status") or {}
            issue_type_obj = fields.get("issuetype") or {}
            assignee_obj = fields.get("assignee") or {}
            priority_obj = fields.get("priority") or {}

            available.append({
                "id": issue_key,
                "title": fields.get("summary", ""),
                "status": status_obj.get("name", ""),
                "type": issue_type_obj.get("name", ""),
                "assignee": assignee_obj.get("displayName") if assignee_obj else None,
                "priority": priority_obj.get("name") if priority_obj else None,
                "html_url": f"{self.instance_url}/browse/{issue_key}",
                "updated_at": fields.get("updated"),
            })

        return available

    # -- Extended methods --------------------------------------------------

    def authenticate(
        self,
        instance_url: str,
        email: str,
        api_token: str,
    ) -> JiraClient:
        """Create an authenticated client and validate connectivity."""
        client = JiraClient(
            base_url=instance_url.rstrip("/"),
            email=email,
            api_token=api_token,
            http_open=self.http_open,
        )
        # Validate by fetching server info
        client.request_json("serverInfo")
        return client

    def fetch_by_jql(
        self,
        jql: str,
        *,
        max_results: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch issues matching a JQL query and return SourceBrief dicts."""
        client = self._build_client()
        result = client.search_issues(jql, max_results=max_results)
        issues = result.get("issues", [])

        return [
            parse_issue_json(
                issue,
                instance_url=self.instance_url,
                type_map=self.type_map,
                state_map=self._effective_state_map(),
                custom_field_map=self.custom_field_map,
            )
            for issue in issues
        ]

    def import_project(
        self,
        project_key: str | None = None,
        *,
        max_results: int = 200,
    ) -> dict[str, Any]:
        """Import all issues for a project as an ExecutionPlan."""
        key = project_key or self.project_key
        if not key:
            raise ValueError("project_key is required to import a project")

        client = self._build_client()
        jql = f"project = {key} ORDER BY rank ASC"
        result = client.search_issues(jql, max_results=max_results)
        issues = result.get("issues", [])

        return import_project_to_plan(
            issues,
            instance_url=self.instance_url,
            project_key=key,
            type_map=self.type_map,
            state_map=self._effective_state_map(),
            custom_field_map=self.custom_field_map,
        )

    def import_epic(
        self,
        epic_key: str,
        *,
        max_results: int = 200,
    ) -> dict[str, Any]:
        """Import an epic and its child issues as an ExecutionPlan."""
        client = self._build_client()

        # Fetch the epic itself
        epic = self._fetch_issue(client, epic_key)

        # Fetch child issues (parent = epic_key)
        jql = f"parent = {epic_key} ORDER BY rank ASC"
        result = client.search_issues(jql, max_results=max_results)
        child_issues = result.get("issues", [])

        all_issues = [epic] + child_issues

        project_key = (epic.get("fields") or {}).get("project", {}).get("key", "")

        return import_project_to_plan(
            all_issues,
            instance_url=self.instance_url,
            project_key=project_key,
            type_map=self.type_map,
            state_map=self._effective_state_map(),
            custom_field_map=self.custom_field_map,
        )

    # -- Private helpers ---------------------------------------------------

    def _build_client(self) -> JiraClient:
        """Build an authenticated client from environment variables."""
        if not self.instance_url:
            raise ValueError("instance_url is required")

        email = os.getenv(self.email_env) or ""
        api_token = os.getenv(self.token_env) or ""

        return JiraClient(
            base_url=self.instance_url,
            email=email,
            api_token=api_token,
            http_open=self.http_open,
        )

    def _effective_state_map(self) -> dict[str, str]:
        """Return the combined state map."""
        if self.state_map:
            return {**DEFAULT_STATE_MAP, **self.state_map}
        return DEFAULT_STATE_MAP

    def _fetch_issue(self, client: JiraClient, issue_key: str) -> dict[str, Any]:
        """Fetch a single Jira issue with all fields."""
        result = client.request_json(f"issue/{issue_key}")
        if not isinstance(result, dict):
            raise ImportError("Jira issue response was not a mapping")
        return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_text_from_adf(adf: Any) -> str:
    """Extract plain text from Atlassian Document Format (ADF).

    ADF is a nested JSON structure used for rich text in Jira Cloud v3.
    This extracts text nodes recursively for a plain-text representation.
    """
    if adf is None:
        return ""
    if isinstance(adf, str):
        return adf

    if not isinstance(adf, dict):
        return ""

    parts: list[str] = []
    for content_node in adf.get("content", []):
        if not isinstance(content_node, dict):
            continue
        node_type = content_node.get("type", "")
        if node_type == "text":
            parts.append(content_node.get("text", ""))
        elif "content" in content_node:
            parts.append(_extract_text_from_adf(content_node))
        # Handle list items, etc.
        elif node_type in ("listItem", "tableCell", "tableRow"):
            parts.append(_extract_text_from_adf(content_node))

    return "\n".join(parts) if parts else ""


def _extract_sprint(fields: dict[str, Any]) -> str | None:
    """Extract sprint name from Jira fields.

    Sprint data is commonly in customfield_10020 as a list of objects.
    """
    sprint_field = fields.get("customfield_10020")
    if isinstance(sprint_field, list) and sprint_field:
        last_sprint = sprint_field[-1]
        if isinstance(last_sprint, dict):
            return last_sprint.get("name")
        if isinstance(last_sprint, str):
            return last_sprint
    if isinstance(sprint_field, str):
        return sprint_field
    # Also check sprint field alias
    sprint_alias = fields.get("sprint")
    if isinstance(sprint_alias, dict):
        return sprint_alias.get("name")
    return None


def _extract_epic_link(fields: dict[str, Any]) -> str | None:
    """Extract epic link from Jira fields.

    Epic link may be in customfield_10014, parent, or epic field.
    """
    # Next-gen / Team-managed: parent field
    parent = fields.get("parent")
    if isinstance(parent, dict):
        parent_type = (parent.get("fields") or {}).get("issuetype", {})
        if isinstance(parent_type, dict) and parent_type.get("name") == "Epic":
            return parent.get("key")

    # Classic: customfield_10014 (Epic Link)
    epic_link = fields.get("customfield_10014")
    if isinstance(epic_link, str) and epic_link:
        return epic_link

    return None


def _extract_story_points(fields: dict[str, Any]) -> float | None:
    """Extract story points from Jira fields.

    Story points are commonly in customfield_10028 or story_points.
    """
    for field_name in ("customfield_10028", "story_points", "customfield_10016"):
        value = fields.get(field_name)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


def _extract_acceptance_criteria(
    fields: dict[str, Any],
    custom_field_map: dict[str, str] | None,
) -> str:
    """Extract acceptance criteria from custom fields or description."""
    if custom_field_map:
        ac_field = custom_field_map.get("acceptance_criteria")
        if ac_field:
            value = fields.get(ac_field)
            if isinstance(value, str):
                return value
            if isinstance(value, dict):
                return _extract_text_from_adf(value)

    # Common custom field for acceptance criteria
    for field_name in ("customfield_10035", "customfield_10036"):
        value = fields.get(field_name)
        if value:
            if isinstance(value, str):
                return value
            if isinstance(value, dict):
                return _extract_text_from_adf(value)
    return ""


def _extract_custom_fields(
    fields: dict[str, Any],
    custom_field_map: dict[str, str],
) -> dict[str, Any]:
    """Extract custom fields based on the mapping configuration."""
    result: dict[str, Any] = {}
    for target_name, source_field in custom_field_map.items():
        if source_field in fields:
            value = fields[source_field]
            if isinstance(value, dict) and "content" in value:
                result[target_name] = _extract_text_from_adf(value)
            else:
                result[target_name] = value
    return result


def _parse_issue_links(
    issue_links: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Parse Jira issue links into normalized link dicts."""
    parsed: list[dict[str, Any]] = []
    for link in issue_links:
        if not isinstance(link, dict):
            continue

        link_type = link.get("type") or {}
        outward_issue = link.get("outwardIssue")
        inward_issue = link.get("inwardIssue")

        if outward_issue and isinstance(outward_issue, dict):
            outward_name = link_type.get("outward", "relates to")
            parsed.append({
                "direction": "outward",
                "type": DEFAULT_LINK_TYPE_MAP.get(outward_name, outward_name),
                "type_name": outward_name,
                "target_key": outward_issue.get("key"),
                "target_summary": (outward_issue.get("fields") or {}).get("summary"),
                "target_status": (
                    (outward_issue.get("fields") or {}).get("status") or {}
                ).get("name"),
            })

        if inward_issue and isinstance(inward_issue, dict):
            inward_name = link_type.get("inward", "relates to")
            parsed.append({
                "direction": "inward",
                "type": DEFAULT_LINK_TYPE_MAP.get(inward_name, inward_name),
                "type_name": inward_name,
                "target_key": inward_issue.get("key"),
                "target_summary": (inward_issue.get("fields") or {}).get("summary"),
                "target_status": (
                    (inward_issue.get("fields") or {}).get("status") or {}
                ).get("name"),
            })

    return parsed


def _parse_attachments(
    attachments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Parse Jira attachments into a simplified list."""
    parsed: list[dict[str, Any]] = []
    for att in attachments:
        if not isinstance(att, dict):
            continue
        parsed.append({
            "id": att.get("id"),
            "filename": att.get("filename"),
            "mime_type": att.get("mimeType"),
            "size": att.get("size"),
            "content_url": att.get("content"),
            "created": att.get("created"),
            "author": (att.get("author") or {}).get("displayName"),
        })
    return parsed


def _priority_to_risk(priority_name: str | None) -> str | None:
    """Map Jira priority name to a risk level."""
    if not priority_name:
        return None
    priority_lower = priority_name.lower()
    if priority_lower in ("highest", "blocker", "critical"):
        return "high"
    if priority_lower in ("high",):
        return "high"
    if priority_lower in ("medium",):
        return "medium"
    if priority_lower in ("low", "lowest", "trivial"):
        return "low"
    return "medium"


def _create_summary(
    *,
    issue_key: str,
    issue_type: str,
    summary: str,
    status: str,
    priority: str | None,
    assignee: str | None,
    labels: list[str],
    components: list[str],
    sprint: str | None,
    description: str,
) -> str:
    """Create a summary string for the SourceBrief."""
    parts = [
        f"Jira {issue_type} {issue_key}",
        f"Status: {status}",
    ]

    if priority:
        parts.append(f"Priority: {priority}")

    if assignee:
        parts.append(f"Assignee: {assignee}")

    if labels:
        parts.append(f"Labels: {', '.join(labels)}")

    if components:
        parts.append(f"Components: {', '.join(components)}")

    if sprint:
        parts.append(f"Sprint: {sprint}")

    body = description.strip() if description else summary
    if body:
        parts.append(f"\n{body}")

    return "\n".join(parts)
