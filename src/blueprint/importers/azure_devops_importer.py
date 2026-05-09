"""Azure DevOps work item importer using REST API v7.0."""

from __future__ import annotations

import base64
import json
import os
import uuid
from dataclasses import dataclass, field
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
# Process template state mappings
# ---------------------------------------------------------------------------

PROCESS_TEMPLATE_STATE_MAPS: dict[str, dict[str, str]] = {
    "agile": {
        "New": "pending",
        "Active": "in_progress",
        "Resolved": "completed",
        "Closed": "completed",
        "Removed": "skipped",
    },
    "scrum": {
        "New": "pending",
        "Approved": "pending",
        "Committed": "in_progress",
        "Done": "completed",
        "Removed": "skipped",
    },
    "cmmi": {
        "Proposed": "pending",
        "Active": "in_progress",
        "Resolved": "completed",
        "Closed": "completed",
    },
}

DEFAULT_STATE_MAP: dict[str, str] = {
    "New": "pending",
    "Active": "in_progress",
    "Resolved": "completed",
    "Closed": "completed",
    "Done": "completed",
    "Removed": "skipped",
}

# ---------------------------------------------------------------------------
# Default work item type → blueprint entity type mapping
# ---------------------------------------------------------------------------

DEFAULT_TYPE_MAP: dict[str, str] = {
    "Epic": "phase",
    "Feature": "phase",
    "User Story": "task",
    "Task": "task",
    "Bug": "task",
    "Product Backlog Item": "task",
    "Requirement": "task",
    "Issue": "task",
}


# ---------------------------------------------------------------------------
# Client dataclass
# ---------------------------------------------------------------------------

@dataclass
class AzureDevOpsClient:
    """Lightweight client for Azure DevOps REST API v7.0."""

    organization: str
    project: str
    pat: str
    api_version: str = "7.0"
    http_open: HttpOpen = urlopen

    @property
    def base_url(self) -> str:
        return f"https://dev.azure.com/{quote(self.organization)}/{quote(self.project)}"

    def request_json(self, path: str, *, base_url: str | None = None) -> Any:
        """Execute a GET request and return parsed JSON."""
        url = f"{base_url or self.base_url}/_apis/{path}"
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}api-version={self.api_version}"

        request = Request(url, headers=self._headers(), method="GET")

        try:
            with self.http_open(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(
                f"Azure DevOps API request failed with HTTP {exc.code}: {path}"
            ) from exc
        except URLError as exc:
            raise ImportError(
                f"Azure DevOps API request failed: {exc.reason}"
            ) from exc

        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ImportError("Azure DevOps API returned invalid JSON") from exc

    def post_json(self, path: str, body: Any, *, base_url: str | None = None) -> Any:
        """Execute a POST request with a JSON body and return parsed JSON."""
        url = f"{base_url or self.base_url}/_apis/{path}"
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}api-version={self.api_version}"

        data = json.dumps(body).encode("utf-8")
        request = Request(
            url,
            data=data,
            headers={**self._headers(), "Content-Type": "application/json"},
            method="POST",
        )

        try:
            with self.http_open(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(
                f"Azure DevOps API POST failed with HTTP {exc.code}: {path}"
            ) from exc
        except URLError as exc:
            raise ImportError(
                f"Azure DevOps API POST failed: {exc.reason}"
            ) from exc

        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ImportError("Azure DevOps API returned invalid JSON") from exc

    def _headers(self) -> dict[str, str]:
        """Build request headers with PAT-based Basic auth."""
        credentials = base64.b64encode(f":{self.pat}".encode()).decode()
        return {
            "Authorization": f"Basic {credentials}",
            "Accept": "application/json",
            "User-Agent": "blueprint-azure-devops-importer",
        }


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class AzureProject:
    """Representation of an Azure DevOps project."""

    id: str
    name: str
    description: str
    url: str
    state: str
    process_template: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Public parsing helpers
# ---------------------------------------------------------------------------

def parse_work_item_json(
    work_item: dict[str, Any],
    *,
    organization: str,
    project: str,
    type_map: dict[str, str] | None = None,
    state_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Normalize an Azure DevOps work item into a SourceBrief dictionary."""
    if not isinstance(work_item, dict):
        raise ValueError("Azure DevOps work item payload must be a mapping")

    fields = work_item.get("fields") or {}
    wi_id = work_item.get("id")
    if wi_id is None:
        raise ValueError("Work item missing required 'id' field")

    wi_type = fields.get("System.WorkItemType", "Task")
    title = fields.get("System.Title", "")
    if not title:
        raise ValueError("Work item missing required 'System.Title' field")

    effective_type_map = {**DEFAULT_TYPE_MAP, **(type_map or {})}
    effective_state_map = {**DEFAULT_STATE_MAP, **(state_map or {})}

    state = fields.get("System.State", "New")
    mapped_state = effective_state_map.get(state, "pending")
    mapped_entity_type = effective_type_map.get(wi_type, "task")

    description = fields.get("System.Description") or ""
    tags_raw = fields.get("System.Tags") or ""
    tags = [t.strip() for t in tags_raw.split(";") if t.strip()] if tags_raw else []

    area_path = fields.get("System.AreaPath") or ""
    iteration_path = fields.get("System.IterationPath") or ""
    assigned_to = _extract_identity(fields.get("System.AssignedTo"))
    created_by = _extract_identity(fields.get("System.CreatedBy"))

    story_points = fields.get("Microsoft.VSTS.Scheduling.StoryPoints")
    remaining_work = fields.get("Microsoft.VSTS.Scheduling.RemainingWork")
    priority = fields.get("Microsoft.VSTS.Common.Priority")
    severity = fields.get("Microsoft.VSTS.Common.Severity")

    # Extract links/relations
    relations = work_item.get("relations") or []
    links = _parse_relations(relations)

    source_id = f"{organization}/{project}/{wi_id}"
    html_url = (
        f"https://dev.azure.com/{quote(organization)}/{quote(project)}"
        f"/_workitems/edit/{wi_id}"
    )

    now = datetime.utcnow()

    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": "azure-devops",
        "summary": _create_summary(
            wi_id=wi_id,
            wi_type=wi_type,
            title=title,
            state=state,
            assigned_to=assigned_to,
            tags=tags,
            area_path=area_path,
            iteration_path=iteration_path,
            description=description,
            organization=organization,
            project=project,
        ),
        "source_project": "azure-devops",
        "source_entity_type": mapped_entity_type,
        "source_id": source_id,
        "source_payload": {
            "work_item": work_item,
            "normalized": {
                "id": wi_id,
                "type": wi_type,
                "title": title,
                "state": state,
                "mapped_state": mapped_state,
                "description": description,
                "assigned_to": assigned_to,
                "created_by": created_by,
                "area_path": area_path,
                "iteration_path": iteration_path,
                "tags": tags,
                "story_points": story_points,
                "remaining_work": remaining_work,
                "priority": priority,
                "severity": severity,
                "links": links,
            },
        },
        "source_links": {
            "html_url": html_url,
            "api_url": work_item.get("url"),
        },
        "created_at": now,
        "updated_at": now,
    }


def map_work_item_to_task(
    work_item: dict[str, Any],
    *,
    organization: str,
    project: str,
    plan_id: str = "",
    state_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Map an Azure DevOps work item to an ExecutionTask dictionary."""
    fields = work_item.get("fields") or {}
    wi_id = work_item.get("id")
    wi_type = fields.get("System.WorkItemType", "Task")
    title = fields.get("System.Title", "")
    description = fields.get("System.Description") or title
    state = fields.get("System.State", "New")

    effective_state_map = {**DEFAULT_STATE_MAP, **(state_map or {})}
    mapped_status = effective_state_map.get(state, "pending")

    story_points = fields.get("Microsoft.VSTS.Scheduling.StoryPoints")
    remaining_work = fields.get("Microsoft.VSTS.Scheduling.RemainingWork")
    priority = fields.get("Microsoft.VSTS.Common.Priority")
    area_path = fields.get("System.AreaPath") or ""
    iteration_path = fields.get("System.IterationPath") or ""
    tags_raw = fields.get("System.Tags") or ""
    tags = [t.strip() for t in tags_raw.split(";") if t.strip()] if tags_raw else []
    assigned_to = _extract_identity(fields.get("System.AssignedTo"))

    acceptance_criteria_raw = fields.get(
        "Microsoft.VSTS.Common.AcceptanceCriteria"
    ) or ""
    acceptance_criteria = (
        [acceptance_criteria_raw] if acceptance_criteria_raw else [f"Complete: {title}"]
    )

    relations = work_item.get("relations") or []
    links = _parse_relations(relations)
    depends_on = [
        str(link["target_id"])
        for link in links
        if link["type"] in ("Predecessor", "Parent")
    ]

    estimated_hours: float | None = None
    if remaining_work is not None:
        try:
            estimated_hours = float(remaining_work)
        except (TypeError, ValueError):
            pass
    elif story_points is not None:
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

    risk_level = None
    if priority is not None:
        try:
            p = int(priority)
            if p == 1:
                risk_level = "high"
            elif p == 2:
                risk_level = "medium"
            else:
                risk_level = "low"
        except (TypeError, ValueError):
            pass

    now = datetime.utcnow()

    return {
        "id": f"ado-{wi_id}",
        "execution_plan_id": plan_id or None,
        "title": title,
        "description": description,
        "milestone": iteration_path or None,
        "owner_type": assigned_to,
        "suggested_engine": None,
        "depends_on": depends_on,
        "files_or_modules": None,
        "acceptance_criteria": acceptance_criteria,
        "estimated_complexity": complexity,
        "estimated_hours": estimated_hours,
        "risk_level": risk_level,
        "test_command": None,
        "status": mapped_status,
        "metadata": {
            "azure_organization": organization,
            "azure_project": project,
            "azure_work_item_id": wi_id,
            "azure_work_item_type": wi_type,
            "azure_area_path": area_path,
            "azure_iteration_path": iteration_path,
            "azure_state": state,
            "azure_priority": priority,
            "tags": tags,
            "links": links,
        },
        "blocked_reason": None,
        "created_at": now,
        "updated_at": now,
    }


def import_area_path_to_plan(
    work_items: list[dict[str, Any]],
    *,
    organization: str,
    project: str,
    area_path: str,
    plan_id: str | None = None,
    type_map: dict[str, str] | None = None,
    state_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Convert a set of work items under an area path into an ExecutionPlan dict."""
    effective_plan_id = plan_id or f"plan-ado-{uuid.uuid4().hex[:12]}"
    effective_type_map = {**DEFAULT_TYPE_MAP, **(type_map or {})}

    milestones: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []

    # Separate epics/features (phases) from leaf work items (tasks)
    seen_iterations: set[str] = set()
    for wi in work_items:
        fields = wi.get("fields") or {}
        wi_type = fields.get("System.WorkItemType", "Task")
        mapped = effective_type_map.get(wi_type, "task")

        if mapped == "phase":
            milestones.append({
                "name": fields.get("System.Title", f"Phase {wi.get('id')}"),
                "description": fields.get("System.Description") or "",
                "azure_work_item_id": wi.get("id"),
                "azure_work_item_type": wi_type,
            })
        else:
            task = map_work_item_to_task(
                wi,
                organization=organization,
                project=project,
                plan_id=effective_plan_id,
                state_map=state_map,
            )
            tasks.append(task)
            iteration = fields.get("System.IterationPath") or ""
            if iteration and iteration not in seen_iterations:
                seen_iterations.add(iteration)

    # If no explicit milestones from epics, derive from iterations
    if not milestones:
        for iteration in sorted(seen_iterations):
            milestones.append({"name": iteration, "description": ""})

    now = datetime.utcnow()
    return {
        "id": effective_plan_id,
        "implementation_brief_id": f"ib-ado-{uuid.uuid4().hex[:12]}",
        "target_engine": None,
        "target_repo": f"{organization}/{project}",
        "project_type": "azure-devops",
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
            "source": "azure-devops",
            "organization": organization,
            "project": project,
            "area_path": area_path,
        },
        "tasks": tasks,
    }


# ---------------------------------------------------------------------------
# Main importer class
# ---------------------------------------------------------------------------

class AzureDevOpsImporter(SourceImporter):
    """Import Azure DevOps work items through the REST API v7.0."""

    def __init__(
        self,
        *,
        organization: str | None = None,
        project: str | None = None,
        token_env: str = "AZURE_DEVOPS_PAT",
        process_template: str = "agile",
        type_map: dict[str, str] | None = None,
        http_open: HttpOpen = urlopen,
    ):
        self.organization = organization
        self.project = project
        self.token_env = token_env
        self.process_template = process_template.lower()
        self.type_map = type_map
        self.http_open = http_open

    # -- SourceImporter interface ------------------------------------------

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Fetch and normalize an Azure DevOps work item."""
        org, proj, wi_id = self._parse_source_id(source_id)
        client = self._build_client(org, proj)
        work_item = self._fetch_work_item(client, wi_id)
        return parse_work_item_json(
            work_item,
            organization=org,
            project=proj,
            type_map=self.type_map,
            state_map=self._state_map(),
        )

    def validate_source(self, source_id: str) -> bool:
        """Check whether an Azure DevOps work item is accessible."""
        try:
            org, proj, wi_id = self._parse_source_id(source_id)
            client = self._build_client(org, proj)
            self._fetch_work_item(client, wi_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List recent work items for the configured project."""
        org = self.organization
        proj = self.project
        if not org or not proj:
            raise ValueError(
                "sources.azure-devops.organization and project are required"
            )

        client = self._build_client(org, proj)
        wiql = {
            "query": (
                f"SELECT [System.Id], [System.Title], [System.State], "
                f"[System.WorkItemType] FROM WorkItems "
                f"WHERE [System.TeamProject] = '{proj}' "
                f"ORDER BY [System.ChangedDate] DESC"
            )
        }
        result = client.post_json("wit/wiql", wiql)
        work_item_refs = result.get("workItems", [])[:limit]

        if not work_item_refs:
            return []

        ids = [str(ref["id"]) for ref in work_item_refs]
        items = self._fetch_work_items_batch(client, ids)

        available = []
        for wi in items:
            fields = wi.get("fields") or {}
            wi_id = wi.get("id")
            available.append({
                "id": f"{org}/{proj}/{wi_id}",
                "work_item_id": wi_id,
                "title": fields.get("System.Title", ""),
                "state": fields.get("System.State", ""),
                "type": fields.get("System.WorkItemType", ""),
                "assigned_to": _extract_identity(fields.get("System.AssignedTo")),
                "html_url": (
                    f"https://dev.azure.com/{quote(org)}/{quote(proj)}"
                    f"/_workitems/edit/{wi_id}"
                ),
                "updated_at": fields.get("System.ChangedDate"),
            })

        return available

    # -- Extended methods --------------------------------------------------

    def authenticate(
        self,
        organization: str,
        project: str,
        pat: str,
    ) -> AzureDevOpsClient:
        """Create an authenticated client for Azure DevOps."""
        client = AzureDevOpsClient(
            organization=organization,
            project=project,
            pat=pat,
            http_open=self.http_open,
        )
        # Validate by fetching project info
        client.request_json("projects")
        return client

    def fetch_project(self, project_name: str) -> AzureProject:
        """Fetch Azure DevOps project details."""
        org = self.organization
        if not org:
            raise ValueError("Organization is required to fetch project")

        client = self._build_client(org, project_name)
        data = client.request_json(
            f"projects/{quote(project_name)}",
            base_url=f"https://dev.azure.com/{quote(org)}",
        )

        process_info = data.get("capabilities", {}).get("processTemplate", {})
        return AzureProject(
            id=data.get("id", ""),
            name=data.get("name", project_name),
            description=data.get("description") or "",
            url=data.get("url") or "",
            state=data.get("state") or "",
            process_template=process_info.get("templateName"),
            raw=data,
        )

    def import_area_path(self, area_path: str) -> dict[str, Any]:
        """Import all work items under an area path as an ExecutionPlan."""
        org = self.organization
        proj = self.project
        if not org or not proj:
            raise ValueError(
                "Organization and project are required to import area path"
            )

        client = self._build_client(org, proj)
        wiql = {
            "query": (
                f"SELECT [System.Id] FROM WorkItems "
                f"WHERE [System.AreaPath] UNDER '{area_path}' "
                f"ORDER BY [System.Id] ASC"
            )
        }
        result = client.post_json("wit/wiql", wiql)
        refs = result.get("workItems", [])

        if not refs:
            return import_area_path_to_plan(
                [],
                organization=org,
                project=proj,
                area_path=area_path,
                type_map=self.type_map,
                state_map=self._state_map(),
            )

        ids = [str(ref["id"]) for ref in refs]
        work_items = self._fetch_work_items_batch(client, ids)

        return import_area_path_to_plan(
            work_items,
            organization=org,
            project=proj,
            area_path=area_path,
            type_map=self.type_map,
            state_map=self._state_map(),
        )

    def sync_updates(
        self,
        project: str,
        since: datetime,
    ) -> list[dict[str, Any]]:
        """Fetch work items updated since a given datetime."""
        org = self.organization
        if not org:
            raise ValueError("Organization is required to sync updates")

        client = self._build_client(org, project)
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        wiql = {
            "query": (
                f"SELECT [System.Id] FROM WorkItems "
                f"WHERE [System.TeamProject] = '{project}' "
                f"AND [System.ChangedDate] >= '{since_str}' "
                f"ORDER BY [System.ChangedDate] DESC"
            )
        }
        result = client.post_json("wit/wiql", wiql)
        refs = result.get("workItems", [])

        if not refs:
            return []

        ids = [str(ref["id"]) for ref in refs]
        work_items = self._fetch_work_items_batch(client, ids)

        return [
            map_work_item_to_task(
                wi,
                organization=org,
                project=project,
                state_map=self._state_map(),
            )
            for wi in work_items
        ]

    # -- Private helpers ---------------------------------------------------

    def _parse_source_id(self, source_id: str) -> tuple[str, str, int]:
        """Parse source_id in format 'org/project/id' or just 'id'."""
        source_id = source_id.strip()
        parts = source_id.split("/")

        if len(parts) == 3:
            org, proj, wi_id_str = parts
        elif len(parts) == 1:
            org = self.organization
            proj = self.project
            wi_id_str = parts[0]
            if not org or not proj:
                raise ValueError(
                    "Source ID must be 'org/project/id' or configure default "
                    "organization and project"
                )
        else:
            raise ValueError(
                f"Invalid Azure DevOps source ID format: {source_id!r}. "
                f"Expected 'org/project/id' or 'id'"
            )

        try:
            wi_id = int(wi_id_str)
        except ValueError as exc:
            raise ValueError(
                f"Invalid Azure DevOps work item ID: {wi_id_str}"
            ) from exc

        return org, proj, wi_id

    def _build_client(self, organization: str, project: str) -> AzureDevOpsClient:
        """Build an authenticated client."""
        pat = os.getenv(self.token_env) or ""
        return AzureDevOpsClient(
            organization=organization,
            project=project,
            pat=pat,
            http_open=self.http_open,
        )

    def _state_map(self) -> dict[str, str]:
        """Return the state map for the configured process template."""
        return PROCESS_TEMPLATE_STATE_MAPS.get(
            self.process_template, DEFAULT_STATE_MAP
        )

    def _fetch_work_item(
        self, client: AzureDevOpsClient, wi_id: int
    ) -> dict[str, Any]:
        """Fetch a single work item with relations expanded."""
        result = client.request_json(f"wit/workitems/{wi_id}?$expand=relations")
        if not isinstance(result, dict):
            raise ImportError("Azure DevOps work item response was not a mapping")
        return result

    def _fetch_work_items_batch(
        self, client: AzureDevOpsClient, ids: list[str]
    ) -> list[dict[str, Any]]:
        """Fetch multiple work items by ID using the batch endpoint."""
        if not ids:
            return []

        # API supports up to 200 IDs per request
        all_items: list[dict[str, Any]] = []
        for i in range(0, len(ids), 200):
            chunk = ids[i : i + 200]
            ids_param = ",".join(chunk)
            fields = (
                "System.Id,System.Title,System.State,System.WorkItemType,"
                "System.Description,System.AssignedTo,System.CreatedBy,"
                "System.AreaPath,System.IterationPath,System.Tags,"
                "System.ChangedDate,"
                "Microsoft.VSTS.Scheduling.StoryPoints,"
                "Microsoft.VSTS.Scheduling.RemainingWork,"
                "Microsoft.VSTS.Common.Priority,"
                "Microsoft.VSTS.Common.Severity,"
                "Microsoft.VSTS.Common.AcceptanceCriteria"
            )
            result = client.request_json(
                f"wit/workitems?ids={ids_param}&fields={fields}&$expand=relations"
            )
            items = result.get("value", [])
            if isinstance(items, list):
                all_items.extend(items)

        return all_items


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_identity(identity: Any) -> str | None:
    """Extract display name from Azure DevOps identity object."""
    if isinstance(identity, dict):
        return identity.get("displayName") or identity.get("uniqueName")
    if isinstance(identity, str):
        return identity
    return None


def _parse_relations(relations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Parse work item relations into a normalized list of link dicts."""
    parsed: list[dict[str, Any]] = []
    for rel in relations:
        if not isinstance(rel, dict):
            continue

        rel_type = rel.get("rel") or ""
        url = rel.get("url") or ""
        attrs = rel.get("attributes") or {}

        # Map Azure relation types to simplified names
        type_name = _relation_type_name(rel_type)

        # Extract target work item ID from URL
        target_id = _extract_id_from_url(url)

        parsed.append({
            "type": type_name,
            "rel": rel_type,
            "target_id": target_id,
            "url": url,
            "comment": attrs.get("comment", ""),
        })

    return parsed


def _relation_type_name(rel_type: str) -> str:
    """Map Azure DevOps relation type URIs to readable names."""
    mapping = {
        "System.LinkTypes.Hierarchy-Reverse": "Parent",
        "System.LinkTypes.Hierarchy-Forward": "Child",
        "System.LinkTypes.Related": "Related",
        "System.LinkTypes.Dependency-Forward": "Successor",
        "System.LinkTypes.Dependency-Reverse": "Predecessor",
    }
    return mapping.get(rel_type, rel_type)


def _extract_id_from_url(url: str) -> int | None:
    """Extract work item ID from an Azure DevOps API URL."""
    if not url:
        return None
    parts = url.rstrip("/").split("/")
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return None


def _create_summary(
    *,
    wi_id: int,
    wi_type: str,
    title: str,
    state: str,
    assigned_to: str | None,
    tags: list[str],
    area_path: str,
    iteration_path: str,
    description: str,
    organization: str,
    project: str,
) -> str:
    """Create a summary string for the SourceBrief."""
    parts = [
        f"Azure DevOps {wi_type} {organization}/{project}#{wi_id}",
        f"State: {state}",
    ]

    if assigned_to:
        parts.append(f"Assigned to: {assigned_to}")

    if tags:
        parts.append(f"Tags: {', '.join(tags)}")

    if area_path:
        parts.append(f"Area Path: {area_path}")

    if iteration_path:
        parts.append(f"Iteration Path: {iteration_path}")

    body = description.strip() if description else title
    if body:
        parts.append(f"\n{body}")

    return "\n".join(parts)
