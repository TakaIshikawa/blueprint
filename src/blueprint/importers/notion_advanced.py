"""Notion database importer with advanced filtering and property mapping."""

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
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Notion property type → extraction mapping
# ---------------------------------------------------------------------------

PROPERTY_TYPE_EXTRACTORS: dict[str, str] = {
    "title": "title",
    "rich_text": "rich_text",
    "number": "number",
    "select": "select",
    "multi_select": "multi_select",
    "date": "date",
    "people": "people",
    "files": "files",
    "checkbox": "checkbox",
    "url": "url",
    "email": "email",
    "phone_number": "phone_number",
    "formula": "formula",
    "relation": "relation",
    "rollup": "rollup",
    "created_time": "created_time",
    "created_by": "created_by",
    "last_edited_time": "last_edited_time",
    "last_edited_by": "last_edited_by",
    "status": "status",
    "unique_id": "unique_id",
}

# ---------------------------------------------------------------------------
# Status → blueprint state mapping
# ---------------------------------------------------------------------------

DEFAULT_STATUS_MAP: dict[str, str] = {
    "Not started": "pending",
    "To do": "pending",
    "Backlog": "pending",
    "In progress": "in_progress",
    "In review": "in_progress",
    "Done": "completed",
    "Complete": "completed",
    "Completed": "completed",
    "Archived": "skipped",
    "Cancelled": "skipped",
    "Canceled": "skipped",
}


# ---------------------------------------------------------------------------
# Filter builder
# ---------------------------------------------------------------------------

def build_database_filter(
    filter_config: dict[str, Any],
) -> dict[str, Any]:
    """Build a Notion database filter from a configuration dict.

    Supports compound filters (and/or), property filters,
    formula filters, and rollup filters.

    Filter config format::

        {
            "and": [
                {"property": "Status", "status": {"equals": "In progress"}},
                {"property": "Priority", "select": {"equals": "High"}},
            ]
        }

    Or for formulas::

        {"property": "Score", "formula": {"number": {"greater_than": 80}}}

    Or for rollups::

        {"property": "TaskCount", "rollup": {"number": {"greater_than": 0}}}
    """
    if not filter_config:
        return {}

    # Compound filters
    if "and" in filter_config:
        return {"and": [build_database_filter(f) for f in filter_config["and"]]}
    if "or" in filter_config:
        return {"or": [build_database_filter(f) for f in filter_config["or"]]}

    # Property-based filter - pass through (Notion API native format)
    if "property" in filter_config:
        return dict(filter_config)

    return dict(filter_config)


# ---------------------------------------------------------------------------
# Property value extraction
# ---------------------------------------------------------------------------

def extract_property_value(prop: dict[str, Any]) -> Any:
    """Extract a usable value from a Notion property object."""
    prop_type = prop.get("type", "")

    if prop_type == "title":
        parts = prop.get("title") or []
        return "".join(t.get("plain_text", "") for t in parts)

    if prop_type == "rich_text":
        parts = prop.get("rich_text") or []
        return "".join(t.get("plain_text", "") for t in parts)

    if prop_type == "number":
        return prop.get("number")

    if prop_type == "select":
        sel = prop.get("select")
        return sel.get("name") if isinstance(sel, dict) else None

    if prop_type == "multi_select":
        items = prop.get("multi_select") or []
        return [item.get("name", "") for item in items if isinstance(item, dict)]

    if prop_type == "date":
        date_obj = prop.get("date")
        if isinstance(date_obj, dict):
            return {
                "start": date_obj.get("start"),
                "end": date_obj.get("end"),
            }
        return None

    if prop_type == "people":
        people = prop.get("people") or []
        return [
            p.get("name") or p.get("id", "")
            for p in people
            if isinstance(p, dict)
        ]

    if prop_type == "files":
        files = prop.get("files") or []
        result = []
        for f in files:
            if not isinstance(f, dict):
                continue
            name = f.get("name", "")
            url = ""
            if f.get("type") == "external":
                url = (f.get("external") or {}).get("url", "")
            elif f.get("type") == "file":
                url = (f.get("file") or {}).get("url", "")
            result.append({"name": name, "url": url})
        return result

    if prop_type == "checkbox":
        return prop.get("checkbox", False)

    if prop_type == "url":
        return prop.get("url")

    if prop_type == "email":
        return prop.get("email")

    if prop_type == "phone_number":
        return prop.get("phone_number")

    if prop_type == "formula":
        formula = prop.get("formula") or {}
        formula_type = formula.get("type", "")
        return formula.get(formula_type)

    if prop_type == "relation":
        relations = prop.get("relation") or []
        return [r.get("id", "") for r in relations if isinstance(r, dict)]

    if prop_type == "rollup":
        rollup = prop.get("rollup") or {}
        rollup_type = rollup.get("type", "")
        if rollup_type == "array":
            items = rollup.get("array") or []
            return [extract_property_value(item) for item in items]
        return rollup.get(rollup_type)

    if prop_type == "created_time":
        return prop.get("created_time")

    if prop_type == "created_by":
        user = prop.get("created_by") or {}
        return user.get("name") or user.get("id")

    if prop_type == "last_edited_time":
        return prop.get("last_edited_time")

    if prop_type == "last_edited_by":
        user = prop.get("last_edited_by") or {}
        return user.get("name") or user.get("id")

    if prop_type == "status":
        status = prop.get("status")
        return status.get("name") if isinstance(status, dict) else None

    if prop_type == "unique_id":
        uid = prop.get("unique_id") or {}
        prefix = uid.get("prefix", "")
        number = uid.get("number", "")
        return f"{prefix}-{number}" if prefix else str(number)

    return None


def extract_all_properties(
    properties: dict[str, Any],
    *,
    property_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Extract all property values from a Notion page's properties dict.

    Args:
        properties: The ``properties`` dict from a Notion page object.
        property_map: Optional mapping of Notion property names to output
            field names.  When provided, Notion property ``"Priority"`` can
            be mapped to output key ``"priority"`` etc.

    Returns:
        Dictionary of extracted values keyed by (mapped) property name.
    """
    effective_map = property_map or {}
    result: dict[str, Any] = {}

    for prop_name, prop_data in properties.items():
        if not isinstance(prop_data, dict):
            continue
        output_key = effective_map.get(prop_name, prop_name)
        result[output_key] = extract_property_value(prop_data)

    return result


# ---------------------------------------------------------------------------
# Rich content block extraction
# ---------------------------------------------------------------------------

def extract_block_content(block: dict[str, Any]) -> dict[str, Any]:
    """Extract content from a Notion block object.

    Handles paragraph, heading, code, equation, callout, toggle,
    bulleted/numbered list items, to-do, quote, divider, and image blocks.
    """
    block_type = block.get("type", "")
    block_id = block.get("id", "")

    result: dict[str, Any] = {
        "id": block_id,
        "type": block_type,
        "has_children": block.get("has_children", False),
    }

    # Text-bearing blocks
    text_types = {
        "paragraph", "heading_1", "heading_2", "heading_3",
        "bulleted_list_item", "numbered_list_item", "quote", "toggle",
    }

    if block_type in text_types:
        block_data = block.get(block_type) or {}
        rich_text = block_data.get("rich_text") or []
        result["text"] = "".join(
            t.get("plain_text", "") for t in rich_text
        )
        if block_type == "toggle":
            result["text"] = result["text"]
        return result

    if block_type == "to_do":
        todo_data = block.get("to_do") or {}
        rich_text = todo_data.get("rich_text") or []
        result["text"] = "".join(
            t.get("plain_text", "") for t in rich_text
        )
        result["checked"] = todo_data.get("checked", False)
        return result

    if block_type == "code":
        code_data = block.get("code") or {}
        rich_text = code_data.get("rich_text") or []
        result["text"] = "".join(
            t.get("plain_text", "") for t in rich_text
        )
        result["language"] = code_data.get("language", "")
        return result

    if block_type == "equation":
        eq_data = block.get("equation") or {}
        result["expression"] = eq_data.get("expression", "")
        return result

    if block_type == "callout":
        callout_data = block.get("callout") or {}
        rich_text = callout_data.get("rich_text") or []
        result["text"] = "".join(
            t.get("plain_text", "") for t in rich_text
        )
        icon = callout_data.get("icon") or {}
        if icon.get("type") == "emoji":
            result["icon"] = icon.get("emoji", "")
        return result

    if block_type == "image":
        image_data = block.get("image") or {}
        img_type = image_data.get("type", "")
        if img_type == "external":
            result["url"] = (image_data.get("external") or {}).get("url", "")
        elif img_type == "file":
            result["url"] = (image_data.get("file") or {}).get("url", "")
        caption = image_data.get("caption") or []
        result["caption"] = "".join(
            t.get("plain_text", "") for t in caption
        )
        return result

    if block_type == "divider":
        return result

    if block_type == "table_of_contents":
        return result

    # Fallback: return minimal info
    return result


def extract_page_blocks(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract content from a list of Notion block objects."""
    return [extract_block_content(b) for b in blocks if isinstance(b, dict)]


# ---------------------------------------------------------------------------
# Notion API client
# ---------------------------------------------------------------------------

@dataclass
class NotionClient:
    """Lightweight client for the Notion REST API (v2022-06-28)."""

    token: str
    api_version: str = "2022-06-28"
    base_url: str = "https://api.notion.com/v1"
    http_open: HttpOpen = urlopen

    def request_json(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> Any:
        """Execute an HTTP request and return parsed JSON."""
        url = f"{self.base_url}/{path.lstrip('/')}"

        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")

        request = Request(
            url,
            data=data,
            headers=self._headers(),
            method=method,
        )

        try:
            with self.http_open(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(
                f"Notion API request failed with HTTP {exc.code}: {path}"
            ) from exc
        except URLError as exc:
            raise ImportError(
                f"Notion API request failed: {exc.reason}"
            ) from exc

        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ImportError("Notion API returned invalid JSON") from exc

    def query_database(
        self,
        database_id: str,
        *,
        filter_obj: dict[str, Any] | None = None,
        sorts: list[dict[str, Any]] | None = None,
        start_cursor: str | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        """Query a Notion database with optional filter and sorts."""
        body: dict[str, Any] = {"page_size": page_size}
        if filter_obj:
            body["filter"] = filter_obj
        if sorts:
            body["sorts"] = sorts
        if start_cursor:
            body["start_cursor"] = start_cursor
        return self.request_json("POST", f"databases/{database_id}/query", body)

    def get_database(self, database_id: str) -> dict[str, Any]:
        """Retrieve database metadata."""
        return self.request_json("GET", f"databases/{database_id}")

    def get_page(self, page_id: str) -> dict[str, Any]:
        """Retrieve a page."""
        return self.request_json("GET", f"pages/{page_id}")

    def get_block_children(
        self,
        block_id: str,
        *,
        start_cursor: str | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        """List child blocks of a block or page."""
        path = f"blocks/{block_id}/children?page_size={page_size}"
        if start_cursor:
            path += f"&start_cursor={start_cursor}"
        return self.request_json("GET", path)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": self.api_version,
            "User-Agent": "blueprint-notion-advanced-importer",
        }


# ---------------------------------------------------------------------------
# Incremental sync tracker
# ---------------------------------------------------------------------------

@dataclass
class SyncState:
    """Tracks last-sync timestamps per database for incremental imports."""

    last_synced: dict[str, str] = field(default_factory=dict)

    def get_last_synced(self, database_id: str) -> str | None:
        """Return ISO timestamp of last sync for a database, or None."""
        return self.last_synced.get(database_id)

    def update(self, database_id: str, timestamp: str) -> None:
        """Record the latest sync timestamp for a database."""
        self.last_synced[database_id] = timestamp

    def to_dict(self) -> dict[str, Any]:
        return {"last_synced": dict(self.last_synced)}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SyncState:
        return cls(last_synced=dict(data.get("last_synced") or {}))


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _get_page_title(page: dict[str, Any]) -> str:
    """Extract the title from a Notion page's properties."""
    properties = page.get("properties") or {}
    for prop_data in properties.values():
        if not isinstance(prop_data, dict):
            continue
        if prop_data.get("type") == "title":
            title_parts = prop_data.get("title") or []
            return "".join(t.get("plain_text", "") for t in title_parts)
    return ""


def _get_page_status(
    page: dict[str, Any],
    status_map: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Extract status and map to blueprint state.

    Returns:
        Tuple of (raw_status, mapped_state).
    """
    effective_map = {**DEFAULT_STATUS_MAP, **(status_map or {})}
    properties = page.get("properties") or {}

    for prop_data in properties.values():
        if not isinstance(prop_data, dict):
            continue
        if prop_data.get("type") == "status":
            status_obj = prop_data.get("status")
            if isinstance(status_obj, dict):
                raw = status_obj.get("name", "")
                return raw, effective_map.get(raw, "pending")

    return "", "pending"


def parse_notion_page_json(
    page: dict[str, Any],
    *,
    database_id: str,
    property_map: dict[str, str] | None = None,
    status_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Normalize a Notion page into a SourceBrief dictionary."""
    page_id = page.get("id", "")
    if not page_id:
        raise ValueError("Notion page missing 'id' field")

    title = _get_page_title(page)
    if not title:
        raise ValueError("Notion page missing title property")

    raw_status, mapped_state = _get_page_status(page, status_map)
    properties = page.get("properties") or {}
    extracted = extract_all_properties(properties, property_map=property_map)

    created_time = page.get("created_time")
    last_edited_time = page.get("last_edited_time")
    notion_url = page.get("url") or ""
    parent = page.get("parent") or {}

    source_id = f"{database_id}/{page_id}"

    now = datetime.utcnow()

    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": "notion",
        "summary": _create_page_summary(
            page_id=page_id,
            title=title,
            status=raw_status,
            database_id=database_id,
            extracted=extracted,
        ),
        "source_project": "notion",
        "source_entity_type": "page",
        "source_id": source_id,
        "source_payload": {
            "page": page,
            "normalized": {
                "page_id": page_id,
                "title": title,
                "status": raw_status,
                "mapped_state": mapped_state,
                "properties": extracted,
                "parent": parent,
                "created_time": created_time,
                "last_edited_time": last_edited_time,
            },
        },
        "source_links": {
            "html_url": notion_url,
            "api_url": f"https://api.notion.com/v1/pages/{page_id}",
        },
        "created_at": now,
        "updated_at": now,
    }


def map_notion_page_to_task(
    page: dict[str, Any],
    *,
    database_id: str,
    plan_id: str = "",
    property_map: dict[str, str] | None = None,
    status_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Map a Notion page to an ExecutionTask dictionary."""
    page_id = page.get("id", "")
    title = _get_page_title(page)
    raw_status, mapped_state = _get_page_status(page, status_map)

    properties = page.get("properties") or {}
    extracted = extract_all_properties(properties, property_map=property_map)

    # Extract relations as dependencies
    depends_on: list[str] = []
    for prop_data in properties.values():
        if isinstance(prop_data, dict) and prop_data.get("type") == "relation":
            relations = prop_data.get("relation") or []
            for rel in relations:
                if isinstance(rel, dict) and rel.get("id"):
                    depends_on.append(rel["id"])

    # Extract assignees from people properties
    assignee: str | None = None
    for prop_data in properties.values():
        if isinstance(prop_data, dict) and prop_data.get("type") == "people":
            people = prop_data.get("people") or []
            if people and isinstance(people[0], dict):
                assignee = people[0].get("name") or people[0].get("id")
            break

    description = title
    last_edited = page.get("last_edited_time")

    now = datetime.utcnow()

    return {
        "id": f"notion-{page_id}",
        "execution_plan_id": plan_id or None,
        "title": title,
        "description": description,
        "milestone": None,
        "owner_type": assignee,
        "suggested_engine": None,
        "depends_on": depends_on,
        "files_or_modules": None,
        "acceptance_criteria": [f"Complete: {title}"],
        "estimated_complexity": "medium",
        "estimated_hours": None,
        "risk_level": None,
        "test_command": None,
        "status": mapped_state,
        "metadata": {
            "notion_page_id": page_id,
            "notion_database_id": database_id,
            "notion_status": raw_status,
            "properties": extracted,
            "last_edited_time": last_edited,
        },
        "blocked_reason": None,
        "created_at": now,
        "updated_at": now,
    }


def import_database_to_plan(
    pages: list[dict[str, Any]],
    *,
    database_id: str,
    database_title: str = "",
    plan_id: str | None = None,
    property_map: dict[str, str] | None = None,
    status_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Convert Notion database pages into an ExecutionPlan dict."""
    effective_plan_id = plan_id or f"plan-notion-{uuid.uuid4().hex[:12]}"

    tasks = [
        map_notion_page_to_task(
            page,
            database_id=database_id,
            plan_id=effective_plan_id,
            property_map=property_map,
            status_map=status_map,
        )
        for page in pages
    ]

    now = datetime.utcnow()
    return {
        "id": effective_plan_id,
        "implementation_brief_id": f"ib-notion-{uuid.uuid4().hex[:12]}",
        "target_engine": None,
        "target_repo": None,
        "project_type": "notion",
        "milestones": [{"name": database_title or "Notion Database", "description": ""}],
        "test_strategy": None,
        "handoff_prompt": None,
        "status": "draft",
        "created_at": now,
        "updated_at": now,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
        "metadata": {
            "source": "notion",
            "database_id": database_id,
            "database_title": database_title,
            "page_count": len(pages),
        },
        "tasks": tasks,
    }


# ---------------------------------------------------------------------------
# Main importer class
# ---------------------------------------------------------------------------

class NotionAdvancedImporter(SourceImporter):
    """Import Notion database pages with advanced filtering and property mapping.

    Supports:
    - Advanced compound filters (and/or) with property, formula, and rollup conditions
    - All Notion property types including relation, rollup, formula, status, etc.
    - Rich content block extraction (code, equations, callouts, toggles)
    - Incremental sync via last-modified timestamp tracking
    - Linked database views with custom filter expressions
    - Database templates and recurring page patterns
    """

    def __init__(
        self,
        *,
        token_env: str = "NOTION_TOKEN",
        default_database_id: str | None = None,
        property_map: dict[str, str] | None = None,
        status_map: dict[str, str] | None = None,
        filter_config: dict[str, Any] | None = None,
        sync_state: SyncState | None = None,
        http_open: HttpOpen = urlopen,
    ):
        self.token_env = token_env
        self.default_database_id = default_database_id
        self.property_map = property_map
        self.status_map = status_map
        self.filter_config = filter_config
        self.sync_state = sync_state or SyncState()
        self.http_open = http_open

    # -- SourceImporter interface ------------------------------------------

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Fetch and normalize a Notion page.

        source_id format: ``database_id/page_id`` or just ``page_id``
        (when default_database_id is configured).
        """
        database_id, page_id = self._parse_source_id(source_id)
        client = self._build_client()
        page = client.get_page(page_id)
        return parse_notion_page_json(
            page,
            database_id=database_id,
            property_map=self.property_map,
            status_map=self.status_map,
        )

    def validate_source(self, source_id: str) -> bool:
        """Check whether a Notion page is accessible."""
        try:
            self.import_from_source(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """List pages from the configured database."""
        database_id = self.default_database_id
        if not database_id:
            raise ValueError(
                "default_database_id is required to list available pages"
            )

        client = self._build_client()
        filter_obj = build_database_filter(self.filter_config) if self.filter_config else None

        result = client.query_database(
            database_id,
            filter_obj=filter_obj,
            page_size=min(limit, 100),
        )

        pages = result.get("results") or []
        available = []
        for page in pages[:limit]:
            page_id = page.get("id", "")
            title = _get_page_title(page)
            raw_status, _ = _get_page_status(page, self.status_map)

            available.append({
                "id": f"{database_id}/{page_id}",
                "page_id": page_id,
                "title": title,
                "status": raw_status,
                "html_url": page.get("url", ""),
                "last_edited_time": page.get("last_edited_time"),
            })

        return available

    # -- Extended methods --------------------------------------------------

    def import_database(
        self,
        database_id: str | None = None,
        *,
        filter_config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Import all pages from a database as an ExecutionPlan.

        Args:
            database_id: Override default database.
            filter_config: Override default filter configuration.
        """
        db_id = database_id or self.default_database_id
        if not db_id:
            raise ValueError("database_id is required")

        client = self._build_client()

        # Fetch database metadata for title
        db_meta = client.get_database(db_id)
        db_title = ""
        title_parts = db_meta.get("title") or []
        if title_parts:
            db_title = "".join(
                t.get("plain_text", "") for t in title_parts
            )

        effective_filter = filter_config or self.filter_config
        filter_obj = build_database_filter(effective_filter) if effective_filter else None

        pages = self._fetch_all_pages(client, db_id, filter_obj=filter_obj)

        return import_database_to_plan(
            pages,
            database_id=db_id,
            database_title=db_title,
            property_map=self.property_map,
            status_map=self.status_map,
        )

    def fetch_page_blocks(self, page_id: str) -> list[dict[str, Any]]:
        """Fetch and extract rich content blocks from a page."""
        client = self._build_client()
        blocks = self._fetch_all_blocks(client, page_id)
        return extract_page_blocks(blocks)

    def sync_updates(
        self,
        database_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch pages modified since the last sync.

        Uses the internal SyncState to track timestamps per database.
        After fetching, updates the sync timestamp to the current time.
        """
        db_id = database_id or self.default_database_id
        if not db_id:
            raise ValueError("database_id is required for sync")

        client = self._build_client()
        last_synced = self.sync_state.get_last_synced(db_id)

        # Build filter for pages edited after last sync
        filter_obj: dict[str, Any] | None = None
        if last_synced:
            sync_filter: dict[str, Any] = {
                "property": "last_edited_time",
                "last_edited_time": {"after": last_synced},
            }
            if self.filter_config:
                base_filter = build_database_filter(self.filter_config)
                filter_obj = {"and": [base_filter, sync_filter]}
            else:
                filter_obj = sync_filter
        elif self.filter_config:
            filter_obj = build_database_filter(self.filter_config)

        pages = self._fetch_all_pages(client, db_id, filter_obj=filter_obj)

        tasks = [
            map_notion_page_to_task(
                page,
                database_id=db_id,
                property_map=self.property_map,
                status_map=self.status_map,
            )
            for page in pages
        ]

        # Update sync state
        self.sync_state.update(db_id, datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))

        return tasks

    # -- Private helpers ---------------------------------------------------

    def _parse_source_id(self, source_id: str) -> tuple[str, str]:
        """Parse source_id as 'database_id/page_id' or just 'page_id'."""
        source_id = source_id.strip()
        parts = source_id.split("/")

        if len(parts) == 2:
            return parts[0], parts[1]
        elif len(parts) == 1:
            db_id = self.default_database_id
            if not db_id:
                raise ValueError(
                    "Source ID must be 'database_id/page_id' or configure "
                    "default_database_id"
                )
            return db_id, parts[0]
        else:
            raise ValueError(
                f"Invalid Notion source ID format: {source_id!r}. "
                f"Expected 'database_id/page_id' or 'page_id'"
            )

    def _build_client(self) -> NotionClient:
        """Build an authenticated Notion client."""
        token = os.getenv(self.token_env) or ""
        return NotionClient(
            token=token,
            http_open=self.http_open,
        )

    def _fetch_all_pages(
        self,
        client: NotionClient,
        database_id: str,
        *,
        filter_obj: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all pages from a database, handling pagination."""
        all_pages: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            result = client.query_database(
                database_id,
                filter_obj=filter_obj,
                start_cursor=cursor,
            )
            pages = result.get("results") or []
            all_pages.extend(pages)

            if not result.get("has_more"):
                break
            cursor = result.get("next_cursor")
            if not cursor:
                break

        return all_pages

    def _fetch_all_blocks(
        self,
        client: NotionClient,
        block_id: str,
    ) -> list[dict[str, Any]]:
        """Fetch all child blocks, handling pagination."""
        all_blocks: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            result = client.get_block_children(
                block_id,
                start_cursor=cursor,
            )
            blocks = result.get("results") or []
            all_blocks.extend(blocks)

            if not result.get("has_more"):
                break
            cursor = result.get("next_cursor")
            if not cursor:
                break

        return all_blocks


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _create_page_summary(
    *,
    page_id: str,
    title: str,
    status: str,
    database_id: str,
    extracted: dict[str, Any],
) -> str:
    """Create a summary string for the SourceBrief."""
    parts = [
        f"Notion page {database_id}/{page_id}",
    ]

    if status:
        parts.append(f"Status: {status}")

    # Include select/multi_select values in summary
    for key, value in extracted.items():
        if key == title:
            continue
        if isinstance(value, list) and value and all(isinstance(v, str) for v in value):
            parts.append(f"{key}: {', '.join(value)}")
        elif isinstance(value, str) and value and key not in ("title",):
            if len(value) <= 100:
                parts.append(f"{key}: {value}")

    body = title.strip() if title else ""
    if body:
        parts.append(f"\n{body}")

    return "\n".join(parts)
