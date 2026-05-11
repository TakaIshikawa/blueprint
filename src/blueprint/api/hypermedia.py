"""Hypermedia link builders for Blueprint resources."""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import urlencode

from pydantic import BaseModel, ConfigDict, Field


class ResourceLink(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rel: str = Field(min_length=1)
    href: str = Field(min_length=1)
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "GET"
    title: str | None = None


def normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def build_href(base_url: str, *parts: str, query: dict[str, Any] | None = None) -> str:
    base = normalize_base_url(base_url)
    path = "/".join(str(part).strip("/") for part in parts if str(part).strip("/"))
    href = f"{base}/{path}" if path else base
    if query:
        href = f"{href}?{urlencode({k: v for k, v in query.items() if v is not None})}"
    return href


def link(
    rel: str,
    href: str,
    *,
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "GET",
    title: str | None = None,
) -> ResourceLink:
    return ResourceLink(rel=rel, href=href, method=method, title=title)


def resource_links(resource_type: str, resource_id: str, *, base_url: str) -> list[ResourceLink]:
    return [
        link("self", build_href(base_url, resource_type, resource_id)),
        link("collection", build_href(base_url, resource_type)),
    ]


def pagination_links(
    *,
    base_url: str,
    collection_path: str,
    next_cursor: str | None = None,
    previous_cursor: str | None = None,
) -> list[ResourceLink]:
    links: list[ResourceLink] = []
    if next_cursor:
        links.append(link("next", build_href(base_url, collection_path, query={"cursor": next_cursor})))
    if previous_cursor:
        links.append(
            link("previous", build_href(base_url, collection_path, query={"cursor": previous_cursor}))
        )
    return links


def action_link(
    rel: str,
    *,
    base_url: str,
    resource_type: str,
    resource_id: str,
    action: str,
    method: Literal["POST", "PUT", "PATCH", "DELETE"] = "POST",
    title: str | None = None,
) -> ResourceLink:
    return link(rel, build_href(base_url, resource_type, resource_id, action), method=method, title=title)


def blueprint_resource_links(resource: dict[str, Any], resource_type: str, *, base_url: str) -> list[ResourceLink]:
    resource_id = str(resource.get("id") or resource.get(f"{resource_type[:-1]}_id") or "")
    links = resource_links(resource_type, resource_id, base_url=base_url)
    if resource_type == "plans":
        for dep_id in resource.get("depends_on", []):
            links.append(link("related", build_href(base_url, "plans", dep_id), title="dependency"))
    if resource_type == "tasks":
        if plan_id := resource.get("plan_id"):
            links.append(link("parent", build_href(base_url, "plans", plan_id), title="plan"))
        for dep_id in resource.get("depends_on", []):
            links.append(link("related", build_href(base_url, "tasks", dep_id), title="dependency"))
    return links


def brief_links(brief_id: str, *, base_url: str) -> list[ResourceLink]:
    return resource_links("briefs", brief_id, base_url=base_url)


def plan_links(plan: dict[str, Any], *, base_url: str) -> list[ResourceLink]:
    return blueprint_resource_links(plan, "plans", base_url=base_url)


def task_links(task: dict[str, Any], *, base_url: str) -> list[ResourceLink]:
    return blueprint_resource_links(task, "tasks", base_url=base_url)


def workspace_links(workspace_id: str, *, base_url: str) -> list[ResourceLink]:
    return resource_links("workspaces", workspace_id, base_url=base_url)


def export_links(export_id: str, *, base_url: str) -> list[ResourceLink]:
    return resource_links("exports", export_id, base_url=base_url)

