from blueprint.api.hypermedia import (
    build_href,
    pagination_links,
    plan_links,
    task_links,
)


def test_resource_links_include_required_fields_and_normalized_urls():
    href = build_href("https://api.example.com/", "/plans/", "p1")

    assert href == "https://api.example.com/plans/p1"


def test_plan_and_task_links_include_related_dependency_or_parent_links():
    plan = plan_links({"id": "p1", "depends_on": ["p0"]}, base_url="https://api.test/")
    task = task_links({"id": "t1", "plan_id": "p1", "depends_on": ["t0"]}, base_url="https://api.test/")

    assert any(item.rel == "related" and item.href.endswith("/plans/p0") for item in plan)
    assert any(item.rel == "parent" and item.href.endswith("/plans/p1") for item in task)
    assert any(item.rel == "related" and item.href.endswith("/tasks/t0") for item in task)


def test_pagination_cursors_convert_to_next_previous_links():
    links = pagination_links(
        base_url="https://api.test",
        collection_path="/tasks",
        next_cursor="n",
        previous_cursor="p",
    )

    assert [item.rel for item in links] == ["next", "previous"]
    assert "cursor=n" in links[0].href

