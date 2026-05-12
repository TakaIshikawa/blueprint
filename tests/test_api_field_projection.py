import pytest

from blueprint.api.field_projection import (
    FieldProjectionError,
    FieldSelector,
    field_selectors_to_query,
    parse_field_selectors,
    project_fields,
)


def test_parse_field_selectors_normalizes_whitespace():
    selectors = parse_field_selectors(" id, title , owner.email ")

    assert selectors == (
        FieldSelector(("id",)),
        FieldSelector(("title",)),
        FieldSelector(("owner", "email")),
    )


def test_parse_field_selectors_validates_allowed_top_level_fields():
    with pytest.raises(FieldProjectionError, match="Unknown field selector: secret"):
        parse_field_selectors("id,secret.token", allowed_fields={"id", "owner"})


def test_project_fields_projects_single_dictionary_without_mutating_source():
    source = {
        "id": "plan-1",
        "title": "Launch",
        "secret": "hidden",
        "owner": {"email": "owner@example.com", "name": "Owner"},
    }

    projected = project_fields(source, "id,owner.email")

    assert projected == {"id": "plan-1", "owner": {"email": "owner@example.com"}}
    assert source["owner"] == {"email": "owner@example.com", "name": "Owner"}


def test_project_fields_projects_list_of_dictionaries():
    source = [
        {"id": "plan-1", "title": "Launch", "owner": {"email": "a@example.com"}},
        {"id": "plan-2", "title": "Operate", "owner": {"email": "b@example.com"}},
    ]

    assert project_fields(source, "id,owner.email") == [
        {"id": "plan-1", "owner": {"email": "a@example.com"}},
        {"id": "plan-2", "owner": {"email": "b@example.com"}},
    ]


def test_project_fields_skips_missing_nested_paths():
    source = {"id": "plan-1", "owner": {}}

    assert project_fields(source, "id,owner.email") == {"id": "plan-1"}


def test_field_selectors_to_query_preserves_selector_order():
    selectors = parse_field_selectors("title,id,owner.email")

    assert field_selectors_to_query(selectors) == "title,id,owner.email"

