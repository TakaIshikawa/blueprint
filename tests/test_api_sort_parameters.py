import pytest

from blueprint.api.sort_parameters import (
    SortCriterion,
    SortParameterError,
    default_sort_criteria,
    parse_sort_parameter,
    sort_criteria_to_query,
)


def test_parse_sort_parameter_preserves_order_and_directions():
    criteria = parse_sort_parameter("created_at,-priority,title")

    assert criteria == (
        SortCriterion("created_at", "asc"),
        SortCriterion("priority", "desc"),
        SortCriterion("title", "asc"),
    )


def test_parse_sort_parameter_normalizes_whitespace():
    criteria = parse_sort_parameter(" created_at , -priority ")

    assert criteria == (
        SortCriterion("created_at", "asc"),
        SortCriterion("priority", "desc"),
    )


def test_parse_sort_parameter_rejects_unknown_allowed_fields():
    with pytest.raises(SortParameterError, match="Unknown sort field: status"):
        parse_sort_parameter("created_at,status", allowed_fields={"created_at", "priority"})


def test_parse_sort_parameter_rejects_duplicate_fields_regardless_of_direction():
    with pytest.raises(SortParameterError, match="Duplicate sort field: priority"):
        parse_sort_parameter("priority,-priority")


def test_parse_sort_parameter_uses_deterministic_default_criteria():
    criteria = parse_sort_parameter(
        "",
        allowed_fields={"created_at", "id"},
        default=(SortCriterion("created_at", "desc"), "id"),
    )

    assert criteria == (
        SortCriterion("created_at", "desc"),
        SortCriterion("id", "asc"),
    )


def test_default_sort_criteria_rejects_duplicate_defaults():
    with pytest.raises(SortParameterError, match="Duplicate sort field: created_at"):
        default_sort_criteria("created_at", "-created_at")


def test_sort_criteria_to_query_serializes_stable_query_value():
    query = sort_criteria_to_query(
        (
            SortCriterion("created_at", "desc"),
            SortCriterion("priority", "asc"),
        )
    )

    assert query == "-created_at,priority"

