from blueprint.api.conditional_requests import (
    evaluate_if_match,
    evaluate_if_none_match,
    generate_etag,
)


def test_equivalent_dicts_produce_same_strong_etag():
    assert generate_etag({"b": 2, "a": 1}) == generate_etag({"a": 1, "b": 2})


def test_if_none_match_returns_not_modified_for_match():
    etag = generate_etag({"id": "a"})
    decision = evaluate_if_none_match(etag, etag)

    assert decision.status == "not_modified"
    assert decision.status_code == 304
    assert decision.should_return_body is False


def test_if_match_returns_precondition_failed_for_mismatch():
    decision = evaluate_if_match('"old"', '"new"')

    assert decision.status == "precondition_failed"
    assert decision.status_code == 412


def test_wildcards_are_handled():
    etag = generate_etag({"id": "a"})

    assert evaluate_if_none_match("*", etag).status == "not_modified"
    assert evaluate_if_match("*", etag).status == "ok"

