from blueprint.api.error_taxonomy import ErrorCategory, api_error, error_from_exception, redact_sensitive


def test_common_api_error_has_stable_fields():
    payload = api_error("not_found", "Missing", category=ErrorCategory.NOT_FOUND)

    assert payload.code == "not_found"
    assert payload.category == ErrorCategory.NOT_FOUND
    assert payload.status == 404
    assert payload.message == "Missing"


def test_sensitive_details_are_redacted_recursively():
    assert redact_sensitive({"token": "a", "nested": {"password": "b"}, "safe": "c"}) == {
        "token": "[REDACTED]",
        "nested": {"password": "[REDACTED]"},
        "safe": "c",
    }


def test_unknown_exception_maps_to_internal_error_without_internals():
    payload = error_from_exception(RuntimeError("database password leaked"))

    assert payload.code == "internal_error"
    assert payload.status == 500
    assert payload.details == {}

