from blueprint.export.data_exporter import sanitize_export_destination


def test_sanitize_export_destination_redacts_url_credentials():
    destination = "s3://deploy:super-secret@exports.example.com/releases"

    assert (
        sanitize_export_destination(destination)
        == "s3://REDACTED:REDACTED@exports.example.com/releases"
    )


def test_sanitize_export_destination_redacts_username_without_password():
    destination = "https://token-user@exports.example.com/archive"

    assert sanitize_export_destination(destination) == "https://REDACTED@exports.example.com/archive"


def test_sanitize_export_destination_redacts_sensitive_query_values():
    destination = (
        "https://exports.example.com/archive?"
        "token=abc&workspace=eng&signature=sig&api_key=key&empty="
    )

    assert sanitize_export_destination(destination) == (
        "https://exports.example.com/archive?"
        "token=REDACTED&workspace=eng&signature=REDACTED&api_key=REDACTED&empty="
    )


def test_sanitize_export_destination_preserves_plain_destination_labels():
    assert sanitize_export_destination("nightly warehouse export") == "nightly warehouse export"
    assert sanitize_export_destination("/mnt/exports/nightly") == "/mnt/exports/nightly"


def test_sanitize_export_destination_handles_invalid_urls_deterministically():
    assert sanitize_export_destination("https://exports.example.com:bad/archive") == (
        "https://exports.example.com:bad/archive"
    )

