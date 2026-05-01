import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_api_breaking_changes import (
    SourceApiBreakingChangeReport,
    SourceApiBreakingChangeRecord,
    build_source_api_breaking_change_report,
    source_api_breaking_change_report_to_dict,
    source_api_breaking_change_report_to_markdown,
    summarize_source_api_breaking_changes,
)


def test_detects_major_api_breaking_change_families_with_evidence_and_questions():
    report = build_source_api_breaking_change_report(
        {
            "id": "sb-api",
            "title": "Partner API contract migration",
            "summary": "Remove field `legacy_id` from GET /v1/customers.",
            "context": [
                "Rename field `accountName` to `display_name` in POST /v1/accounts.",
                "The response shape for GET /v1/orders changes from an array to an object envelope.",
                "A new required parameter `tenant_id` must be supplied on GET /v1/orders.",
                "HTTP status code 200 now returns 202 instead for POST /v1/jobs.",
                "Pagination changes from offset to cursor pagination on GET /v1/search.",
                "API version header `X-API-Version` is required for all /v2 endpoints.",
            ],
        }
    )

    by_type = {record.change_type: record for record in report.records}

    assert {
        "removed_field",
        "renamed_field",
        "response_shape_change",
        "required_parameter_change",
        "status_code_change",
        "pagination_change",
        "versioning_requirement",
    } <= set(by_type)
    assert isinstance(by_type["removed_field"], SourceApiBreakingChangeRecord)
    assert by_type["removed_field"].affected_surface == "/v1/customers"
    assert by_type["removed_field"].severity == "high"
    assert any("legacy_id" in item for item in by_type["removed_field"].evidence)
    assert any("consumers" in item for item in by_type["removed_field"].migration_questions)
    assert "api-contract" in by_type["removed_field"].suggested_plan_tags
    assert by_type["pagination_change"].severity == "medium"


def test_escalates_authentication_and_webhook_payload_changes_to_high_severity():
    report = build_source_api_breaking_change_report(
        {
            "id": "sb-high",
            "summary": (
                "Authentication changes require OAuth scopes instead of API keys on /v2/payments. "
                "Webhook payload schema changes remove field `customer.email` from invoice.paid callbacks."
            ),
        }
    )

    by_type = {record.change_type: record for record in report.records}

    assert by_type["authentication_change"].severity == "high"
    assert by_type["webhook_payload_change"].severity == "high"
    assert report.summary["high_severity_count"] == 2
    assert report.summary["severity_counts"]["high"] == 2
    assert report.summary["change_type_counts"]["authentication_change"] == 1
    assert "webhook-migration" in by_type["webhook_payload_change"].suggested_plan_tags


def test_markdown_and_dict_serializers_are_stable_and_json_compatible():
    report = build_source_api_breaking_change_report(
        {
            "id": "sb-md",
            "title": "API | migration",
            "summary": "Remove field `old|name` from GET /v1/widgets.",
            "constraints": ["Require version header `X-API-Version` for partner clients."],
        }
    )

    payload = source_api_breaking_change_report_to_dict(report)
    markdown = source_api_breaking_change_report_to_markdown(report)

    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_brief_id", "title", "summary", "records"]
    assert list(payload["records"][0]) == [
        "change_type",
        "affected_surface",
        "severity",
        "evidence",
        "migration_questions",
        "suggested_plan_tags",
    ]
    assert markdown == report.to_markdown()
    assert markdown.startswith("# Source API Breaking Changes: sb-md")
    assert "- Change count:" in markdown
    assert "old\\|name" in markdown


def test_record_ordering_is_deterministic_by_severity_type_and_surface():
    source = {
        "summary": "Pagination changes from offset to cursor pagination on GET /v1/search.",
        "context": [
            "Rename field `zeta` to `z` in GET /v1/zeta.",
            "Remove field `alpha` from GET /v1/alpha.",
            "Authentication changes require OAuth scopes on /v1/auth.",
            "Webhook payload schema changes remove field `event.data` from callbacks.",
        ],
    }

    report = build_source_api_breaking_change_report(source)

    assert [(record.severity, record.change_type) for record in report.records] == [
        ("high", "removed_field"),
        ("high", "authentication_change"),
        ("high", "webhook_payload_change"),
        ("medium", "renamed_field"),
        ("medium", "pagination_change"),
    ]
    assert report.to_dict() == build_source_api_breaking_change_report(copy.deepcopy(source)).to_dict()


def test_empty_and_low_signal_briefs_return_empty_report_with_stable_counts():
    report = build_source_api_breaking_change_report(
        {
            "id": "sb-empty",
            "title": "Planning sync",
            "summary": "Coordinate follow-up notes for the next roadmap conversation.",
            "metadata": {"priority": "later"},
        }
    )

    assert isinstance(report, SourceApiBreakingChangeReport)
    assert report.to_dict() == {
        "source_brief_id": "sb-empty",
        "title": "Planning sync",
        "summary": {
            "change_count": 0,
            "high_severity_count": 0,
            "change_types": [],
            "severity_counts": {"high": 0, "medium": 0, "low": 0},
            "change_type_counts": {
                "removed_field": 0,
                "renamed_field": 0,
                "response_shape_change": 0,
                "required_parameter_change": 0,
                "authentication_change": 0,
                "status_code_change": 0,
                "pagination_change": 0,
                "webhook_payload_change": 0,
                "versioning_requirement": 0,
            },
        },
        "records": [],
    }
    assert report.to_markdown() == (
        "# Source API Breaking Changes: sb-empty\n\n"
        "## Summary\n\n"
        "- Change count: 0\n"
        "- High severity count: 0\n"
        "- Change types: none\n\n"
        "No source API breaking-change signals were inferred."
    )
    assert build_source_api_breaking_change_report(None).summary["change_count"] == 0


def test_source_brief_model_input_uses_source_payload_fields_and_alias_matches():
    brief = SourceBrief(
        id="sb-model",
        title="Webhook auth migration",
        summary="Partner contract updates.",
        source_project="manual",
        source_entity_type="note",
        source_id="note-1",
        source_payload={
            "context": "Webhook payload schema changes remove field `customer.email` from invoice.paid callbacks.",
            "requirements": "Authentication changes require OAuth scopes on /v2/hooks.",
        },
        source_links={},
    )

    report = build_source_api_breaking_change_report(brief)
    alias_report = summarize_source_api_breaking_changes(brief)

    assert alias_report.to_dict() == report.to_dict()
    assert report.source_brief_id == "sb-model"
    assert {"webhook_payload_change", "authentication_change"} <= {
        record.change_type for record in report.records
    }
    assert any(
        "source_payload.context" in evidence
        for record in report.records
        for evidence in record.evidence
    )


def test_mapping_input_is_not_mutated():
    brief = {
        "id": "sb-mutation",
        "summary": "A new required parameter `tenant_id` must be supplied on GET /v1/orders.",
        "metadata": {"nested": {"risk": "Response shape for /v1/orders changes to an object envelope."}},
    }
    original = copy.deepcopy(brief)

    report = build_source_api_breaking_change_report(brief)

    assert brief == original
    assert report.to_dicts() == source_api_breaking_change_report_to_dict(report)["records"]
