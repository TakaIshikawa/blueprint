import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_data_classification import (
    SourceDataClassificationReport,
    SourceDataClassificationRecord,
    build_source_data_classification_report,
    source_data_classification_report_to_dict,
    source_data_classification_report_to_markdown,
    summarize_source_data_classification,
)


def test_detects_sensitive_classifications_with_evidence_and_handling():
    report = build_source_data_classification_report(
        {
            "id": "sb-sensitive",
            "title": "Patient billing portal",
            "summary": "Collect patient email addresses and medical records for support triage.",
            "problem_statement": "Agents need access to billing data and transaction history.",
            "data_requirements": "Store API keys in the secrets manager and redact access tokens from logs.",
            "risks": ["The system handles children under 13 and requires parental consent."],
            "constraints": ["Restricted data must go through privacy and security review."],
            "metadata": {"telemetry": "Track analytics events for profile updates."},
        }
    )

    by_classification = {record.classification: record for record in report.classifications}

    assert {
        "restricted",
        "credentials",
        "health",
        "minors",
        "financial",
        "pii",
        "telemetry",
    } <= set(by_classification)
    assert isinstance(by_classification["credentials"], SourceDataClassificationRecord)
    assert any("access tokens" in item for item in by_classification["credentials"].evidence)
    assert any("secrets manager" in item for item in by_classification["credentials"].recommended_handling)
    assert by_classification["restricted"].confidence >= by_classification["telemetry"].confidence
    assert report.summary["restricted_category_counts"] == {
        "credentials": 1,
        "health": 1,
        "minors": 1,
        "financial": 1,
        "pii": 1,
    }
    assert report.summary["restricted_category_count"] == 5


def test_restricted_categories_are_separate_from_generic_internal_data():
    report = build_source_data_classification_report(
        {
            "id": "sb-internal",
            "summary": "Internal only runbook for staff data.",
            "data_requirements": "Includes passwords for a legacy importer and customer email addresses.",
        }
    )

    classifications = [record.classification for record in report.classifications]

    assert "internal" in classifications
    assert "credentials" in classifications
    assert "pii" in classifications
    assert report.summary["classification_counts"]["internal"] == 1
    assert report.summary["restricted_category_counts"]["credentials"] == 1
    assert report.summary["restricted_category_counts"]["pii"] == 1


def test_confidence_ordering_is_deterministic():
    report = build_source_data_classification_report(
        {
            "summary": "Publicly available launch copy.",
            "context": [
                "PII includes email addresses.",
                "Personal data includes phone numbers and home addresses.",
                "User identifiers are retained for support workflows.",
            ],
            "metadata": {"classification": "confidential"},
        }
    )

    confidences = [record.confidence for record in report.classifications]
    assert confidences == sorted(confidences, reverse=True)
    assert [record.classification for record in report.classifications[:2]] == ["restricted", "pii"]
    assert report.classifications[0].classification in {"restricted", "pii"}
    assert report.classifications[0].confidence > report.classifications[-1].confidence


def test_markdown_and_dict_serializers_are_stable_and_json_compatible():
    report = build_source_data_classification_report(
        {
            "id": "sb-md",
            "title": "Telemetry | privacy",
            "summary": "Confidential customer data and telemetry logs require review.",
            "metadata": {"note": "Telemetry | logs are retained for 30 days."},
        }
    )

    payload = source_data_classification_report_to_dict(report)
    markdown = source_data_classification_report_to_markdown(report)

    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_brief_id", "title", "summary", "classifications"]
    assert list(payload["classifications"][0]) == [
        "classification",
        "confidence",
        "evidence",
        "recommended_handling",
        "review_questions",
    ]
    assert markdown == report.to_markdown()
    assert markdown.startswith("# Source Data Classification: sb-md")
    assert "- Classification count:" in markdown
    assert "Telemetry \\| logs are retained for 30 days." in markdown


def test_empty_and_low_signal_briefs_return_empty_report_with_stable_counts():
    report = build_source_data_classification_report(
        {
            "id": "sb-empty",
            "title": "Planning sync",
            "summary": "Coordinate the next planning conversation.",
            "metadata": {"priority": "later"},
        }
    )

    assert isinstance(report, SourceDataClassificationReport)
    assert report.to_dict() == {
        "source_brief_id": "sb-empty",
        "title": "Planning sync",
        "summary": {
            "classification_count": 0,
            "restricted_category_count": 0,
            "highest_confidence": 0.0,
            "classification_counts": {
                "restricted": 0,
                "credentials": 0,
                "health": 0,
                "minors": 0,
                "financial": 0,
                "pii": 0,
                "confidential": 0,
                "internal": 0,
                "telemetry": 0,
                "public": 0,
            },
            "restricted_category_counts": {
                "credentials": 0,
                "health": 0,
                "minors": 0,
                "financial": 0,
                "pii": 0,
            },
        },
        "classifications": [],
    }
    assert report.to_markdown() == (
        "# Source Data Classification: sb-empty\n\n"
        "## Summary\n\n"
        "- Classification count: 0\n"
        "- Restricted category count: 0\n"
        "- Highest confidence: 0.00\n\n"
        "No source data classification signals were inferred."
    )
    assert build_source_data_classification_report(None).summary["classification_count"] == 0


def test_source_brief_input_uses_source_payload_fields_and_alias_matches():
    brief = SourceBrief(
        id="sb-model",
        title="Student payment import",
        summary="Import restricted customer records.",
        source_project="manual",
        source_entity_type="note",
        source_id="note-1",
        source_payload={
            "context": "Students and minors may upload financial data.",
            "data_requirements": "Collect credit card metadata but not full card numbers.",
            "metadata": {"health": "No health data expected."},
        },
        source_links={},
    )

    report = build_source_data_classification_report(brief)
    alias_report = summarize_source_data_classification(brief)

    assert alias_report.to_dict() == report.to_dict()
    assert report.source_brief_id == "sb-model"
    assert {"minors", "financial", "health", "restricted"} <= {
        record.classification for record in report.classifications
    }
    assert any(
        "source_payload.context" in evidence
        for record in report.classifications
        for evidence in record.evidence
    )


def test_mapping_input_is_not_mutated():
    brief = {
        "id": "sb-mutation",
        "summary": "Credentials include API keys.",
        "metadata": {"nested": {"risk": "Confidential customer data."}},
    }
    original = copy.deepcopy(brief)

    report = build_source_data_classification_report(brief)

    assert brief == original
    assert report.to_dicts() == source_data_classification_report_to_dict(report)["classifications"]
