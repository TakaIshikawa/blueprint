import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_file_handling_requirements import (
    SourceFileHandlingRequirement,
    SourceFileHandlingRequirementsReport,
    build_source_file_handling_requirements,
    extract_source_file_handling_requirements,
    generate_source_file_handling_requirements,
    source_file_handling_requirements_to_dict,
    source_file_handling_requirements_to_dicts,
    source_file_handling_requirements_to_markdown,
)


def test_raw_markdown_detects_file_handling_requirements():
    result = build_source_file_handling_requirements(
        """
        # Intake portal

        - Users must upload attachments for each claim.
        - Downloads use signed URLs that expire after 15 minutes.
        - PDF and CSV file types are accepted with a 25 MB file size limit.
        - Malware scanning quarantines infected files before storage access is granted.
        """
    )

    assert isinstance(result, SourceFileHandlingRequirementsReport)
    assert all(isinstance(record, SourceFileHandlingRequirement) for record in result.records)
    assert _types(result) == (
        "upload",
        "download",
        "attachment",
        "signed_url",
        "file_size",
        "file_type",
        "malware_scanning",
        "storage_access",
    )
    assert result.summary["requirement_count"] == 8
    assert result.summary["source_count"] == 1
    assert result.summary["requirement_type_counts"]["upload"] == 1
    assert result.summary["confidence_counts"]["high"] == 8


def test_mapping_input_detects_import_export_and_metadata_signals_without_mutation():
    source = {
        "id": "brief-files",
        "title": "Partner data exchange",
        "body": "Support CSV import for customers and export audit evidence as PDF.",
        "metadata": {
            "upload_constraints": {
                "max_file_size": "10 MB maximum upload size",
                "allowed_mime_types": ["text/csv", "application/pdf"],
            },
            "storage_access": "S3 bucket permissions must restrict files to authorized users only.",
        },
    }
    original = copy.deepcopy(source)

    result = build_source_file_handling_requirements(source)

    assert source == original
    assert result.source_id == "brief-files"
    assert _types(result) == (
        "upload",
        "import",
        "export",
        "file_size",
        "file_type",
        "storage_access",
    )
    storage = _record(result, "storage_access")
    assert storage.confidence == "high"
    assert storage.source_brief_id == "brief-files"
    assert any("metadata.storage_access" in evidence for evidence in storage.evidence)


def test_duplicate_evidence_folds_into_single_record():
    result = build_source_file_handling_requirements(
        """
        Users must upload files.
        Users must upload files.
        Users must upload files.
        """
    )

    assert _types(result) == ("upload",)
    record = result.records[0]
    assert record.detected_signals == ("upload",)
    assert record.evidence == ("body: Users must upload files.",)
    assert result.summary["requirement_type_counts"]["upload"] == 1


def test_json_compatible_serialization_and_aliases_are_deterministic():
    source = {
        "id": "brief-serialize",
        "title": "File handling serialization",
        "summary": "File exchange requirements.",
        "source_project": "manual",
        "source_entity_type": "note",
        "source_id": "note-serialize",
        "source_payload": {
            "body": "Allow file uploads, attachment downloads, and signed URL exports.",
            "requirements": ["Virus scanning is required for uploaded CSV files."],
        },
        "source_links": {},
    }
    model = SourceBrief.model_validate(source)

    result = generate_source_file_handling_requirements(model)
    payload = source_file_handling_requirements_to_dict(result)

    assert payload == result.to_dict()
    assert result.to_dicts() == payload["requirements"]
    assert source_file_handling_requirements_to_dicts(result.records) == result.to_dicts()
    assert extract_source_file_handling_requirements(source) == result.records
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "detected_signals",
        "evidence",
        "confidence",
        "planning_implications",
    ]
    assert source_file_handling_requirements_to_dict(
        build_source_file_handling_requirements(source)
    ) == payload


def test_markdown_output_is_useful_and_stable():
    result = build_source_file_handling_requirements(
        {
            "id": "brief-markdown",
            "body": "Export CSV files and download via signed URLs.",
            "acceptance_criteria": ["Access control prevents unauthorized storage access."],
        }
    )

    markdown = source_file_handling_requirements_to_markdown(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source File Handling Requirements Report: brief-markdown")
    assert "## Summary" in markdown
    assert "| Source Brief | Type | Confidence | Signals | Evidence | Planning Implications |" in markdown
    assert "| brief-markdown | download | high | download | body: Export CSV files and download via signed URLs." in markdown
    assert "Plan signed URL expiry" in markdown
    assert "storage_access" in markdown


def test_empty_and_no_match_behavior_has_stable_summary_counts():
    result = build_source_file_handling_requirements(
        {"id": "empty", "title": "Profile polish", "body": "Update labels and settings copy."}
    )
    invalid = build_source_file_handling_requirements(17)

    expected_counts = {
        "upload": 0,
        "download": 0,
        "attachment": 0,
        "import": 0,
        "export": 0,
        "signed_url": 0,
        "file_size": 0,
        "file_type": 0,
        "malware_scanning": 0,
        "storage_access": 0,
    }

    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "requirement_type_counts": expected_counts,
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
    }
    assert "No file handling requirements were found" in result.to_markdown()
    assert invalid.records == ()
    assert invalid.summary["requirement_type_counts"] == expected_counts


def _types(result):
    return tuple(record.requirement_type for record in result.records)


def _record(result, requirement_type):
    return next(record for record in result.records if record.requirement_type == requirement_type)
