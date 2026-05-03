import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_file_attachment_requirements import (
    SourceFileAttachmentRequirement,
    SourceFileAttachmentRequirementsReport,
    build_source_file_attachment_requirements,
    build_source_file_attachment_requirements_report,
    derive_source_file_attachment_requirements,
    extract_source_file_attachment_requirements,
    generate_source_file_attachment_requirements,
    source_file_attachment_requirements_to_dict,
    source_file_attachment_requirements_to_dicts,
    source_file_attachment_requirements_to_markdown,
    summarize_source_file_attachment_requirements,
)


def test_structured_source_payload_extracts_attachment_requirement_types():
    result = build_source_file_attachment_requirements(
        _source(
            source_payload={
                "file_uploads": {
                    "types": "Allow image uploads for PNG, JPEG, and PDF documents only.",
                    "size": "Uploaded files must be no larger than 10 MB.",
                    "count": "Users can attach up to 5 files per request.",
                    "scanning": "Every attachment requires virus scanning before it is stored.",
                    "storage": "Uploaded attachments must be stored in a private S3 bucket.",
                    "preview": "Provide image preview thumbnails and PDF preview.",
                    "access": "Attachment access controls require signed URLs for authorized users.",
                    "progress": "Show upload progress with a progress bar and retry upload status.",
                    "download": "Admins must be able to download original attachments.",
                    "metadata": "Capture original filename, MIME type, checksum, uploaded by, and file size metadata.",
                    "retention": "Temporary uploads are deleted after 30 days.",
                    "deletion": "Deletion lifecycle must support soft delete before hard delete.",
                }
            }
        )
    )

    assert isinstance(result, SourceFileAttachmentRequirementsReport)
    assert result.source_id == "source-files"
    assert all(isinstance(record, SourceFileAttachmentRequirement) for record in result.records)
    assert {
        "allowed_file_type",
        "max_file_size",
        "attachment_count",
        "virus_scanning",
        "storage_location",
        "preview",
        "access_control",
        "upload_progress",
        "download",
        "metadata_capture",
        "retention",
        "deletion_lifecycle",
    } <= {record.requirement_type for record in result.records}
    assert result.summary["requirement_count"] >= 12
    assert result.summary["type_counts"]["virus_scanning"] >= 1
    assert result.summary["surface_counts"]["image_upload"] >= 1
    assert any("source_payload.file_uploads.scanning" in record.evidence for record in result.records)
    assert _record(result, "max_file_size").value == "10 mb"
    assert _record(result, "attachment_count").value == "5"
    assert _record(result, "storage_location").value == "s3 bucket"
    assert _record(result, "access_control").value == "signed urls"
    assert _record(result, "upload_progress").value == "progress bar"
    assert "checksum" in (_record(result, "metadata_capture").value or "")
    assert _record(result, "retention").value == "30 days"
    assert _record(result, "deletion_lifecycle").value in {"30 days", "hard delete", "soft delete"}


def test_markdown_string_and_implementation_brief_scanning_are_stable():
    text_result = build_source_file_attachment_requirements(
        """
# Attachments

- Files must be limited to PDF and DOCX.
- Uploads have a maximum file size of 25MB.
- Attachments require malware scan and preview.
"""
    )
    implementation = ImplementationBrief.model_validate(
        _implementation(
            scope=["Customers upload images and documents."],
            architecture_notes="Image uploads should support PNG and JPEG previews.",
            data_requirements="Retain uploaded documents for 90 days, then purge after 90 days.",
            definition_of_done=[
                "Download links for original files work.",
                "At most 3 attachments can be uploaded.",
            ],
        )
    )
    implementation_result = build_source_file_attachment_requirements_report(implementation)
    markdown = source_file_attachment_requirements_to_markdown(text_result)

    assert [record.requirement_type for record in text_result.records] == [
        "allowed_file_type",
        "max_file_size",
        "virus_scanning",
        "preview",
    ]
    assert text_result.source_id is None
    assert markdown == text_result.to_markdown()
    assert markdown.startswith("# Source File Attachment Requirements")
    assert "| Source | Type | Surface | Value | Confidence | Evidence |" in markdown
    assert "body: Files must be limited to PDF and DOCX." in markdown
    assert implementation_result.source_id == "impl-files"
    assert {"allowed_file_type", "preview", "download", "retention", "attachment_count"} <= {
        record.requirement_type for record in implementation_result.records
    }
    assert any(record.evidence.startswith("definition_of_done[1]:") for record in implementation_result.records)


def test_models_objects_lists_and_aliases_are_supported_without_mutating_inputs():
    source = _source(
        source_id="model-files",
        summary="Users must upload PDF attachments only.",
        source_payload={"requirements": ["Attachments must be scanned for malware."]},
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    object_result = build_source_file_attachment_requirements(
        SimpleNamespace(id="object-files", body="Profile image uploads allow PNG and JPEG previews.")
    )
    list_result = build_source_file_attachment_requirements(
        [
            source,
            _source(source_id="source-b", summary="Support download of uploaded files."),
        ]
    )

    mapping_result = build_source_file_attachment_requirements(source)
    model_result = build_source_file_attachment_requirements(model)
    generated = generate_source_file_attachment_requirements(model)
    derived = derive_source_file_attachment_requirements(model)
    extracted = extract_source_file_attachment_requirements(model)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert object_result.source_id == "object-files"
    assert {record.requirement_type for record in object_result.records} >= {"allowed_file_type", "preview"}
    assert list_result.source_id is None
    assert list_result.summary["source_count"] == 2
    assert list_result.summary["source_ids"] == ["model-files", "source-b"]


def test_deduplication_serialization_empty_and_invalid_inputs_are_stable():
    source = _source(
        source_payload={
            "requirements": [
                "Attachments must be scanned for malware.",
                "attachments shall be scanned for malware",
                "Maximum file size is 8 MB.",
            ],
            "metadata": {"upload_limits": "Maximum file size is 8 MB."},
        }
    )
    result = build_source_file_attachment_requirements(source)
    empty = build_source_file_attachment_requirements(
        _source(title="Copy", summary="Polish onboarding copy.", source_payload={})
    )
    repeat = build_source_file_attachment_requirements(
        _source(title="Copy", summary="Polish onboarding copy.", source_payload={})
    )
    invalid = build_source_file_attachment_requirements(17)
    payload = source_file_attachment_requirements_to_dict(result)

    assert [record.requirement_type for record in result.records] == ["max_file_size", "virus_scanning"]
    assert _record(result, "virus_scanning").evidence == (
        "source_payload.requirements[0]: Attachments must be scanned for malware."
    )
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["requirements"]
    assert source_file_attachment_requirements_to_dicts(result) == payload["requirements"]
    assert source_file_attachment_requirements_to_dicts(result.records) == payload["records"]
    assert summarize_source_file_attachment_requirements(result) == result.summary
    assert list(payload) == ["source_id", "requirements", "records", "recommendations", "summary"]
    assert list(payload["requirements"][0]) == [
        "source_id",
        "requirement_type",
        "attachment_surface",
        "value",
        "evidence",
        "confidence",
    ]
    assert empty.to_dict() == repeat.to_dict()
    assert empty.records == ()
    assert empty.recommendations == ()
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "requirement_types": [],
        "attachment_surfaces": [],
        "type_counts": {
            "allowed_file_type": 0,
            "max_file_size": 0,
            "attachment_count": 0,
            "virus_scanning": 0,
            "storage_location": 0,
            "preview": 0,
            "access_control": 0,
            "upload_progress": 0,
            "download": 0,
            "metadata_capture": 0,
            "retention": 0,
            "deletion_lifecycle": 0,
        },
        "surface_counts": {
            "file_upload": 0,
            "image_upload": 0,
            "document_upload": 0,
            "attachment": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "source_ids": [],
    }
    assert "No source file attachment requirements were found." in empty.to_markdown()
    assert invalid.source_id is None
    assert invalid.records == ()


def test_attachment_false_positive_filtering_ignores_general_documents_and_copy():
    result = build_source_file_attachment_requirements(
        _source(
            title="Documentation updates",
            summary="Documents should explain onboarding copy and export messaging.",
            source_payload={
                "metadata": {
                    "status": "The design document is stored in the project folder.",
                    "notes": "Document the access control decisions for reviewers.",
                }
            },
        )
    )

    assert result.records == ()


def _record(result, requirement_type):
    return next(record for record in result.records if record.requirement_type == requirement_type)


def _source(*, source_id="source-files", title="Attachment requirements", summary="File upload requirements.", source_payload=None):
    return {
        "id": source_id,
        "title": title,
        "domain": "collaboration",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "issue",
        "source_id": source_id,
        "source_payload": source_payload or {},
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation(*, scope=None, architecture_notes=None, data_requirements=None, definition_of_done=None):
    return {
        "id": "impl-files",
        "source_brief_id": "source-files",
        "title": "Attachment handling",
        "domain": "collaboration",
        "target_user": "ops",
        "buyer": "support",
        "workflow_context": "Preserve upload requirements before task generation.",
        "problem_statement": "Customers need safe attachment handling.",
        "mvp_goal": "Capture file upload constraints in the plan.",
        "product_surface": "messages",
        "scope": scope or [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": data_requirements,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review attachment requirement evidence.",
        "definition_of_done": definition_of_done or [],
    }
