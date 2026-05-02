import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_encryption_requirements import (
    SourceEncryptionRequirement,
    SourceEncryptionRequirementsReport,
    build_source_encryption_requirements,
    extract_source_encryption_requirements,
    generate_source_encryption_requirements,
    source_encryption_requirements_to_dict,
    source_encryption_requirements_to_dicts,
    source_encryption_requirements_to_markdown,
    summarize_source_encryption_requirements,
)


def test_text_fields_extract_encryption_and_key_management_requirements():
    result = build_source_encryption_requirements(
        _source_brief(
            summary=(
                "Customer data must be encrypted at rest with AWS KMS owned by the security team. "
                "All API traffic requires TLS 1.2 encryption in transit before launch."
            ),
            description=(
                "Customer-managed keys are required for enterprise tenants. "
                "Key rotation must happen every 90 days."
            ),
        )
    )

    assert isinstance(result, SourceEncryptionRequirementsReport)
    assert result.source_id == "sb-encryption"
    assert all(isinstance(record, SourceEncryptionRequirement) for record in result.records)
    assert {
        "at_rest_encryption",
        "in_transit_encryption",
        "tls",
        "kms",
        "customer_managed_keys",
        "key_rotation",
    } <= {record.category for record in result.records}
    assert result.summary["category_counts"]["tls"] >= 1
    assert result.summary["confidence_counts"]["high"] >= 4
    at_rest = next(record for record in result.records if record.category == "at_rest_encryption")
    assert at_rest.scope == "customer data"
    assert "key owner" not in at_rest.missing_inputs


def test_source_payload_fields_extract_tokens_secrets_envelope_and_field_level_encryption():
    result = build_source_encryption_requirements(
        _source_brief(
            source_payload={
                "security": {
                    "secrets": "Client secrets must be stored in Vault with secret rotation.",
                    "tokens": "Refresh tokens should be encrypted using KMS.",
                    "envelope_encryption": "Envelope encryption uses DEKs for uploaded files.",
                    "field_level": "PII fields require field-level encryption.",
                }
            }
        )
    )

    by_category = {record.category: record for record in result.records}

    assert {
        "secrets_management",
        "token_encryption",
        "kms",
        "key_rotation",
        "envelope_encryption",
        "field_level_encryption",
    } <= set(by_category)
    assert by_category["secrets_management"].scope == "client secrets"
    assert "rotation policy" not in by_category["secrets_management"].missing_inputs
    assert "data scope" not in by_category["field_level_encryption"].missing_inputs
    assert any(
        "source_payload.security.envelope_encryption" in evidence
        for evidence in by_category["envelope_encryption"].evidence
    )


def test_duplicate_evidence_is_merged_deterministically():
    source = {
        "id": "dupe-encryption",
        "summary": "Customer data must be encrypted at rest. Customer data must be encrypted at rest.",
        "source_payload": {
            "requirements": [
                "Customer data must be encrypted at rest.",
                "customer data must be encrypted at rest.",
            ],
            "metadata": {"encryption": "Customer data must be encrypted at rest."},
        },
    }

    result = build_source_encryption_requirements(source)
    repeat = build_source_encryption_requirements(source)
    at_rest = next(record for record in result.records if record.category == "at_rest_encryption")

    assert result.to_dict() == repeat.to_dict()
    assert at_rest.evidence == tuple(sorted(set(at_rest.evidence), key=str.casefold))
    assert len(at_rest.evidence) == len({item.casefold() for item in at_rest.evidence})
    assert result.summary["requirement_count"] == len(result.records)


def test_confidence_uses_explicit_requirement_language():
    result = build_source_encryption_requirements(
        "TLS protects service traffic. Tokens must be encrypted before launch."
    )

    tls = next(record for record in result.records if record.category == "tls")
    token = next(record for record in result.records if record.category == "token_encryption")

    assert tls.confidence in {"low", "medium"}
    assert token.confidence == "high"


def test_empty_and_malformed_inputs_return_empty_report():
    empty = build_source_encryption_requirements(
        _source_brief(
            title="Dashboard copy",
            summary="Improve onboarding copy.",
            source_payload={"body": "No encryption or key management changes are in scope."},
        )
    )
    malformed = build_source_encryption_requirements(object())

    assert empty.source_id == "sb-encryption"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "at_rest_encryption": 0,
            "in_transit_encryption": 0,
            "tls": 0,
            "kms": 0,
            "customer_managed_keys": 0,
            "key_rotation": 0,
            "secrets_management": 0,
            "token_encryption": 0,
            "envelope_encryption": 0,
            "field_level_encryption": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
    }
    assert "No encryption requirements were found" in empty.to_markdown()
    assert malformed.source_id is None
    assert malformed.requirements == ()


def test_sourcebrief_model_serialization_markdown_and_aliases_do_not_mutate_input():
    source = _source_brief(
        source_id="encryption-model",
        title="Security | encryption",
        summary="Tenant data must use encryption at rest with customer-managed keys.",
        source_payload={
            "requirements": [
                "TLS 1.2 is required for service traffic.",
                "KMS key rotation must be owned by the platform team.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_encryption_requirements(source)
    model_result = generate_source_encryption_requirements(model)
    extracted = extract_source_encryption_requirements(model)
    payload = source_encryption_requirements_to_dict(model_result)
    markdown = source_encryption_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_encryption_requirements_to_dict(mapping_result)
    assert json.loads(json.dumps(payload)) == payload
    assert extracted == model_result
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_encryption_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_encryption_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_encryption_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "scope",
        "missing_inputs",
        "confidence",
        "evidence",
    ]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source Encryption Requirements Report: encryption-model")
    assert "| Source Brief | Category | Scope | Confidence | Missing Inputs | Evidence |" in markdown


def _source_brief(
    *,
    source_id="sb-encryption",
    title="Encryption requirements",
    domain="security",
    summary="General encryption requirements.",
    source_payload=None,
    source_links=None,
    description=None,
):
    payload = {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }
    if description is not None:
        payload["description"] = description
    return payload
