import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import SourceBrief
from blueprint.source_release_freeze_requirements import (
    SourceReleaseFreezeRequirement,
    SourceReleaseFreezeRequirementsReport,
    build_source_release_freeze_requirements,
    derive_source_release_freeze_requirements,
    extract_source_release_freeze_requirements,
    generate_source_release_freeze_requirements,
    source_release_freeze_requirements_to_dict,
    source_release_freeze_requirements_to_dicts,
    summarize_source_release_freeze_requirements,
)


def test_complete_release_freeze_text_extracts_all_requirement_signals():
    report = build_source_release_freeze_requirements(
        _source_brief(
            source_payload={
                "release_freeze": {
                    "window": "Release freeze window runs from December 15 to January 2.",
                    "environments": "Production, staging, and UAT environments are affected by the freeze.",
                    "restrictions": "Deployments and schema migrations must not ship during the change freeze.",
                    "exceptions": "Exception approval requires CAB approval and the release manager sign-off.",
                    "comms": "Notify stakeholders and support through the release calendar announcement.",
                    "rollback": "Rollback-only changes are allowed during freeze with approved hotfix tracking.",
                    "owner": "Owner is release operations; escalate in PagerDuty to the release manager.",
                }
            }
        )
    )

    by_signal = {requirement.signal: requirement for requirement in report.records}

    assert isinstance(report, SourceReleaseFreezeRequirementsReport)
    assert all(isinstance(record, SourceReleaseFreezeRequirement) for record in report.records)
    assert list(by_signal) == [
        "freeze_window",
        "exception_approval",
        "affected_environments",
        "deployment_restrictions",
        "communication_requirements",
        "rollback_only_permissions",
        "owner_escalation",
    ]
    assert by_signal["freeze_window"].value == "from December 15 to January 2"
    assert by_signal["affected_environments"].value == "Production, staging, and UAT"
    assert (
        "source_payload.release_freeze.exceptions: Exception approval requires CAB approval and the release manager sign-off"
        in by_signal["exception_approval"].evidence
    )
    assert report.missing_signals == ()
    assert report.weak_signals == ()
    assert report.summary["signal_counts"] == {
        "freeze_window": 1,
        "exception_approval": 1,
        "affected_environments": 1,
        "deployment_restrictions": 1,
        "communication_requirements": 1,
        "rollback_only_permissions": 1,
        "owner_escalation": 1,
    }


def test_partial_freeze_text_reports_missing_and_weak_signals():
    report = build_source_release_freeze_requirements(
        "We have a release freeze. Avoid releases unless approved."
    )

    assert [requirement.signal for requirement in report.records] == ["exception_approval"]
    assert "freeze_window" in report.missing_signals
    assert "affected_environments" in report.missing_signals
    assert "rollback_only_permissions" in report.missing_signals
    assert report.weak_signals
    assert "clarify concrete release freeze rule" in report.weak_signals[0]


def test_model_object_serialization_helpers_are_deterministic_without_mutation():
    source = _source_brief(
        summary="Release freeze requires CAB exceptions for production.",
        source_payload={
            "freeze": [
                "Change freeze runs from 2026-12-20 to 2027-01-03 for production.",
                "Rollback only fixes may deploy with release manager approval.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    obj = SimpleNamespace(
        id="object-freeze",
        summary="Code freeze is vague.",
        source_payload={"owner": "Release freeze owner is launch ops and escalation is PagerDuty."},
    )

    mapping_report = build_source_release_freeze_requirements(source)
    model_report = derive_source_release_freeze_requirements(model)
    generated = generate_source_release_freeze_requirements(model)
    object_report = build_source_release_freeze_requirements(obj)
    payload = source_release_freeze_requirements_to_dict(model_report)

    assert source == original
    assert mapping_report.to_dict() == model_report.to_dict()
    assert generated.to_dict() == model_report.to_dict()
    assert extract_source_release_freeze_requirements(model) == model_report.requirements
    assert source_release_freeze_requirements_to_dicts(model_report) == payload["requirements"]
    assert source_release_freeze_requirements_to_dicts(model_report.records) == payload["records"]
    assert summarize_source_release_freeze_requirements(model_report) == model_report.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "source_id",
        "requirements",
        "records",
        "missing_signals",
        "weak_signals",
        "summary",
    ]
    assert object_report.source_id == "object-freeze"
    assert [record.signal for record in object_report.records] == ["owner_escalation"]


def test_absent_and_negated_freeze_language_returns_stable_empty_report():
    empty = build_source_release_freeze_requirements({"summary": "Update onboarding copy only."})
    negated = build_source_release_freeze_requirements(
        {"summary": "No release freeze or deployment freeze changes are in scope."}
    )

    assert empty.records == ()
    assert negated.records == ()
    assert empty.missing_signals == (
        "freeze_window",
        "exception_approval",
        "affected_environments",
        "deployment_restrictions",
        "communication_requirements",
        "rollback_only_permissions",
        "owner_escalation",
    )
    assert empty.summary["requirement_count"] == 0
    assert source_release_freeze_requirements_to_dict(empty)["requirements"] == []


def _source_brief(**overrides):
    payload = {
        "id": "source-release-freeze",
        "source_project": "blueprint",
        "source_entity_type": "brief",
        "source_id": "source-release-freeze",
        "source_links": {},
        "title": "Release freeze source",
        "summary": "Release freeze constraints.",
        "source_payload": {},
    }
    payload.update(overrides)
    return payload
