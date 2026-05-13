from blueprint.domain.models import SourceBrief
from blueprint.source_release_train_requirements import (
    SourceReleaseTrainRequirementsReport,
    build_source_release_train_requirements,
    extract_source_release_train_requirements,
    source_release_train_requirements_to_dict,
    source_release_train_requirements_to_dicts,
    source_release_train_requirements_to_markdown,
)


def test_scheduled_release_train_brief_extracts_ordered_records():
    report = build_source_release_train_requirements(
        _source(
            [
                "Release train cadence must be weekly on the release calendar with release owner.",
                "Code freeze window starts Monday and freeze ends Wednesday with hotfix exception approval.",
                "Branch cutoff must happen by 12:00 UTC on the release branch with captain approval.",
                "Environment promotion must move dev to staging to production after smoke validation by QA.",
                "Release captain ownership requires a backup and Slack handoff channel.",
                "Rollback window is 24 hours with rollback trigger on high error rate and owner.",
                "Stakeholder sign-off requires product and support approval in Jira before EOD.",
            ]
        )
    )

    assert isinstance(report, SourceReleaseTrainRequirementsReport)
    assert [record.requirement_type for record in report.records] == [
        "train_cadence",
        "freeze_window",
        "branch_cutoff",
        "environment_promotion",
        "release_captain_ownership",
        "rollback_window",
        "stakeholder_signoff",
    ]
    assert all(record.evidence for record in report.records)
    assert report.summary["requirement_count"] == 7


def test_partial_release_train_brief_has_missing_details():
    report = build_source_release_train_requirements(_source(["Release train is needed.", "Rollback policy is required."]))

    by_type = {record.requirement_type: record for record in report.records}
    assert by_type["train_cadence"].missing_details == ("cadence", "calendar", "owner")
    assert by_type["rollback_window"].missing_details == ("rollback_window", "rollback_trigger", "owner")
    assert by_type["rollback_window"].missing_detail_guidance == "rollback_window; rollback_trigger; owner"


def test_negated_and_serialization_paths_are_stable():
    source = _source(["No release train, release coordination, code freeze, branch cutoff, or sign-off work is required."])
    empty = build_source_release_train_requirements(source)
    model = SourceBrief.model_validate(_source(["Release captain must own sign-off approval."]))
    report = extract_source_release_train_requirements(model)
    payload = source_release_train_requirements_to_dict(report)

    assert empty.records == ()
    assert source_release_train_requirements_to_dicts(report) == payload["requirements"]
    assert source_release_train_requirements_to_markdown(report) == report.to_markdown()
    assert payload["requirements"][0]["requirement_type"] == "release_captain_ownership"


def _source(requirements):
    return {
        "id": "sb-release-train",
        "title": "Release train",
        "domain": "release",
        "summary": "Release train planning.",
        "source_project": "project",
        "source_entity_type": "issue",
        "source_id": "issue-release",
        "source_payload": {"requirements": requirements},
        "source_links": {},
    }
