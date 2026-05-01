import copy
import json

from blueprint.brief_requirement_contradictions import (
    BriefRequirementContradictionReport,
    brief_requirement_contradictions_to_dict,
    detect_brief_requirement_contradictions,
    find_brief_requirement_contradictions,
)
from blueprint.domain.models import ImplementationBrief


def test_direct_negation_across_goal_and_exclusion_is_high_severity():
    result = detect_brief_requirement_contradictions(
        _brief(
            mvp_goal="Support CSV export for invoice reports.",
            non_goals=["Do not support CSV export."],
        )
    )

    finding = result.findings[0]

    assert finding.conflict_type == "direct_negation"
    assert finding.field_names == ("mvp_goal", "non_goals[0]")
    assert finding.matched_text == (
        "Support CSV export for invoice reports.",
        "Do not support CSV export.",
    )
    assert finding.matched_terms == ("csv", "export")
    assert finding.severity == "high"
    assert finding.clarification_question == (
        "Should `Support CSV export for invoice reports` be included or excluded from this brief?"
    )
    assert result.summary["high_severity_count"] == 1


def test_deadline_mismatch_reports_conflicting_fields_and_dates():
    result = detect_brief_requirement_contradictions(
        _brief(
            constraints=["Ship no later than 2026-05-10."],
            assumptions=["Vendor sandbox will not be available before 2026-05-20."],
        )
    )

    finding = result.findings[0]

    assert finding.conflict_type == "deadline_mismatch"
    assert finding.field_names == ("constraints[0]", "assumptions[0]")
    assert finding.matched_terms == ("2026-05-10", "2026-05-20")
    assert finding.severity == "high"
    assert "Which deadline should govern" in finding.clarification_question


def test_platform_mismatch_between_requirement_fields_is_high_severity():
    result = detect_brief_requirement_contradictions(
        _brief(
            scope=["Build a web-only admin dashboard."],
            definition_of_done=["Native mobile app supports the full approval workflow."],
        )
    )

    finding = result.findings[0]

    assert finding.conflict_type == "platform_mismatch"
    assert finding.field_names == ("scope[0]", "definition_of_done[0]")
    assert finding.matched_terms == ("mobile", "web")
    assert finding.severity == "high"
    assert finding.clarification_question == (
        "Which platform requirement is authoritative: web or mobile?"
    )


def test_benign_overlap_does_not_create_high_severity_finding():
    result = detect_brief_requirement_contradictions(
        _brief(
            mvp_goal="Improve reporting dashboard filters.",
            scope=["Update dashboard filter copy and empty states."],
            assumptions=["Existing dashboard permissions remain unchanged."],
            definition_of_done=["Dashboard filter tests pass."],
        )
    )

    assert result.findings == ()
    assert result.summary == {
        "requirement_count": 5,
        "finding_count": 0,
        "high_severity_count": 0,
        "medium_severity_count": 0,
        "low_severity_count": 0,
    }


def test_model_input_alias_and_serialization_are_stable_without_mutation():
    brief = _brief(
        mvp_goal="Support Slack notifications for approvals.",
        non_goals=["Exclude Slack notifications."],
    )
    original = copy.deepcopy(brief)

    result = find_brief_requirement_contradictions(ImplementationBrief.model_validate(brief))
    payload = brief_requirement_contradictions_to_dict(result)

    assert brief == original
    assert isinstance(result, BriefRequirementContradictionReport)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["findings"]
    assert list(payload) == ["brief_id", "findings", "summary"]
    assert list(payload["findings"][0]) == [
        "conflict_type",
        "field_names",
        "matched_text",
        "matched_terms",
        "severity",
        "clarification_question",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_source_brief_normalized_requirements_are_checked():
    result = detect_brief_requirement_contradictions(
        {
            "id": "source-brief",
            "title": "Source brief",
            "summary": "Planning summary",
            "source_project": "Project",
            "source_entity_type": "issue",
            "source_id": "ISSUE-1",
            "source_payload": {
                "normalized": {
                    "goals": ["Support iOS checkout."],
                    "constraints": ["Web-only checkout for this release."],
                }
            },
            "source_links": {},
        }
    )

    assert result.brief_id == "source-brief"
    assert result.findings[0].conflict_type == "platform_mismatch"


def _brief(
    *,
    mvp_goal="Improve approval workflow.",
    scope=None,
    non_goals=None,
    assumptions=None,
    constraints=None,
    exclusions=None,
    definition_of_done=None,
):
    payload = {
        "id": "brief-contradictions",
        "source_brief_id": "source-contradictions",
        "title": "Requirement contradictions",
        "problem_statement": "Planning fails when requirements disagree.",
        "mvp_goal": mvp_goal,
        "scope": ["Improve approval workflow"] if scope is None else scope,
        "non_goals": [] if non_goals is None else non_goals,
        "assumptions": [] if assumptions is None else assumptions,
        "risks": [],
        "validation_plan": "Run focused pytest coverage.",
        "definition_of_done": (
            ["Contradiction findings are deterministic"]
            if definition_of_done is None
            else definition_of_done
        ),
    }
    if constraints is not None:
        payload["constraints"] = constraints
    if exclusions is not None:
        payload["exclusions"] = exclusions
    return payload
