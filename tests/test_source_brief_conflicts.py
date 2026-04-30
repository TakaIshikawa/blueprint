from copy import deepcopy

from blueprint.source_brief_conflicts import detect_source_brief_conflicts


def test_source_brief_conflicts_returns_empty_list_for_compatible_briefs():
    result = detect_source_brief_conflicts(
        [
            _source_brief(
                "sb-1",
                source_payload={
                    "scope": ["Build CSV export"],
                    "non_goals": ["Hosted dashboard"],
                    "constraints": ["Must use existing exporters"],
                    "target_repo": "acme/widgets",
                    "priority": "high",
                },
            ),
            _source_brief(
                "sb-2",
                source_payload={
                    "scope": ["Include validation command"],
                    "non_goals": ["Mobile application"],
                    "constraints": ["Use existing exporters"],
                    "target_repo": "https://github.com/acme/widgets.git",
                    "priority": "high",
                },
            ),
        ]
    )

    assert result == []


def test_source_brief_conflicts_detects_scope_versus_non_goal_contradiction():
    result = detect_source_brief_conflicts(
        [
            _source_brief(
                "sb-scope",
                source_payload={"scope": ["Add Slack export"]},
            ),
            _source_brief(
                "sb-non-goal",
                source_payload={"non_goals": ["Do not add Slack export"]},
            ),
        ]
    )

    assert [conflict.to_dict() for conflict in result] == [
        {
            "field": "scope_vs_non_goals",
            "source_ids": ["sb-non-goal", "sb-scope"],
            "values": [
                {
                    "source_id": "sb-non-goal",
                    "role": "non-goal",
                    "value": "Do not add Slack export",
                },
                {
                    "source_id": "sb-scope",
                    "role": "scope",
                    "value": "Add Slack export",
                },
            ],
            "severity": "high",
            "explanation": (
                "A scoped item in one source brief is explicitly excluded by another "
                "source brief."
            ),
        }
    ]


def test_source_brief_conflicts_detects_different_target_repositories():
    result = detect_source_brief_conflicts(
        [
            _source_brief("sb-api", source_payload={"target_repo": "acme/api"}),
            _source_brief("sb-web", source_payload={"target_repo": "acme/web"}),
        ]
    )

    assert [conflict.to_dict() for conflict in result] == [
        {
            "field": "target_repo",
            "source_ids": ["sb-api", "sb-web"],
            "values": [
                {"source_id": "sb-api", "value": "acme/api"},
                {"source_id": "sb-web", "value": "acme/web"},
            ],
            "severity": "high",
            "explanation": "Source briefs point to different target repositories.",
        }
    ]


def test_source_brief_conflicts_detects_constraint_conflicts():
    result = detect_source_brief_conflicts(
        [
            _source_brief(
                "sb-required",
                source_payload={
                    "constraints": [
                        "Must use Postgres",
                        "Must keep CSV output",
                    ],
                },
            ),
            _source_brief(
                "sb-forbidden",
                source_payload={"constraints": ["Must not use Postgres"]},
            ),
        ]
    )

    assert [conflict.to_dict() for conflict in result] == [
        {
            "field": "constraints",
            "source_ids": ["sb-forbidden", "sb-required"],
            "values": [
                {
                    "source_id": "sb-forbidden",
                    "role": "constraint",
                    "value": "Must not use Postgres",
                },
                {
                    "source_id": "sb-required",
                    "role": "constraint",
                    "value": "Must use Postgres",
                },
            ],
            "severity": "medium",
            "explanation": (
                "Constraint language requires and forbids the same item across "
                "source briefs."
            ),
        }
    ]


def test_source_brief_conflicts_detects_priority_like_metadata_conflicts():
    result = detect_source_brief_conflicts(
        [
            _source_brief("sb-low", source_payload={"priority": "low"}),
            _source_brief("sb-high", source_payload={"priority": "high"}),
        ]
    )

    assert [conflict.to_dict() for conflict in result] == [
        {
            "field": "priority",
            "source_ids": ["sb-high", "sb-low"],
            "values": [
                {"source_id": "sb-high", "value": "high"},
                {"source_id": "sb-low", "value": "low"},
            ],
            "severity": "medium",
            "explanation": "Priority-like metadata 'priority' has incompatible values.",
        }
    ]


def test_source_brief_conflicts_are_stable_for_dictionary_ordering():
    briefs = [
        _source_brief(
            "sb-b",
            source_payload={
                "priority": "low",
                "target_repo": "acme/web",
                "scope": ["Build admin dashboard"],
            },
        ),
        _source_brief(
            "sb-a",
            source_payload={
                "non_goals": ["No admin dashboard"],
                "target_repo": "acme/api",
                "priority": "high",
            },
        ),
    ]
    reordered = [
        {
            **brief,
            "source_payload": {
                key: brief["source_payload"][key]
                for key in reversed(list(brief["source_payload"].keys()))
            },
        }
        for brief in deepcopy(briefs)
    ]

    assert [conflict.to_dict() for conflict in detect_source_brief_conflicts(briefs)] == [
        conflict.to_dict() for conflict in detect_source_brief_conflicts(reordered)
    ]


def _source_brief(source_brief_id, *, source_payload=None):
    return {
        "id": source_brief_id,
        "title": f"Source Brief {source_brief_id}",
        "domain": "testing",
        "summary": "Normalize upstream work into a plan-ready brief.",
        "source_project": "manual",
        "source_entity_type": "brief",
        "source_id": source_brief_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }
