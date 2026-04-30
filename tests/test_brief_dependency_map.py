import json

from blueprint.brief_dependency_map import (
    BriefDependencyEntry,
    BriefDependencyMap,
    brief_dependency_map_to_dict,
    build_brief_dependency_map,
)
from blueprint.domain.models import ImplementationBrief


def test_dependency_map_builds_integration_and_data_entries_with_coverage():
    dependency_map = build_brief_dependency_map(_brief())

    assert isinstance(dependency_map, BriefDependencyMap)
    assert dependency_map.to_dict() == {
        "brief_id": "ib-dependencies",
        "dependencies": [
            {
                "id": "integration-github-api",
                "name": "GitHub API",
                "category": "integration",
                "evidence": [
                    "integration_points: GitHub API",
                    "assumptions: GitHub API tokens are available in CI.",
                    "architecture_notes: The GitHub API client should wrap retries.",
                    (
                        "validation_plan: Run focused tests for GitHub API and customer "
                        "records handling. Manually inspect Slack webhook delivery."
                    ),
                ],
                "risk_hints": ["GitHub API rate limits may block validation."],
                "validation_covered": True,
            },
            {
                "id": "integration-slack-webhook",
                "name": "Slack webhook",
                "category": "integration",
                "evidence": [
                    "integration_points: Slack webhook",
                    (
                        "validation_plan: Run focused tests for GitHub API and customer "
                        "records handling. Manually inspect Slack webhook delivery."
                    ),
                ],
                "risk_hints": [],
                "validation_covered": True,
            },
            {
                "id": "data-customer-records",
                "name": "customer records",
                "category": "data",
                "evidence": [
                    "data_requirements: customer records",
                    "assumptions: Customer records include account identifiers.",
                    (
                        "validation_plan: Run focused tests for GitHub API and customer "
                        "records handling. Manually inspect Slack webhook delivery."
                    ),
                ],
                "risk_hints": [
                    "Customer records may include incomplete account identifiers."
                ],
                "validation_covered": True,
            },
            {
                "id": "data-account-identifiers",
                "name": "account identifiers",
                "category": "data",
                "evidence": [
                    "data_requirements: account identifiers",
                    "assumptions: Customer records include account identifiers.",
                ],
                "risk_hints": [
                    "Customer records may include incomplete account identifiers."
                ],
                "validation_covered": False,
            },
        ],
        "uncovered_dependency_ids": ["data-account-identifiers"],
    }


def test_accepts_implementation_brief_models_and_serializes_stably():
    brief_model = ImplementationBrief.model_validate(_brief())

    dependency_map = build_brief_dependency_map(brief_model)
    payload = brief_dependency_map_to_dict(dependency_map)

    assert payload == dependency_map.to_dict()
    assert list(payload) == [
        "brief_id",
        "dependencies",
        "uncovered_dependency_ids",
    ]
    assert isinstance(dependency_map.dependencies[0], BriefDependencyEntry)
    assert json.loads(json.dumps(payload)) == payload


def test_partial_mapping_input_uses_available_dependency_fields():
    dependency_map = build_brief_dependency_map(
        {
            "id": "ib-partial",
            "integration_points": ["Local repository metadata"],
            "data_requirements": "Plan rows\nTask rows",
            "assumptions": [
                "Local repository metadata is readable before planning.",
                "Plan rows are normalized.",
            ],
            "risks": ["Task rows may be missing owners."],
            "validation_plan": "Run tests for local repository metadata.",
            "unexpected": "forces validation fallback",
        }
    )

    assert dependency_map.to_dict() == {
        "brief_id": "ib-partial",
        "dependencies": [
            {
                "id": "integration-local-repository-metadata",
                "name": "Local repository metadata",
                "category": "integration",
                "evidence": [
                    "integration_points: Local repository metadata",
                    "assumptions: Local repository metadata is readable before planning.",
                    "validation_plan: Run tests for local repository metadata.",
                ],
                "risk_hints": [],
                "validation_covered": True,
            },
            {
                "id": "data-plan-rows",
                "name": "Plan rows",
                "category": "data",
                "evidence": [
                    "data_requirements: Plan rows",
                    "assumptions: Plan rows are normalized.",
                ],
                "risk_hints": [],
                "validation_covered": False,
            },
            {
                "id": "data-task-rows",
                "name": "Task rows",
                "category": "data",
                "evidence": ["data_requirements: Task rows"],
                "risk_hints": ["Task rows may be missing owners."],
                "validation_covered": False,
            },
        ],
        "uncovered_dependency_ids": ["data-plan-rows", "data-task-rows"],
    }


def test_duplicate_dependency_names_get_stable_ids_without_deduping():
    dependency_map = build_brief_dependency_map(
        {
            "id": "ib-duplicates",
            "integration_points": ["Warehouse API", "Warehouse API"],
            "data_requirements": [],
            "validation_plan": "Run Warehouse API smoke tests.",
        }
    )

    assert [dependency.id for dependency in dependency_map.dependencies] == [
        "integration-warehouse-api",
        "integration-warehouse-api-2",
    ]


def _brief():
    return {
        "id": "ib-dependencies",
        "source_brief_id": "sb-dependencies",
        "title": "Dependency Map",
        "domain": "planning",
        "target_user": "Planning agents",
        "buyer": "Engineering leadership",
        "workflow_context": "Agents split work before implementation.",
        "problem_statement": "Dependency ownership is unclear before planning.",
        "mvp_goal": "Expose a concise dependency inventory.",
        "product_surface": "Planning workspace",
        "scope": ["Build dependency map helper"],
        "non_goals": [],
        "assumptions": [
            "GitHub API tokens are available in CI.",
            "Customer records include account identifiers.",
        ],
        "architecture_notes": "The GitHub API client should wrap retries.",
        "data_requirements": "customer records; account identifiers",
        "integration_points": ["GitHub API", "Slack webhook"],
        "risks": [
            "GitHub API rate limits may block validation.",
            "Customer records may include incomplete account identifiers.",
        ],
        "validation_plan": (
            "Run focused tests for GitHub API and customer records handling. "
            "Manually inspect Slack webhook delivery."
        ),
        "definition_of_done": ["Dependency map is deterministic"],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
