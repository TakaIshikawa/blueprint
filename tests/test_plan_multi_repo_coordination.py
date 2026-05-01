import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_multi_repo_coordination import (
    PlanMultiRepoCoordinationMap,
    PlanMultiRepoCoordinationRecord,
    build_plan_multi_repo_coordination_map,
    derive_plan_multi_repo_coordination_map,
    plan_multi_repo_coordination_map_to_dict,
    plan_multi_repo_coordination_map_to_dicts,
    plan_multi_repo_coordination_map_to_markdown,
    summarize_plan_multi_repo_coordination,
)


def test_metadata_repositories_and_components_are_grouped_with_coordination_risks():
    result = build_plan_multi_repo_coordination_map(
        _plan(
            [
                _task(
                    "task-api",
                    title="Update checkout service API contract",
                    description="Publish the API payload contract before rollout.",
                    metadata={
                        "repository": "acme/payments-api",
                        "service": "checkout-service",
                        "owner": "platform",
                    },
                    acceptance_criteria=["Contract tests pass in CI."],
                ),
                _task(
                    "task-web",
                    title="Wire checkout client package",
                    description="Consume the SDK from acme/checkout-web after the API contract lands.",
                    metadata={
                        "repo": "acme/checkout-web",
                        "package": "checkout-client",
                        "owner": "frontend",
                    },
                ),
            ]
        )
    )

    assert isinstance(result, PlanMultiRepoCoordinationMap)
    assert all(isinstance(record, PlanMultiRepoCoordinationRecord) for record in result.records)
    assert result.summary["repository_count"] == 2
    assert result.summary["component_count"] == 2
    assert result.summary["risk_counts"]["contract_boundary"] >= 1
    assert result.summary["risk_counts"]["ownership_split"] == 0
    assert _record_for_repo(result, "acme/payments-api").task_ids == ("task-api",)
    assert _record_for_component(result, "checkout-client").task_ids == ("task-web",)


def test_path_prefixes_infer_components_and_single_repo_plan_is_empty():
    multi_component = build_plan_multi_repo_coordination_map(
        _plan(
            [
                _task(
                    "task-web",
                    title="Update web checkout",
                    files_or_modules=["apps/web/src/checkout.tsx"],
                ),
                _task(
                    "task-worker",
                    title="Update settlement worker",
                    files_or_modules=["services/settlement/worker.py"],
                ),
            ],
            target_repo="acme/monorepo",
        )
    )
    single_repo = build_plan_multi_repo_coordination_map(
        _plan(
            [
                _task("task-a", files_or_modules=["src/a.py"]),
                _task("task-b", files_or_modules=["src/b.py"]),
            ],
            target_repo="acme/single",
        )
    )

    assert [record.components for record in multi_component.records] == [
        ("apps/web",),
        ("services/settlement",),
        (),
    ]
    assert multi_component.summary["boundary_count"] == 3
    assert single_repo.records == ()
    assert single_repo.summary == {
        "task_count": 2,
        "record_count": 0,
        "boundary_count": 0,
        "repository_count": 0,
        "component_count": 0,
        "cross_boundary_dependency_count": 0,
        "risk_counts": {
            "cross_repo_dependency": 0,
            "shared_release_order": 0,
            "contract_boundary": 0,
            "ownership_split": 0,
            "validation_gap": 0,
        },
    }
    assert single_repo.to_markdown() == "\n".join(
        [
            "# Plan Multi-Repo Coordination Map: plan-multi-repo",
            "",
            "No multi-repo or cross-component coordination boundaries detected.",
        ]
    )


def test_cross_boundary_dependencies_are_flagged_with_sequence_guidance():
    result = build_plan_multi_repo_coordination_map(
        _plan(
            [
                _task(
                    "task-schema",
                    title="Add events schema package",
                    description="Update package @acme/events and publish schema contract.",
                    files_or_modules=["packages/events/schema.json"],
                    metadata={"owner": "data"},
                    acceptance_criteria=["Schema contract tests pass."],
                ),
                _task(
                    "task-api",
                    title="Emit checkout event",
                    description="Backend service depends on the package schema.",
                    files_or_modules=["services/api/events.py"],
                    depends_on=["task-schema"],
                    metadata={"owner": "backend"},
                ),
            ],
            target_repo="acme/monorepo",
        )
    )

    dependency = result.records[0]

    assert dependency.coordination_type == "dependency_chain"
    assert dependency.task_ids == ("task-schema", "task-api")
    assert dependency.components == ("packages/events", "services/api")
    assert dependency.risk_codes == (
        "cross_repo_dependency",
        "shared_release_order",
        "contract_boundary",
        "ownership_split",
        "validation_gap",
    )
    assert dependency.recommended_sequence == (
        "Finish task-schema in packages/events before starting task-api in services/api."
    )
    assert "Add or run contract tests at the handoff point." in dependency.recommended_actions
    assert result.summary["cross_boundary_dependency_count"] == 1


def test_serialization_aliases_to_dicts_summary_model_inputs_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-schema",
                title="Add checkout event schema",
                files_or_modules=["packages/events/schema.json"],
                acceptance_criteria=["Run schema validation."],
            ),
            _task(
                "task-worker",
                title="Consume checkout event in worker",
                description="Worker deployment depends on package event schema.",
                files_or_modules=["services/worker/checkout.py"],
                depends_on=["task-schema"],
            ),
        ],
        plan_id="plan-model",
        target_repo="acme/monorepo",
    )

    result = build_plan_multi_repo_coordination_map(ExecutionPlan.model_validate(plan))
    alias_result = derive_plan_multi_repo_coordination_map(plan)
    payload = plan_multi_repo_coordination_map_to_dict(result)

    assert payload == result.to_dict()
    assert plan_multi_repo_coordination_map_to_dicts(result) == result.to_dicts()
    assert summarize_plan_multi_repo_coordination(result) == result.summary
    assert alias_result.to_dict() == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "summary"]
    assert list(payload["records"][0]) == [
        "coordination_id",
        "coordination_type",
        "task_ids",
        "repositories",
        "components",
        "risk_codes",
        "recommended_sequence",
        "recommended_actions",
        "evidence",
    ]
    assert plan_multi_repo_coordination_map_to_markdown(result) == "\n".join(
        [
            "# Plan Multi-Repo Coordination Map: plan-model",
            "",
            "| Coordination | Tasks | Repos | Components | Risks | Sequence | Actions | Evidence |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
            (
                "| dependency-task-schema-to-task-worker | task-schema, task-worker | acme/monorepo | "
                "packages/events, services/worker | cross_repo_dependency, shared_release_order, "
                "contract_boundary, validation_gap | Finish task-schema in packages/events before "
                "starting task-worker in services/worker. | Serialize dependent work across the boundary "
                "instead of launching both branches blindly.; Publish the prerequisite branch, artifact, "
                "or contract before starting the dependent task.; Add or run contract tests at the handoff "
                "point.; Define validation ownership for both sides of the dependency. | depends_on: "
                "task-worker -> task-schema; files_or_modules: packages/events/schema.json; "
                "target_repo: acme/monorepo; files_or_modules: services/worker/checkout.py |"
            ),
            (
                "| boundary-1 | task-schema | acme/monorepo | packages/events | contract_boundary | "
                "Keep packages/events changes on an owned branch or PR; merge after declared prerequisites "
                "and before dependent boundaries. | Assign one coordinator for component packages/events.; "
                "Confirm branch ownership and merge window before dispatching parallel agents.; Freeze the "
                "interface contract or schema before dependent implementation starts. | files_or_modules: "
                "packages/events/schema.json; target_repo: acme/monorepo |"
            ),
            (
                "| boundary-2 | task-worker | acme/monorepo | services/worker | contract_boundary, "
                "validation_gap | Keep services/worker changes on an owned branch "
                "or PR; merge after declared prerequisites and before dependent boundaries. | Assign one "
                "coordinator for component services/worker.; Confirm branch ownership and merge window "
                "before dispatching parallel agents.; Freeze the interface contract or schema before "
                "dependent implementation starts.; Add explicit validation evidence for this boundary "
                "before release. | files_or_modules: services/worker/checkout.py; target_repo: "
                "acme/monorepo |"
            ),
            (
                "| boundary-3 | task-schema, task-worker | acme/monorepo |  | contract_boundary, "
                "validation_gap | Keep acme/monorepo changes on an owned branch or PR; merge after "
                "declared prerequisites and before dependent boundaries. | Assign one coordinator for "
                "repository acme/monorepo.; Confirm branch ownership and merge window before dispatching "
                "parallel agents.; Freeze the interface contract or schema before dependent implementation "
                "starts.; Add explicit validation evidence for this boundary before release. | "
                "files_or_modules: packages/events/schema.json; target_repo: acme/monorepo; "
                "files_or_modules: services/worker/checkout.py |"
            ),
        ]
    )


def _record_for_repo(result, repo):
    return next(record for record in result.records if repo in record.repositories)


def _record_for_component(result, component):
    return next(record for record in result.records if component in record.components)


def _plan(tasks, *, plan_id="plan-multi-repo", target_repo=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-multi-repo",
        "target_engine": "codex",
        "target_repo": target_repo,
        "project_type": "application",
        "milestones": [],
        "test_strategy": None,
        "handoff_prompt": None,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
        "metadata": {},
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    depends_on=None,
    acceptance_criteria=None,
    metadata=None,
):
    return {
        "id": task_id,
        "execution_plan_id": None,
        "title": title or task_id.replace("-", " ").title(),
        "description": description or "Implement the planned work.",
        "milestone": None,
        "owner_type": None,
        "suggested_engine": None,
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": (
            ["Implementation is complete."] if acceptance_criteria is None else acceptance_criteria
        ),
        "estimated_complexity": None,
        "estimated_hours": None,
        "risk_level": None,
        "test_command": None,
        "status": "pending",
        "metadata": {} if metadata is None else metadata,
        "blocked_reason": None,
        "created_at": None,
        "updated_at": None,
    }
