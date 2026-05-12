from blueprint.workspace.team_workspace import TeamWorkspace


def test_instantiate_template_returns_plan_scaffold_with_workspace_and_template_data():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Product")
    updated = manager.add_template(
        workspace.workspace_id,
        "Launch plan",
        template_data={
            "title": "Launch",
            "status": "draft",
            "tasks": [{"id": "task-template", "title": "Prepare"}],
        },
    )
    template = updated.templates[0]

    scaffold = manager.instantiate_template(
        workspace.workspace_id,
        template.template_id,
        plan_id="plan-launch",
    )

    assert scaffold == {
        "id": "plan-launch",
        "workspace_id": workspace.workspace_id,
        "template_id": template.template_id,
        "template_name": "Launch plan",
        "title": "Launch",
        "status": "draft",
        "tasks": [{"id": "task-template", "title": "Prepare"}],
    }


def test_instantiate_template_overrides_take_precedence_over_defaults():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Product")
    updated = manager.add_template(
        workspace.workspace_id,
        "Operational plan",
        template_data={"title": "Default", "status": "draft", "priority": "medium"},
    )
    template = updated.templates[0]

    scaffold = manager.instantiate_template(
        workspace.workspace_id,
        template.template_id,
        overrides={"title": "Custom", "priority": "high"},
        plan_id="plan-custom",
    )

    assert scaffold["title"] == "Custom"
    assert scaffold["priority"] == "high"
    assert scaffold["status"] == "draft"


def test_instantiate_template_can_add_generated_plan_id_to_workspace():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Product")
    updated = manager.add_template(
        workspace.workspace_id,
        "Roadmap",
        template_data={"title": "Roadmap"},
    )
    template = updated.templates[0]

    scaffold = manager.instantiate_template(
        workspace.workspace_id,
        template.template_id,
        plan_id="plan-roadmap",
        add_to_workspace=True,
    )
    workspace_after = manager.get_workspace(workspace.workspace_id)

    assert scaffold["id"] == "plan-roadmap"
    assert workspace_after.plan_ids == ["plan-roadmap"]


def test_instantiate_template_returns_none_for_unknown_template():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Product")

    assert manager.instantiate_template(workspace.workspace_id, "missing") is None
