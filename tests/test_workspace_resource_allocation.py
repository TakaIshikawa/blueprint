from blueprint.workspace import ResourceAllocation, TeamWorkspace


def _resource_workspace() -> tuple[TeamWorkspace, str, str]:
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    updated = manager.add_resource(
        workspace.workspace_id,
        "GPU Cluster",
        "compute",
        total_capacity=10.0,
    )
    assert updated is not None
    return manager, workspace.workspace_id, updated.resources[0].resource_id


def test_allocate_resource_records_ledger_and_updates_resource():
    manager, workspace_id, resource_id = _resource_workspace()

    allocation = manager.allocate_resource(
        workspace_id,
        resource_id,
        "plan-1",
        4.0,
        reason="training",
        created_by="owner",
    )

    assert isinstance(allocation, ResourceAllocation)
    assert allocation.amount == 4.0
    assert manager.get_resources(workspace_id)[0].allocated == 4.0
    assert manager.list_resource_allocations(workspace_id) == [allocation]
    assert manager.list_resource_allocations(workspace_id, resource_id=resource_id) == [allocation]


def test_release_resource_reduces_allocation_and_removes_ledger_entry():
    manager, workspace_id, resource_id = _resource_workspace()
    allocation = manager.allocate_resource(workspace_id, resource_id, "plan-1", 4.0)

    assert allocation is not None
    assert manager.release_resource(workspace_id, allocation.allocation_id) is True

    assert manager.get_resources(workspace_id)[0].allocated == 0.0
    assert manager.list_resource_allocations(workspace_id) == []


def test_over_allocation_is_rejected_without_mutating_resource():
    manager, workspace_id, resource_id = _resource_workspace()

    allocation = manager.allocate_resource(workspace_id, resource_id, "plan-1", 11.0)

    assert allocation is None
    assert manager.get_resources(workspace_id)[0].allocated == 0.0


def test_missing_resource_allocation_returns_none():
    manager, workspace_id, _resource_id = _resource_workspace()

    assert manager.allocate_resource(workspace_id, "missing", "plan-1", 1.0) is None
    assert manager.release_resource(workspace_id, "missing") is False

