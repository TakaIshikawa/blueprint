from blueprint.workspace import TeamWorkspace


def test_search_workspaces_matches_name_and_description_case_insensitively():
    manager = TeamWorkspace()
    first = manager.create_workspace("Platform", description="Runtime services")
    second = manager.create_workspace("Design", description="Interface Systems")

    assert manager.search_workspaces(query="platform") == [first]
    assert manager.search_workspaces(query="systems") == [second]


def test_search_workspaces_filters_by_owner():
    manager = TeamWorkspace()
    first = manager.create_workspace("A", owner_id="alice")
    manager.create_workspace("B", owner_id="bob")

    assert manager.search_workspaces(owner_id="alice") == [first]


def test_search_workspaces_filters_by_metadata():
    manager = TeamWorkspace()
    first = manager.create_workspace("A", metadata={"tier": "gold", "region": "us"})
    manager.create_workspace("B", metadata={"tier": "silver", "region": "us"})

    assert manager.search_workspaces(metadata={"tier": "gold"}) == [first]


def test_search_workspaces_filters_by_member_user_id():
    manager = TeamWorkspace()
    first = manager.create_workspace("A")
    manager.create_workspace("B")
    manager.add_member(first.workspace_id, "user-1", "User")

    assert manager.search_workspaces(member_user_id="user-1") == [manager.get_workspace(first.workspace_id)]


def test_search_workspaces_combines_filters_and_preserves_creation_order():
    manager = TeamWorkspace()
    first = manager.create_workspace("Alpha Platform", owner_id="alice", metadata={"tier": "gold"})
    manager.create_workspace("Beta Platform", owner_id="bob", metadata={"tier": "gold"})
    third = manager.create_workspace("Gamma Platform", owner_id="alice", metadata={"tier": "gold"})
    manager.add_member(first.workspace_id, "user-1", "User")
    manager.add_member(third.workspace_id, "user-1", "User")

    assert manager.search_workspaces(
        query="platform",
        owner_id="alice",
        metadata={"tier": "gold"},
        member_user_id="user-1",
    ) == [
        manager.get_workspace(first.workspace_id),
        manager.get_workspace(third.workspace_id),
    ]


def test_search_workspaces_returns_empty_list_for_no_matches():
    manager = TeamWorkspace()
    manager.create_workspace("Platform")

    assert manager.search_workspaces(query="missing") == []
