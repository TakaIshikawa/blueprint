"""Tests for permission manager covering permission checks and inheritance."""

import pytest

from blueprint.permissions.permission_manager import PermissionManager
from blueprint.permissions.permission_model import (
    BuiltInRole,
    Operation,
    ResourceType,
)


@pytest.fixture
def pm() -> PermissionManager:
    return PermissionManager()


class TestPermissionGrant:
    def test_grant_with_built_in_role(self, pm: PermissionManager):
        perm = pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "editor")
        assert perm.role == "editor"
        assert Operation.READ in perm.operations
        assert Operation.UPDATE in perm.operations
        assert Operation.DELETE not in perm.operations

    def test_grant_admin_role(self, pm: PermissionManager):
        perm = pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "admin")
        assert Operation.DELETE in perm.operations

    def test_grant_with_custom_operations(self, pm: PermissionManager):
        ops = frozenset({Operation.READ, Operation.CREATE})
        perm = pm.grant_permission(
            ResourceType.TASK, "task-1", "user-2", "custom", operations=ops
        )
        assert perm.operations == ops


class TestPermissionCheck:
    def test_check_allowed(self, pm: PermissionManager):
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "editor")
        assert pm.check_permission(ResourceType.PLAN, "plan-1", "user-1", Operation.READ) is True
        assert pm.check_permission(ResourceType.PLAN, "plan-1", "user-1", Operation.UPDATE) is True

    def test_check_denied(self, pm: PermissionManager):
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "viewer")
        assert pm.check_permission(ResourceType.PLAN, "plan-1", "user-1", Operation.DELETE) is False

    def test_check_no_permission(self, pm: PermissionManager):
        assert pm.check_permission(ResourceType.PLAN, "plan-1", "user-1", Operation.READ) is False


class TestPermissionInheritance:
    def test_inherit_from_parent(self, pm: PermissionManager):
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "editor")
        pm.set_hierarchy(ResourceType.TASK, "task-1", ResourceType.PLAN, "plan-1")
        assert pm.check_permission(ResourceType.TASK, "task-1", "user-1", Operation.READ) is True

    def test_no_inheritance_without_hierarchy(self, pm: PermissionManager):
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "editor")
        assert pm.check_permission(ResourceType.TASK, "task-1", "user-1", Operation.READ) is False


class TestCustomRoles:
    def test_create_custom_role(self, pm: PermissionManager):
        role = pm.create_custom_role("reviewer", frozenset({Operation.READ, Operation.CREATE}))
        assert role.name == "reviewer"
        retrieved = pm.get_custom_role("reviewer")
        assert retrieved is not None

    def test_grant_with_custom_role(self, pm: PermissionManager):
        pm.create_custom_role("reviewer", frozenset({Operation.READ}))
        perm = pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "reviewer")
        assert Operation.READ in perm.operations
        assert Operation.DELETE not in perm.operations

    def test_list_roles(self, pm: PermissionManager):
        pm.create_custom_role("reviewer", frozenset({Operation.READ}))
        roles = pm.list_roles()
        assert "viewer" in roles
        assert "admin" in roles
        assert "reviewer" in roles


class TestRevocation:
    def test_revoke_permission(self, pm: PermissionManager):
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "editor")
        count = pm.revoke_permission(ResourceType.PLAN, "plan-1", "user-1")
        assert count == 1
        assert pm.check_permission(ResourceType.PLAN, "plan-1", "user-1", Operation.READ) is False

    def test_revoke_nonexistent(self, pm: PermissionManager):
        assert pm.revoke_permission(ResourceType.PLAN, "plan-1", "user-1") == 0


class TestBulkAndAudit:
    def test_bulk_grant(self, pm: PermissionManager):
        perms = pm.bulk_grant(
            ResourceType.PLAN, "plan-1", ["u1", "u2", "u3"], "viewer"
        )
        assert len(perms) == 3
        assert all(Operation.READ in p.operations for p in perms)

    def test_audit_by_user(self, pm: PermissionManager):
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "editor")
        pm.grant_permission(ResourceType.TASK, "task-1", "user-1", "viewer")
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-2", "viewer")
        entries = pm.audit_permissions(user_id="user-1")
        assert len(entries) == 2

    def test_audit_by_resource(self, pm: PermissionManager):
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "editor")
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-2", "viewer")
        entries = pm.audit_permissions(resource_id="plan-1")
        assert len(entries) == 2

    def test_get_user_permissions(self, pm: PermissionManager):
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "editor")
        pm.grant_permission(ResourceType.TASK, "task-1", "user-1", "viewer")
        perms = pm.get_user_permissions("user-1")
        assert len(perms) == 2

    def test_get_resource_permissions(self, pm: PermissionManager):
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-1", "editor")
        pm.grant_permission(ResourceType.PLAN, "plan-1", "user-2", "admin")
        perms = pm.get_resource_permissions(ResourceType.PLAN, "plan-1")
        assert len(perms) == 2
