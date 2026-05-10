"""Role-based access control manager with granular permissions.

Provides built-in and custom roles, resource-level permissions,
permission inheritance, auditing, and bulk assignment.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from blueprint.permissions.permission_model import (
    BuiltInRole,
    CustomRole,
    Operation,
    Permission,
    PermissionAuditEntry,
    ResourceHierarchy,
    ResourceType,
    ROLE_DEFAULTS,
    _gen_id,
    _now_iso,
)


class PermissionManager:
    """Manages role-based access control with granular permissions."""

    def __init__(self) -> None:
        self._permissions: list[Permission] = []
        self._custom_roles: dict[str, CustomRole] = {}
        self._hierarchy: dict[tuple[ResourceType, str], ResourceHierarchy] = {}

    # ------------------------------------------------------------------
    # Permission CRUD
    # ------------------------------------------------------------------

    def grant_permission(
        self,
        resource_type: ResourceType,
        resource_id: str,
        user_id: str,
        role: str,
        *,
        operations: frozenset[Operation] | None = None,
        granted_by: str = "",
    ) -> Permission:
        if operations is None:
            operations = self._resolve_role_operations(role)
        perm = Permission(
            permission_id=_gen_id("perm"),
            resource_type=resource_type,
            resource_id=resource_id,
            user_id=user_id,
            role=role,
            operations=operations,
            granted_by=granted_by,
        )
        self._permissions.append(perm)
        return perm

    def revoke_permission(self, resource_type: ResourceType, resource_id: str, user_id: str) -> int:
        before = len(self._permissions)
        self._permissions = [
            p
            for p in self._permissions
            if not (
                p.resource_type == resource_type
                and p.resource_id == resource_id
                and p.user_id == user_id
            )
        ]
        return before - len(self._permissions)

    # ------------------------------------------------------------------
    # Permission checking
    # ------------------------------------------------------------------

    def check_permission(
        self,
        resource_type: ResourceType,
        resource_id: str,
        user_id: str,
        operation: Operation,
    ) -> bool:
        # Direct permissions
        for p in self._permissions:
            if (
                p.resource_type == resource_type
                and p.resource_id == resource_id
                and p.user_id == user_id
                and operation in p.operations
            ):
                return True
        # Inherited from parent
        key = (resource_type, resource_id)
        hier = self._hierarchy.get(key)
        if hier and hier.parent_type and hier.parent_id:
            return self.check_permission(hier.parent_type, hier.parent_id, user_id, operation)
        return False

    # ------------------------------------------------------------------
    # Roles
    # ------------------------------------------------------------------

    def create_custom_role(
        self,
        name: str,
        operations: frozenset[Operation],
        *,
        description: str = "",
    ) -> CustomRole:
        role = CustomRole(
            role_id=_gen_id("role"),
            name=name,
            description=description,
            operations=operations,
        )
        self._custom_roles[role.name] = role
        return role

    def get_custom_role(self, name: str) -> CustomRole | None:
        return self._custom_roles.get(name)

    def list_roles(self) -> list[str]:
        built_in = [r.value for r in BuiltInRole]
        custom = list(self._custom_roles.keys())
        return built_in + custom

    def _resolve_role_operations(self, role: str) -> frozenset[Operation]:
        try:
            built_in = BuiltInRole(role)
            return frozenset(ROLE_DEFAULTS[built_in])
        except ValueError:
            custom = self._custom_roles.get(role)
            if custom:
                return custom.operations
            return frozenset()

    # ------------------------------------------------------------------
    # Hierarchy (permission inheritance)
    # ------------------------------------------------------------------

    def set_hierarchy(
        self,
        resource_type: ResourceType,
        resource_id: str,
        parent_type: ResourceType,
        parent_id: str,
    ) -> None:
        self._hierarchy[(resource_type, resource_id)] = ResourceHierarchy(
            resource_type=resource_type,
            resource_id=resource_id,
            parent_type=parent_type,
            parent_id=parent_id,
        )

    # ------------------------------------------------------------------
    # Auditing
    # ------------------------------------------------------------------

    def audit_permissions(
        self, *, user_id: str | None = None, resource_id: str | None = None
    ) -> list[PermissionAuditEntry]:
        entries: list[PermissionAuditEntry] = []
        for p in self._permissions:
            if user_id and p.user_id != user_id:
                continue
            if resource_id and p.resource_id != resource_id:
                continue
            entries.append(
                PermissionAuditEntry(
                    entry_id=_gen_id("audit"),
                    user_id=p.user_id,
                    resource_type=p.resource_type,
                    resource_id=p.resource_id,
                    role=p.role,
                    operations=p.operations,
                )
            )
        return entries

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    def bulk_grant(
        self,
        resource_type: ResourceType,
        resource_id: str,
        user_ids: list[str],
        role: str,
        *,
        granted_by: str = "",
    ) -> list[Permission]:
        return [
            self.grant_permission(
                resource_type, resource_id, uid, role, granted_by=granted_by
            )
            for uid in user_ids
        ]

    def get_user_permissions(self, user_id: str) -> list[Permission]:
        return [p for p in self._permissions if p.user_id == user_id]

    def get_resource_permissions(
        self, resource_type: ResourceType, resource_id: str
    ) -> list[Permission]:
        return [
            p
            for p in self._permissions
            if p.resource_type == resource_type and p.resource_id == resource_id
        ]


__all__ = ["PermissionManager"]
