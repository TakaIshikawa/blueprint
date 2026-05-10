"""Version management system with semantic versioning for plans."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal

from blueprint.versioning.plan_version import PlanVersion


class VersionManager:
    """Manages plan versions with semantic versioning and branching support."""

    def __init__(self) -> None:
        """Initialize version manager with empty version store."""
        self._versions: dict[str, list[PlanVersion]] = {}  # plan_id -> versions
        self._version_index: dict[str, PlanVersion] = {}  # version_id -> version
        self._next_version_id = 1

    def create_version(
        self,
        plan_id: str,
        snapshot_data: dict[str, Any],
        author: str,
        description: str = "",
        version_type: Literal["major", "minor", "patch"] = "patch",
        parent_version_id: str | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PlanVersion:
        """Create a new version of a plan.

        Args:
            plan_id: ID of the plan
            snapshot_data: Complete plan data snapshot
            author: User creating the version
            description: Version description
            version_type: Type of version increment (major, minor, or patch)
            parent_version_id: Parent version for branching (defaults to latest)
            tags: Optional tags for this version
            metadata: Additional metadata

        Returns:
            Created PlanVersion
        """
        # Determine version number and parent
        parent_id = None
        if plan_id not in self._versions or not self._versions[plan_id]:
            # First version
            version_number = "1.0.0"
        else:
            # Find parent version
            if parent_version_id:
                parent = self._version_index.get(parent_version_id)
                if not parent:
                    raise ValueError(f"Parent version not found: {parent_version_id}")
                parent_id = parent_version_id
            else:
                # Use latest version as parent
                parent = self._versions[plan_id][-1]
                parent_id = parent.id

            # Increment version
            version_number = self._increment_version(parent.version_number, version_type)

        # Create version
        version_id = f"ver_{self._next_version_id:08d}"
        self._next_version_id += 1

        version = PlanVersion(
            id=version_id,
            plan_id=plan_id,
            version_number=version_number,
            created_at=datetime.now(timezone.utc),
            author=author,
            description=description,
            snapshot_data=snapshot_data,
            parent_version=parent_id,
            tags=tags or [],
            metadata=metadata or {},
        )

        # Store version
        if plan_id not in self._versions:
            self._versions[plan_id] = []
        self._versions[plan_id].append(version)
        self._version_index[version_id] = version

        return version

    def _increment_version(
        self, current: str, version_type: Literal["major", "minor", "patch"]
    ) -> str:
        """Increment a semantic version number.

        Args:
            current: Current version string (e.g., "1.2.3")
            version_type: Type of increment

        Returns:
            Incremented version string
        """
        parts = current.split(".")
        major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

        if version_type == "major":
            major += 1
            minor = 0
            patch = 0
        elif version_type == "minor":
            minor += 1
            patch = 0
        else:  # patch
            patch += 1

        return f"{major}.{minor}.{patch}"

    def get_version(self, version_id: str) -> PlanVersion | None:
        """Get a specific version by ID.

        Args:
            version_id: Version ID

        Returns:
            PlanVersion or None if not found
        """
        return self._version_index.get(version_id)

    def get_plan_versions(self, plan_id: str) -> list[PlanVersion]:
        """Get all versions of a plan, sorted by creation time.

        Args:
            plan_id: Plan ID

        Returns:
            List of versions, oldest first
        """
        return self._versions.get(plan_id, [])

    def get_latest_version(self, plan_id: str) -> PlanVersion | None:
        """Get the latest version of a plan.

        Args:
            plan_id: Plan ID

        Returns:
            Latest PlanVersion or None if no versions exist
        """
        versions = self._versions.get(plan_id, [])
        return versions[-1] if versions else None

    def get_version_by_number(self, plan_id: str, version_number: str) -> PlanVersion | None:
        """Get a version by its semantic version number.

        Args:
            plan_id: Plan ID
            version_number: Semantic version (e.g., "1.2.3")

        Returns:
            PlanVersion or None if not found
        """
        versions = self._versions.get(plan_id, [])
        for version in versions:
            if version.version_number == version_number:
                return version
        return None

    def compare_versions(
        self, version1_id: str, version2_id: str
    ) -> dict[str, Any] | None:
        """Compare two versions and show differences.

        Args:
            version1_id: First version ID
            version2_id: Second version ID

        Returns:
            Dict with comparison results, or None if versions not found
        """
        version1 = self._version_index.get(version1_id)
        version2 = self._version_index.get(version2_id)

        if not version1 or not version2:
            return None

        # Compute field-level changes
        changes = {}
        all_keys = set(version1.snapshot_data.keys()) | set(version2.snapshot_data.keys())

        for key in all_keys:
            val1 = version1.snapshot_data.get(key)
            val2 = version2.snapshot_data.get(key)
            if val1 != val2:
                changes[key] = {"from": val1, "to": val2}

        return {
            "version1": {
                "id": version1.id,
                "number": version1.version_number,
                "created_at": version1.created_at.isoformat(),
                "author": version1.author,
            },
            "version2": {
                "id": version2.id,
                "number": version2.version_number,
                "created_at": version2.created_at.isoformat(),
                "author": version2.author,
            },
            "changes": changes,
        }

    def create_branch(
        self,
        parent_version_id: str,
        snapshot_data: dict[str, Any],
        author: str,
        description: str = "",
        tags: list[str] | None = None,
    ) -> PlanVersion:
        """Create a new branch from a parent version.

        Args:
            parent_version_id: Version to branch from
            snapshot_data: Initial branch data
            author: User creating the branch
            description: Branch description
            tags: Optional tags

        Returns:
            Created branch version
        """
        parent = self._version_index.get(parent_version_id)
        if not parent:
            raise ValueError(f"Parent version not found: {parent_version_id}")

        # Create branch with minor version increment
        return self.create_version(
            plan_id=parent.plan_id,
            snapshot_data=snapshot_data,
            author=author,
            description=description,
            version_type="minor",
            parent_version_id=parent_version_id,
            tags=tags,
            metadata={"branch": True},
        )

    def merge_versions(
        self,
        base_version_id: str,
        branch_version_id: str,
        author: str,
        description: str = "",
        conflict_resolution: Literal["base", "branch", "manual"] = "branch",
        manual_resolution: dict[str, Any] | None = None,
    ) -> PlanVersion:
        """Merge two versions, creating a new version with combined changes.

        Args:
            base_version_id: Base version ID
            branch_version_id: Branch version ID to merge
            author: User performing the merge
            description: Merge description
            conflict_resolution: How to resolve conflicts (prefer base, branch, or manual)
            manual_resolution: Manual resolution data if conflict_resolution is 'manual'

        Returns:
            Created merge version

        Raises:
            ValueError: If versions not found or belong to different plans
        """
        base = self._version_index.get(base_version_id)
        branch = self._version_index.get(branch_version_id)

        if not base or not branch:
            raise ValueError("One or both versions not found")

        if base.plan_id != branch.plan_id:
            raise ValueError("Cannot merge versions from different plans")

        # Merge snapshots
        merged_data = base.snapshot_data.copy()

        # Find conflicting keys
        conflicts = {}
        for key in branch.snapshot_data:
            if key in merged_data and merged_data[key] != branch.snapshot_data[key]:
                conflicts[key] = {
                    "base": merged_data[key],
                    "branch": branch.snapshot_data[key],
                }

        # Resolve conflicts
        if conflicts:
            if conflict_resolution == "branch":
                # Prefer branch values
                for key in conflicts:
                    merged_data[key] = branch.snapshot_data[key]
            elif conflict_resolution == "manual":
                if not manual_resolution:
                    raise ValueError("Manual resolution required but not provided")
                for key in conflicts:
                    if key in manual_resolution:
                        merged_data[key] = manual_resolution[key]
            # If "base", keep base values (already in merged_data)

        # Add keys only in branch
        for key in branch.snapshot_data:
            if key not in merged_data:
                merged_data[key] = branch.snapshot_data[key]

        # Create merge version
        merge_metadata = {
            "merge": True,
            "base_version": base_version_id,
            "branch_version": branch_version_id,
            "conflicts": list(conflicts.keys()) if conflicts else [],
            "conflict_resolution": conflict_resolution,
        }

        return self.create_version(
            plan_id=base.plan_id,
            snapshot_data=merged_data,
            author=author,
            description=description or f"Merge {branch.version_number} into {base.version_number}",
            version_type="minor",
            parent_version_id=base_version_id,
            tags=["merge"],
            metadata=merge_metadata,
        )

    def get_version_tree(self, plan_id: str) -> dict[str, Any]:
        """Get version tree showing branching and merging history.

        Args:
            plan_id: Plan ID

        Returns:
            Dict representing version tree structure
        """
        versions = self._versions.get(plan_id, [])

        if not versions:
            return {"plan_id": plan_id, "versions": []}

        # Build tree structure
        tree_nodes = []
        for version in versions:
            node = {
                "id": version.id,
                "version": version.version_number,
                "author": version.author,
                "created_at": version.created_at.isoformat(),
                "description": version.description,
                "tags": version.tags,
                "parent": version.parent_version,
                "is_branch": version.metadata.get("branch", False),
                "is_merge": version.metadata.get("merge", False),
            }
            tree_nodes.append(node)

        return {"plan_id": plan_id, "versions": tree_nodes}

    def restore_version(
        self, version_id: str, author: str, description: str = ""
    ) -> PlanVersion:
        """Restore a plan to a specific version, creating a new version.

        Args:
            version_id: Version to restore
            author: User performing restore
            description: Restore description

        Returns:
            New version with restored data
        """
        version = self._version_index.get(version_id)
        if not version:
            raise ValueError(f"Version not found: {version_id}")

        restore_metadata = {
            "restore": True,
            "restored_from": version_id,
            "restored_version": version.version_number,
        }

        return self.create_version(
            plan_id=version.plan_id,
            snapshot_data=version.snapshot_data.copy(),
            author=author,
            description=description or f"Restore to version {version.version_number}",
            version_type="patch",
            tags=["restore"],
            metadata=restore_metadata,
        )

    def tag_version(self, version_id: str, tag: str) -> PlanVersion | None:
        """Add a tag to a version.

        Args:
            version_id: Version ID
            tag: Tag to add

        Returns:
            Updated version or None if not found
        """
        version = self._version_index.get(version_id)
        if version:
            version.add_tag(tag)
        return version

    def get_versions_by_tag(self, plan_id: str, tag: str) -> list[PlanVersion]:
        """Get all versions of a plan with a specific tag.

        Args:
            plan_id: Plan ID
            tag: Tag to filter by

        Returns:
            List of matching versions
        """
        versions = self._versions.get(plan_id, [])
        return [v for v in versions if v.is_tagged(tag)]

    def export_version_history(
        self, plan_id: str, format: Literal["json", "markdown"] = "json"
    ) -> str:
        """Export version history in specified format.

        Args:
            plan_id: Plan ID
            format: Export format

        Returns:
            Formatted version history string
        """
        versions = self._versions.get(plan_id, [])

        if format == "json":
            history = []
            for v in versions:
                history.append({
                    "id": v.id,
                    "version": v.version_number,
                    "created_at": v.created_at.isoformat(),
                    "author": v.author,
                    "description": v.description,
                    "tags": v.tags,
                    "parent": v.parent_version,
                })
            return json.dumps({"plan_id": plan_id, "versions": history}, indent=2)

        elif format == "markdown":
            lines = [f"# Version History: {plan_id}\n"]
            for v in versions:
                lines.append(f"## Version {v.version_number}")
                lines.append(f"- **ID**: {v.id}")
                lines.append(f"- **Created**: {v.created_at.isoformat()}")
                lines.append(f"- **Author**: {v.author}")
                if v.description:
                    lines.append(f"- **Description**: {v.description}")
                if v.tags:
                    lines.append(f"- **Tags**: {', '.join(v.tags)}")
                if v.parent_version:
                    lines.append(f"- **Parent**: {v.parent_version}")
                lines.append("")
            return "\n".join(lines)

        return ""
