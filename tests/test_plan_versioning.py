"""Tests for plan versioning system."""

import json
from datetime import datetime, timezone

from blueprint.versioning.plan_version import PlanVersion
from blueprint.versioning.version_manager import VersionManager


class TestPlanVersion:
    """Test PlanVersion model."""

    def test_create_version(self):
        """Test creating a plan version."""
        timestamp = datetime.now(timezone.utc)
        version = PlanVersion(
            id="ver_001",
            plan_id="plan_123",
            version_number="1.2.3",
            created_at=timestamp,
            author="alice",
            description="Test version",
            snapshot_data={"title": "Test Plan", "status": "ready"},
            tags=["approved"],
        )

        assert version.id == "ver_001"
        assert version.plan_id == "plan_123"
        assert version.version_number == "1.2.3"
        assert version.author == "alice"
        assert version.description == "Test version"
        assert version.snapshot_data == {"title": "Test Plan", "status": "ready"}
        assert "approved" in version.tags

    def test_get_version_parts(self):
        """Test parsing version number into parts."""
        version = PlanVersion(
            id="ver_001",
            plan_id="plan_123",
            version_number="2.5.9",
            created_at=datetime.now(timezone.utc),
            author="alice",
            snapshot_data={},
        )

        major, minor, patch = version.get_version_parts()
        assert major == 2
        assert minor == 5
        assert patch == 9

    def test_is_major_version(self):
        """Test checking if version is major."""
        major_version = PlanVersion(
            id="ver_001",
            plan_id="plan_123",
            version_number="2.0.0",
            created_at=datetime.now(timezone.utc),
            author="alice",
            snapshot_data={},
        )

        minor_version = PlanVersion(
            id="ver_002",
            plan_id="plan_123",
            version_number="2.1.0",
            created_at=datetime.now(timezone.utc),
            author="alice",
            snapshot_data={},
        )

        assert major_version.is_major_version() is True
        assert minor_version.is_major_version() is False

    def test_tag_operations(self):
        """Test adding and removing tags."""
        version = PlanVersion(
            id="ver_001",
            plan_id="plan_123",
            version_number="1.0.0",
            created_at=datetime.now(timezone.utc),
            author="alice",
            snapshot_data={},
        )

        # Add tag
        version.add_tag("approved")
        assert version.is_tagged("approved")

        # Add duplicate tag (should not duplicate)
        version.add_tag("approved")
        assert version.tags.count("approved") == 1

        # Remove tag
        version.remove_tag("approved")
        assert not version.is_tagged("approved")


class TestVersionManager:
    """Test VersionManager functionality."""

    def test_create_first_version(self):
        """Test creating the first version of a plan."""
        manager = VersionManager()

        version = manager.create_version(
            plan_id="plan_001",
            snapshot_data={"title": "First Plan", "status": "draft"},
            author="alice",
            description="Initial version",
        )

        assert version.version_number == "1.0.0"
        assert version.plan_id == "plan_001"
        assert version.author == "alice"
        assert version.snapshot_data == {"title": "First Plan", "status": "draft"}

    def test_create_patch_version(self):
        """Test creating a patch version."""
        manager = VersionManager()

        v1 = manager.create_version(
            plan_id="plan_001",
            snapshot_data={"title": "Plan v1"},
            author="alice",
        )

        v2 = manager.create_version(
            plan_id="plan_001",
            snapshot_data={"title": "Plan v1.0.1"},
            author="alice",
            version_type="patch",
        )

        assert v1.version_number == "1.0.0"
        assert v2.version_number == "1.0.1"

    def test_create_minor_version(self):
        """Test creating a minor version."""
        manager = VersionManager()

        v1 = manager.create_version(
            plan_id="plan_001",
            snapshot_data={"title": "Plan v1"},
            author="alice",
        )

        v2 = manager.create_version(
            plan_id="plan_001",
            snapshot_data={"title": "Plan v1.1"},
            author="alice",
            version_type="minor",
        )

        assert v1.version_number == "1.0.0"
        assert v2.version_number == "1.1.0"

    def test_create_major_version(self):
        """Test creating a major version."""
        manager = VersionManager()

        v1 = manager.create_version(
            plan_id="plan_001",
            snapshot_data={"title": "Plan v1"},
            author="alice",
        )

        v2 = manager.create_version(
            plan_id="plan_001",
            snapshot_data={"title": "Plan v2"},
            author="alice",
            version_type="major",
        )

        assert v1.version_number == "1.0.0"
        assert v2.version_number == "2.0.0"

    def test_get_version(self):
        """Test retrieving a version by ID."""
        manager = VersionManager()

        v1 = manager.create_version(
            plan_id="plan_001",
            snapshot_data={},
            author="alice",
        )

        retrieved = manager.get_version(v1.id)
        assert retrieved is not None
        assert retrieved.id == v1.id

    def test_get_plan_versions(self):
        """Test getting all versions of a plan."""
        manager = VersionManager()

        manager.create_version("plan_001", {}, "alice")
        manager.create_version("plan_001", {}, "bob")
        manager.create_version("plan_001", {}, "charlie")

        versions = manager.get_plan_versions("plan_001")
        assert len(versions) == 3

    def test_get_latest_version(self):
        """Test getting the latest version."""
        manager = VersionManager()

        v1 = manager.create_version("plan_001", {"v": 1}, "alice")
        v2 = manager.create_version("plan_001", {"v": 2}, "alice")
        v3 = manager.create_version("plan_001", {"v": 3}, "alice")

        latest = manager.get_latest_version("plan_001")
        assert latest is not None
        assert latest.id == v3.id

    def test_get_version_by_number(self):
        """Test getting a version by its semantic version number."""
        manager = VersionManager()

        manager.create_version("plan_001", {}, "alice")
        manager.create_version("plan_001", {}, "alice", version_type="minor")

        version = manager.get_version_by_number("plan_001", "1.1.0")
        assert version is not None
        assert version.version_number == "1.1.0"

    def test_compare_versions(self):
        """Test comparing two versions."""
        manager = VersionManager()

        v1 = manager.create_version(
            "plan_001",
            {"title": "Original", "status": "draft", "priority": "low"},
            "alice",
        )

        v2 = manager.create_version(
            "plan_001",
            {"title": "Updated", "status": "ready", "priority": "low"},
            "bob",
        )

        comparison = manager.compare_versions(v1.id, v2.id)

        assert comparison is not None
        assert comparison["version1"]["number"] == "1.0.0"
        assert comparison["version2"]["number"] == "1.0.1"
        assert "changes" in comparison
        assert "title" in comparison["changes"]
        assert comparison["changes"]["title"]["from"] == "Original"
        assert comparison["changes"]["title"]["to"] == "Updated"
        assert "priority" not in comparison["changes"]

    def test_compare_nonexistent_versions(self):
        """Test comparing with nonexistent versions."""
        manager = VersionManager()

        result = manager.compare_versions("nonexistent_1", "nonexistent_2")
        assert result is None

    def test_create_branch(self):
        """Test creating a branch from a version."""
        manager = VersionManager()

        v1 = manager.create_version("plan_001", {"base": "data"}, "alice")

        branch = manager.create_branch(
            parent_version_id=v1.id,
            snapshot_data={"base": "data", "branch": "feature"},
            author="bob",
            description="Feature branch",
        )

        assert branch.version_number == "1.1.0"
        assert branch.parent_version == v1.id
        assert branch.metadata.get("branch") is True

    def test_merge_versions_no_conflicts(self):
        """Test merging versions without conflicts."""
        manager = VersionManager()

        base = manager.create_version("plan_001", {"a": 1, "b": 2}, "alice")

        branch = manager.create_branch(
            parent_version_id=base.id,
            snapshot_data={"a": 1, "b": 2, "c": 3},
            author="bob",
        )

        merged = manager.merge_versions(
            base_version_id=base.id,
            branch_version_id=branch.id,
            author="alice",
            description="Merge feature",
        )

        assert merged.snapshot_data == {"a": 1, "b": 2, "c": 3}
        assert merged.metadata["merge"] is True
        assert merged.metadata["conflicts"] == []

    def test_merge_versions_with_conflicts_prefer_branch(self):
        """Test merging with conflicts, preferring branch."""
        manager = VersionManager()

        base = manager.create_version("plan_001", {"title": "Base Title"}, "alice")

        branch = manager.create_branch(
            parent_version_id=base.id,
            snapshot_data={"title": "Branch Title"},
            author="bob",
        )

        merged = manager.merge_versions(
            base_version_id=base.id,
            branch_version_id=branch.id,
            author="alice",
            conflict_resolution="branch",
        )

        assert merged.snapshot_data["title"] == "Branch Title"
        assert "title" in merged.metadata["conflicts"]

    def test_merge_versions_with_conflicts_prefer_base(self):
        """Test merging with conflicts, preferring base."""
        manager = VersionManager()

        base = manager.create_version("plan_001", {"title": "Base Title"}, "alice")

        branch = manager.create_branch(
            parent_version_id=base.id,
            snapshot_data={"title": "Branch Title"},
            author="bob",
        )

        merged = manager.merge_versions(
            base_version_id=base.id,
            branch_version_id=branch.id,
            author="alice",
            conflict_resolution="base",
        )

        assert merged.snapshot_data["title"] == "Base Title"

    def test_merge_versions_manual_resolution(self):
        """Test merging with manual conflict resolution."""
        manager = VersionManager()

        base = manager.create_version("plan_001", {"title": "Base", "status": "draft"}, "alice")

        branch = manager.create_branch(
            parent_version_id=base.id,
            snapshot_data={"title": "Branch", "status": "ready"},
            author="bob",
        )

        merged = manager.merge_versions(
            base_version_id=base.id,
            branch_version_id=branch.id,
            author="admin",
            conflict_resolution="manual",
            manual_resolution={"title": "Merged Title", "status": "ready"},
        )

        assert merged.snapshot_data["title"] == "Merged Title"
        assert merged.snapshot_data["status"] == "ready"

    def test_merge_different_plans_fails(self):
        """Test that merging versions from different plans fails."""
        manager = VersionManager()

        v1 = manager.create_version("plan_001", {}, "alice")
        v2 = manager.create_version("plan_002", {}, "bob")

        try:
            manager.merge_versions(v1.id, v2.id, "admin")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "different plans" in str(e)

    def test_get_version_tree(self):
        """Test getting version tree structure."""
        manager = VersionManager()

        v1 = manager.create_version("plan_001", {}, "alice")
        v2 = manager.create_branch(v1.id, {}, "bob")
        v3 = manager.merge_versions(v1.id, v2.id, "alice")

        tree = manager.get_version_tree("plan_001")

        assert tree["plan_id"] == "plan_001"
        assert len(tree["versions"]) == 3
        assert tree["versions"][1]["is_branch"] is True
        assert tree["versions"][2]["is_merge"] is True

    def test_restore_version(self):
        """Test restoring to a previous version."""
        manager = VersionManager()

        v1 = manager.create_version("plan_001", {"title": "Original"}, "alice")
        manager.create_version("plan_001", {"title": "Modified"}, "alice")

        restored = manager.restore_version(v1.id, "admin", "Restore to v1")

        assert restored.snapshot_data["title"] == "Original"
        assert restored.metadata["restore"] is True
        assert restored.metadata["restored_from"] == v1.id
        assert "restore" in restored.tags

    def test_tag_version(self):
        """Test adding tags to versions."""
        manager = VersionManager()

        version = manager.create_version("plan_001", {}, "alice")

        tagged = manager.tag_version(version.id, "approved")

        assert tagged is not None
        assert tagged.is_tagged("approved")

    def test_get_versions_by_tag(self):
        """Test getting versions by tag."""
        manager = VersionManager()

        v1 = manager.create_version("plan_001", {}, "alice", tags=["approved"])
        v2 = manager.create_version("plan_001", {}, "bob")
        v3 = manager.create_version("plan_001", {}, "charlie", tags=["approved"])

        approved = manager.get_versions_by_tag("plan_001", "approved")

        assert len(approved) == 2
        assert v1 in approved
        assert v3 in approved
        assert v2 not in approved

    def test_export_version_history_json(self):
        """Test exporting version history as JSON."""
        manager = VersionManager()

        manager.create_version("plan_001", {}, "alice", description="First version")
        manager.create_version("plan_001", {}, "bob", description="Second version")

        json_export = manager.export_version_history("plan_001", format="json")

        data = json.loads(json_export)
        assert data["plan_id"] == "plan_001"
        assert len(data["versions"]) == 2
        assert data["versions"][0]["version"] == "1.0.0"
        assert data["versions"][1]["version"] == "1.0.1"

    def test_export_version_history_markdown(self):
        """Test exporting version history as Markdown."""
        manager = VersionManager()

        manager.create_version("plan_001", {}, "alice", description="First version")
        manager.create_version("plan_001", {}, "bob", description="Second version")

        md_export = manager.export_version_history("plan_001", format="markdown")

        assert "# Version History: plan_001" in md_export
        assert "## Version 1.0.0" in md_export
        assert "## Version 1.0.1" in md_export
        assert "alice" in md_export
        assert "bob" in md_export

    def test_version_with_metadata(self):
        """Test creating version with custom metadata."""
        manager = VersionManager()

        version = manager.create_version(
            "plan_001",
            {},
            "alice",
            metadata={"milestone": "v1-release", "trigger": "manual"},
        )

        assert version.metadata["milestone"] == "v1-release"
        assert version.metadata["trigger"] == "manual"

    def test_version_id_generation(self):
        """Test that version IDs are generated sequentially."""
        manager = VersionManager()

        v1 = manager.create_version("plan_001", {}, "alice")
        v2 = manager.create_version("plan_001", {}, "alice")
        v3 = manager.create_version("plan_002", {}, "bob")

        assert v1.id == "ver_00000001"
        assert v2.id == "ver_00000002"
        assert v3.id == "ver_00000003"

    def test_parent_version_tracking(self):
        """Test parent version tracking."""
        manager = VersionManager()

        v1 = manager.create_version("plan_001", {}, "alice")
        v2 = manager.create_version("plan_001", {}, "alice")

        # v2 should have v1 as parent (implicit)
        assert v2.parent_version == v1.id

    def test_explicit_parent_version(self):
        """Test creating version with explicit parent."""
        manager = VersionManager()

        v1 = manager.create_version("plan_001", {}, "alice")
        v2 = manager.create_version("plan_001", {}, "alice")

        # Create v3 branching from v1 (not v2)
        v3 = manager.create_version(
            "plan_001",
            {},
            "bob",
            parent_version_id=v1.id,
        )

        assert v3.parent_version == v1.id
        # v3 increments from v1 (1.0.0), so it becomes 1.0.1, same as v2
        # This is expected - branching from same parent creates same version number
        assert v3.version_number == "1.0.1"

    def test_empty_plan_versions(self):
        """Test getting versions for plan with no versions."""
        manager = VersionManager()

        versions = manager.get_plan_versions("nonexistent_plan")
        latest = manager.get_latest_version("nonexistent_plan")

        assert versions == []
        assert latest is None
