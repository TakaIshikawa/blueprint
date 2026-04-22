"""JUnit XML task exporter for CI workflows."""

from collections import OrderedDict
from typing import Any
from xml.etree import ElementTree

from blueprint.exporters.base import TargetExporter


class JUnitTasksExporter(TargetExporter):
    """Export execution-plan tasks as JUnit XML test cases."""

    DEFAULT_BLOCKED_MESSAGE = "Task is blocked."

    def get_format(self) -> str:
        """Get export format."""
        return "xml"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".xml"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export execution-plan tasks to JUnit XML."""
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        self.ensure_output_dir(output_path)

        root = self._build_testsuites(execution_plan)
        tree = ElementTree.ElementTree(root)
        ElementTree.indent(tree, space="  ")
        tree.write(output_path, encoding="utf-8", xml_declaration=True)

        return output_path

    def _build_testsuites(self, plan: dict[str, Any]) -> ElementTree.Element:
        """Build the JUnit XML root element."""
        suites = self._tasks_by_milestone(plan)
        all_tasks = plan.get("tasks", [])
        root = ElementTree.Element(
            "testsuites",
            {
                "name": plan["id"],
                "tests": str(len(all_tasks)),
                "failures": str(self._failure_count(all_tasks)),
                "skipped": str(self._skipped_count(all_tasks)),
            },
        )

        for milestone_name, tasks in suites.items():
            suite = ElementTree.SubElement(
                root,
                "testsuite",
                {
                    "name": milestone_name,
                    "tests": str(len(tasks)),
                    "failures": str(self._failure_count(tasks)),
                    "skipped": str(self._skipped_count(tasks)),
                    "time": "0",
                },
            )
            for task in tasks:
                suite.append(self._testcase(plan["id"], milestone_name, task))

        return root

    def _tasks_by_milestone(
        self,
        plan: dict[str, Any],
    ) -> OrderedDict[str, list[dict[str, Any]]]:
        """Group tasks by milestone while preserving plan and task order."""
        groups: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        for index, milestone in enumerate(plan.get("milestones", []), 1):
            milestone_name = self._milestone_name(milestone, index)
            groups[milestone_name] = []

        for task in plan.get("tasks", []):
            milestone_name = task.get("milestone") or "Ungrouped"
            if milestone_name not in groups:
                groups[milestone_name] = []
            groups[milestone_name].append(task)

        return OrderedDict((name, tasks) for name, tasks in groups.items() if tasks)

    def _testcase(
        self,
        plan_id: str,
        milestone_name: str,
        task: dict[str, Any],
    ) -> ElementTree.Element:
        """Build a JUnit testcase element for an execution task."""
        testcase = ElementTree.Element(
            "testcase",
            {
                "classname": f"{plan_id}.{milestone_name}",
                "name": f"{task['id']}: {task['title']}",
                "time": "0",
            },
        )
        properties = ElementTree.SubElement(testcase, "properties")
        ElementTree.SubElement(properties, "property", {"name": "task_id", "value": task["id"]})
        ElementTree.SubElement(
            properties,
            "property",
            {"name": "status", "value": task.get("status") or "pending"},
        )

        status = task.get("status") or "pending"
        if status == "skipped":
            ElementTree.SubElement(
                testcase,
                "skipped",
                {"message": "Task was skipped."},
            )
        elif status == "blocked":
            message = self._blocked_message(task)
            failure = ElementTree.SubElement(
                testcase,
                "failure",
                {"message": message, "type": "blocked"},
            )
            failure.text = message
        elif status != "completed":
            message = f"Task is not complete: status is {status}."
            failure = ElementTree.SubElement(
                testcase,
                "failure",
                {"message": message, "type": "incomplete"},
            )
            failure.text = message

        return testcase

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Get a display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _blocked_message(self, task: dict[str, Any]) -> str:
        """Get the JUnit failure message for a blocked task."""
        metadata = task.get("metadata") or {}
        return (
            task.get("blocked_reason")
            or metadata.get("blocked_reason")
            or self.DEFAULT_BLOCKED_MESSAGE
        )

    def _failure_count(self, tasks: list[dict[str, Any]]) -> int:
        """Count tasks that should fail in JUnit."""
        return sum(
            1 for task in tasks if (task.get("status") or "pending") not in {"completed", "skipped"}
        )

    def _skipped_count(self, tasks: list[dict[str, Any]]) -> int:
        """Count tasks that should be skipped in JUnit."""
        return sum(1 for task in tasks if task.get("status") == "skipped")
