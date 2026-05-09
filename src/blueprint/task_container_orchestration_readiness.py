"""Analyze container orchestration readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for container orchestration concepts
_KUBERNETES_MANIFEST_RE = re.compile(
    r"\b(?:kubernetes[_\s-]*(?:manifest[s]?|deployment)|k8s[_\s]+(?:manifest|/)|"
    r"deployment[_\s]*\.?(?:yaml|yml)|"
    r"service[_\s]*\.?(?:yaml|yml)|configmap|ingress[_\s]+(?:config|manifest)|"
    r"pod[_\s]+spec(?:ification)?|helm[_\s]+chart|kustomize|"
    r"test[_\s]+(?:kubernetes[_\s-]+)?manifest|kubectl[_\s]+apply)\b",
    re.I,
)
_DOCKER_COMPOSE_RE = re.compile(
    r"\b(?:docker[_\s-]+compose|compose[_\s]+(?:file|yaml|yml)|"
    r"docker[_\s-]+compose\.ya?ml|test[_\s]+(?:docker[_\s-]+)?compose)\b",
    re.I,
)
_HEALTH_CHECK_RE = re.compile(
    r"\b(?:health[_\s-]*check[s]?|healthcheck|liveness[_\s]+probe|readiness[_\s]+probe|"
    r"startup[_\s]+probe|health[_\s]+endpoint|health[_\s]+status|health[_\s]+probe|"
    r"test[_\s-]+health[_\s-]*check)\b",
    re.I,
)
_RESOURCE_LIMIT_RE = re.compile(
    r"\b(?:resource[_\s-]+limit(?:s)?|cpu[_\s]+limit(?:s)?|memory[_\s]+limit(?:s)?|"
    r"resource[_\s-]+request(?:s)?|cpu[_\s]+request(?:s)?|memory[_\s]+request(?:s)?|"
    r"resource[_\s-]+quota|limit[_\s]+range|resource[_\s]+constraint|"
    r"test[_\s-]+resource[_\s-]+limit)\b",
    re.I,
)
_AUTOSCALING_RE = re.compile(
    r"\b(?:(?:horizontal|vertical)?[_\s-]*pod[_\s-]+autoscal(?:ing|er)|hpa|vpa|"
    r"auto[_\s-]*scal(?:ing|e)|scale[_\s]+(?:up|down|policy)|"
    r"scaling[_\s]+strateg(?:y|ies)|cluster[_\s]+autoscal(?:ing|er)|"
    r"test[_\s-]+autoscal(?:ing|er)|enable[_\s]+(?:autoscaling|hpa))\b",
    re.I,
)
_SERVICE_MESH_RE = re.compile(
    r"\b(?:service[_\s]+mesh|istio|linkerd|consul[_\s]+connect|"
    r"sidecar[_\s-]+(?:proxy|injection|pattern|container)|envoy[_\s]+proxy|"
    r"mesh[_\s]+integration|test[_\s]+service[_\s]+mesh)\b",
    re.I,
)
_PERSISTENT_VOLUME_RE = re.compile(
    r"\b(?:persistent[_\s]+volume(?:[_\s]+claim)?|pvc|pv|"
    r"storage[_\s]+class|volume[_\s]+mount|stateful[_\s-]*set|stateful[_\s]+storage|"
    r"dynamic[_\s]+provisioning|test[_\s]+(?:persistent[_\s]+)?volume)\b",
    re.I,
)
_NETWORK_POLICY_RE = re.compile(
    r"\b(?:network[_\s]+polic(?:y|ies)|ingress[_\s]+rule|egress[_\s]+rule|"
    r"pod[_\s]+network|network[_\s]+isolation|network[_\s]+segmentation|"
    r"calico|cilium|test[_\s]+network[_\s]+polic(?:y|ies))\b",
    re.I,
)
_GRACEFUL_SHUTDOWN_RE = re.compile(
    r"\b(?:graceful[_\s-]+shutdown|graceful[_\s-]+termination|"
    r"pre[_\s-]*stop[_\s-]+hook|sigterm[_\s]+handling|termination[_\s]+grace[_\s]+period|"
    r"shutdown[_\s]+hook|drain[_\s]+connection(?:s)?|test[_\s-]+graceful[_\s-]+shutdown|"
    r"zero[_\s-]*downtime[_\s]+deployment)\b",
    re.I,
)
_INIT_CONTAINER_RE = re.compile(
    r"\b(?:init[_\s]+container(?:s)?|initialization[_\s]+container|"
    r"init[_\s]+script|pre[_\s-]*start[_\s]+(?:hook|script)|"
    r"test[_\s]+init[_\s]+container)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class ContainerOrchestrationReadiness:
    """Container orchestration readiness analysis for a task."""

    kubernetes_manifests_present: bool = False
    docker_compose_configured: bool = False
    health_checks_defined: bool = False
    resource_limits_specified: bool = False
    autoscaling_configured: bool = False
    service_mesh_integrated: bool = False
    persistent_volumes_configured: bool = False
    network_policies_defined: bool = False
    graceful_shutdown_handled: bool = False
    init_containers_used: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        # Core requirements (critical for production)
        core_checks = [
            self.health_checks_defined,
            self.resource_limits_specified,
            self.graceful_shutdown_handled,
        ]

        # Orchestration platform (at least one required)
        platform_checks = [
            self.kubernetes_manifests_present,
            self.docker_compose_configured,
        ]

        # Advanced features (optional but recommended)
        advanced_checks = [
            self.autoscaling_configured,
            self.service_mesh_integrated,
            self.persistent_volumes_configured,
            self.network_policies_defined,
            self.init_containers_used,
        ]

        # Weight: core=50%, platform=30%, advanced=20%
        core_score = sum(core_checks) / len(core_checks) * 0.5
        platform_score = (1.0 if any(platform_checks) else 0.0) * 0.3
        advanced_score = sum(advanced_checks) / len(advanced_checks) * 0.2

        return core_score + platform_score + advanced_score

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "kubernetes_manifests_present": self.kubernetes_manifests_present,
            "docker_compose_configured": self.docker_compose_configured,
            "health_checks_defined": self.health_checks_defined,
            "resource_limits_specified": self.resource_limits_specified,
            "autoscaling_configured": self.autoscaling_configured,
            "service_mesh_integrated": self.service_mesh_integrated,
            "persistent_volumes_configured": self.persistent_volumes_configured,
            "network_policies_defined": self.network_policies_defined,
            "graceful_shutdown_handled": self.graceful_shutdown_handled,
            "init_containers_used": self.init_containers_used,
            "readiness_score": self.readiness_score,
        }


def analyze_container_orchestration_readiness(task_data: Mapping[str, Any]) -> ContainerOrchestrationReadiness:
    """
    Analyze container orchestration readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        ContainerOrchestrationReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return ContainerOrchestrationReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return ContainerOrchestrationReadiness(
        kubernetes_manifests_present=bool(_KUBERNETES_MANIFEST_RE.search(searchable_text)),
        docker_compose_configured=bool(_DOCKER_COMPOSE_RE.search(searchable_text)),
        health_checks_defined=bool(_HEALTH_CHECK_RE.search(searchable_text)),
        resource_limits_specified=bool(_RESOURCE_LIMIT_RE.search(searchable_text)),
        autoscaling_configured=bool(_AUTOSCALING_RE.search(searchable_text)),
        service_mesh_integrated=bool(_SERVICE_MESH_RE.search(searchable_text)),
        persistent_volumes_configured=bool(_PERSISTENT_VOLUME_RE.search(searchable_text)),
        network_policies_defined=bool(_NETWORK_POLICY_RE.search(searchable_text)),
        graceful_shutdown_handled=bool(_GRACEFUL_SHUTDOWN_RE.search(searchable_text)),
        init_containers_used=bool(_INIT_CONTAINER_RE.search(searchable_text)),
    )


def _extract_searchable_text(task_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the task data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = task_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = task_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = task_data.get("validation_command") or task_data.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "ContainerOrchestrationReadiness",
    "analyze_container_orchestration_readiness",
]
