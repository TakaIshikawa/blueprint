"""Tests for container orchestration readiness analyzer."""

import pytest

from blueprint.task_container_orchestration_readiness import (
    ContainerOrchestrationReadiness,
    analyze_container_orchestration_readiness,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_container_orchestration_readiness({})

    assert isinstance(result, ContainerOrchestrationReadiness)
    assert result.kubernetes_manifests_present is False
    assert result.docker_compose_configured is False
    assert result.health_checks_defined is False
    assert result.resource_limits_specified is False
    assert result.autoscaling_configured is False
    assert result.service_mesh_integrated is False
    assert result.persistent_volumes_configured is False
    assert result.network_policies_defined is False
    assert result.graceful_shutdown_handled is False
    assert result.init_containers_used is False
    assert result.readiness_score == 0.0


def test_kubernetes_manifests_detected():
    """Detect Kubernetes manifests in task data."""
    task = {
        "title": "Create Kubernetes deployment",
        "description": "Write Kubernetes manifest for production deployment",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.docker_compose_configured is False


def test_docker_compose_detected():
    """Detect Docker Compose configuration in task data."""
    task = {
        "description": "Set up docker-compose.yml for local development",
        "acceptance_criteria": ["Docker compose file created"],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.docker_compose_configured is True
    assert result.kubernetes_manifests_present is False


def test_health_checks_detected():
    """Detect health check configuration in task data."""
    task = {
        "description": "Configure liveness probe and readiness probe for pods",
        "acceptance_criteria": ["Health checks defined", "Health endpoint implemented"],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.health_checks_defined is True


def test_resource_limits_detected():
    """Detect resource limit specifications in task data."""
    task = {
        "title": "Set resource limits",
        "description": "Configure CPU limit and memory request for containers",
        "acceptance_criteria": ["Resource quotas defined"],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.resource_limits_specified is True


def test_autoscaling_detected():
    """Detect autoscaling configuration in task data."""
    task = {
        "description": "Configure horizontal pod autoscaler for deployment",
        "acceptance_criteria": ["HPA configured", "Scaling policy defined"],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.autoscaling_configured is True


def test_service_mesh_detected():
    """Detect service mesh integration in task data."""
    task = {
        "title": "Integrate Istio service mesh",
        "description": "Enable sidecar injection for Istio integration",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.service_mesh_integrated is True


def test_persistent_volume_detected():
    """Detect persistent volume configuration in task data."""
    task = {
        "description": "Configure persistent volume claim for StatefulSet",
        "acceptance_criteria": ["PVC created", "Storage class configured"],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.persistent_volumes_configured is True


def test_network_policy_detected():
    """Detect network policy configuration in task data."""
    task = {
        "description": "Define network policies with ingress and egress rules",
        "acceptance_criteria": ["Network isolation configured"],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.network_policies_defined is True


def test_graceful_shutdown_detected():
    """Detect graceful shutdown handling in task data."""
    task = {
        "description": "Implement graceful shutdown with SIGTERM handling",
        "acceptance_criteria": ["Pre-stop hook configured", "Drain connections on shutdown"],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.graceful_shutdown_handled is True


def test_init_containers_detected():
    """Detect init container usage in task data."""
    task = {
        "description": "Add init container for database migration",
        "acceptance_criteria": ["Init script configured"],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.init_containers_used is True


def test_comprehensive_orchestration_all_detected():
    """Test comprehensive orchestration with all aspects present."""
    task = {
        "title": "Complete Kubernetes deployment setup",
        "description": (
            "Create Kubernetes manifests with deployment.yaml and docker-compose.yml. "
            "Configure liveness probe and readiness probe with health checks. "
            "Set CPU limit and memory request resource limits. "
            "Enable horizontal pod autoscaler and Istio service mesh with sidecar injection. "
            "Configure persistent volume claim for StatefulSet storage. "
            "Define network policy with ingress rules. "
            "Implement graceful shutdown with pre-stop hook. "
            "Add init container for setup tasks."
        ),
        "acceptance_criteria": [
            "K8s manifest created",
            "Docker compose file ready",
            "Health checks configured",
            "Resource limits specified",
            "HPA enabled",
            "Service mesh integrated",
            "PVC configured",
            "Network policies defined",
            "Graceful termination implemented",
            "Init containers configured",
        ],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.docker_compose_configured is True
    assert result.health_checks_defined is True
    assert result.resource_limits_specified is True
    assert result.autoscaling_configured is True
    assert result.service_mesh_integrated is True
    assert result.persistent_volumes_configured is True
    assert result.network_policies_defined is True
    assert result.graceful_shutdown_handled is True
    assert result.init_containers_used is True
    assert result.readiness_score == 1.0


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_container_orchestration_readiness(None)  # type: ignore

    assert isinstance(result, ContainerOrchestrationReadiness)
    assert result.kubernetes_manifests_present is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_container_orchestration_readiness([{"key": "value"}])  # type: ignore

    assert isinstance(result, ContainerOrchestrationReadiness)
    assert result.kubernetes_manifests_present is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_container_orchestration_readiness("not a mapping")  # type: ignore

    assert isinstance(result, ContainerOrchestrationReadiness)
    assert result.kubernetes_manifests_present is False


def test_invalid_task_data_tuple():
    """Test with tuple input instead of mapping."""
    result = analyze_container_orchestration_readiness(("tuple", "data"))  # type: ignore

    assert isinstance(result, ContainerOrchestrationReadiness)
    assert result.kubernetes_manifests_present is False


def test_partial_data_missing_fields():
    """Test with partial task data missing some fields."""
    task = {
        "title": "Container setup",
        # Missing description, acceptance_criteria, etc.
    }

    result = analyze_container_orchestration_readiness(task)

    assert isinstance(result, ContainerOrchestrationReadiness)
    # No orchestration patterns should match
    assert result.readiness_score == 0.0


def test_core_requirements_readiness():
    """Test core requirements affect score significantly."""
    task = {
        "description": "Configure health check, resource limits, and graceful shutdown",
        "acceptance_criteria": [
            "Liveness probe configured",
            "CPU limit set",
            "Pre-stop hook implemented",
        ],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.health_checks_defined is True
    assert result.resource_limits_specified is True
    assert result.graceful_shutdown_handled is True
    # Core checks all pass (50%), no platform (0%), no advanced (0%)
    assert result.readiness_score == 0.5


def test_platform_only_readiness():
    """Test platform presence contributes to score."""
    task = {
        "description": "Create Kubernetes manifest",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    # No core (0%), platform present (30%), no advanced (0%)
    assert result.readiness_score == 0.3


def test_advanced_features_readiness():
    """Test advanced features contribute to score."""
    task = {
        "description": "Configure HPA, service mesh, PVC, network policy, and init container",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.autoscaling_configured is True
    assert result.service_mesh_integrated is True
    assert result.persistent_volumes_configured is True
    assert result.network_policies_defined is True
    assert result.init_containers_used is True
    # No core (0%), no platform (0%), all advanced (20%)
    assert result.readiness_score == 0.2


def test_task_data_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    task = {
        "title": "Container improvements",
        "acceptance_criteria": [
            "Deployment YAML with pod spec",
            "Healthcheck endpoint configured",
            "Memory request specified",
        ],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.health_checks_defined is True
    assert result.resource_limits_specified is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    task = {
        "title": "Setup orchestration",
        "validation_command": "kubectl apply -f deployment.yaml && test-health-check.sh",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.health_checks_defined is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "KUBERNETES MANIFEST with DOCKER COMPOSE and HEALTH CHECK",
        "acceptance_criteria": ["RESOURCE LIMITS configured", "GRACEFUL SHUTDOWN enabled"],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.docker_compose_configured is True
    assert result.health_checks_defined is True
    assert result.resource_limits_specified is True
    assert result.graceful_shutdown_handled is True


def test_alternative_terminology_kubernetes():
    """Test alternative Kubernetes terminology is recognized."""
    task = {
        "description": "Create k8s manifest with pod specification and helm chart",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True


def test_alternative_terminology_health_check():
    """Test alternative health check terminology is recognized."""
    task = {
        "description": "Configure startup probe with health endpoint",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.health_checks_defined is True


def test_alternative_terminology_resource_limits():
    """Test alternative resource limit terminology is recognized."""
    task = {
        "description": "Set resource quota and limit range for namespace",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.resource_limits_specified is True


def test_alternative_terminology_autoscaling():
    """Test alternative autoscaling terminology is recognized."""
    task = {
        "description": "Configure vertical pod autoscaler with scale policy",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.autoscaling_configured is True


def test_alternative_terminology_service_mesh():
    """Test alternative service mesh terminology is recognized."""
    task = {
        "description": "Enable Linkerd sidecar proxy for mesh integration",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.service_mesh_integrated is True


def test_alternative_terminology_persistent_volume():
    """Test alternative persistent volume terminology is recognized."""
    task = {
        "description": "Configure StatefulSet with dynamic provisioning and volume mount",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.persistent_volumes_configured is True


def test_alternative_terminology_network_policy():
    """Test alternative network policy terminology is recognized."""
    task = {
        "description": "Implement pod network isolation with Calico",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.network_policies_defined is True


def test_alternative_terminology_graceful_shutdown():
    """Test alternative graceful shutdown terminology is recognized."""
    task = {
        "description": "Handle SIGTERM with termination grace period",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.graceful_shutdown_handled is True


def test_to_dict_method():
    """Test ContainerOrchestrationReadiness.to_dict() serialization."""
    readiness = ContainerOrchestrationReadiness(
        kubernetes_manifests_present=True,
        docker_compose_configured=False,
        health_checks_defined=True,
        resource_limits_specified=True,
        autoscaling_configured=False,
        service_mesh_integrated=True,
        persistent_volumes_configured=False,
        network_policies_defined=False,
        graceful_shutdown_handled=True,
        init_containers_used=False,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["kubernetes_manifests_present"] is True
    assert result["docker_compose_configured"] is False
    assert result["health_checks_defined"] is True
    assert result["resource_limits_specified"] is True
    assert result["autoscaling_configured"] is False
    assert result["service_mesh_integrated"] is True
    assert result["persistent_volumes_configured"] is False
    assert result["network_policies_defined"] is False
    assert result["graceful_shutdown_handled"] is True
    assert result["init_containers_used"] is False
    # Core: 3/3=1.0*0.5=0.5, Platform: 1.0*0.3=0.3, Advanced: 1/5=0.2*0.2=0.04
    assert abs(result["readiness_score"] - 0.84) < 0.01


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "Orchestration setup",
        "description": "Create Kubernetes-manifests",
        "acceptance_criteria": ["Health-checks configured"],
        "requirements": ["Resource-limits defined"],
        "notes": ["Enable autoscaling"],
        "risks": ["No graceful-shutdown plan"],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.health_checks_defined is True
    assert result.resource_limits_specified is True
    assert result.autoscaling_configured is True
    assert result.graceful_shutdown_handled is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    task = {
        "validation_commands": [
            "kubectl apply -f k8s/",
            "test-health-check.py",
            "test-resource-limits.py",
        ],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.health_checks_defined is True
    assert result.resource_limits_specified is True


def test_dataclass_immutability():
    """Test that ContainerOrchestrationReadiness is frozen/immutable."""
    readiness = ContainerOrchestrationReadiness(kubernetes_manifests_present=True)

    with pytest.raises(AttributeError):
        readiness.kubernetes_manifests_present = False  # type: ignore


def test_statefulset_detection():
    """Test StatefulSet as a valid container pattern."""
    task = {
        "description": "Deploy application as StatefulSet with ordered deployment",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.persistent_volumes_configured is True


def test_daemonset_detection():
    """Test DaemonSet detection doesn't trigger false positives."""
    task = {
        "description": "Deploy logging agent as DaemonSet on every node",
    }

    result = analyze_container_orchestration_readiness(task)

    # DaemonSet itself doesn't match current patterns
    # But it's a valid K8s workload type
    assert result.kubernetes_manifests_present is False


def test_sidecar_pattern_detection():
    """Test sidecar pattern detection."""
    task = {
        "description": "Add logging sidecar-container to main application pod",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.service_mesh_integrated is True


def test_helm_chart_detection():
    """Test Helm chart as Kubernetes manifest."""
    task = {
        "description": "Package application using Helm chart",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True


def test_kustomize_detection():
    """Test Kustomize as Kubernetes manifest tool."""
    task = {
        "description": "Manage configurations with Kustomize overlays",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True


def test_consul_connect_service_mesh():
    """Test Consul Connect as service mesh."""
    task = {
        "description": "Integrate Consul Connect for service-to-service communication",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.service_mesh_integrated is True


def test_envoy_proxy_detection():
    """Test Envoy proxy as service mesh component."""
    task = {
        "description": "Deploy Envoy proxy for traffic management",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.service_mesh_integrated is True


def test_cilium_network_policy():
    """Test Cilium as network policy provider."""
    task = {
        "description": "Use Cilium for advanced network policies",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.network_policies_defined is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Configure Kubernetes-deployment with health-checks",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.health_checks_defined is True


def test_readiness_score_weighted_calculation():
    """Test readiness score uses weighted calculation."""
    # Only core requirements
    task1 = {
        "description": "Health-check, resource-limits, graceful-shutdown",
    }
    result1 = analyze_container_orchestration_readiness(task1)
    assert result1.readiness_score == 0.5  # 50% weight

    # Only platform
    task2 = {
        "description": "Kubernetes manifest",
    }
    result2 = analyze_container_orchestration_readiness(task2)
    assert result2.readiness_score == 0.3  # 30% weight

    # Only advanced (all 5)
    task3 = {
        "description": "HPA, service mesh, PVC, network policy, init container",
    }
    result3 = analyze_container_orchestration_readiness(task3)
    assert result3.readiness_score == 0.2  # 20% weight

    # Core + Platform
    task4 = {
        "description": "Kubernetes-deployment with health-check, resource-limits, graceful-shutdown",
    }
    result4 = analyze_container_orchestration_readiness(task4)
    assert result4.readiness_score == 0.8  # 50% + 30%


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is False
    assert result.readiness_score == 0.0


def test_both_platforms_detected():
    """Test both Kubernetes and Docker Compose can be present."""
    task = {
        "description": "Create kubernetes manifest for prod and docker-compose.yml for dev",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.docker_compose_configured is True
    # Platform score is 1.0 if ANY platform is present
    assert result.readiness_score == 0.3


def test_production_ready_deployment():
    """Test a production-ready deployment scenario."""
    task = {
        "title": "Production-ready container deployment",
        "description": (
            "Deploy service with Kubernetes-manifests including "
            "liveness and readiness probes for health-checks, "
            "CPU and memory limits for resource management, "
            "horizontal-pod-autoscaler for scaling, "
            "graceful-shutdown with pre-stop hooks, "
            "and init-container for database migrations."
        ),
        "acceptance_criteria": [
            "Health probes configured and tested",
            "Resource quotas prevent resource exhaustion",
            "HPA scales based on CPU utilization",
            "Zero-downtime deployments with graceful shutdown",
            "Init container runs migrations before app start",
        ],
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.health_checks_defined is True
    assert result.resource_limits_specified is True
    assert result.autoscaling_configured is True
    assert result.graceful_shutdown_handled is True
    assert result.init_containers_used is True
    # Core: 3/3*0.5=0.5, Platform: 1.0*0.3=0.3, Advanced: 2/5*0.2=0.08
    assert abs(result.readiness_score - 0.88) < 0.01


def test_partial_orchestration_readiness():
    """Test partial orchestration readiness score calculation."""
    task = {
        "description": "Basic Kubernetes-deployment with health-checks",
    }

    result = analyze_container_orchestration_readiness(task)

    assert result.kubernetes_manifests_present is True
    assert result.health_checks_defined is True
    assert result.resource_limits_specified is False
    assert result.graceful_shutdown_handled is False
    # Core: 1/3*0.5=0.167, Platform: 1.0*0.3=0.3, Advanced: 0/5*0.2=0
    expected_score = (1/3 * 0.5) + 0.3
    assert abs(result.readiness_score - expected_score) < 0.01
