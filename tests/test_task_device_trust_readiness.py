import json

from blueprint.task_device_trust_readiness import (
    build_task_device_trust_readiness_plan,
    task_device_trust_readiness_plan_to_dict,
    task_device_trust_readiness_plan_to_dicts,
    task_device_trust_readiness_plan_to_markdown,
)


def test_complete_device_trust_task_is_ready():
    result = build_task_device_trust_readiness_plan(
        _plan(
            [
                _task(
                    "device-ready",
                    "Enforce managed device trust",
                    (
                        "Require trusted device and device posture checks for managed device access. "
                        "Enrollment source is Intune MDM device inventory. Posture checks include OS version, "
                        "disk encryption, screen lock, and jailbreak status. Enforcement scope covers admin apps "
                        "and user groups. Exception path uses breakglass waivers. Audit monitoring has logs, "
                        "alerts, dashboard, and compliance report evidence."
                    ),
                    ["src/security/device_trust/mdm_compliance.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.present_criteria == ("enrollment_source", "posture_checks", "enforcement_scope", "exception_path", "audit_monitoring")
    assert record.missing_criteria == ()


def test_detects_device_trust_mdm_attestation_and_compliance_with_suppression():
    result = build_task_device_trust_readiness_plan(
        _plan(
            [
                _task("attest", "Device attestation", "Add certificate-bound device access.", ["src/devices/attestation.py"]),
                _task("posture", "Device posture", "Check compliant device posture from MDM.", ["src/mdm/compliance.py"]),
                _task("docs", "Docs", "No device trust or managed device impact is expected.", []),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert result.impacted_task_ids == ("attest", "posture")
    assert result.ignored_task_ids == ("docs",)
    assert "device_attestation" in by_id["attest"].detected_signals
    assert "device_posture" in by_id["posture"].detected_signals
    assert "mdm" in by_id["posture"].detected_signals


def test_serialization_and_markdown_are_stable():
    result = build_task_device_trust_readiness_plan(_plan([_task("alias", "MDM", "Managed device MDM enforcement with audit logs.", ["security/mdm.py"])]))
    payload = task_device_trust_readiness_plan_to_dict(result)

    assert task_device_trust_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task Device Trust Readiness: plan-device-trust" in task_device_trust_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-device-trust", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
