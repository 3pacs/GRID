import asyncio

import pytest
from fastapi import HTTPException

from scripts import compute_coordinator as coordinator


def boogerbots_job(**overrides):
    job = {
        "tenant": "boogerbots",
        "labels": {
            "owner": "boogerbots",
            "repo": "3pacs/storymill",
            "purpose": "w1-dry-run",
        },
        "workload": {
            "type": "tts",
            "command": ["python", "scripts/render_episode_audio.py", "--dry-run"],
        },
        "priority": {
            "class": "boogerbots-background",
            "value": 10,
        },
        "resources": {
            "gpu": {"required": False},
            "off_hours_only": True,
        },
        "yield_policy": {
            "yield_to": ["ocmri"],
            "check_interval_seconds": 30,
            "idle_window_required": False,
            "on_ocmri_demand": "exit_without_start",
        },
        "preemption": {
            "enabled": True,
            "max_seconds_to_yield": 30,
        },
        "kill_switch": {
            "path": "/data/storymill/control/boogerbots.kill",
            "action": "stop_new_work_and_release_leases",
        },
        "isolation": {
            "vm_user": "boogerbots",
            "no_sudo": True,
            "phi_network_blocked": True,
            "separate_log_sink": True,
        },
        "audit": {
            "log_sink": "/data/storymill/logs/compute-coordinator-audit.jsonl",
            "correlation_id": "boogerbots-w1-20260630",
            "events": [
                "submitted",
                "leased",
                "yielded_or_preempted",
                "completed_or_failed",
            ],
        },
    }
    job.update(overrides)
    return job


def test_worker_state_is_derived_from_active_jobs():
    assert coordinator.worker_state_for_active_jobs(0) == "IDLE"
    assert coordinator.worker_state_for_active_jobs(-1) == "IDLE"
    assert coordinator.worker_state_for_active_jobs(1) == "BUSY"
    assert coordinator.worker_state_for_active_jobs(3) == "BUSY"


def test_heartbeat_update_reconciles_idle_busy_state():
    sql = coordinator.worker_heartbeat_update_sql()

    assert "last_heartbeat=NOW()" in sql
    assert "CASE WHEN active_jobs > 0 THEN 'BUSY' ELSE 'IDLE' END" in sql
    assert "WHERE id=%s" in sql


def test_heartbeat_with_active_jobs_update_trusts_worker_runtime_count():
    sql = coordinator.worker_heartbeat_with_active_jobs_update_sql()

    assert "active_jobs=GREATEST(%s,0)" in sql
    assert "CASE WHEN GREATEST(%s,0) > 0 THEN 'BUSY' ELSE 'IDLE' END" in sql
    assert "WHERE id=%s" in sql


def test_completion_update_decrements_and_sets_state_atomically():
    sql = coordinator.worker_complete_update_sql()

    assert "active_jobs=GREATEST(active_jobs-1,0)" in sql
    assert "CASE WHEN GREATEST(active_jobs-1,0) > 0 THEN 'BUSY' ELSE 'IDLE' END" in sql
    assert "last_heartbeat=NOW()" in sql
    assert "WHERE id=%s" in sql


def test_job_error_clear_sql_is_scoped_to_one_job():
    sql = coordinator.clear_job_error_sql()

    assert sql == "UPDATE compute_jobs SET error_message=NULL WHERE id=%s"


def test_openapi_exposes_boogerbots_contract_fields_and_dry_run_path():
    openapi = coordinator.app.openapi()
    job_properties = openapi["components"]["schemas"]["JobCreate"]["properties"]

    for field in [
        "tenant",
        "labels",
        "workload",
        "priority",
        "yield_policy",
        "preemption",
        "kill_switch",
        "isolation",
        "audit",
    ]:
        assert field in job_properties
    assert "/jobs/dry-run" in openapi["paths"]
    priority_schema = job_properties["priority"]
    assert "anyOf" in priority_schema


def test_boogerbots_dry_run_accepts_valid_contract_without_mutation():
    response = asyncio.run(coordinator.dry_run_job(boogerbots_job()))

    assert response["status"] == "accepted"
    assert response["dry_run"] is True
    assert response["would_enqueue"] is False
    assert response["would_accept_new_work"] is True
    assert response["would_release_leases"] is False
    assert response["mutating_actions_performed"] == []
    assert response["tenant"] == "boogerbots"
    assert response["errors"] == []
    assert response["w1_proof"]["ready"] is True
    assert response["w1_proof"]["non_mutating"] is True


def test_boogerbots_w1_proof_shows_ocmri_priority_wins_and_audit_is_separate():
    proof = coordinator.boogerbots_w1_proof(boogerbots_job())

    scheduler = proof["scheduler"]
    assert scheduler["boogerbots_priority_value"] == 10
    assert scheduler["boogerbots_priority_ceiling"] == 30
    assert scheduler["ocmri_priority_floor"] == 31
    assert scheduler["ocmri_priority_wins"] is True
    assert scheduler["yield_to_ocmri"] is True
    assert scheduler["preemption_enabled"] is True
    assert scheduler["claim_order_proof"] == [
        {"tenant": "ocmri", "priority": 31},
        {"tenant": "boogerbots", "priority": 10},
    ]

    audit = proof["audit"]
    assert audit["log_sink"] == "/data/storymill/logs/compute-coordinator-audit.jsonl"
    assert audit["separate_from_ocmri_sentry"] is True
    assert audit["required_events_present"] is True
    assert audit["missing_events"] == []


def test_boogerbots_kill_switch_env_stops_new_work_and_releases_leases(monkeypatch):
    monkeypatch.setenv("BOOGERBOTS_W1_KILL", "1")
    response = asyncio.run(
        coordinator.dry_run_job(
            boogerbots_job(
                kill_switch={
                    "env": "BOOGERBOTS_W1_KILL",
                    "action": "stop_new_work_and_release_leases",
                }
            )
        )
    )

    assert response["status"] == "accepted"
    assert response["would_accept_new_work"] is False
    assert response["would_release_leases"] is True
    kill_switch = response["w1_proof"]["kill_switch"]
    assert kill_switch["configured"] is True
    assert kill_switch["active"] is True
    assert kill_switch["would_accept_new_work"] is False
    assert kill_switch["would_release_leases"] is True


def test_boogerbots_kill_switch_path_stops_new_work_and_releases_leases(tmp_path):
    tripwire = tmp_path / "boogerbots.kill"
    tripwire.write_text("stop", encoding="utf-8")

    state = coordinator.boogerbots_kill_switch_state(
        boogerbots_job(
            kill_switch={
                "path": str(tripwire),
                "action": "stop_new_work_and_release_leases",
            }
        )
    )

    assert state["configured"] is True
    assert state["active"] is True
    assert state["would_accept_new_work"] is False
    assert state["would_release_leases"] is True
    assert state["tripwires"] == [
        {"type": "path", "value": str(tripwire), "active": True}
    ]


def test_non_boogerbots_live_jobs_must_use_integer_priority():
    assert coordinator.db_priority_value(7) == 7

    with pytest.raises(HTTPException) as exc:
        coordinator.db_priority_value({"class": "boogerbots-background", "value": 10})

    assert exc.value.status_code == 400
    assert "/jobs/dry-run" in exc.value.detail


def test_boogerbots_dry_run_rejects_ocmri_escalation_and_missing_audit():
    errors = coordinator.boogerbots_contract_errors(
        boogerbots_job(
            tenant="ocmri",
            priority={"class": "ocmri-critical", "value": 90},
            audit={
                "log_sink": "ocmri-sentry",
                "correlation_id": "",
                "events": ["submitted"],
            },
        )
    )

    assert "tenant must be 'boogerbots'" in errors
    assert "priority.class must be 'boogerbots-low' or 'boogerbots-background'" in errors
    assert "priority.value must be an integer from 0 through 30" in errors
    assert "audit.log_sink must be separate from OCMRI/Sentry" in errors
    assert "audit.correlation_id is required" in errors
    assert any(error.startswith("audit.events missing") for error in errors)


def test_boogerbots_live_submit_is_blocked_until_w1_enabled():
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            coordinator.create_job(
                coordinator.JobCreate(
                    job_type=coordinator.JobType.LLM_INFERENCE,
                    name="boogerbots live submit should fail closed",
                    tenant="boogerbots",
                    priority={"class": "boogerbots-background", "value": 10},
                )
            )
        )

    assert exc.value.status_code == 400
    assert "/jobs/dry-run" in exc.value.detail
