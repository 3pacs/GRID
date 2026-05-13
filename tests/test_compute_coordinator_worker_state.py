from scripts import compute_coordinator as coordinator


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
