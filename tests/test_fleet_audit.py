from scripts import fleet_audit


def test_parse_gpu_csv_handles_units_and_uuid_rows():
    rows = "\n".join(
        [
            "0, GPU-aaa, NVIDIA RTX PRO 2000 Blackwell, 4 %, 6900 MiB, 16311 MiB",
            "1, GPU-bbb, NVIDIA RTX A2000 12GB, 0, 10, 12282",
        ]
    )

    gpus = fleet_audit.parse_gpu_csv(rows)

    assert gpus[0].index == 0
    assert gpus[0].uuid == "GPU-aaa"
    assert gpus[0].util_pct == 4
    assert gpus[0].mem_used_mb == 6900
    assert gpus[1].name == "NVIDIA RTX A2000 12GB"
    assert gpus[1].mem_total_mb == 12282


def test_parse_compute_apps_and_services_filters_relevant_units():
    apps = fleet_audit.parse_compute_apps_csv(
        "GPU-aaa, 1234, /usr/bin/ollama, 1840 MiB\n"
        "GPU-aaa, 2222, /opt/llama-server, 6000 MiB\n"
    )
    services = fleet_audit.parse_systemd_services(
        "grid-worker.service loaded active running GRID worker\n"
        "cron.service loaded active running cron\n"
        "ollama.service loaded failed failed Ollama\n"
    )

    assert [app.pid for app in apps] == [1234, 2222]
    # v0.5: postgres is now in SERVICE_NEEDLES, so the noise unit was switched
    # to a non-matched one (cron) to keep the parity assertion meaningful.
    assert [service.name for service in services] == ["grid-worker.service", "ollama.service"]
    assert services[1].active_state == "failed"


def test_service_needles_v05_covers_grid_svr_stack():
    """v0.5 task #38 — needles must catch hermes/oracle/prefect/postgres/etc."""
    services = fleet_audit.parse_systemd_services(
        "grid-hermes.service loaded active running Hermes operator\n"
        "grid-oracle.service loaded active running Oracle\n"
        "prefect-trust-scores.service loaded active running Prefect trust scores\n"
        "redpanda.service loaded active running Redpanda broker\n"
        "minio.service loaded active running MinIO\n"
        "langfuse-web.service loaded active running Langfuse\n"
        "postgresql.service loaded active running PostgreSQL\n"
        "gemma-micro-1.service loaded active running Gemma micro 1\n"
        "embed-worker.service loaded active running embedding worker\n"
        "gem-hunter.service loaded active running gem hunter\n"
        "permutation-worker.service loaded active running permutation worker\n"
        "kill-predictor.service loaded active running kill predictor\n"
        "llm-groundtruth.service loaded active running llm groundtruth\n"
        "cron.service loaded active running cron\n"  # must NOT match
    )
    names = [service.name for service in services]
    assert "cron.service" not in names
    assert "grid-hermes.service" in names
    assert "grid-oracle.service" in names
    assert "prefect-trust-scores.service" in names
    assert "redpanda.service" in names
    assert "minio.service" in names
    assert "langfuse-web.service" in names
    assert "postgresql.service" in names
    assert "gemma-micro-1.service" in names
    assert "embed-worker.service" in names
    assert "gem-hunter.service" in names
    assert "permutation-worker.service" in names
    assert "kill-predictor.service" in names
    assert "llm-groundtruth.service" in names


def test_build_findings_detects_queue_starvation_and_failed_service():
    snapshot = fleet_audit.HostSnapshot(
        host="ocr-node",
        ok=True,
        hostname="ocr-node",
        gpus=[
            fleet_audit.GPUState(
                index=0,
                uuid="GPU-0",
                name="RTX 2070 SUPER",
                util_pct=0,
                mem_used_mb=100,
                mem_total_mb=8192,
            )
        ],
        services=[
            fleet_audit.ServiceState(
                name="grid-worker.service",
                load_state="loaded",
                active_state="failed",
                sub_state="failed",
                description="GRID worker",
            )
        ],
    )
    coordinator = {
        "ok": True,
        "workers": [{"hostname": "ocr-node", "state": "IDLE", "max_concurrent": 2, "active_jobs": 0}],
        "stats": {"job_states": {"QUEUED": 0}},
    }

    findings = fleet_audit.build_findings([snapshot], coordinator)
    codes = {finding.code for finding in findings}

    assert "queue_starvation" in codes
    assert "service_failed" in codes


def test_build_findings_detects_idle_gpu_when_queue_has_work():
    snapshot = fleet_audit.HostSnapshot(
        host="p9d",
        ok=True,
        hostname="p9d",
        gpus=[
            fleet_audit.GPUState(
                index=1,
                uuid="GPU-1",
                name="NVIDIA RTX A2000 12GB",
                util_pct=0,
                mem_used_mb=50,
                mem_total_mb=12282,
            )
        ],
        services=[
            fleet_audit.ServiceState(
                name="comfyui.service",
                load_state="loaded",
                active_state="active",
                sub_state="running",
                description="ComfyUI",
            )
        ],
    )
    coordinator = {
        "ok": True,
        "workers": [{"hostname": "p9d", "state": "IDLE", "max_concurrent": 2, "active_jobs": 0}],
        "stats": {"job_states": {"QUEUED": 5}},
    }

    findings = fleet_audit.build_findings([snapshot], coordinator)
    codes = {finding.code for finding in findings}

    assert "idle_gpu_with_queue" in codes
    assert "p9d_no_ollama" in codes


def test_report_markdown_is_read_only_and_actionable():
    payload = fleet_audit.report_payload(
        snapshots=[
            fleet_audit.HostSnapshot(
                host="koala",
                ok=False,
                error="ssh timeout",
            )
        ],
        coordinator_state={"ok": True, "workers": [], "stats": {"job_states": {}}},
        findings=[
            fleet_audit.Finding(
                severity="warning",
                code="host_probe_failed",
                host="koala",
                summary="koala could not be probed.",
                proposed_action="Check SSH.",
            )
        ],
    )

    markdown = fleet_audit.markdown_report(payload)

    assert "GRID Fleet-Hermes Audit" in markdown
    assert "host_probe_failed" in markdown
    assert "read-only" in markdown


def test_schema_contains_required_indexes():
    schema = fleet_audit.fleet_state_schema()

    assert "CREATE TABLE IF NOT EXISTS fleet_state" in schema
    assert "idx_fleet_state_host_ts" in schema
    assert "queue_depths JSONB" in schema


def test_db_connect_info_accepts_sqlalchemy_url_and_pg_env(monkeypatch):
    assert (
        fleet_audit._db_connect_info("postgresql+psycopg2://grid:pw@host/db")
        == "postgresql://grid:pw@host/db"
    )

    monkeypatch.delenv("GRID_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "grid-svr")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "griddb")
    monkeypatch.setenv("DB_USER", "grid")
    monkeypatch.setenv("DB_PASSWORD", "secret")

    assert fleet_audit._db_connect_info() == {
        "host": "grid-svr",
        "port": 5432,
        "dbname": "griddb",
        "user": "grid",
        "password": "secret",
    }


def test_default_hosts_include_known_stale_worker_nodes():
    assert "panda" in fleet_audit.DEFAULT_HOSTS
