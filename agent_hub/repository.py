from __future__ import annotations

from typing import Any, Protocol

import psycopg2
import psycopg2.extras
from psycopg2.extras import Json


class ReportRepository(Protocol):
    def get_by_idempotency_key(self, idempotency_key: str) -> dict[str, Any] | None:
        """Return an existing report row by idempotency key."""

    def insert_report(self, report: dict[str, Any]) -> dict[str, Any]:
        """Insert a new report row and return its id + report URI."""

    def check_health(self) -> bool:
        """Check database connection health."""


class PostgresReportRepository:
    def _connect(self):
        from config import settings

        return psycopg2.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            dbname=settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
        )

    def get_by_idempotency_key(self, idempotency_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id::text AS id, report_uri, idempotency_key
                    FROM agent_reports
                    WHERE idempotency_key = %s
                    """,
                    (idempotency_key,),
                )
                row = cur.fetchone()
                return dict(row) if row else None

    def insert_report(self, report: dict[str, Any]) -> dict[str, Any]:
        body_json = report.get("body_json")
        body_json_param = Json(body_json) if body_json is not None else None
        try:
            with self._connect() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        INSERT INTO agent_reports (
                            date, agent, host, title, body_md, body_json, tags,
                            report_uri, idempotency_key
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (idempotency_key) DO UPDATE
                        SET idempotency_key = EXCLUDED.idempotency_key
                        RETURNING id::text AS id, report_uri, idempotency_key
                        """,
                        (
                            report["date"],
                            report["agent"],
                            report["host"],
                            report["title"],
                            report["body_md"],
                            body_json_param,
                            report.get("tags", []),
                            report["report_uri"],
                            report["idempotency_key"],
                        ),
                    )
                    row = cur.fetchone()
                    return dict(row)
        except psycopg2.errors.UndefinedTable as exc:
            raise RuntimeError(
                "agent_reports table is missing; run migrations/0050_agent_reports.sql"
            ) from exc

    def check_health(self) -> bool:
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    cur.fetchone()
            return True
        except Exception as exc:
            import logging
            logging.error(f"Postgres database health check failed: {exc}")
            return False


