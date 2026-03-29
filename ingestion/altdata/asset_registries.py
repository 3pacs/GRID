"""
GRID Asset Registry ingestion module.

Fetches luxury asset registration data from public government registries
and cross-references owners against known ICIJ offshore entity officers.

Data sources:
    1. FAA N-Number Registry (aircraft ownership)
       https://registry.faa.gov/AircraftInquiry/Search/NNumberResult
    2. US Coast Guard vessel documentation
       https://www.st.nmfs.noaa.gov/st1/CoastGuard/VesselByOwner.html

Series stored with pattern: ASSET:{type}:{registration}:{field}
Signal source_type: 'asset_registry'

This module:
    1. Searches aircraft/vessel registries by owner name
    2. Cross-references results against ICIJ offshore officers
    3. Stores findings in raw_series + signal_sources
    4. Emits signals when offshore entity owners hold luxury assets
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── Configuration ────────────────────────────────────────────────────────

# Rate limiting: 1 request per 3 seconds
_REQUEST_DELAY: float = 3.0
_REQUEST_TIMEOUT: int = 30

_USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

_HEADERS: dict[str, str] = {
    "User-Agent": _USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# FAA N-Number Registry
_FAA_SEARCH_URL: str = (
    "https://registry.faa.gov/AircraftInquiry/Search/NNumberResult"
)
_FAA_NAME_SEARCH_URL: str = (
    "https://registry.faa.gov/AircraftInquiry/Search/NameResult"
)

# US Coast Guard vessel documentation
_USCG_VESSEL_URL: str = (
    "https://www.st.nmfs.noaa.gov/st1/CoastGuard/VesselByOwner.html"
)


# ══════════════════════════════════════════════════════════════════════════
# HTML PARSING UTILITIES
# ══════════════════════════════════════════════════════════════════════════

def _extract_tables_from_html(html: str) -> list[list[list[str]]]:
    """Extract all HTML tables as lists of rows of cells.

    Simple regex-based parser — avoids requiring BeautifulSoup as a
    hard dependency.  Falls back gracefully on malformed HTML.

    Parameters:
        html: Raw HTML string.

    Returns:
        List of tables, each a list of rows, each a list of cell text.
    """
    tables: list[list[list[str]]] = []
    table_pattern = re.compile(
        r"<table[^>]*>(.*?)</table>", re.DOTALL | re.IGNORECASE,
    )
    row_pattern = re.compile(
        r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE,
    )
    cell_pattern = re.compile(
        r"<t[dh][^>]*>(.*?)</t[dh]>", re.DOTALL | re.IGNORECASE,
    )

    for table_match in table_pattern.finditer(html):
        table_html = table_match.group(1)
        rows: list[list[str]] = []
        for row_match in row_pattern.finditer(table_html):
            row_html = row_match.group(1)
            cells: list[str] = []
            for cell_match in cell_pattern.finditer(row_html):
                # Strip HTML tags from cell content
                cell_text = re.sub(r"<[^>]+>", "", cell_match.group(1))
                cell_text = cell_text.strip()
                # Collapse whitespace
                cell_text = re.sub(r"\s+", " ", cell_text)
                cells.append(cell_text)
            if cells:
                rows.append(cells)
        if rows:
            tables.append(rows)

    return tables


def _clean_name(name: str) -> str:
    """Normalize a name for comparison: lowercase, strip punctuation."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


# ══════════════════════════════════════════════════════════════════════════
# PULLER CLASS
# ══════════════════════════════════════════════════════════════════════════

class AssetRegistryPuller(BasePuller):
    """Pulls luxury asset registration data from public government registries.

    Searches the FAA aircraft registry and US Coast Guard vessel
    documentation by owner name, then cross-references against known
    ICIJ offshore entity officers to flag hidden wealth.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Asset_Registries.
        _last_request_time: Timestamp of last HTTP request (rate limiting).
    """

    SOURCE_NAME: str = "Asset_Registries"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://registry.faa.gov",
        "cost_tier": "FREE",
        "latency_class": "DAILY",
        "pit_available": False,
        "revision_behavior": "APPEND",
        "trust_score": "HIGH",
        "priority_rank": 55,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialize the asset registry puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        self._last_request_time: float = 0.0
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)
        log.info(
            "AssetRegistryPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------
    def _throttle(self) -> None:
        """Enforce minimum delay between HTTP requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < _REQUEST_DELAY:
            sleep_time = _REQUEST_DELAY - elapsed
            log.debug("Rate limiting: sleeping {t:.1f}s", t=sleep_time)
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    # ------------------------------------------------------------------
    # FAA Aircraft Search
    # ------------------------------------------------------------------
    @retry_on_failure(max_attempts=2, backoff=5.0)
    def search_aircraft(self, owner_name: str) -> list[dict[str, Any]]:
        """Search the FAA N-Number Registry by owner name.

        Queries the FAA aircraft registration database for aircraft
        registered to the given owner name.

        Parameters:
            owner_name: Owner name to search for (person or entity).

        Returns:
            List of aircraft records with keys: n_number, serial_number,
            manufacturer, model, year_mfr, registrant_name, city, state,
            registration_type.
        """
        if not owner_name or len(owner_name.strip()) < 2:
            log.warning("search_aircraft: owner_name too short, skipping")
            return []

        self._throttle()

        results: list[dict[str, Any]] = []
        try:
            # FAA name-based search uses POST with form data
            form_data = {
                "NameSearchValue": owner_name.strip().upper(),
                "SortColumn": "1",
                "SortOrder": "1",
            }
            resp = self._session.post(
                _FAA_NAME_SEARCH_URL,
                data=form_data,
                timeout=_REQUEST_TIMEOUT,
            )

            if resp.status_code != 200:
                log.warning(
                    "FAA search returned HTTP {code} for owner={name}",
                    code=resp.status_code,
                    name=owner_name,
                )
                return []

            html = resp.text
            tables = _extract_tables_from_html(html)

            # The results table typically has columns:
            # N-Number, Serial Number, Mfr/Model, Status, Name, City, State
            for table in tables:
                if len(table) < 2:
                    continue
                header = [c.lower() for c in table[0]]

                # Identify the results table by looking for key column names
                n_num_idx = _find_column(header, ["n-number", "n number", "nnumber"])
                name_idx = _find_column(header, ["name", "registrant"])
                if n_num_idx is None:
                    continue

                for row in table[1:]:
                    if len(row) <= max(
                        n_num_idx, name_idx or 0,
                    ):
                        continue

                    record: dict[str, Any] = {
                        "n_number": row[n_num_idx] if n_num_idx is not None else "",
                        "serial_number": _safe_get(row, header, [
                            "serial number", "serial",
                        ]),
                        "manufacturer": _safe_get(row, header, [
                            "mfr mdl", "manufacturer", "mfr/model",
                        ]),
                        "model": _safe_get(row, header, ["model"]),
                        "year_mfr": _safe_get(row, header, [
                            "year mfr", "year", "yr mfr",
                        ]),
                        "registrant_name": (
                            row[name_idx] if name_idx is not None else ""
                        ),
                        "city": _safe_get(row, header, ["city"]),
                        "state": _safe_get(row, header, ["state"]),
                        "status": _safe_get(row, header, ["status"]),
                        "source": "FAA",
                        "search_name": owner_name,
                    }
                    # Only include if we got an N-number
                    if record["n_number"]:
                        results.append(record)

            log.info(
                "FAA search for '{name}': {n} aircraft found",
                name=owner_name,
                n=len(results),
            )

        except requests.RequestException as exc:
            log.warning(
                "FAA aircraft search failed for '{name}': {e}",
                name=owner_name,
                e=str(exc),
            )
            raise ConnectionError(str(exc)) from exc
        except Exception as exc:
            log.error(
                "Unexpected error in FAA search for '{name}': {e}",
                name=owner_name,
                e=str(exc),
            )

        return results

    # ------------------------------------------------------------------
    # Coast Guard Vessel Search
    # ------------------------------------------------------------------
    @retry_on_failure(max_attempts=2, backoff=5.0)
    def search_vessels(self, owner_name: str) -> list[dict[str, Any]]:
        """Search US Coast Guard vessel documentation by owner name.

        Queries the NOAA/Coast Guard vessel documentation database for
        vessels registered to the given owner name.

        Parameters:
            owner_name: Owner name to search for (person or entity).

        Returns:
            List of vessel records with keys: vessel_name, hin,
            owner_name, hull_id, service, gross_tons, hailing_port.
        """
        if not owner_name or len(owner_name.strip()) < 2:
            log.warning("search_vessels: owner_name too short, skipping")
            return []

        self._throttle()

        results: list[dict[str, Any]] = []
        try:
            # Coast Guard search uses GET with query parameters
            params = {
                "owner": owner_name.strip().upper(),
            }
            resp = self._session.get(
                _USCG_VESSEL_URL,
                params=params,
                timeout=_REQUEST_TIMEOUT,
            )

            if resp.status_code != 200:
                log.warning(
                    "Coast Guard search returned HTTP {code} for owner={name}",
                    code=resp.status_code,
                    name=owner_name,
                )
                return []

            html = resp.text
            tables = _extract_tables_from_html(html)

            # Vessel results table typically has:
            # Vessel Name, HIN, Owner, Hull ID, Service, Gross Tons, Hailing Port
            for table in tables:
                if len(table) < 2:
                    continue
                header = [c.lower() for c in table[0]]

                vessel_idx = _find_column(header, [
                    "vessel name", "vessel", "name",
                ])
                owner_idx = _find_column(header, ["owner", "owner name"])
                if vessel_idx is None and owner_idx is None:
                    continue

                for row in table[1:]:
                    if len(row) < 2:
                        continue

                    record: dict[str, Any] = {
                        "vessel_name": (
                            row[vessel_idx] if vessel_idx is not None else ""
                        ),
                        "hin": _safe_get(row, header, ["hin", "hull id number"]),
                        "owner_name": (
                            row[owner_idx] if owner_idx is not None else ""
                        ),
                        "hull_id": _safe_get(row, header, [
                            "hull id", "hull identification",
                        ]),
                        "service": _safe_get(row, header, [
                            "service", "trade", "vessel use",
                        ]),
                        "gross_tons": _safe_get(row, header, [
                            "gross tons", "tonnage", "gross tonnage",
                        ]),
                        "hailing_port": _safe_get(row, header, [
                            "hailing port", "port",
                        ]),
                        "doc_number": _safe_get(row, header, [
                            "document number", "doc no", "documentation number",
                        ]),
                        "source": "USCG",
                        "search_name": owner_name,
                    }
                    if record["vessel_name"] or record["doc_number"]:
                        results.append(record)

            log.info(
                "Coast Guard search for '{name}': {n} vessels found",
                name=owner_name,
                n=len(results),
            )

        except requests.RequestException as exc:
            log.warning(
                "Coast Guard vessel search failed for '{name}': {e}",
                name=owner_name,
                e=str(exc),
            )
            raise ConnectionError(str(exc)) from exc
        except Exception as exc:
            log.error(
                "Unexpected error in Coast Guard search for '{name}': {e}",
                name=owner_name,
                e=str(exc),
            )

        return results

    # ------------------------------------------------------------------
    # Cross-reference with ICIJ offshore officers
    # ------------------------------------------------------------------
    def cross_reference_icij(self, engine: Engine) -> dict[str, Any]:
        """Cross-reference top ICIJ offshore officers against asset registries.

        Pulls the top offshore leak officers from the database, searches
        for their aircraft and vessel registrations, and emits signals
        when matches are found.

        Parameters:
            engine: SQLAlchemy engine (used for querying ICIJ data).

        Returns:
            Dict with counts and match details.
        """
        # Fetch top ICIJ officers from raw_series
        officer_names: list[str] = []
        try:
            with engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT
                        raw_payload->>'officer_name' AS officer_name,
                        raw_payload->>'actor_name' AS actor_name,
                        raw_payload->>'actor_id' AS actor_id
                    FROM raw_series
                    WHERE source_id = (
                        SELECT id FROM source_catalog
                        WHERE name = 'ICIJ_OFFSHORE' LIMIT 1
                    )
                    AND raw_payload->>'officer_name' IS NOT NULL
                    ORDER BY officer_name
                    LIMIT 200
                """)).fetchall()

                officers = [
                    {
                        "officer_name": r[0],
                        "actor_name": r[1] or r[0],
                        "actor_id": r[2] or "",
                    }
                    for r in rows
                    if r[0] and len(r[0].strip()) >= 3
                ]
        except Exception as exc:
            log.warning(
                "Failed to fetch ICIJ officers for cross-reference: {e}",
                e=str(exc),
            )
            officers = []

        if not officers:
            log.info("No ICIJ officers found for asset cross-reference")
            return {
                "status": "NO_OFFICERS",
                "aircraft_found": 0,
                "vessels_found": 0,
            }

        log.info(
            "Cross-referencing {n} ICIJ officers against asset registries",
            n=len(officers),
        )

        total_aircraft: list[dict] = []
        total_vessels: list[dict] = []
        errors: int = 0

        for officer in officers:
            name = officer["officer_name"]

            # Search aircraft
            try:
                aircraft = self.search_aircraft(name)
                for ac in aircraft:
                    ac["icij_actor_name"] = officer["actor_name"]
                    ac["icij_actor_id"] = officer["actor_id"]
                total_aircraft.extend(aircraft)
            except Exception as exc:
                log.debug(
                    "Aircraft search failed for '{name}': {e}",
                    name=name,
                    e=str(exc),
                )
                errors += 1

            # Search vessels
            try:
                vessels = self.search_vessels(name)
                for v in vessels:
                    v["icij_actor_name"] = officer["actor_name"]
                    v["icij_actor_id"] = officer["actor_id"]
                total_vessels.extend(vessels)
            except Exception as exc:
                log.debug(
                    "Vessel search failed for '{name}': {e}",
                    name=name,
                    e=str(exc),
                )
                errors += 1

        # Store results
        stored = self._store_asset_results(total_aircraft, total_vessels)

        result = {
            "status": "SUCCESS",
            "officers_searched": len(officers),
            "aircraft_found": len(total_aircraft),
            "vessels_found": len(total_vessels),
            "raw_series_inserted": stored["raw_count"],
            "signals_emitted": stored["signal_count"],
            "errors": errors,
        }

        if total_aircraft or total_vessels:
            log.warning(
                "ASSET CROSS-REFERENCE: {ac} aircraft and {vs} vessels found "
                "for ICIJ offshore officers!",
                ac=len(total_aircraft),
                vs=len(total_vessels),
            )

        return result

    # ------------------------------------------------------------------
    # Store results in raw_series + signal_sources
    # ------------------------------------------------------------------
    def _store_asset_results(
        self,
        aircraft: list[dict],
        vessels: list[dict],
    ) -> dict[str, int]:
        """Persist asset registry findings to raw_series and signal_sources.

        Parameters:
            aircraft: List of aircraft records.
            vessels: List of vessel records.

        Returns:
            Dict with raw_count and signal_count.
        """
        raw_count = 0
        signal_count = 0
        today = date.today()
        now = datetime.now(timezone.utc)

        with self.engine.begin() as conn:
            # Store aircraft
            for ac in aircraft:
                registration = ac.get("n_number", "UNKNOWN")
                series_id = self._build_series_id(
                    "AIRCRAFT", registration, "registrant",
                )

                if self._row_exists(series_id, today, conn, dedup_hours=720):
                    continue

                try:
                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=today,
                        value=1.0,
                        raw_payload={
                            "asset_type": "aircraft",
                            "n_number": registration,
                            "serial_number": ac.get("serial_number", ""),
                            "manufacturer": ac.get("manufacturer", ""),
                            "model": ac.get("model", ""),
                            "year_mfr": ac.get("year_mfr", ""),
                            "registrant_name": ac.get("registrant_name", ""),
                            "city": ac.get("city", ""),
                            "state": ac.get("state", ""),
                            "status": ac.get("status", ""),
                            "icij_actor_name": ac.get("icij_actor_name", ""),
                            "icij_actor_id": ac.get("icij_actor_id", ""),
                            "search_name": ac.get("search_name", ""),
                            "confidence": "confirmed",
                        },
                    )
                    raw_count += 1
                except Exception as exc:
                    log.debug(
                        "Failed to insert aircraft raw_series: {e}",
                        e=str(exc),
                    )
                    continue

                # Emit signal if this is an ICIJ-linked owner
                if ac.get("icij_actor_id"):
                    signal_count += self._emit_asset_signal(
                        conn, ac, "aircraft", registration, now,
                    )

            # Store vessels
            for v in vessels:
                registration = (
                    v.get("doc_number")
                    or v.get("hin")
                    or v.get("vessel_name", "UNKNOWN")
                )
                series_id = self._build_series_id(
                    "VESSEL", registration, "owner",
                )

                if self._row_exists(series_id, today, conn, dedup_hours=720):
                    continue

                try:
                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=today,
                        value=1.0,
                        raw_payload={
                            "asset_type": "vessel",
                            "vessel_name": v.get("vessel_name", ""),
                            "hin": v.get("hin", ""),
                            "hull_id": v.get("hull_id", ""),
                            "doc_number": v.get("doc_number", ""),
                            "owner_name": v.get("owner_name", ""),
                            "service": v.get("service", ""),
                            "gross_tons": v.get("gross_tons", ""),
                            "hailing_port": v.get("hailing_port", ""),
                            "icij_actor_name": v.get("icij_actor_name", ""),
                            "icij_actor_id": v.get("icij_actor_id", ""),
                            "search_name": v.get("search_name", ""),
                            "confidence": "confirmed",
                        },
                    )
                    raw_count += 1
                except Exception as exc:
                    log.debug(
                        "Failed to insert vessel raw_series: {e}",
                        e=str(exc),
                    )
                    continue

                # Emit signal if this is an ICIJ-linked owner
                if v.get("icij_actor_id"):
                    signal_count += self._emit_asset_signal(
                        conn, v, "vessel", registration, now,
                    )

        log.info(
            "Asset registries stored: {r} raw_series, {s} signals emitted",
            r=raw_count,
            s=signal_count,
        )
        return {"raw_count": raw_count, "signal_count": signal_count}

    # ------------------------------------------------------------------
    # Emit signal for offshore-linked asset owners
    # ------------------------------------------------------------------
    def _emit_asset_signal(
        self,
        conn: Any,
        record: dict[str, Any],
        asset_type: str,
        registration: str,
        signal_date: datetime,
    ) -> int:
        """Emit a signal_sources entry for an offshore-linked asset owner.

        Parameters:
            conn: Active database connection (within a transaction).
            record: Asset record dict.
            asset_type: 'aircraft' or 'vessel'.
            registration: Registration identifier.
            signal_date: Timestamp for the signal.

        Returns:
            1 if signal emitted, 0 if failed.
        """
        actor_id = record.get("icij_actor_id", "")
        actor_name = record.get("icij_actor_name", "")
        owner_name = record.get(
            "registrant_name",
            record.get("owner_name", ""),
        )
        series_id = self._build_series_id(asset_type.upper(), registration, "owner")

        try:
            conn.execute(text("""
                INSERT INTO signal_sources
                    (source_type, source_id, ticker, signal_type,
                     signal_date, signal_value, metadata)
                VALUES
                    (:stype, :sid, :ticker, :signal_type,
                     :sdate, :sval, :meta)
            """), {
                "stype": "asset_registry",
                "sid": series_id,
                "ticker": actor_id,
                "signal_type": "ASSET_OFFSHORE_LINK",
                "sdate": signal_date,
                "sval": 1.0,
                "meta": json.dumps({
                    "asset_type": asset_type,
                    "registration": registration,
                    "owner_name": owner_name,
                    "actor_name": actor_name,
                    "actor_id": actor_id,
                    "confidence": "confirmed",
                    "description": (
                        f"ICIJ offshore officer '{actor_name}' owns "
                        f"{asset_type} (reg: {registration}) under name "
                        f"'{owner_name}'"
                    ),
                }, default=str, ensure_ascii=False),
            })
            return 1
        except Exception as exc:
            log.debug(
                "Failed to emit asset signal for {name}: {e}",
                name=actor_name,
                e=str(exc),
            )
            return 0

    # ------------------------------------------------------------------
    # Build series_id
    # ------------------------------------------------------------------
    @staticmethod
    def _build_series_id(
        asset_type: str,
        registration: str,
        field: str,
    ) -> str:
        """Build the ASSET series_id.

        Parameters:
            asset_type: Asset type (AIRCRAFT, VESSEL).
            registration: Registration number or identifier.
            field: Field descriptor (registrant, owner, etc.).

        Returns:
            Series ID string in format ASSET:{type}:{registration}:{field}.
        """
        type_clean = re.sub(r"[^a-zA-Z0-9_]", "_", asset_type)[:20]
        reg_clean = re.sub(r"[^a-zA-Z0-9_]", "_", registration)[:40]
        field_clean = re.sub(r"[^a-zA-Z0-9_]", "_", field)[:30]
        return f"ASSET:{type_clean}:{reg_clean}:{field_clean}"

    # ------------------------------------------------------------------
    # Full pull: cross-reference ICIJ officers against asset registries
    # ------------------------------------------------------------------
    def pull_all(self) -> dict[str, Any]:
        """Full asset registry ingestion pipeline.

        1. Fetch top ICIJ offshore officers from the database
        2. Search FAA and Coast Guard registries for each officer
        3. Store results and emit signals
        4. Return summary

        Returns:
            Dict with match counts and status.
        """
        log.info("Starting asset registry cross-reference pull")
        return self.cross_reference_icij(self.engine)


# ══════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════

def _find_column(
    header: list[str],
    candidates: list[str],
) -> int | None:
    """Find the index of a column by matching against candidate names.

    Parameters:
        header: Lowercase header row.
        candidates: List of possible column name substrings.

    Returns:
        Column index if found, None otherwise.
    """
    for i, col in enumerate(header):
        for candidate in candidates:
            if candidate in col:
                return i
    return None


def _safe_get(
    row: list[str],
    header: list[str],
    candidates: list[str],
) -> str:
    """Safely extract a cell value by column name candidates.

    Parameters:
        row: Data row (list of cell values).
        header: Lowercase header row.
        candidates: List of possible column name substrings.

    Returns:
        Cell value if found, empty string otherwise.
    """
    idx = _find_column(header, candidates)
    if idx is not None and idx < len(row):
        return row[idx]
    return ""
