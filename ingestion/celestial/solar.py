"""GRID -- Solar activity data ingestion.

Pulls solar activity data from NOAA Space Weather Prediction Center (SWPC)
and computes solar cycle features.  This has actual scientific backing --
geomagnetic storms correlate with market returns in academic literature
(Krivelyova & Robotti, 2003; Kamstra et al., 2003).

Data sources:
- NOAA SWPC: https://services.swpc.noaa.gov/json/
- Kp index: https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json
- Solar wind: https://services.swpc.noaa.gov/products/solar-wind/plasma-7-day.json
- Sunspot: https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json

Features generated:
- sunspot_number: daily sunspot count
- solar_flux_10_7cm: F10.7 solar radio flux (SFU)
- geomagnetic_kp_index: planetary Kp geomagnetic index (0-9)
- geomagnetic_ap_index: planetary Ap index (derived from Kp)
- solar_wind_speed: km/s
- solar_storm_probability: NOAA 3-day forecast probability (0-100)
- solar_cycle_phase: position in ~11-year solar cycle (0-1)

No API key required -- all NOAA SWPC data is public.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# -- Solar cycle constants ---------------------------------------------------
# Solar cycle 25 started December 2019, predicted peak ~July 2025
_CYCLE_25_START = date(2019, 12, 1)
_CYCLE_LENGTH_YEARS = 11.0  # average solar cycle length
_CYCLE_LENGTH_DAYS = _CYCLE_LENGTH_YEARS * 365.25

# -- Kp to Ap conversion table (standard NOAA mapping) ----------------------
_KP_TO_AP = {
    0: 0, 0.33: 2, 0.67: 3, 1: 4, 1.33: 5, 1.67: 6, 2: 7,
    2.33: 9, 2.67: 12, 3: 15, 3.33: 18, 3.67: 22, 4: 27,
    4.33: 32, 4.67: 39, 5: 48, 5.33: 56, 5.67: 67, 6: 80,
    6.33: 94, 6.67: 111, 7: 132, 7.33: 154, 7.67: 179, 8: 207,
    8.33: 236, 8.67: 300, 9: 400,
}


def _kp_to_ap(kp: float) -> float:
    """Convert Kp index to approximate Ap index."""
    # Find the closest Kp value in the conversion table
    closest_kp = min(_KP_TO_AP.keys(), key=lambda k: abs(k - kp))
    return float(_KP_TO_AP[closest_kp])


def _solar_cycle_phase(d: date) -> float:
    """Return position in the ~11-year solar cycle (0-1).

    Uses Solar Cycle 25 start date as reference.
    """
    days_into_cycle = (d - _CYCLE_25_START).days
    # Handle dates before cycle 25 by wrapping
    phase = (days_into_cycle % _CYCLE_LENGTH_DAYS) / _CYCLE_LENGTH_DAYS
    return phase


class SolarActivityPuller(BasePuller):
    """Pulls solar activity data from NOAA SWPC.

    Uses public NOAA Space Weather Prediction Center JSON APIs.
    No API key required.

    Attributes:
        SOURCE_NAME: 'NOAA_SWPC' in source_catalog.
    """

    SOURCE_NAME = "NOAA_SWPC"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://services.swpc.noaa.gov",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 94,
    }

    _SERIES = [
        "sunspot_number",
        "solar_flux_10_7cm",
        "geomagnetic_kp_index",
        "geomagnetic_ap_index",
        "solar_wind_speed",
        "solar_storm_probability",
        "solar_cycle_phase",
    ]

    # NOAA SWPC API endpoints
    _KP_URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
    _SOLAR_WIND_URL = "https://services.swpc.noaa.gov/products/solar-wind/plasma-7-day.json"
    _SUNSPOT_URL = "https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json"

    def __init__(self, db_engine: Engine, lookback_days: int = 30) -> None:
        super().__init__(db_engine)
        self.lookback_days = lookback_days

    @retry_on_failure(max_attempts=3, retryable_exceptions=(ConnectionError, TimeoutError, OSError))
    def _fetch_json(self, url: str, timeout: int = 30) -> Any:
        """Fetch JSON from a URL with retry logic.

        Parameters:
            url: The URL to fetch.
            timeout: Request timeout in seconds.

        Returns:
            Parsed JSON data.

        Raises:
            ConnectionError: If the request fails after retries.
        """
        import requests

        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def _pull_kp_index(self) -> dict[date, float]:
        """Fetch recent Kp index values from NOAA SWPC.

        Returns:
            dict mapping date to daily average Kp value.
        """
        try:
            data = self._fetch_json(self._KP_URL)
        except Exception as exc:
            log.warning("Failed to fetch Kp index: {e}", e=str(exc))
            return {}

        # Data format: [[time_tag, Kp, Kp_fraction, a_running, station_count], ...]
        # First row is header
        kp_by_date: dict[date, list[float]] = {}
        for row in data[1:]:  # skip header
            try:
                time_tag = row[0]
                kp_val = float(row[1])
                dt = datetime.fromisoformat(time_tag.replace("Z", "+00:00"))
                d = dt.date()
                kp_by_date.setdefault(d, []).append(kp_val)
            except (ValueError, IndexError, TypeError):
                continue

        # Average Kp per day
        return {d: sum(vals) / len(vals) for d, vals in kp_by_date.items() if vals}

    def _pull_solar_wind(self) -> dict[date, float]:
        """Fetch recent solar wind speed from NOAA SWPC.

        Returns:
            dict mapping date to daily average solar wind speed (km/s).
        """
        try:
            data = self._fetch_json(self._SOLAR_WIND_URL)
        except Exception as exc:
            log.warning("Failed to fetch solar wind data: {e}", e=str(exc))
            return {}

        # Data format: [[time_tag, density, speed, temperature], ...]
        wind_by_date: dict[date, list[float]] = {}
        for row in data[1:]:  # skip header
            try:
                time_tag = row[0]
                speed = row[2]
                if speed is None or speed == "":
                    continue
                speed_val = float(speed)
                if speed_val <= 0:
                    continue
                dt = datetime.fromisoformat(time_tag.replace("Z", "+00:00"))
                d = dt.date()
                wind_by_date.setdefault(d, []).append(speed_val)
            except (ValueError, IndexError, TypeError):
                continue

        return {d: sum(vals) / len(vals) for d, vals in wind_by_date.items() if vals}

    def _pull_sunspot_data(self) -> dict[str, dict[date, float]]:
        """Fetch observed solar cycle indices (sunspot number, F10.7 flux).

        Returns:
            dict with 'sunspot' and 'flux' keys mapping date to values.
        """
        try:
            data = self._fetch_json(self._SUNSPOT_URL)
        except Exception as exc:
            log.warning("Failed to fetch sunspot data: {e}", e=str(exc))
            return {"sunspot": {}, "flux": {}}

        sunspot_by_date: dict[date, float] = {}
        flux_by_date: dict[date, float] = {}

        for record in data:
            try:
                # Records have time-tag, ssn (sunspot number), f10.7 flux, etc.
                time_tag = record.get("time-tag", "")
                ssn = record.get("ssn")
                flux = record.get("f10.7")

                # Parse date (format: "YYYY-MM")
                if len(time_tag) == 7:
                    # Monthly data -- assign to 1st of month
                    d = date(int(time_tag[:4]), int(time_tag[5:7]), 1)
                elif len(time_tag) >= 10:
                    d = date.fromisoformat(time_tag[:10])
                else:
                    continue

                if ssn is not None:
                    try:
                        sunspot_by_date[d] = float(ssn)
                    except (ValueError, TypeError):
                        pass
                if flux is not None:
                    try:
                        flux_by_date[d] = float(flux)
                    except (ValueError, TypeError):
                        pass
            except (ValueError, KeyError, TypeError):
                continue

        return {"sunspot": sunspot_by_date, "flux": flux_by_date}

    def pull_all(self, start_date: date | None = None) -> dict[str, Any]:
        """Fetch and store solar activity features.

        Pulls from NOAA SWPC APIs and computes derived features.
        For dates without API data, computes deterministic features
        (solar_cycle_phase) only.

        Parameters:
            start_date: Override for the first date.
                        Defaults to today minus ``lookback_days``.

        Returns:
            dict with 'rows_inserted' and 'status' keys.
        """
        today = date.today()
        if start_date is None:
            start_date = today - timedelta(days=self.lookback_days)

        # Fetch all available API data
        kp_data = self._pull_kp_index()
        wind_data = self._pull_solar_wind()
        solar_data = self._pull_sunspot_data()
        sunspot_data = solar_data.get("sunspot", {})
        flux_data = solar_data.get("flux", {})

        rows_inserted = 0
        errors = 0

        with self.engine.begin() as conn:
            d = start_date
            while d <= today:
                try:
                    if self._row_exists("solar_cycle_phase", d, conn, dedup_hours=23):
                        d += timedelta(days=1)
                        continue

                    # Solar cycle phase is always computable
                    phase = _solar_cycle_phase(d)
                    self._insert_raw(
                        conn=conn,
                        series_id="solar_cycle_phase",
                        obs_date=d,
                        value=round(phase, 6),
                        raw_payload={"source": "computed", "date": d.isoformat()},
                    )
                    rows_inserted += 1

                    # Kp and derived Ap
                    if d in kp_data:
                        kp = kp_data[d]
                        self._insert_raw(
                            conn=conn,
                            series_id="geomagnetic_kp_index",
                            obs_date=d,
                            value=round(kp, 2),
                            raw_payload={"source": "noaa_swpc", "date": d.isoformat()},
                        )
                        rows_inserted += 1

                        ap = _kp_to_ap(kp)
                        self._insert_raw(
                            conn=conn,
                            series_id="geomagnetic_ap_index",
                            obs_date=d,
                            value=ap,
                            raw_payload={"source": "noaa_swpc_derived", "date": d.isoformat()},
                        )
                        rows_inserted += 1

                    # Solar wind speed
                    if d in wind_data:
                        self._insert_raw(
                            conn=conn,
                            series_id="solar_wind_speed",
                            obs_date=d,
                            value=round(wind_data[d], 1),
                            raw_payload={"source": "noaa_swpc", "date": d.isoformat()},
                        )
                        rows_inserted += 1

                    # Sunspot number (monthly data -- only on 1st of month or matching date)
                    # Try exact date first, then 1st of month for monthly data
                    ssn = sunspot_data.get(d) or sunspot_data.get(
                        date(d.year, d.month, 1)
                    )
                    if ssn is not None:
                        self._insert_raw(
                            conn=conn,
                            series_id="sunspot_number",
                            obs_date=d,
                            value=float(ssn),
                            raw_payload={"source": "noaa_swpc", "date": d.isoformat()},
                        )
                        rows_inserted += 1

                    # F10.7 flux
                    flux = flux_data.get(d) or flux_data.get(
                        date(d.year, d.month, 1)
                    )
                    if flux is not None:
                        self._insert_raw(
                            conn=conn,
                            series_id="solar_flux_10_7cm",
                            obs_date=d,
                            value=float(flux),
                            raw_payload={"source": "noaa_swpc", "date": d.isoformat()},
                        )
                        rows_inserted += 1

                    # Storm probability: use Kp >= 5 as a simple proxy
                    # (true forecast would require the 3-day forecast endpoint)
                    if d in kp_data:
                        kp = kp_data[d]
                        # Simple probability mapping: Kp 0-9 -> storm probability
                        storm_prob = min(100.0, max(0.0, (kp - 3.0) * 25.0))
                        self._insert_raw(
                            conn=conn,
                            series_id="solar_storm_probability",
                            obs_date=d,
                            value=round(storm_prob, 1),
                            raw_payload={
                                "source": "noaa_swpc_derived",
                                "kp": kp,
                                "date": d.isoformat(),
                            },
                        )
                        rows_inserted += 1

                except Exception as exc:
                    errors += 1
                    log.warning(
                        "SolarActivityPuller error on {d}: {e}", d=d, e=str(exc)
                    )
                d += timedelta(days=1)

        status = "SUCCESS" if errors == 0 else "PARTIAL"
        log.info(
            "SolarActivityPuller complete -- {rows} rows, {errs} errors",
            rows=rows_inserted, errs=errors,
        )
        return {"rows_inserted": rows_inserted, "status": status}
