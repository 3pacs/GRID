"""Typed read/write access to `people_events` (GD2 of the granular-discovery plan).

Why this exists
----------------
`people_events` (migrations/versions/people_events_20260927.py) is the
canonical, point-in-time, de-duplicated layer the granular-discovery plan
(wha/outputs/GRID-GRANULAR-DISCOVERY-PLAN-20260927.md, section 2.1) puts
above the existing people-linked channels. This module is its store layer,
mirroring the shape of `store/observations.py` for `raw_series`: a frozen
dataclass for one row, a write path that enforces the table's own dedup rule
instead of re-deriving it ad hoc at each call site, and a read path that
never returns a row the caller could not have known about at `as_of`.

This module does not decide *what* is a people event or *how* to compute a
dedup key / known_at for a given channel -- that per-channel logic lives in
`intelligence/people_events_materializer.py`. This module only knows how to
store and retrieve `PeopleEvent` rows once they exist.

PIT correctness
----------------
`read_events`'s `as_of` parameter filters on `known_at <= as_of`, never on
`event_time`. Per CLAUDE.md's non-negotiable PIT rule and the plan's own
anti-look-ahead rule (section 2.2/2.3): an event cannot contribute to a
decision made before the public could see it, no matter how long ago the
underlying act happened.

This module writes only to `people_events`. It never reads or writes
`signal_sources`, `insider_trades`, `congressional_trades`, or any other
existing table.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

CHANNELS = (
    "form4",
    "congress",
    "thirteen_f",
    "gov_contract",
    "gov_contract_qq_aggregate",
    "lobbying",
    "news",
)

KNOWN_AT_BASES = (
    "filing",
    "qq_last_modified",
    "statutory_bound",
    "first_seen",
    "publish",
)

ACTOR_ID_BASES = (
    "owner_cik",
    "bioguide",
    "filer_cik",
    "agency_code",
    "registrant_id",
    "normalized_name",
)

DIRECTIONS = ("buy", "sell", "award", "positive", "negative", "neutral")


@dataclass(frozen=True)
class PeopleEvent:
    """One canonical, de-duplicated people-linked act.

    Field meanings follow plan section 2.1's canonical-event tuple. Every
    field that the table constrains with a CHECK is validated again here at
    construction time, so a bad value fails in Python with a clear message
    instead of surfacing as an opaque `IntegrityError` from the database.
    """

    channel: str
    dedup_key: str
    event_time: datetime
    known_at: datetime
    known_at_basis: str
    actor_id: str
    actor_id_basis: str
    actor_type: str
    source: str
    co_actor_ids: tuple[str, ...] = ()
    entity_ticker: str | None = None
    entity_cik: str | None = None
    security_id: int | None = None
    direction: str | None = None
    size_usd: float | None = None
    source_record_id: str | None = None
    source_refs: tuple[dict[str, Any], ...] = ()
    n_sources: int = 1
    echo_of: int | None = None
    provenance: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.channel not in CHANNELS:
            raise ValueError(f"unknown channel {self.channel!r}; expected one of {CHANNELS}")
        if self.known_at_basis not in KNOWN_AT_BASES:
            raise ValueError(
                f"unknown known_at_basis {self.known_at_basis!r}; expected one of {KNOWN_AT_BASES}"
            )
        if self.actor_id_basis not in ACTOR_ID_BASES:
            raise ValueError(
                f"unknown actor_id_basis {self.actor_id_basis!r}; expected one of {ACTOR_ID_BASES}"
            )
        if self.direction is not None and self.direction not in DIRECTIONS:
            raise ValueError(f"unknown direction {self.direction!r}; expected one of {DIRECTIONS} or None")
        if self.size_usd is not None and self.size_usd < 0:
            raise ValueError(f"size_usd must be >= 0, got {self.size_usd!r}")
        if self.n_sources < 1:
            raise ValueError(f"n_sources must be >= 1, got {self.n_sources!r}")
        if not self.dedup_key:
            raise ValueError("dedup_key must not be empty")
        if not self.actor_id:
            raise ValueError("actor_id must not be empty")


_INSERT_SQL = text("""
    INSERT INTO people_events (
        channel, dedup_key, event_time, known_at, known_at_basis,
        actor_id, actor_id_basis, actor_type, co_actor_ids,
        entity_ticker, entity_cik, security_id, direction, size_usd,
        source, source_record_id, source_refs, n_sources, echo_of, provenance
    ) VALUES (
        :channel, :dedup_key, :event_time, :known_at, :known_at_basis,
        :actor_id, :actor_id_basis, :actor_type, :co_actor_ids,
        :entity_ticker, :entity_cik, :security_id, :direction, :size_usd,
        :source, :source_record_id, CAST(:source_refs AS jsonb), :n_sources,
        :echo_of, CAST(:provenance AS jsonb)
    )
    ON CONFLICT (channel, dedup_key) DO UPDATE SET
        -- The act itself never changes on a re-materialize; only provenance
        -- of *how many sources now agree* does. event_time/known_at/actor
        -- fields are intentionally left untouched so a second source cannot
        -- silently rewrite the first source's known_at.
        source_refs = (
            SELECT jsonb_agg(DISTINCT elem)
            FROM jsonb_array_elements(
                people_events.source_refs || EXCLUDED.source_refs
            ) AS elem
        ),
        n_sources = (
            SELECT count(DISTINCT elem)
            FROM jsonb_array_elements(
                people_events.source_refs || EXCLUDED.source_refs
            ) AS elem
        )
    RETURNING id
""")


def upsert_event(engine: Engine, event: PeopleEvent) -> int:
    """Insert a `PeopleEvent`, or merge it into the existing row for its dedup key.

    A second source describing the same act (same `channel` + `dedup_key`)
    merges into `source_refs`/`n_sources` rather than creating a duplicate
    row or overwriting the first source's `known_at` -- see the ON CONFLICT
    clause above. Returns the row's id either way.
    """
    with engine.begin() as conn:
        row = conn.execute(
            _INSERT_SQL,
            {
                "channel": event.channel,
                "dedup_key": event.dedup_key,
                "event_time": event.event_time,
                "known_at": event.known_at,
                "known_at_basis": event.known_at_basis,
                "actor_id": event.actor_id,
                "actor_id_basis": event.actor_id_basis,
                "actor_type": event.actor_type,
                "co_actor_ids": list(event.co_actor_ids),
                "entity_ticker": event.entity_ticker,
                "entity_cik": event.entity_cik,
                "security_id": event.security_id,
                "direction": event.direction,
                "size_usd": event.size_usd,
                "source": event.source,
                "source_record_id": event.source_record_id,
                "source_refs": json.dumps(list(event.source_refs)),
                "n_sources": event.n_sources,
                "echo_of": event.echo_of,
                "provenance": json.dumps(event.provenance),
            },
        ).fetchone()
    return int(row[0])


_READ_SQL_TEMPLATE = """
    SELECT
        channel, dedup_key, event_time, known_at, known_at_basis,
        actor_id, actor_id_basis, actor_type, co_actor_ids,
        entity_ticker, entity_cik, security_id, direction, size_usd,
        source, source_record_id, source_refs, n_sources, echo_of, provenance
    FROM people_events
    WHERE known_at <= :as_of
    {extra_filters}
    ORDER BY known_at DESC
"""


def read_events(
    engine: Engine,
    as_of: datetime,
    *,
    entity_ticker: str | None = None,
    actor_id: str | None = None,
    channel: str | None = None,
    known_at_after: datetime | None = None,
    exclude_echoes: bool = False,
) -> list[PeopleEvent]:
    """Read `PeopleEvent` rows knowable as of `as_of` (PIT: `known_at <= as_of` only).

    This is the only supported read path onto `people_events`; it never
    filters on `event_time`, so a caller cannot accidentally build a
    look-ahead feature by forgetting to bound the query on `known_at`.
    """
    filters = []
    params: dict[str, Any] = {"as_of": as_of}
    if entity_ticker is not None:
        filters.append("AND entity_ticker = :entity_ticker")
        params["entity_ticker"] = entity_ticker
    if actor_id is not None:
        filters.append("AND actor_id = :actor_id")
        params["actor_id"] = actor_id
    if channel is not None:
        filters.append("AND channel = :channel")
        params["channel"] = channel
    if known_at_after is not None:
        filters.append("AND known_at > :known_at_after")
        params["known_at_after"] = known_at_after
    if exclude_echoes:
        filters.append("AND echo_of IS NULL")

    sql = text(_READ_SQL_TEMPLATE.format(extra_filters="\n".join(filters)))
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    events = []
    for r in rows:
        source_refs = r[16] if isinstance(r[16], list) else json.loads(r[16] or "[]")
        provenance = r[19] if isinstance(r[19], dict) else json.loads(r[19] or "{}")
        events.append(
            PeopleEvent(
                channel=r[0],
                dedup_key=r[1],
                event_time=r[2],
                known_at=r[3],
                known_at_basis=r[4],
                actor_id=r[5],
                actor_id_basis=r[6],
                actor_type=r[7],
                co_actor_ids=tuple(r[8] or ()),
                entity_ticker=r[9],
                entity_cik=r[10],
                security_id=r[11],
                direction=r[12],
                size_usd=r[13],
                source=r[14],
                source_record_id=r[15],
                source_refs=tuple(source_refs),
                n_sources=r[17],
                echo_of=r[18],
                provenance=provenance,
            )
        )
    return events
