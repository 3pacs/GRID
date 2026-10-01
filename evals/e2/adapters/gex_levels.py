"""Adapter for the GEX-levels v1 paper log (``/data/grid/paper_log/gex_levels_v1/``).

The log is frozen and pre-registered (``docs/paper_log/gex-levels-v1-preregistration.md``,
sha256 ce9b55e2..., code pinned at 07e1fc16). E2 only READS it: one
``preopen`` record per session (levels, regime, P0; written before 09:30 ET)
and one ``postclose`` record (5-minute bars, OHLC, reaches, the H3 trade).

Normalized predictions per valid pre-open session, for arm in (real, placebo):

* ``gex.level_hold.v1`` -- one per tested level present (gamma_flip,
  put_wall, call_wall; placebo levels that were not dropped): "if first
  reached today, it holds". Family ``gex_level_hold_<arm>``. Resolved from
  the post-close ``reaches``: reached -> hit = held; gap_through and none ->
  void (the pre-registration's H2 definitions).
* ``gex.h3_net_pnl.v1`` -- one per session: the pre-registered H3 rule trade
  on the arm's walls. Family ``gex_h3_<arm>``. Resolved from the post-close
  ``h3_trade``: triggered -> raw entry/exit/direction, net of the E2 cost
  model; not triggered -> void ``no_trigger``.

PIT checks re-done here from the log itself: the pre-open must be written
before 09:30 ET on its session; the post-close must follow it in the chain,
be written at or before the run instant, and its bars and OHLC must have
been fetched at or after the session close. H1 (regime vs range) is a
single cross-session regression with no per-prediction score; E2 v1 does
not score it -- the stream's own one-time look at 60 sessions governs it.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

from evals.e2.adapters import SourceView, build_view
from evals.e2.chain import verify_source_chain
from evals.e2.records import LookAheadError, iso, parse_ts, session_close_utc, session_open_utc

STREAM = "gex_levels_v1"
LOG_FILENAME = "gex_levels_v1.jsonl"
LEVELS = ("gamma_flip", "put_wall", "call_wall")
ARMS = ("real", "placebo")
GRACE = timedelta(days=5)


class GexLevelsAdapter:
    stream = STREAM

    def __init__(self, log_dir: Path, stream_rules: dict) -> None:
        self.path = Path(log_dir) / LOG_FILENAME
        self.rules = stream_rules

    def load(self, now: datetime) -> SourceView:
        pairs = verify_source_chain(self.path, prereg_sha256=self.rules["prereg_sha256"])
        view = build_view(STREAM, self.path, pairs, now)
        sessions: dict[str, dict] = {}
        for i, record in enumerate(view.records):
            kind, session = record.get("kind"), record.get("session_date")
            if kind not in ("preopen", "postclose") or not session:
                continue
            slot = sessions.setdefault(session, {})
            if kind in slot:
                slot.setdefault("duplicates", []).append(i)  # first record wins; duplicates reported
                continue
            slot[kind] = i
        view.extra["sessions"] = sessions
        return view

    # -- predictions -------------------------------------------------------------------

    def _preopen_valid(self, view: SourceView, index: int) -> str | None:
        """None if the pre-open counts, else the reason it does not."""
        record = view.records[index]
        if record.get("excluded"):
            return f"preopen_excluded:{record.get('exclusion_reason')}"
        session = date.fromisoformat(record["session_date"])
        if parse_ts(record["run_at"]) >= session_open_utc(session):
            return "late_preopen"
        return None

    def predictions(self, view: SourceView) -> list[dict]:
        out = []
        for session, slot in sorted(view.extra["sessions"].items()):
            if "preopen" not in slot or self._preopen_valid(view, slot["preopen"]) is not None:
                continue
            i = slot["preopen"]
            pre = view.records[i]
            day = date.fromisoformat(session)
            receipt = view.receipt(i, writer_code_sha=pre.get("code_sha"),
                                   witness="stream hash chain + self-reported run_at; vault mirror of the log")
            base = {
                "stream": STREAM,
                "sector": self.rules.get("sector"),
                "issued_at": iso(parse_ts(pre["run_at"])),
                "log_receipt": receipt,
                "target": {"instrument": self.rules["instrument"],
                           "instrument_class": self.rules["instrument_class"],
                           "reference_price_p0": (pre.get("p0") or {}).get("price")},
                "horizon": {"label": "session_close", "session_date": session,
                            "ends_at": iso(session_close_utc(day))},
                # earliest possible close (a 13:00 ET early close): conservative for timing claims
                "outcome_not_before": iso(session_close_utc(day, early=True)),
                "unit": None,
            }
            regime = (pre.get("engine") or {}).get("regime")
            for arm in ARMS:
                levels = (pre.get("levels") or {}).get(arm) or {}
                present = {}
                for name in LEVELS:
                    value = _level_value(levels, name, arm)
                    if value is None:
                        continue
                    present[name] = value
                    out.append({
                        **base,
                        "family": f"gex_level_hold_{arm}",
                        "prediction_id": f"{STREAM}:{session}:{arm}:hold:{name}",
                        "call": {"kind": "conditional_binary", "condition": "first_reach_today",
                                 "predicts": "held", "level_name": name, "level": value, "arm": arm},
                        "rule_id": "gex.level_hold.v1",
                    })
                out.append({
                    **base,
                    "family": f"gex_h3_{arm}",
                    "prediction_id": f"{STREAM}:{session}:{arm}:h3",
                    "call": {"kind": "rule_trade", "rule": "gex_levels_v1_h3", "regime": regime,
                             "walls": {k: v for k, v in present.items() if k != "gamma_flip"}, "arm": arm},
                    "rule_id": "gex.h3_net_pnl.v1",
                })
        return out

    # -- resolution --------------------------------------------------------------------

    def resolve(self, view: SourceView, pred: dict, now: datetime) -> dict | None:
        session = pred["horizon"]["session_date"]
        day = date.fromisoformat(session)
        slot = view.extra["sessions"].get(session, {})
        if "postclose" not in slot or slot["postclose"] < slot.get("preopen", -1):
            if now >= session_close_utc(day) + GRACE:
                return _void("no_postclose", now)
            return None
        post = view.records[slot["postclose"]]
        early = bool((post.get("bars") or {}).get("early_close"))
        close_at = session_close_utc(day, early=early)
        written = parse_ts(post["run_at"])
        if written > now:  # build_view already drops these; belt and braces
            return None
        receipt = {
            "price_source": "yfinance SPY 5-minute regular-session bars and session OHLC (unadjusted), "
                            f"logged by paper_log.gex_levels at code {post.get('code_sha')}",
            "postclose_line_sha256": view.line_sha256[slot["postclose"]],
            "postclose_line_index": slot["postclose"],
            "postclose_run_at": post.get("run_at"),
            "bars_fetched_at": (post.get("bars") or {}).get("fetched_at"),
            "ohlc_fetched_at": (post.get("session_ohlc") or {}).get("fetched_at"),
            "session_close_at": iso(close_at),
        }
        if post.get("excluded"):
            return {**_void(f"postclose_excluded:{post.get('exclusion_reason')}", written), "receipt": receipt}
        for key in ("bars_fetched_at", "ohlc_fetched_at"):
            fetched = receipt[key]
            if fetched is None:
                return {**_void("postclose_without_fetch_time", written), "receipt": receipt}
            if parse_ts(fetched) < close_at:
                raise LookAheadError(f"{pred['prediction_id']}: post-close {key} {fetched} precedes the "
                                     f"session close {iso(close_at)}")
        available = max(parse_ts(receipt["bars_fetched_at"]), parse_ts(receipt["ohlc_fetched_at"]))
        arm = pred["call"]["arm"]
        if pred["rule_id"] == "gex.level_hold.v1":
            reach = ((post.get("reaches") or {}).get(arm) or {}).get(pred["call"]["level_name"]) or {}
            status = reach.get("status")
            if status == "reached" and isinstance(reach.get("held"), bool):
                return {"status": "resolved", "reason": None, "available_at": iso(available), "receipt": receipt,
                        "outcome": {"reach_status": status, "held": reach["held"], "bar_time": reach.get("bar_time"),
                                    "side": reach.get("side")}}
            reason = {"gap_through": "gap_through", "none": "not_reached"}.get(status, "reach_missing")
            return {"status": "void", "reason": reason, "available_at": iso(available), "receipt": receipt,
                    "outcome": {"reach_status": status}}
        trade = ((post.get("h3_trade") or {}).get(arm)) or {}
        if not trade.get("triggered"):
            return {"status": "void", "reason": "no_trigger", "available_at": iso(available), "receipt": receipt,
                    "outcome": {"triggered": False, "regime_rule": trade.get("regime_rule")}}
        direction = {"long": 1, "short": -1}.get(trade.get("direction"))
        entry, exit_ = trade.get("raw_entry_price"), trade.get("raw_exit_price")
        if direction is None or not entry or not exit_:
            return {"status": "void", "reason": "trade_fields_missing", "available_at": iso(available),
                    "receipt": receipt, "outcome": {"triggered": True}}
        return {"status": "resolved", "reason": None, "available_at": iso(available), "receipt": receipt,
                "outcome": {"triggered": True, "side": direction, "entry_price": float(entry),
                            "exit_price": float(exit_), "trigger_time": trade.get("trigger_time"),
                            "trigger_level_name": trade.get("trigger_level_name"),
                            "regime_rule": trade.get("regime_rule"),
                            "stream_return_after_prereg_cost": trade.get("return_pct")}}

    def unit_scores(self, view: SourceView, ledger_state: dict, now: datetime) -> list[dict]:
        return []

    def activity(self, view: SourceView) -> dict:
        sessions = view.extra["sessions"]
        excluded: dict[str, int] = {}
        valid = 0
        for _, slot in sorted(sessions.items()):
            reason = None
            if "preopen" not in slot:
                reason = "no_preopen"
            else:
                reason = self._preopen_valid(view, slot["preopen"])
            if reason is None and "postclose" in slot and view.records[slot["postclose"]].get("excluded"):
                reason = f"postclose_excluded:{view.records[slot['postclose']].get('exclusion_reason')}"
            if reason is None and "postclose" in slot:
                valid += 1
            elif reason is not None:
                excluded[reason] = excluded.get(reason, 0) + 1
        evaluation_at = int(self.rules.get("evaluation_after_valid_sessions", 0))
        return {
            "sessions_logged": len(sessions),
            "valid_sessions": valid,
            "excluded_sessions": excluded,
            "duplicate_records": sum(len(s.get("duplicates", [])) for s in sessions.values()),
            "interim": valid < evaluation_at,
            "evaluation_after_valid_sessions": evaluation_at,
        }


def _level_value(levels: dict, name: str, arm: str) -> float | None:
    if arm == "real":
        if levels.get(f"{name}_missing"):
            return None
        value = levels.get(name)
        return float(value) if isinstance(value, (int, float)) else None
    entry = levels.get(name)
    if not isinstance(entry, dict) or entry.get("dropped") or not isinstance(entry.get("value"), (int, float)):
        return None
    return float(entry["value"])


def _void(reason: str, at: datetime) -> dict:
    return {"status": "void", "reason": reason, "available_at": iso(at), "receipt": None, "outcome": None}
