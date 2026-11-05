"""Tests for S11-X catalog extension (analysis.ledger_steered_exploration)."""

import json
from dataclasses import asdict

import pandas as pd
import pytest

import analysis.ledger_steered_exploration as lse

FIXED = "2026-09-26T12:00:00+00:00"


def window(k, days=400):
    start = pd.Timestamp("2001-01-01", tz="UTC") + pd.Timedelta(days=k * days)
    return {
        "start": start.isoformat(),
        "split": (start + pd.Timedelta(days=days // 2)).isoformat(),
        "end": (start + pd.Timedelta(days=days - 1)).isoformat(),
    }


def new_ledger(tmp_path=None, q=0.10, catalog=None, ledger_id="s11-test"):
    path = None if tmp_path is None else tmp_path / "ledger.jsonl"
    anchor = None if tmp_path is None else tmp_path / "ledger.anchor.jsonl"
    return lse.Ledger.create(
        path,
        catalog=catalog or lse.synthetic_catalog(),
        anchor=anchor,
        ledger_id=ledger_id,
        q=q,
        recorded_at=FIXED,
    )


def open_ledger(directory):
    return lse.Ledger(directory / "ledger.jsonl", anchor=directory / "ledger.anchor.jsonl")


def test_valid_and_repeated_catalog_extensions(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)
    assert ledger.catalog_sha256 == cat0.sha256()

    cat1 = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag,
    )
    res1 = lse.extend_catalog(
        ledger, cat1, approved_by="owner", approval_ref="RFC-001", recorded_at=FIXED
    )
    assert res1["record"]["kind"] == "catalog_extension"
    assert res1["record"]["prior_catalog_sha256"] == cat0.sha256()
    assert res1["record"]["catalog_sha256"] == cat1.sha256()
    assert res1["record"]["approved_by"] == "owner"
    assert res1["record"]["approval_ref"] == "RFC-001"
    assert res1["record"]["promotion_allowed"] is False
    assert ledger.catalog_sha256 == cat1.sha256()
    assert ledger.effective_catalog == cat1

    cat2 = lse.Catalog(
        families=cat1.families,
        features=cat1.features + ("macro_new",),
        classes=cat1.classes + (("macro_new", "rates"),),
        self_lag=cat1.self_lag,
    )
    res2 = lse.extend_catalog(
        ledger, cat2, approved_by="owner", approval_ref="RFC-002", recorded_at=FIXED
    )
    assert res2["record"]["prior_catalog_sha256"] == cat1.sha256()
    assert res2["record"]["catalog_sha256"] == cat2.sha256()
    assert ledger.catalog_sha256 == cat2.sha256()
    assert ledger.effective_catalog == cat2

    reopened = open_ledger(tmp_path)
    assert reopened.head == ledger.head
    assert reopened.catalog_sha256 == cat2.sha256()
    assert reopened.effective_catalog == cat2


def test_allocation_and_protocol_with_extended_catalog(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)
    alloc0 = lse.allocate(
        ledger, cat0, lse.Policy(budget=21), run_id="r1", windows=window(1), recorded_at=FIXED
    )
    assert alloc0["catalog"] == json.loads(lse.canonical(asdict(cat0)))
    lse.abandon(ledger, "done r1", recorded_at=FIXED)

    cat1 = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag,
    )
    lse.extend_catalog(ledger, cat1, approved_by="owner", approval_ref="RFC-001", recorded_at=FIXED)

    alloc1 = lse.allocate(
        ledger, cat1, lse.Policy(budget=21), run_id="r2", windows=window(2), recorded_at=FIXED
    )
    assert alloc1["catalog"] == json.loads(lse.canonical(asdict(cat1)))
    assert alloc1["record"]["catalog_sha256"] == cat1.sha256()
    assert alloc1["record"]["run_index"] == 2

    proto = lse.protocol_for_allocation(alloc1)
    assert isinstance(proto, lse.Protocol)
    assert set(proto.features) == set(cat1.features)


def test_detached_catalog_payload_cannot_poison_state():
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path=None, catalog=cat0)
    p = ledger.catalog_payload
    p["families"].append("HACK")
    p["classes"].append(["HACK", "rates"])
    assert "HACK" not in ledger.catalog_payload["families"]
    assert ledger.effective_catalog == cat0
    assert ledger.effective_catalog.sha256() == cat0.sha256()

    cat1 = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag,
    )
    lse.extend_catalog(ledger, cat1, approved_by="owner", approval_ref="RFC-001", recorded_at=FIXED)
    p1 = ledger.catalog_payload
    p1["features"].append("HACK2")
    assert "HACK2" not in ledger.catalog_payload["features"]
    assert ledger.effective_catalog == cat1


def test_rejection_prefix_reorder_removal_and_reclassification(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)

    cat_removed = lse.Catalog(
        families=cat0.families[:-1],
        features=cat0.features + ("new_f",),
        classes=cat0.classes + (("new_f", "rates"),),
        self_lag=cat0.self_lag,
    )
    with pytest.raises(ValueError, match="exact prefixes"):
        lse.extend_catalog(ledger, cat_removed, approved_by="owner", approval_ref="R1")

    cat_reordered = lse.Catalog(
        families=tuple(reversed(cat0.families)) + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag,
    )
    with pytest.raises(ValueError, match="exact prefixes"):
        lse.extend_catalog(ledger, cat_reordered, approved_by="owner", approval_ref="R2")

    mutated_classes = list(cat0.classes)
    mutated_classes[0] = (mutated_classes[0][0], "credit")
    cat_mutated = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=tuple(mutated_classes),
        self_lag=cat0.self_lag,
    )
    with pytest.raises(ValueError, match="exact prefixes"):
        lse.extend_catalog(ledger, cat_mutated, approved_by="owner", approval_ref="R3")


def test_rejection_noop_extension(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)
    with pytest.raises(ValueError, match="at least one new family or feature"):
        lse.extend_catalog(ledger, cat0, approved_by="owner", approval_ref="R1")


def test_rejection_self_lag_old_pair(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)

    cat_old_pair = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag + ((cat0.families[0], cat0.features[0]),),
    )
    with pytest.raises(ValueError, match="cannot reclassify"):
        lse.extend_catalog(ledger, cat_old_pair, approved_by="owner", approval_ref="R1")

    cat_valid_pair = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag + (("T1|change|fwd5", cat0.features[0]),),
    )
    res = lse.extend_catalog(ledger, cat_valid_pair, approved_by="owner", approval_ref="R2")
    assert res["record"]["catalog_sha256"] == cat_valid_pair.sha256()


def test_rejection_alias_collision(tmp_path):
    cat_base = lse.Catalog(
        families=("T1|change|fwd1",),
        features=("F",),
        classes=(("F", "rates"),),
        self_lag=(),
    )
    ledger = new_ledger(tmp_path, catalog=cat_base)

    # Collision on feature: F| produces series F, suffix "" which collides with F
    cat_feat_alias = lse.Catalog(
        families=("T1|change|fwd1",),
        features=("F", "F|"),
        classes=(("F", "rates"), ("F|", "rates")),
        self_lag=(),
    )
    with pytest.raises(ValueError, match="aliases existing scientific identity"):
        lse.extend_catalog(ledger, cat_feat_alias, approved_by="owner", approval_ref="R1")

    # Collision on horizon: fwd01 produces horizon_sessions=1 which collides with fwd1
    cat_fam_alias = lse.Catalog(
        families=("T1|change|fwd1", "T1|change|fwd01"),
        features=("F",),
        classes=(("F", "rates"),),
        self_lag=(),
    )
    with pytest.raises(ValueError, match="aliases existing scientific identity"):
        lse.extend_catalog(ledger, cat_fam_alias, approved_by="owner", approval_ref="R2")

    # Collision between two newly introduced families (fwd2 vs fwd02)
    cat_two_new_alias = lse.Catalog(
        families=("T1|change|fwd1", "T1|change|fwd2", "T1|change|fwd02"),
        features=("F",),
        classes=(("F", "rates"),),
        self_lag=(),
    )
    with pytest.raises(ValueError, match="collides with new pair"):
        lse.extend_catalog(ledger, cat_two_new_alias, approved_by="owner", approval_ref="R3")


def test_rejection_open_allocation_and_blank_evidence(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)

    cat1 = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag,
    )
    for blank in ("", "   "):
        with pytest.raises(ValueError, match="approved_by"):
            lse.extend_catalog(ledger, cat1, approved_by=blank, approval_ref="R1")
        with pytest.raises(ValueError, match="approval_ref"):
            lse.extend_catalog(ledger, cat1, approved_by="owner", approval_ref=blank)

    lse.allocate(ledger, cat0, lse.Policy(budget=21), run_id="r1", windows=window(1), recorded_at=FIXED)
    with pytest.raises(ValueError, match="allocation is open"):
        lse.extend_catalog(ledger, cat1, approved_by="owner", approval_ref="R1")


def test_rejection_extra_unexpected_fields_and_schema_types(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)

    cat1 = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag,
    )
    bad_rec = {
        "kind": "catalog_extension",
        "prior_catalog_sha256": cat0.sha256(),
        "catalog": json.loads(lse.canonical(asdict(cat1))),
        "catalog_sha256": cat1.sha256(),
        "approved_by": "owner",
        "approval_ref": "RFC-001",
        "promotion_allowed": False,
        "recorded_at": FIXED,
        "q": 0.10,  # Unexpected field
    }
    with pytest.raises(ValueError, match="unexpected"):
        ledger._append(bad_rec)

    # Bad catalog payload schema (unknown field)
    bad_payload = json.loads(lse.canonical(asdict(cat1)))
    bad_payload["unexpected_field"] = "bad"
    with pytest.raises(ValueError, match="catalog payload keys must be exactly"):
        lse.catalog_from_dict(bad_payload)

    # Bad types: classes with 3 items instead of 2
    bad_type_payload = json.loads(lse.canonical(asdict(cat1)))
    bad_type_payload["classes"][0] = ["alpha1|raw", "rates", "extra"]
    with pytest.raises(ValueError, match="must be a 2-tuple"):
        lse.catalog_from_dict(bad_type_payload)


def test_atomic_preappend_rejection(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)
    path = tmp_path / "ledger.jsonl"
    anchor_path = tmp_path / "ledger.anchor.jsonl"
    original_bytes = path.read_bytes()
    original_anchor_bytes = anchor_path.read_bytes()

    cat_invalid = lse.Catalog(
        families=cat0.families[:-1],
        features=cat0.features + ("f_new",),
        classes=cat0.classes + (("f_new", "rates"),),
        self_lag=cat0.self_lag,
    )
    with pytest.raises(ValueError):
        lse.extend_catalog(ledger, cat_invalid, approved_by="owner", approval_ref="R1")

    assert path.read_bytes() == original_bytes
    assert anchor_path.read_bytes() == original_anchor_bytes
    reopened = open_ledger(tmp_path)
    assert reopened.head == ledger.head


def test_forged_canonical_hash_chained_semantic_records(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)
    path = tmp_path / "ledger.jsonl"

    cat_reordered = lse.Catalog(
        families=tuple(reversed(cat0.families)) + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag,
    )
    record = {
        "kind": "catalog_extension",
        "seq": 1,
        "prev_sha256": ledger.head,
        "prior_catalog_sha256": cat0.sha256(),
        "catalog": json.loads(lse.canonical(asdict(cat_reordered))),
        "catalog_sha256": cat_reordered.sha256(),
        "approved_by": "attacker",
        "approval_ref": "fake",
        "promotion_allowed": False,
        "recorded_at": FIXED,
    }
    line = lse.canonical(record)
    with path.open("ab") as stream:
        stream.write(line + b"\n")
    ledger.anchor.append(ledger.genesis["ledger_id"], 1, lse.sha256_bytes(line))

    with pytest.raises(ValueError, match="exact prefixes"):
        open_ledger(tmp_path)


def test_replay_tracks_single_open_allocation_and_stale_pin(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)
    alloc0 = lse.allocate(
        ledger, cat0, lse.Policy(budget=21), run_id="r1", windows=window(1), recorded_at=FIXED
    )
    path = tmp_path / "ledger.jsonl"

    # Replaying two open allocations in a row must fail
    alloc_dup = {
        "kind": "allocation",
        "seq": 2,
        "prev_sha256": ledger.head,
        "catalog_sha256": cat0.sha256(),
        "run_id": "r2",
        "run_index": 2,
        "alpha": lse.run_alpha(2, ledger.q),
        "alpha_spent_after": alloc0["record"]["alpha"] + lse.run_alpha(2, ledger.q),
        "q": ledger.q,
        "spending": lse.SPENDING,
        "within_run": lse.WITHIN_RUN,
        "windows": window(2),
        "policy": asdict(lse.Policy(budget=21)),
        "arms": {},
        "trials": [],
        "trial_count": 0,
        "min_perms_for_first_step": 100,
        "promotion_allowed": False,
        "recorded_at": FIXED,
    }
    line = lse.canonical(alloc_dup)
    with path.open("ab") as stream:
        stream.write(line + b"\n")
    ledger.anchor.append(ledger.genesis["ledger_id"], 2, lse.sha256_bytes(line))

    with pytest.raises(ValueError, match="second allocation opened"):
        open_ledger(tmp_path)


def test_e3_no_catalog_genesis_compatible():
    e3_genesis = {
        "kind": "genesis",
        "seq": 0,
        "prev_sha256": None,
        "ledger_id": "e3-test",
        "storage": "memory",
        "promotion_allowed": False,
    }
    line = lse.canonical(e3_genesis)
    records = lse.verify_chain([line])
    assert len(records) == 1

    ext_line = lse.canonical({
        "kind": "catalog_extension",
        "seq": 1,
        "prev_sha256": lse.sha256_bytes(line),
        "prior_catalog_sha256": "fake",
        "catalog": {},
        "catalog_sha256": "fake",
        "approved_by": "owner",
        "approval_ref": "RFC-001",
        "promotion_allowed": False,
        "recorded_at": FIXED,
    })
    with pytest.raises(ValueError, match="genesis has no catalog"):
        lse.verify_chain([line, ext_line])


def test_returned_extension_record_mutation_does_not_poison_ledger(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)
    cat1 = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag,
    )
    res = lse.extend_catalog(
        ledger, cat1, approved_by="owner", approval_ref="RFC-001", recorded_at=FIXED
    )
    res["record"]["kind"] = "tampered"
    res["record"]["catalog"] = {}
    res["record"]["catalog_sha256"] = "tampered_sha"

    assert ledger._records[-1]["kind"] == "catalog_extension"
    assert ledger._records[-1]["catalog_sha256"] == cat1.sha256()
    assert ledger.catalog_sha256 == cat1.sha256()

    alloc = lse.allocate(
        ledger, cat1, lse.Policy(budget=21), run_id="r1", windows=window(1), recorded_at=FIXED
    )
    assert alloc["record"]["kind"] == "allocation"
    ledger.verify()


def test_invalid_recorded_at_rejected_pre_append_and_replay(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)
    cat1 = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag,
    )
    head_before = ledger.head
    lines_before = len(ledger._lines)

    invalid_timestamps = ["", "   ", "2026-01-01T12:00:00", 12345, [], "invalid-date"]
    for ts in invalid_timestamps:
        with pytest.raises(ValueError):
            lse.extend_catalog(
                ledger, cat1, approved_by="owner", approval_ref="RFC-001", recorded_at=ts
            )
        assert ledger.head == head_before
        assert len(ledger._lines) == lines_before


def test_append_promotion_allowed_true_precedence(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)
    with pytest.raises(ValueError, match="the ledger never allows promotion"):
        ledger._append({"kind": "run_result", "promotion_allowed": True})


def test_allocate_catalog_mismatch_messages(tmp_path):
    cat0 = lse.synthetic_catalog()
    ledger = new_ledger(tmp_path, catalog=cat0)
    cat_mismatch = lse.Catalog(
        families=cat0.families + ("T1|change|fwd5",),
        features=cat0.features,
        classes=cat0.classes,
        self_lag=cat0.self_lag,
    )
    with pytest.raises(ValueError, match="catalog differs from the one frozen at the ledger's genesis"):
        lse.allocate(
            ledger, cat_mismatch, lse.Policy(budget=21), run_id="r1", windows=window(1), recorded_at=FIXED
        )

    lse.extend_catalog(
        ledger, cat_mismatch, approved_by="owner", approval_ref="RFC-001", recorded_at=FIXED
    )
    with pytest.raises(ValueError, match="catalog differs from the effective catalog of the ledger"):
        lse.allocate(
            ledger, cat0, lse.Policy(budget=21), run_id="r2", windows=window(2), recorded_at=FIXED
        )
