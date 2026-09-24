from __future__ import annotations

from paper_log.gex_levels.placebo import build_placebos, mirror


def test_mirror_formula() -> None:
    assert mirror(105.0, 100.0) == 95.0
    assert mirror(90.0, 100.0) == 110.0
    assert mirror(100.0, 100.0) == 100.0


def test_placebo_kept_when_far_from_every_real_level() -> None:
    real = {"gamma_flip": 500.0, "put_wall": 490.0, "call_wall": 510.0}
    p0 = 500.0
    placebos = build_placebos(real, p0)

    # gamma_flip mirrors to itself (500 -> 500) — collides with gamma_flip.
    assert placebos["gamma_flip"].dropped is True
    assert placebos["gamma_flip"].collided_with == "gamma_flip"

    # put_wall (490) mirrors to 510 -> collides with call_wall (510).
    assert placebos["put_wall"].value == 510.0
    assert placebos["put_wall"].dropped is True
    assert placebos["put_wall"].collided_with == "call_wall"

    # call_wall (510) mirrors to 490 -> collides with put_wall (490).
    assert placebos["call_wall"].value == 490.0
    assert placebos["call_wall"].dropped is True
    assert placebos["call_wall"].collided_with == "put_wall"


def test_placebo_kept_when_walls_are_asymmetric() -> None:
    # put_wall 480 (20 below P0=500) mirrors to 520; call_wall is 530, not
    # within 0.10% of 520 (|520-530|/530 ≈ 1.9%) -> kept.
    real = {"gamma_flip": 500.0, "put_wall": 480.0, "call_wall": 530.0}
    placebos = build_placebos(real, 500.0)

    assert placebos["put_wall"].value == 520.0
    assert placebos["put_wall"].dropped is False
    assert placebos["put_wall"].collided_with is None


def test_collision_threshold_is_exactly_0_10_percent() -> None:
    real = {"gamma_flip": 500.0, "put_wall": 400.0, "call_wall": 600.4}  # 600.4 is 0.10% above (400 mirrored to) 600? let's compute directly
    # put_wall=400 mirrors (P0=500) to 600. call_wall=600.4 -> |600-600.4|/600.4 = 0.0666% < 0.10% -> dropped.
    placebos = build_placebos(real, 500.0)
    assert placebos["put_wall"].value == 600.0
    assert placebos["put_wall"].dropped is True
    assert placebos["put_wall"].collided_with == "call_wall"


def test_collision_threshold_boundary_just_outside_is_kept() -> None:
    # put_wall=400 -> mirrors to 600. call_wall=601.0 -> |600-601|/601 ≈ 0.166% > 0.10% -> kept.
    real = {"gamma_flip": 500.0, "put_wall": 400.0, "call_wall": 601.0}
    placebos = build_placebos(real, 500.0)
    assert placebos["put_wall"].dropped is False


def test_missing_real_level_has_no_placebo() -> None:
    real = {"put_wall": 490.0, "call_wall": 510.0}  # gamma_flip absent
    placebos = build_placebos(real, 500.0)
    assert "gamma_flip" not in placebos
    assert "put_wall" in placebos and "call_wall" in placebos
