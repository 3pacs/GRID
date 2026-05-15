"""Regression guard for the ``CalibrationReport`` dataclass-name collision.

``oracle/calibration.py`` and ``inference/calibration.py`` both defined a
dataclass named ``CalibrationReport`` with divergent field shapes. An
accidental cross-import would type-check but blow up at runtime on attribute
access. The oracle copy is now ``OracleCalibrationReport``; these tests pin
that rename so the collision cannot silently return.
"""

import oracle.calibration as oc
from inference.calibration import CalibrationReport
from oracle.calibration import OracleCalibrationReport, compute_calibration


def test_oracle_report_uses_disambiguated_name():
    assert oc.OracleCalibrationReport is OracleCalibrationReport


def test_old_bare_name_not_re_exported_from_oracle():
    assert not hasattr(oc, "CalibrationReport")


def test_oracle_and_inference_reports_are_distinct_classes():
    assert OracleCalibrationReport is not CalibrationReport


def test_field_shapes_still_differ():
    oracle_fields = set(OracleCalibrationReport.__dataclass_fields__)
    inference_fields = set(CalibrationReport.__dataclass_fields__)
    assert oracle_fields != inference_fields
    # Fields unique to each shape — the reason they must not be merged.
    assert "sharpness" in oracle_fields
    assert "recommendations" in inference_fields


def test_compute_calibration_return_annotation_updated():
    # ``oracle.calibration`` uses ``from __future__ import annotations``,
    # so the annotation is stored as a string.
    assert compute_calibration.__annotations__["return"] == "OracleCalibrationReport"
