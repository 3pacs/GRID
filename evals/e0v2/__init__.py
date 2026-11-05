"""E0 v2: the E0 machinery-calibration benchmark with a calibrated outcome model.

A sibling of ``evals/e0`` (which stays e0-v1 for good: VS1 v8 verifies the
e0-v1 manifest at run time). v2 re-uses the frozen v1 generator, machinery
adapter, runners, scorer, structure and data files by import, verifies the v1
manifest before every run, and adds:

* the EVAL-E0C2 calibrated realistic outcome model (``factor_t_garch_calibrated``),
  fitted on pre-2007-11-01 non-Technology prices outside every VS1 universe and
  window (receipt in ``data/calibration_receipt.json``, sha256 pinned in config);
* a style-exposure sensitivity grid (0, 0.15, 0.30, 0.45);
* the IC 0.01 profile (``ic01``) and a VS1 v8 confirmatory-rule cross-check
  (information only, not a gate);
* v1's known-effect replication restricted to dates before 2007-11-01.

Every file here is hash-pinned in ``MANIFEST.sha256``; a change is a new
version. See ``evals/e0v2/README.md``.
"""

VERSION = "e0-v2"
BUILDS_ON_VERSION = "e0-v1"
BUILDS_ON_MANIFEST_SHA256 = "75489d5091d82af64312f4523c41bdb52b0951a11e017644a022baa08a172822"
