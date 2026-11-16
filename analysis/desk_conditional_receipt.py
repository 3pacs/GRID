"""Receipt-bound conditional development cards. No IO or outcome inference."""
import json
from datetime import datetime, timedelta

from analysis.desk_conditional_cards import conditional_factory_cards
from analysis.desk_factory_receipt import receipt_bound_factory_cards


def receipt_bound_conditional_cards(
    report_raw: bytes, result_raw: bytes, *, expected_result_sha256: str,
    receipt_available_at: datetime | None, as_of: datetime, max_age: timedelta,
) -> dict:
    """Authenticate the existing factory receipt before adapting its same report.

    expected_result_sha256 is independently anchored by the caller, never
    established by this function. The receipt supplies observation time; missing
    availability remains withheld. This verifies no ledger chain, confirmation,
    forward outcome, frozen discovery or point-in-time sector classification.
    """
    base = receipt_bound_factory_cards(
        report_raw, result_raw, expected_result_sha256=expected_result_sha256,
        receipt_available_at=receipt_available_at, as_of=as_of, max_age=max_age,
    )
    # Only parse after the existing caller has checked canonical bytes, digest,
    # record clock, report identity, all trial identities and ranked metrics.
    recorded_at = json.loads(result_raw).get("recorded_at")
    result = conditional_factory_cards(
        report_raw, expected_sha256=base["report_sha256"],
        receipt_id=expected_result_sha256,
        observed_at=datetime.fromisoformat(recorded_at) if recorded_at is not None else None,
        available_at=receipt_available_at, as_of=as_of, max_age=max_age,
    )
    # Preserve the stronger receipt reconciliation rather than accidentally
    # reverting to the raw-report adapter's unverified selection status.
    result["base"] = base
    return result
