"""EDGAR submission re-read for one accession (pre-registration §2.1).

GRID's ``SEC_INSIDER`` ingest keeps the first line per (ticker, insider,
BUY/SELL, transaction date) and the first reporting owner only. The full
submission text (``{accession}.txt``) carries every line, every reporting
owner, the submission type and the EDGAR acceptance time, so the tracker
reads it once per candidate accession.

Reuses the ownership-XML helpers of ``ingestion/altdata/insider_filings.py``
(footnote collection, 10b5-1 detection, flag parsing, ticker normalisation).
The single network seam is :func:`http_get`; tests never touch the network.
"""

from __future__ import annotations

import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Any, Callable

from ingestion.altdata.insider_filings import (
    InsiderFilingsPuller,
    _collect_footnotes,
    _normalize_ticker,
    _xml_flag_is_true,
)
from paper_log.trade_edge.config import EASTERN, SEC_ATTEMPTS, SEC_DELAY_S

_ARCHIVE_RE = re.compile(r"/Archives/edgar/data/(\d+)/(\d{18})/")
_ACCEPT_RE = re.compile(r"<ACCEPTANCE-DATETIME>\s*(\d{14})")
_TYPE_RE = re.compile(r"CONFORMED SUBMISSION TYPE:\s*(\S+)")
_FILED_RE = re.compile(r"FILED AS OF DATE:\s*(\d{8})")
_XML_RE = re.compile(r"<XML>\s*(.*?)\s*</XML>", re.DOTALL | re.IGNORECASE)


class SubmissionError(RuntimeError):
    """The submission could not be fetched or parsed (recorded as ``grid_db``)."""


class FetchDeferred(RuntimeError):
    """The per-run fetch cap is reached: leave the accession for the next run."""


def submission_url(filing_url: str, accession: str) -> str | None:
    """Full-submission URL from the filing document URL GRID stored."""
    m = _ARCHIVE_RE.search(filing_url or "")
    if not m or not accession:
        return None
    cik, folder = m.group(1), m.group(2)
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{folder}/{accession}.txt"


def _text(elem: Any, path: str) -> str | None:
    node = elem.find(path)
    if node is None or node.text is None:
        return None
    value = node.text.strip()
    return value or None


def _float(elem: Any, path: str) -> float | None:
    raw = _text(elem, path)
    if raw is None:
        return None
    try:
        return float(raw.replace(",", ""))
    except ValueError:
        return None


def parse_submission(text: str) -> dict[str, Any]:
    """Header fields plus every code-P non-derivative line of the ownership XML."""
    accept = _ACCEPT_RE.search(text)
    stype = _TYPE_RE.search(text)
    filed = _FILED_RE.search(text)
    xml_match = _XML_RE.search(text)
    if not stype or not filed or not xml_match:
        raise SubmissionError("submission header or ownership XML missing")
    try:
        root = ET.fromstring(xml_match.group(1).strip())
    except ET.ParseError as exc:
        raise SubmissionError(f"ownership XML does not parse: {exc}") from exc

    acceptance_at = None
    if accept:
        acceptance_at = datetime.strptime(accept.group(1), "%Y%m%d%H%M%S").replace(tzinfo=EASTERN)

    issuer_cik = _text(root, "issuer/issuerCik")
    raw_ticker = _text(root, "issuer/issuerTradingSymbol") or ""
    owners = []
    for owner in root.findall("reportingOwner"):
        cik = _text(owner, "reportingOwnerId/rptOwnerCik")
        owners.append(
            {
                "cik": int(cik) if cik and cik.isdigit() else None,
                "name": _text(owner, "reportingOwnerId/rptOwnerName") or "",
            }
        )

    doc_flag = root.find("aff10b5One")
    doc_10b5_1 = _xml_flag_is_true(doc_flag.text if doc_flag is not None else None)
    footnotes = _collect_footnotes(root)
    lines = []
    for txn in root.findall(".//nonDerivativeTransaction"):
        code = (_text(txn, "transactionCoding/transactionCode") or "").upper()
        if code != "P":
            continue
        shares = _float(txn, "transactionAmounts/transactionShares/value")
        price = _float(txn, "transactionAmounts/transactionPricePerShare/value")
        trans = _text(txn, "transactionDate/value")
        lines.append(
            {
                "security_title": _text(txn, "securityTitle/value") or "",
                "trans_date": trans[:10] if trans else None,
                "code": code,
                "acq_disp": (_text(txn, "transactionAmounts/transactionAcquiredDisposedCode/value") or "").upper(),
                "shares": shares,
                "price": price,
                "is_derivative": False,
                "equity_swap": _xml_flag_is_true(_text(txn, "transactionCoding/equitySwapInvolved")),
                "is_10b5_1": InsiderFilingsPuller._transaction_is_10b5_1(txn, footnotes, doc_10b5_1),
            }
        )

    filed_raw = filed.group(1)
    return {
        "acceptance_at": acceptance_at,
        "submission_type": stype.group(1).strip().upper(),
        "filing_date": date(int(filed_raw[:4]), int(filed_raw[4:6]), int(filed_raw[6:8])),
        "issuer_cik": int(issuer_cik) if issuer_cik and issuer_cik.isdigit() else None,
        "issuer_name": _text(root, "issuer/issuerName") or "",
        "ticker": _normalize_ticker(raw_ticker) if raw_ticker else "",
        "owners": owners,
        "lines": lines,
    }


def http_get(url: str, user_agent: str, timeout: float = 30.0) -> str:
    """The only network call in this module."""
    import requests

    resp = requests.get(url, headers={"User-Agent": user_agent}, timeout=timeout)
    if resp.status_code == 429:
        raise SubmissionError("SEC returned 429")
    resp.raise_for_status()
    return resp.text


class SecReader:
    """Rate-limited submission reader with one retry and a per-run cap."""

    def __init__(
        self,
        user_agent: str | None = None,
        *,
        get: Callable[[str, str], str] = http_get,
        max_fetches: int = 300,
        delay_s: float = SEC_DELAY_S,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.user_agent = user_agent if user_agent is not None else os.environ.get("SEC_USER_AGENT", "")
        self._get = get
        self.max_fetches = max_fetches
        self.delay_s = delay_s
        self._sleep = sleep
        self.fetches = 0

    @property
    def enabled(self) -> bool:
        return bool(self.user_agent)

    def read(self, filing_url: str, accession: str) -> dict[str, Any]:
        if not self.enabled:
            raise SubmissionError("SEC_USER_AGENT not set")
        url = submission_url(filing_url, accession)
        if url is None:
            raise SubmissionError("no EDGAR archive path in the stored filing URL")
        last: Exception | None = None
        for _ in range(SEC_ATTEMPTS):
            if self.fetches >= self.max_fetches:
                raise FetchDeferred("per-run SEC fetch cap reached")
            self.fetches += 1
            self._sleep(self.delay_s)
            try:
                return parse_submission(self._get(url, self.user_agent))
            except Exception as exc:  # noqa: BLE001 - recorded on the filing record
                last = exc
        raise SubmissionError(f"{type(last).__name__}: {last}")
