"""Extract URLs, markdown links, and DOIs from LLM-generated text."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractedRef:
    """A reference extracted from LLM-generated text."""

    url: str
    anchor_text: str | None
    position: int  # char offset in original text
    ref_type: str  # "markdown_link", "raw_url", "doi"


# ── Patterns ────────────────────────────────────────────────────────────────

_MARKDOWN_LINK = re.compile(r"\[([^\]]+)\]\((https?://[^\)]+)\)")
_RAW_URL = re.compile(r"(?<!\()(https?://[^\s\)\]>\"']+)")
_DOI = re.compile(r"doi:(10\.\d{4,}/[^\s]+)", re.IGNORECASE)


def extract_refs(text: str) -> list[ExtractedRef]:
    """Extract all URLs, markdown links, and DOIs from text.

    Returns deduplicated refs sorted by position in text.
    """
    refs: list[ExtractedRef] = []
    seen_urls: set[str] = set()

    # 1. Markdown links: [text](url)
    for m in _MARKDOWN_LINK.finditer(text):
        url = m.group(2).rstrip(".")
        if url not in seen_urls:
            refs.append(ExtractedRef(
                url=url,
                anchor_text=m.group(1),
                position=m.start(),
                ref_type="markdown_link",
            ))
            seen_urls.add(url)

    # 2. DOIs: doi:10.xxxx/... → https://doi.org/...
    for m in _DOI.finditer(text):
        doi_url = f"https://doi.org/{m.group(1)}"
        if doi_url not in seen_urls:
            refs.append(ExtractedRef(
                url=doi_url,
                anchor_text=None,
                position=m.start(),
                ref_type="doi",
            ))
            seen_urls.add(doi_url)

    # 3. Raw URLs (not already captured as markdown links)
    for m in _RAW_URL.finditer(text):
        url = m.group(0).rstrip(".,;:)")
        if url not in seen_urls:
            refs.append(ExtractedRef(
                url=url,
                anchor_text=None,
                position=m.start(),
                ref_type="raw_url",
            ))
            seen_urls.add(url)

    refs.sort(key=lambda r: r.position)
    return refs
