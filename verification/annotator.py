"""Output annotator — clean/replace bad references in LLM text.

Replaces dead URLs with Wayback Machine archived versions,
removes hallucinated URLs, and tags unverified ones.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from verification.url_health import URLCheckResult, URLClassification


@dataclass(frozen=True)
class Replacement:
    """Record of a single URL replacement."""

    original_url: str
    replacement: str
    classification: str
    action: str  # "kept", "replaced", "removed", "tagged"


@dataclass(frozen=True)
class AnnotatedOutput:
    """Result of annotating LLM output."""

    original_text: str
    cleaned_text: str
    replacements: tuple[Replacement, ...]
    removed_count: int
    replaced_count: int


def annotate_output(
    text: str, url_results: list[URLCheckResult],
) -> AnnotatedOutput:
    """Replace dead refs with Wayback URLs, remove hallucinated refs.

    Rules:
        LIVE: keep as-is
        DEAD with wayback_url: replace URL with wayback_url
        LIKELY_HALLUCINATED: remove markdown link, keep anchor text + tag
        UNKNOWN: append [unverified] after the link
    """
    if not url_results:
        return AnnotatedOutput(
            original_text=text, cleaned_text=text,
            replacements=(), removed_count=0, replaced_count=0,
        )

    result_map = {r.url: r for r in url_results}
    cleaned = text
    replacements: list[Replacement] = []
    removed = 0
    replaced = 0

    # Process each URL result
    for url, result in result_map.items():
        if result.classification in (URLClassification.LIVE, URLClassification.BOT_BLOCKED):
            replacements.append(Replacement(
                original_url=url, replacement=url,
                classification=result.classification.value, action="kept",
            ))
            continue

        if result.classification == URLClassification.DEAD and result.wayback_url:
            # Replace URL with Wayback version in both markdown and raw forms
            cleaned = cleaned.replace(url, result.wayback_url)
            replaced += 1
            replacements.append(Replacement(
                original_url=url, replacement=result.wayback_url,
                classification="DEAD", action="replaced",
            ))
            continue

        if result.classification == URLClassification.LIKELY_HALLUCINATED:
            # For markdown links: [text](url) → text [source not verified]
            md_pattern = re.compile(
                r"\[([^\]]+)\]\(" + re.escape(url) + r"\)",
            )
            cleaned = md_pattern.sub(r"\1 [source not verified]", cleaned)

            # For raw URLs: just replace with tag
            if url in cleaned:
                cleaned = cleaned.replace(url, "[hallucinated URL removed]")

            removed += 1
            replacements.append(Replacement(
                original_url=url, replacement="[source not verified]",
                classification="LIKELY_HALLUCINATED", action="removed",
            ))
            continue

        if result.classification == URLClassification.UNKNOWN:
            # Tag unverified — for markdown links
            md_pattern = re.compile(
                r"(\[([^\]]+)\]\(" + re.escape(url) + r"\))",
            )
            cleaned = md_pattern.sub(r"\1 [unverified]", cleaned)

            # For raw URLs
            if url in cleaned and "[unverified]" not in cleaned[cleaned.index(url):cleaned.index(url) + len(url) + 20]:
                cleaned = cleaned.replace(url, f"{url} [unverified]")

            replacements.append(Replacement(
                original_url=url, replacement=f"{url} [unverified]",
                classification="UNKNOWN", action="tagged",
            ))

    return AnnotatedOutput(
        original_text=text,
        cleaned_text=cleaned,
        replacements=tuple(replacements),
        removed_count=removed,
        replaced_count=replaced,
    )
