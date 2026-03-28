"""
Consolidated knowledge document loading and injection.

Centralises the knowledge-loading logic that was previously duplicated
across ``ollama/client.py`` and ``llamacpp/client.py``.  All LLM clients
now delegate to these functions rather than carrying their own copies.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger as log

# Default knowledge directory (ollama/knowledge/ relative to the repo root)
_DEFAULT_KNOWLEDGE_DIR = Path(__file__).parent.parent / "ollama" / "knowledge"


def load_knowledge_doc(
    cache: dict[str, str],
    doc_name: str,
    knowledge_dir: Path = _DEFAULT_KNOWLEDGE_DIR,
) -> str | None:
    """Load a single knowledge ``.md`` document into *cache*.

    Parameters:
        cache: Mutable dict used as an in-memory document cache.
        doc_name: Filename with or without the ``.md`` extension.
        knowledge_dir: Directory containing knowledge markdown files.

    Returns:
        The document text, or ``None`` if the file does not exist.
    """
    if doc_name in cache:
        return cache[doc_name]

    if not doc_name.endswith(".md"):
        doc_name += ".md"

    path = knowledge_dir / doc_name
    if not path.exists():
        log.debug("Knowledge doc not found: {p}", p=path)
        return None

    content = path.read_text(encoding="utf-8")
    cache[doc_name] = content
    log.debug("Loaded knowledge doc: {p} ({n} chars)", p=doc_name, n=len(content))
    return content


def load_all_knowledge_docs(
    cache: dict[str, str],
    knowledge_dir: Path = _DEFAULT_KNOWLEDGE_DIR,
) -> str:
    """Load and concatenate every ``.md`` file in *knowledge_dir*.

    Parameters:
        cache: Mutable dict used as an in-memory document cache.
        knowledge_dir: Directory containing knowledge markdown files.

    Returns:
        A single string with all documents separated by headers,
        or an empty string if the directory is missing.
    """
    if not knowledge_dir.exists():
        return ""

    parts: list[str] = []
    for path in sorted(knowledge_dir.glob("*.md")):
        content = load_knowledge_doc(cache, path.name, knowledge_dir)
        if content:
            parts.append(f"--- {path.stem} ---\n{content}")

    combined = "\n\n".join(parts)
    log.info("Loaded {n} knowledge docs ({c} total chars)", n=len(parts), c=len(combined))
    return combined


def inject_knowledge(
    messages: list[dict[str, str]],
    system_knowledge: list[str] | None,
    cache: dict[str, str],
    knowledge_dir: Path = _DEFAULT_KNOWLEDGE_DIR,
) -> list[dict[str, str]]:
    """Inject selected knowledge docs into the system prompt.

    Uses TF-IDF + orthogonality selection (via ``knowledge.selector``) to
    pick only the most relevant, non-redundant docs that fit within the
    token budget.

    Parameters:
        messages: Chat message list (not mutated).
        system_knowledge: Names of knowledge docs to consider.
        cache: Mutable dict used as an in-memory document cache.
        knowledge_dir: Directory containing knowledge markdown files.

    Returns:
        A (possibly new) message list with knowledge injected into the
        system prompt.  Returns *messages* unchanged when
        *system_knowledge* is empty or no relevant docs are found.
    """
    if not system_knowledge:
        return messages

    from knowledge.selector import select_and_format

    candidates: dict[str, str] = {}
    for doc in system_knowledge:
        content = load_knowledge_doc(cache, doc, knowledge_dir)
        if content:
            candidates[doc] = content

    prompt_text = " ".join(
        m["content"] for m in messages if m["role"] in ("user", "system")
    )

    knowledge_block = select_and_format(
        prompt_text, candidates, char_budget=12000, max_docs=4,
    )
    if not knowledge_block:
        return messages

    patched = [dict(message) for message in messages]
    has_system = any(m["role"] == "system" for m in patched)
    if has_system:
        for message in patched:
            if message["role"] == "system":
                message["content"] = (
                    f"{message['content']}\n\n"
                    f"## Reference Knowledge\n\n{knowledge_block}"
                )
                break
    else:
        patched.insert(0, {
            "role": "system",
            "content": f"## Reference Knowledge\n\n{knowledge_block}",
        })
    return patched
