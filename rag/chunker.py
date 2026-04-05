"""
Smart text chunker for RAG indexing.

Markdown-aware chunking with configurable overlap.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Chunk:
    """A text chunk with metadata."""
    text: str
    index: int
    source_id: str
    start_char: int
    end_char: int


def chunk_markdown(
    text: str,
    source_id: str,
    max_tokens: int = 512,
    overlap_tokens: int = 64,
    chars_per_token: int = 4,
) -> list[Chunk]:
    """Split markdown text into overlapping chunks.

    Splits on heading boundaries first, then falls back to paragraph
    boundaries, then to character-level splits.

    Args:
        text: Full document text.
        source_id: Identifier for the source document.
        max_tokens: Maximum tokens per chunk.
        overlap_tokens: Token overlap between chunks.
        chars_per_token: Approximate chars per token.

    Returns:
        List of Chunk objects.
    """
    max_chars = max_tokens * chars_per_token
    overlap_chars = overlap_tokens * chars_per_token

    if not text.strip():
        return []

    # Split on markdown headings first
    sections = re.split(r'(?=^#{1,3}\s)', text, flags=re.MULTILINE)
    sections = [s for s in sections if s.strip()]

    # If a section is too long, split on double newlines
    paragraphs: list[str] = []
    for section in sections:
        if len(section) <= max_chars:
            paragraphs.append(section)
        else:
            parts = section.split("\n\n")
            paragraphs.extend(p for p in parts if p.strip())

    # Merge small paragraphs, split large ones
    chunks: list[Chunk] = []
    buffer = ""
    char_offset = 0

    for para in paragraphs:
        if len(buffer) + len(para) + 1 <= max_chars:
            buffer = f"{buffer}\n{para}" if buffer else para
        else:
            if buffer:
                chunks.append(Chunk(
                    text=buffer.strip(),
                    index=len(chunks),
                    source_id=source_id,
                    start_char=char_offset,
                    end_char=char_offset + len(buffer),
                ))
                # Keep overlap from end of buffer
                char_offset += len(buffer) - overlap_chars
                buffer = buffer[-overlap_chars:] + "\n" + para if overlap_chars > 0 else para
            else:
                # Single paragraph exceeds max — hard split
                for i in range(0, len(para), max_chars - overlap_chars):
                    chunk_text = para[i:i + max_chars]
                    chunks.append(Chunk(
                        text=chunk_text.strip(),
                        index=len(chunks),
                        source_id=source_id,
                        start_char=char_offset + i,
                        end_char=char_offset + i + len(chunk_text),
                    ))
                char_offset += len(para)
                buffer = ""
                continue

    # Flush remaining buffer
    if buffer.strip():
        chunks.append(Chunk(
            text=buffer.strip(),
            index=len(chunks),
            source_id=source_id,
            start_char=char_offset,
            end_char=char_offset + len(buffer),
        ))

    return chunks
