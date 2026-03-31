from __future__ import annotations

from typing import Any


def format_report_context(chunks: list[dict], *, max_chars: int = 2200) -> str:
    if not chunks:
        return ""

    sections: list[str] = ["Report Context:"]
    current_length = len(sections[0]) + 1

    for index, chunk in enumerate(chunks, start=1):
        date_value = (
            chunk.get("published_date")
            or chunk.get("release_date")
            or chunk.get("period_ending")
            or "Unknown"
        )
        chunk_text = _clean_text(chunk.get("text", ""))
        section = (
            f"[{index}] Title: {chunk.get('title') or 'Untitled'}\n"
            f"Date: {date_value}\n"
            f"Type: {chunk.get('report_type') or 'Unknown'}\n"
            f"Chunk:\n"
            f"{chunk_text}"
        )
        projected = current_length + len(section) + 2
        if projected > max_chars:
            remaining = max_chars - current_length - 32
            if remaining <= 0:
                break
            truncated = section[:remaining].rstrip()
            sections.append(truncated + "...")
            break

        sections.append(section)
        current_length = projected

    return "\n\n".join(sections)


def _clean_text(value: Any, *, max_chars: int = 600) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."
