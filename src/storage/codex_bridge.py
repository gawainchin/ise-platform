#!/usr/bin/env python3
"""Codex deep-dive trigger helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


UTC = timezone.utc
DEEP_DIVE_THRESHOLD = 8


def build_research_prompt(signal: dict[str, Any]) -> str:
    headlines = signal.get("news_context", {}).get("headlines", [])
    headline_lines = []
    for item in headlines[:5]:
        headline_lines.append(f"- {item.get('source')}: {item.get('title')}")
    headline_text = "\n".join(headline_lines) if headline_lines else "- No related headlines attached"

    return (
        "Prepare a market deep-dive for the following signal.\n"
        f"Ticker: {signal.get('ticker')}\n"
        f"Timestamp: {signal.get('timestamp')}\n"
        f"Price: {signal.get('price')}\n"
        f"Delta: {signal.get('delta_pct')}%\n"
        f"Trigger reason: {signal.get('trigger_reason')}\n"
        f"Importance: {signal.get('importance_score')}/10\n"
        "News context:\n"
        f"{headline_text}\n\n"
        "Output sections:\n"
        "1. What happened\n"
        "2. Why it matters\n"
        "3. Bull and bear cases\n"
        "4. Follow-up questions\n"
        "5. Notion-ready summary"
    )


def build_deep_dive_request(signal: dict[str, Any]) -> dict[str, Any] | None:
    if int(signal.get("importance_score", 0)) < DEEP_DIVE_THRESHOLD:
        return None
    return {
        "status": "queued_stub",
        "queued_at": datetime.now(UTC).isoformat(),
        "importance_score": signal["importance_score"],
        "ticker": signal["ticker"],
        "prompt": build_research_prompt(signal),
        "notion_ready": {
            "title": f"ISE Deep Dive: {signal['ticker']}",
            "tags": ["ise", "deep-dive", signal["ticker"]],
            "summary": signal.get("summary", signal.get("trigger_reason", "")),
        },
    }


def maybe_trigger_codex(signal: dict[str, Any]) -> dict[str, Any] | None:
    """Return the payload that a real Codex worker would consume."""
    return build_deep_dive_request(signal)
