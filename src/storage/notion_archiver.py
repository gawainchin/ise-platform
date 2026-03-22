#!/usr/bin/env python3
"""Notion archival stubs for ISE."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any


UTC = timezone.utc


def build_notion_archive_payload(signal: dict[str, Any]) -> dict[str, Any]:
    news_context = signal.get("news_context", {})
    return {
        "status": "stubbed",
        "requested_at": datetime.now(UTC).isoformat(),
        "notion_key_present": bool(os.getenv("NOTION_KEY")),
        "archive_record": {
            "timestamp": signal.get("timestamp"),
            "ticker": signal.get("ticker"),
            "price": signal.get("price"),
            "delta_pct": signal.get("delta_pct"),
            "trigger_reason": signal.get("trigger_reason"),
            "importance_score": signal.get("importance_score"),
            "news_context": {
                "correlation": news_context.get("correlation"),
                "aggregate_sentiment": news_context.get("aggregate_sentiment"),
                "headlines": news_context.get("headlines", [])[:5],
            },
        },
    }


def archive_signal_to_notion(signal: dict[str, Any]) -> dict[str, Any]:
    """Return the payload a real Notion client would submit."""
    return build_notion_archive_payload(signal)
