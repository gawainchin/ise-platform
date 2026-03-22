#!/usr/bin/env python3
"""Feedback loop support for ISE."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


UTC = timezone.utc
OVERRIDE_PATTERN = re.compile(
    r"(?P<ticker>[A-Z0-9=^.-]+)\s*:\s*(?P<action>boost|lower|resolve|ignore)(?:\s+(?P<value>-?\d+))?",
    re.IGNORECASE,
)


def parse_telegram_reaction(reaction: str) -> int:
    if reaction == "👍":
        return 1
    if reaction == "👎":
        return -1
    return 0


def apply_feedback_adjustment(base_score: int, adjustments: list[int]) -> int:
    adjusted = base_score + sum(adjustments)
    return max(1, min(10, adjusted))


def record_reaction(signal_state: dict[str, Any], reaction: str) -> dict[str, Any]:
    feedback = signal_state.setdefault("feedback", [])
    feedback.append(
        {
            "type": "telegram_reaction",
            "reaction": reaction,
            "delta": parse_telegram_reaction(reaction),
            "recorded_at": datetime.now(UTC).isoformat(),
        }
    )
    return signal_state


def parse_mc_comment(comment: str) -> dict[str, Any] | None:
    match = OVERRIDE_PATTERN.search(comment.strip())
    if not match:
        return None
    payload = match.groupdict()
    return {
        "ticker": payload["ticker"].upper(),
        "action": payload["action"].lower(),
        "value": int(payload["value"]) if payload.get("value") else None,
    }


def apply_mc_override(signal_state: dict[str, Any], comment: str) -> dict[str, Any]:
    override = parse_mc_comment(comment)
    if override is None:
        return signal_state

    signal_state.setdefault("feedback", []).append(
        {
            "type": "mc_comment",
            "comment": comment,
            "override": override,
            "recorded_at": datetime.now(UTC).isoformat(),
        }
    )

    if override["action"] == "resolve":
        signal_state["status"] = "resolved"
    elif override["action"] == "ignore":
        signal_state["suppressed"] = True
    else:
        current = int(signal_state.get("importance_score", 1))
        delta = override["value"] if override["value"] is not None else (1 if override["action"] == "boost" else -1)
        signal_state["importance_score"] = apply_feedback_adjustment(current, [delta])
    return signal_state


def collect_feedback_adjustments(signal_state: dict[str, Any]) -> list[int]:
    adjustments: list[int] = []
    for item in signal_state.get("feedback", []):
        delta = item.get("delta")
        if isinstance(delta, int):
            adjustments.append(delta)
        override = item.get("override")
        if isinstance(override, dict) and override.get("action") in {"boost", "lower"}:
            value = override.get("value")
            if isinstance(value, int):
                adjustments.append(value if override["action"] == "boost" else -abs(value))
    return adjustments
