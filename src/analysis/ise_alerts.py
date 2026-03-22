#!/usr/bin/env python3
"""ISE Alert Manager - Tiered Telegram alerts with feedback loop integration."""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

LOGGER = logging.getLogger("ise.alerts")
TELEGRAM_API_BASE = "https://api.telegram.org"

BOARD_ID = "6aade7c0-d61c-427e-84f1-5e9c862fbfdf"
MC_API_BASE = "http://localhost:8000"

# Tier configuration
TIER_CONFIG = {
    "T1": {
        "name": "Critical",
        "emoji": "🚨",
        "delivery": "IMMEDIATE",
        "min_score": 9,
        "max_score": 10,
        "header": "🚨 CRITICAL MARKET ALERT",
        "description": "Immediate attention required",
    },
    "T2": {
        "name": "Major",
        "emoji": "⚠️",
        "delivery": "STANDARD",
        "min_score": 7,
        "max_score": 8,
        "header": "⚠️ MAJOR MARKET SIGNAL",
        "description": "Significant market development",
    },
    "T3": {
        "name": "Moderate",
        "emoji": "ℹ️",
        "delivery": "DIGEST",
        "min_score": 5,
        "max_score": 6,
        "header": "ℹ️ MARKET INTELLIGENCE",
        "description": "Worthy of attention",
    },
}


@dataclass
class Alert:
    """Structured alert with tier classification."""
    signal: str
    score: int
    title: str
    source: str
    link: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tier: str = field(init=False)
    tier_config: dict = field(init=False)

    def __post_init__(self):
        self.tier = self._classify_tier()
        self.tier_config = TIER_CONFIG.get(self.tier, TIER_CONFIG["T3"])

    def _classify_tier(self) -> str:
        """Classify alert into tier based on score."""
        for tier, config in TIER_CONFIG.items():
            if config["min_score"] <= self.score <= config["max_score"]:
                return tier
        return "T3"  # Default to lowest tier

    def format_telegram_message(self) -> str:
        """Format alert as tiered Telegram message."""
        config = self.tier_config
        lines = [
            f"{config['header']}",
            f"",
            f"📊 <b>Signal:</b> <code>{self.signal}</code>",
            f"🎯 <b>Importance:</b> {self.score}/10",
            f"🏷️ <b>Tier:</b> {self.tier} - {config['name']}",
            f"",
            f"📰 <b>{self.title}</b>",
            f"📡 Source: {self.source}",
            f"",
            f"🔗 <a href='{self.link}'>Read full article</a>",
            f"",
            f"⏰ {self.format_timestamp()}",
        ]
        return "\n".join(lines)

    def format_digest_entry(self) -> str:
        """Format as digest entry for batched alerts."""
        config = self.tier_config
        return f"{config['emoji']} <b>{self.signal}</b> ({self.score}/10): {self.title[:60]}..."

    def format_timestamp(self) -> str:
        """Format timestamp for display."""
        try:
            dt = datetime.fromisoformat(self.timestamp.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M UTC")
        except:
            return self.timestamp


def get_mc_token() -> str | None:
    """Get Mission Control auth token from Docker container."""
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "exec", "openclaw-mission-control-backend-1", "env"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        for line in result.stdout.split("\n"):
            if line.startswith("LOCAL_AUTH_TOKEN="):
                return line.split("=", 1)[1].strip()
    except Exception as e:
        LOGGER.warning(f"Failed to get MC token: {e}")
    return None


def create_mc_task(signal: str, score: int, title: str, source: str, link: str) -> dict[str, Any] | None:
    """Create a Mission Control task for T3 signals (score 5-6)."""
    token = get_mc_token()
    if not token:
        LOGGER.warning("MC token not available, skipping task creation")
        return None

    task_data = {
        "title": f"[ISE {signal}] {title[:60]}{'...' if len(title) > 60 else ''}",
        "description": f"Signal: {signal}\nScore: {score}/10\nSource: {source}\nLink: {link}\n\nCreated by ISE Alert Manager for further monitoring.",
        "status": "inbox",
        "tags": ["ISE", "signal", signal, f"T3-{score}"],
    }

    try:
        request = Request(
            f"{MC_API_BASE}/api/v1/boards/{BOARD_ID}/tasks",
            data=json.dumps(task_data).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urlopen(request, timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
            LOGGER.info(f"Created MC task for {signal}: {result.get('id')}")
            return result
    except Exception as e:
        LOGGER.warning(f"Failed to create MC task: {e}")
        return None


def send_telegram_alert(
    alert: Alert,
    token: str | None = None,
    chat_id: str | None = None,
    state_path: Path | None = None,
) -> dict[str, Any] | None:
    """Send alert to Telegram and track message for feedback."""
    token = token or os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        LOGGER.info("Telegram not configured; message:\n%s", alert.format_telegram_message())
        return None

    # For T3 signals, also create MC task
    mc_task = None
    if alert.tier == "T3":
        mc_task = create_mc_task(alert.signal, alert.score, alert.title, alert.source, alert.link)

    message = alert.format_telegram_message()
    if mc_task:
        message += f"\n\n📋 <b>Task Created:</b> Tracking in Mission Control"

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }

    request = Request(
        f"{TELEGRAM_API_BASE}/bot{token}/sendMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=20) as response:
            result = json.loads(response.read().decode("utf-8"))
            message_id = result.get("result", {}).get("message_id")
            LOGGER.info(f"Sent {alert.tier} alert for {alert.signal} (message_id: {message_id})")

            # Track for feedback
            if state_path and message_id:
                track_alert_for_feedback(state_path, alert, message_id)

            return result
    except Exception as e:
        LOGGER.warning(f"Failed to send Telegram alert: {e}")
        return None


def track_alert_for_feedback(state_path: Path, alert: Alert, message_id: int) -> None:
    """Track sent alert for feedback loop."""
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)

        state.setdefault("tracked_alerts", [])
        state["tracked_alerts"].append({
            "message_id": message_id,
            "signal": alert.signal,
            "score": alert.score,
            "tier": alert.tier,
            "title": alert.title,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "feedback": None,
            "adjusted_score": alert.score,
        })

        # Keep only last 100 tracked alerts
        state["tracked_alerts"] = state["tracked_alerts"][-100:]

        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.write("\n")

        LOGGER.debug(f"Tracked alert {message_id} for {alert.signal}")
    except Exception as e:
        LOGGER.warning(f"Failed to track alert for feedback: {e}")


def send_digest_alerts(
    alerts: list[Alert],
    token: str | None = None,
    chat_id: str | None = None,
) -> dict[str, Any] | None:
    """Send batched digest for lower-tier alerts."""
    if not alerts:
        return None

    token = token or os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        return None

    # Group by tier
    by_tier: dict[str, list[Alert]] = {}
    for alert in alerts:
        by_tier.setdefault(alert.tier, []).append(alert)

    lines = ["📊 <b>ISE Market Intelligence Digest</b>", ""]

    for tier in ["T1", "T2", "T3"]:
        if tier in by_tier:
            config = TIER_CONFIG[tier]
            lines.append(f"{config['emoji']} <b>{config['name']} ({tier})</b>")
            for alert in by_tier[tier]:
                lines.append(f"  • {alert.signal} ({alert.score}/10): {alert.title[:50]}...")
            lines.append("")

    lines.append(f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    payload = {
        "chat_id": chat_id,
        "text": "\n".join(lines),
        "parse_mode": "HTML",
    }

    request = Request(
        f"{TELEGRAM_API_BASE}/bot{token}/sendMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=20) as response:
            result = json.loads(response.read().decode("utf-8"))
            LOGGER.info(f"Sent digest with {len(alerts)} alerts")
            return result
    except Exception as e:
        LOGGER.warning(f"Failed to send digest: {e}")
        return None


def format_legacy_alert(signal: str, score: int, detail: str) -> str:
    """Legacy format for backward compatibility."""
    for tier, config in TIER_CONFIG.items():
        if config["min_score"] <= score <= config["max_score"]:
            return f"{config['emoji']} [{tier} {config['name']}] {signal}: score {score} {detail}"
    return f"ℹ️ [T3 Moderate] {signal}: score {score} {detail}"


if __name__ == "__main__":
    # Test the alert formatting
    test_alert = Alert(
        signal="BTC",
        score=9,
        title="Bitcoin Surges Past Key Resistance Level",
        source="CoinDesk",
        link="https://example.com/article",
    )
    print(test_alert.format_telegram_message())
    print("\n" + "=" * 50 + "\n")

    test_alert2 = Alert(
        signal="CL=F",
        score=6,
        title="Oil Market Outlook Remains Uncertain",
        source="Reuters",
        link="https://example.com/oil",
    )
    print(test_alert2.format_telegram_message())
