#!/usr/bin/env python3
"""Phase 1 Intelligent Signal Engine market monitor - v2.1 with tiered alerts."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

# Import new ISE alert module
sys.path.insert(0, str(Path(__file__).parent))
try:
    from ise_alerts import Alert, send_telegram_alert, format_legacy_alert
    ISE_ALERTS_AVAILABLE = True
except ImportError:
    ISE_ALERTS_AVAILABLE = False
    print("Warning: ise_alerts.py not available, using legacy formatting")


LOGGER = logging.getLogger("ise.market_check")
SUPPRESSION_HOURS = 4
DEFAULT_STATE_PATH = Path("signals_state.json")
DEFAULT_FEEDS_PATH = Path("rss-feeds.yaml")
TELEGRAM_API_BASE = "https://api.telegram.org"

SIGNAL_KEYWORDS = {
    "BTC": ["bitcoin", "btc", "crypto", "cryptocurrency", "digital asset"],
    "CL=F": ["oil", "crude", "brent", "wti", "energy", "opec"],
}

CRITICAL_KEYWORDS = {
    "halt",
    "crash",
    "enforcement",
    "fraud",
    "sanction",
    "lawsuit",
    "emergency",
    "volatility",
    "collapse",
    "probe",
}

MAJOR_KEYWORDS = {
    "surge",
    "plunge",
    "breach",
    "breakout",
    "warning",
    "investigation",
    "regulatory",
    "policy",
    "tariff",
    "restriction",
}

MODERATE_KEYWORDS = {
    "outlook",
    "forecast",
    "trend",
    "signal",
    "commentary",
    "guidance",
    "update",
}

TIER_RULES = (
    ("T1", "Critical", "🚨 Immediate", 9, 10),
    ("T2", "Important", "⚠️ Standard", 7, 8),
    ("T3", "Intelligence", "ℹ️ Digest", 5, 6),
)


@dataclass
class Feed:
    name: str
    url: str
    feed_type: str = "rss"
    category: str = "general"


@dataclass
class FeedItem:
    feed_name: str
    title: str
    link: str
    summary: str

    @property
    def text(self) -> str:
        return " ".join(part for part in [self.title, self.summary] if part).strip()


class LinkCollector(HTMLParser):
    """Collect visible anchor text for non-RSS news pages."""

    def __init__(self) -> None:
        super().__init__()
        self._in_anchor = False
        self._href = ""
        self._text_parts: list[str] = []
        self.items: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if not href:
            return
        self._in_anchor = True
        self._href = href
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_anchor:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or not self._in_anchor:
            return
        text = re.sub(r"\s+", " ", "".join(self._text_parts)).strip()
        if len(text) >= 12:
            self.items.append((text, self._href))
        self._in_anchor = False
        self._href = ""
        self._text_parts = []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the ISE Phase 1 market monitor.")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH, help="Path to signals state JSON.")
    parser.add_argument("--feeds", type=Path, default=DEFAULT_FEEDS_PATH, help="Path to RSS feed config YAML.")
    parser.add_argument("--dry-run", action="store_true", help="Do not persist updated signal state.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    parser.add_argument("--legacy-alerts", action="store_true", help="Use legacy alert formatting.")
    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def load_state(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        state = json.load(handle)
    state.setdefault("signals", {})
    state.setdefault("suppression_windows", {})
    state.setdefault("shared_urls", [])
    state.setdefault("user_feedback", [])
    state.setdefault("deep_dive_queue", [])
    state.setdefault("tracked_alerts", [])  # New: track alerts for feedback
    for signal_state in state["signals"].values():
        signal_state.setdefault("user_feedback", [])
        base_score = int(signal_state.get("importance_score", 0) or 0)
        signal_state.setdefault("adjusted_importance_score", base_score)
    return state


def load_feeds(path: Path) -> list[Feed]:
    try:
        import yaml  # type: ignore

        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except ModuleNotFoundError:
        data = parse_simple_yaml(path)
    feeds: list[Feed] = []
    for entry in data.get("feeds", []):
        feeds.append(
            Feed(
                name=entry["name"],
                url=entry["url"],
                feed_type=entry.get("type", "rss"),
                category=entry.get("category", "general"),
            )
        )
    return feeds


def parse_simple_yaml(path: Path) -> dict[str, Any]:
    feeds: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.rstrip()
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or stripped == "feeds:":
                continue
            if stripped.startswith("- "):
                if current:
                    feeds.append(current)
                current = {}
                stripped = stripped[2:]
            if ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            if current is None:
                current = {}
            current[key.strip()] = value.strip().strip("'\"")
    if current:
        feeds.append(current)
    return {"feeds": feeds}


def fetch_feed_items(feed: Feed) -> list[FeedItem]:
    request = Request(
        feed.url,
        headers={
            "User-Agent": "ISE-Market-Check/1.0 (+https://example.local)",
            "Accept": "application/rss+xml, application/xml, text/html;q=0.9, */*;q=0.8",
        },
    )
    try:
        with urlopen(request, timeout=20) as response:
            payload = response.read()
            content_type = response.headers.get("Content-Type", "")
    except (HTTPError, URLError, TimeoutError) as exc:
        LOGGER.warning("Failed to fetch %s (%s): %s", feed.name, feed.url, exc)
        return []
    except Exception as exc:  # pragma: no cover
        LOGGER.exception("Unexpected error fetching %s: %s", feed.name, exc)
        return []

    if feed.feed_type == "html" or "html" in content_type:
        return parse_html_items(feed, payload)
    return parse_rss_items(feed, payload)


def parse_rss_items(feed: Feed, payload: bytes) -> list[FeedItem]:
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        LOGGER.warning("Invalid XML from %s: %s", feed.name, exc)
        return []

    items: list[FeedItem] = []
    for item in root.findall(".//item")[:10]:
        title = item.findtext("title", default="").strip()
        link = item.findtext("link", default=feed.url).strip()
        summary = item.findtext("description", default="").strip()
        if title:
            items.append(FeedItem(feed.name, title, link, summary))
    if not items:
        for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry")[:10]:
            title = entry.findtext("{http://www.w3.org/2005/Atom}title", default="").strip()
            summary = entry.findtext("{http://www.w3.org/2005/Atom}summary", default="").strip()
            link = feed.url
            for link_node in entry.findall("{http://www.w3.org/2005/Atom}link"):
                href = link_node.attrib.get("href")
                if href:
                    link = href
                    break
            if title:
                items.append(FeedItem(feed.name, title, link, summary))
    return items


def parse_html_items(feed: Feed, payload: bytes) -> list[FeedItem]:
    parser = LinkCollector()
    try:
        parser.feed(payload.decode("utf-8", errors="ignore"))
    except Exception as exc:  # pragma: no cover
        LOGGER.warning("Failed to parse HTML from %s: %s", feed.name, exc)
        return []

    items: list[FeedItem] = []
    seen: set[str] = set()
    for text, link in parser.items:
        if text in seen:
            continue
        seen.add(text)
        absolute_link = link if link.startswith("http") else f"{feed.url.rstrip('/')}/{link.lstrip('/')}"
        items.append(FeedItem(feed.name, text, absolute_link, ""))
        if len(items) >= 10:
            break
    return items


def score_item(signal: str, item: FeedItem) -> int:
    text = item.text.lower()
    if not any(keyword in text for keyword in SIGNAL_KEYWORDS.get(signal, [])):
        return 0

    score = 5
    critical_hits = sum(keyword in text for keyword in CRITICAL_KEYWORDS)
    major_hits = sum(keyword in text for keyword in MAJOR_KEYWORDS)
    moderate_hits = sum(keyword in text for keyword in MODERATE_KEYWORDS)

    if item.feed_name in {"HKMA", "SFC"} and any(keyword in text for keyword in {"enforcement", "disciplinary", "sanction", "fine"}):
        return 10
    if critical_hits:
        score = 9 + min(critical_hits - 1, 1)
    elif major_hits >= 2:
        score = 8
    elif major_hits == 1:
        score = 7
    elif moderate_hits:
        score = 6

    if item.feed_name in {"HKMA", "SFC"}:
        score = min(10, score + 1)
    return min(score, 10)


def parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def to_iso8601(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def is_suppressed(state: dict[str, Any], signal: str, now: datetime) -> bool:
    until = parse_iso8601(state.get("suppression_windows", {}).get(signal))
    return until is not None and now < until


def add_user_feedback(
    state: dict[str, Any],
    signal: str,
    feedback: str,
    score_delta: int = 0,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    signal_state = state["signals"].setdefault(signal, {})
    signal_state.setdefault("user_feedback", [])
    signal_state.setdefault("adjusted_importance_score", int(signal_state.get("importance_score", 0) or 0))
    recorded_at = to_iso8601(timestamp or datetime.now(timezone.utc))
    entry = {
        "signal": signal,
        "feedback": feedback,
        "score_delta": score_delta,
        "timestamp": recorded_at,
    }
    signal_state["user_feedback"].append(entry)
    state.setdefault("user_feedback", []).append(entry)
    signal_state["adjusted_importance_score"] = max(
        1,
        min(10, int(signal_state.get("importance_score", 0) or 0) + score_delta),
    )
    return entry


def get_adjusted_importance_score(signal_state: dict[str, Any]) -> int:
    adjusted = signal_state.get("adjusted_importance_score")
    if adjusted is None:
        return int(signal_state.get("importance_score", 0) or 0)
    return int(adjusted)


def update_signal_state(
    state: dict[str, Any],
    signal: str,
    score: int,
    now: datetime,
) -> None:
    signal_state = state["signals"].setdefault(signal, {})
    signal_state["importance_score"] = score
    signal_state["adjusted_importance_score"] = score
    signal_state["last_alerted"] = to_iso8601(now)
    signal_state["alert_count"] = int(signal_state.get("alert_count", 0)) + 1
    signal_state.setdefault("user_feedback", [])
    signal_state["status"] = "active" if score >= 7 else "monitoring"
    state["suppression_windows"][signal] = to_iso8601(now + timedelta(hours=SUPPRESSION_HOURS))


def classify_tier(score: int) -> tuple[str, str, str]:
    for tier, label, delivery, minimum, maximum in TIER_RULES:
        if minimum <= score <= maximum:
            return tier, label, delivery
    return "T0", "Background", "Muted"


def format_alert(signal: str, score: int, detail: str) -> str:
    # Use new tiered formatting if available
    if ISE_ALERTS_AVAILABLE and not args.legacy_alerts:
        return format_legacy_alert(signal, score, detail)
    
    # Legacy formatting
    tier, label, delivery = classify_tier(score)
    return f"{delivery} [{tier} {label}] {signal}: score {score} {detail}"


def send_tiered_telegram_alert(
    signal: str,
    score: int,
    item: FeedItem,
    state_path: Path,
    dry_run: bool = False,
) -> None:
    """Send tiered Telegram alert with feedback tracking."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        LOGGER.info("Telegram not configured")
        return

    if ISE_ALERTS_AVAILABLE and not args.legacy_alerts:
        # Use new structured alert system
        alert = Alert(
            signal=signal,
            score=score,
            title=item.title,
            source=item.feed_name,
            link=item.link,
        )
        if not dry_run:
            send_telegram_alert(alert, token, chat_id, state_path)
        else:
            LOGGER.info(f"[DRY RUN] Would send {alert.tier} alert:\n{alert.format_telegram_message()}")
    else:
        # Legacy alert format
        message = format_alert(signal, score, f"via {item.feed_name} - {item.title}")
        payload = json.dumps({"chat_id": chat_id, "text": message}).encode("utf-8")
        request = Request(
            f"{TELEGRAM_API_BASE}/bot{token}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=20) as response:
                response.read()
            LOGGER.info(f"Sent alert for {signal}")
        except Exception as exc:
            LOGGER.warning(f"Failed to send Telegram alert: {exc}")


def build_telegram_message(alerts: list[str]) -> str:
    lines = ["ISE Alert Summary"]
    lines.extend(f"- {alert}" for alert in alerts)
    return "\n".join(lines)


def send_telegram_alerts(alerts: list[str]) -> None:
    if not alerts:
        return
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    message = build_telegram_message(alerts)
    if not token or not chat_id:
        LOGGER.info("Telegram not configured; generated message:\n%s", message)
        return

    payload = json.dumps({"chat_id": chat_id, "text": message}).encode("utf-8")
    request = Request(
        f"{TELEGRAM_API_BASE}/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=20) as response:
            response.read()
        LOGGER.info("Sent %d alerts to Telegram", len(alerts))
    except Exception as exc:
        LOGGER.warning("Failed to send Telegram alert: %s", exc)


def trigger_deep_dive_analysis(state_path: Path, state: dict[str, Any], now: datetime) -> None:
    try:
        from analysis_engine import prepare_deep_dive_analysis

        queued = prepare_deep_dive_analysis(state_path, state=state, now=now)
        if queued:
            LOGGER.info("Queued %d signals for deep-dive analysis", len(queued))
    except Exception as exc:
        LOGGER.warning("Deep-dive analysis trigger failed: %s", exc)


def save_state(path: Path, state: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)
        handle.write("\n")


def process_signals(state: dict[str, Any], feeds: list[Feed], now: datetime, state_path: Path) -> list[str]:
    alerts: list[str] = []
    all_items: list[FeedItem] = []

    for feed in feeds:
        items = fetch_feed_items(feed)
        LOGGER.info("Fetched %d items from %s", len(items), feed.name)
        all_items.extend(items)

    for signal in state["signals"]:
        if is_suppressed(state, signal, now):
            alerts.append(format_alert(signal, 1, f"suppressed until {state['suppression_windows'][signal]}"))
            continue

        best_score = 0
        best_item: FeedItem | None = None
        for item in all_items:
            score = score_item(signal, item)
            if score > best_score:
                best_score = score
                best_item = item

        if best_score >= 5 and best_item is not None:
            update_signal_state(state, signal, best_score, now)
            alerts.append(format_alert(signal, best_score, f"via {best_item.feed_name} - {best_item.title}"))
            
            # Send tiered alert with feedback tracking
            send_tiered_telegram_alert(signal, best_score, best_item, state_path, dry_run=False)
            
            if best_score >= 8:
                trigger_deep_dive_analysis(state_path, state, now)
        elif best_score > 0:
            alerts.append(format_alert(signal, best_score, "filtered as low relevance"))
        else:
            alerts.append(format_alert(signal, 2, "no relevant items found"))

    return alerts


def main() -> int:
    global args  # Make args available to format_alert function
    args = parse_args()
    configure_logging(args.verbose)

    try:
        state = load_state(args.state)
        feeds = load_feeds(args.feeds)
    except FileNotFoundError as exc:
        LOGGER.error("Missing required file: %s", exc)
        return 1
    except json.JSONDecodeError as exc:
        LOGGER.error("Invalid JSON in state file: %s", exc)
        return 1
    except Exception as exc:
        LOGGER.exception("Startup failure: %s", exc)
        return 1

    now = datetime.now(timezone.utc)
    alerts = process_signals(state, feeds, now, args.state)

    if not args.dry_run:
        try:
            save_state(args.state, state)
        except Exception as exc:
            LOGGER.exception("Failed to save state: %s", exc)
            return 1

    print("ISE Alert Summary")
    print("=================")
    for alert in alerts:
        print(f"- {alert}")

    # Only send summary if using legacy mode (new mode sends individual alerts)
    if args.legacy_alerts or not ISE_ALERTS_AVAILABLE:
        send_telegram_alerts(alerts)

    return 0


if __name__ == "__main__":
    sys.exit(main())
