#!/usr/bin/env python3
"""News correlation helpers for ISE."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any

try:
    import feedparser
except ImportError:  # pragma: no cover
    feedparser = None

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None


UTC = timezone.utc
POSITIVE_TERMS = {
    "beat",
    "bullish",
    "gain",
    "growth",
    "jump",
    "rally",
    "record",
    "rise",
    "surge",
    "upgrade",
}
NEGATIVE_TERMS = {
    "crash",
    "cut",
    "decline",
    "drop",
    "fall",
    "fraud",
    "probe",
    "recession",
    "risk",
    "selloff",
    "slump",
    "warning",
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


@dataclass
class NewsItem:
    source: str
    title: str
    url: str
    published_at: datetime | None
    summary: str = ""
    sentiment: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "title": self.title,
            "url": self.url,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "summary": self.summary,
            "sentiment": self.sentiment,
        }


def sentiment_score(text: str) -> int:
    lowered = text.lower()
    positive = sum(term in lowered for term in POSITIVE_TERMS)
    negative = sum(term in lowered for term in NEGATIVE_TERMS)
    return positive - negative


def direction_bias(delta_pct: float | None) -> str:
    if delta_pct is None:
        return "neutral"
    if delta_pct > 0:
        return "up"
    if delta_pct < 0:
        return "down"
    return "neutral"


def correlation_label(delta_pct: float | None, aggregate_sentiment: float) -> str:
    bias = direction_bias(delta_pct)
    if bias == "neutral" or aggregate_sentiment == 0:
        return "unclear"
    if bias == "up" and aggregate_sentiment > 0:
        return "supportive"
    if bias == "down" and aggregate_sentiment < 0:
        return "supportive"
    return "contradictory"


def parse_rss_feeds(
    feeds: list[dict[str, Any]],
    keywords: list[str],
    lookback_hours: int = 12,
    headline_limit: int = 20,
) -> list[NewsItem]:
    if feedparser is None:
        return []

    items: list[NewsItem] = []
    cutoff = utc_now() - timedelta(hours=lookback_hours)
    keyword_set = [keyword.lower() for keyword in keywords]

    for feed in feeds:
        if not feed.get("enabled", True):
            continue
        parsed = feedparser.parse(feed["url"])
        for entry in parsed.entries[:headline_limit]:
            title = unescape(getattr(entry, "title", "") or "").strip()
            summary = unescape(getattr(entry, "summary", "") or "").strip()
            haystack = f"{title} {summary}".lower()
            if keyword_set and not any(keyword in haystack for keyword in keyword_set):
                continue
            published_at = _entry_published_at(entry)
            if published_at and published_at < cutoff:
                continue
            items.append(
                NewsItem(
                    source=feed["name"],
                    title=title,
                    url=getattr(entry, "link", "") or "",
                    published_at=published_at,
                    summary=summary[:400],
                    sentiment=sentiment_score(haystack),
                )
            )

    deduped: dict[tuple[str, str], NewsItem] = {}
    for item in items:
        deduped[(item.source, item.title)] = item
    return list(deduped.values())


def fetch_brave_headlines(
    query: str,
    api_key: str | None,
    headline_limit: int = 5,
) -> list[NewsItem]:
    if not api_key or requests is None:
        return []

    response = requests.get(
        "https://api.search.brave.com/res/v1/news/search",
        headers={
            "Accept": "application/json",
            "X-Subscription-Token": api_key,
        },
        params={"q": query, "count": headline_limit},
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()

    results: list[NewsItem] = []
    for item in payload.get("results", []):
        title = str(item.get("title", "")).strip()
        summary = str(item.get("description", "")).strip()
        results.append(
            NewsItem(
                source=str(item.get("source", "Brave News")).strip(),
                title=title,
                url=str(item.get("url", "")).strip(),
                published_at=parse_timestamp(item.get("page_age")),
                summary=summary,
                sentiment=sentiment_score(f"{title} {summary}"),
            )
        )
    return results


def correlate_news(
    ticker: str,
    delta_pct: float | None,
    keywords: list[str],
    feeds: list[dict[str, Any]],
    brave_api_key: str | None = None,
    headline_limit: int = 5,
    lookback_hours: int = 12,
) -> dict[str, Any]:
    rss_items = parse_rss_feeds(
        feeds=feeds,
        keywords=[ticker, *keywords],
        lookback_hours=lookback_hours,
        headline_limit=headline_limit * 3,
    )
    brave_items = fetch_brave_headlines(
        query=" OR ".join([ticker, *keywords[:3]]),
        api_key=brave_api_key or os.getenv("BRAVE_API_KEY"),
        headline_limit=headline_limit,
    )

    combined = rss_items + brave_items
    combined.sort(key=lambda item: item.published_at or utc_now(), reverse=True)
    top_items = combined[:headline_limit]
    aggregate_sentiment = sum(item.sentiment for item in top_items) / max(len(top_items), 1)

    return {
        "ticker": ticker,
        "headline_count": len(top_items),
        "aggregate_sentiment": round(aggregate_sentiment, 2) if top_items else 0.0,
        "correlation": correlation_label(delta_pct, aggregate_sentiment),
        "headlines": [item.to_dict() for item in top_items],
    }


def _entry_published_at(entry: Any) -> datetime | None:
    value = getattr(entry, "published", None) or getattr(entry, "updated", None)
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
