#!/usr/bin/env python3
"""RSS Feed Fetcher for ISE (Intelligent Signal Engine).

Fetches and parses RSS feeds, extracts relevant market news headlines,
converts feed entries to MarketEvent objects compatible with market_check_v2.py.
"""

from __future__ import annotations

import argparse
import html
import hashlib
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

# Third-party imports with graceful fallback
try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
except ImportError:  # pragma: no cover
    FEEDPARSER_AVAILABLE = False

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config.yaml")
DEFAULT_STATE_PATH = Path("signals_state.json")
KNOWN_FEED_URL_OVERRIDES = {
    "https://news.rthk.hk/rthk/en/rss.htm": "https://rthk.hk/rthk/news/rss/e_expressnews_efinance.xml",
}

# Default market-related keywords for filtering
DEFAULT_RSS_KEYWORDS = [
    # Market terms
    "price", "prices", "trading", "trade", "stock", "stocks", "share", "shares",
    "market", "markets", "bull", "bear", "rally", "sell-off", "selloff",
    # Commodities
    "oil", "gold", "silver", "copper", "commodity", "commodities", "crude",
    "brent", "wti", "metals", "precious",
    # Crypto
    "crypto", "cryptocurrency", "bitcoin", "btc", "ethereum", "eth",
    "blockchain", "defi", "nft", "token", "tokens",
    # Finance
    "rate", "rates", "interest", "fed", "federal reserve", "ecb", "boe",
    "central bank", "inflation", "deflation", "gdp", "economy", "economic",
    "recession", "growth", "fiscal", "monetary", "policy",
    # Corporate
    "earnings", "revenue", "profit", "loss", "dividend", "ipo", "merger",
    "acquisition", "buyout", "bankruptcy", "default", "debt", "bond",
    "bonds", "yield", "yields",
    # Forex
    "currency", "currencies", "forex", "fx", "dollar", "euro", "yen",
    "yuan", "pound", "sterling", "usd", "eur", "jpy", "cny",
    # Risk/Geopolitical
    "sanctions", "tariff", "tariffs", "trade war", "geopolitical", "war",
    "conflict", "election", "vote", "regulation", "regulatory", "probe",
    "investigation", "fine", "penalty", "compliance",
    # Sentiment
    "upgrade", "downgrade", "outperform", "underperform", "buy", "sell",
    "hold", "overweight", "underweight", "neutral", "target",
]


@dataclass
class RSSEntry:
    """Represents a parsed RSS entry."""
    title: str
    link: str
    source: str
    published: datetime | None = None
    summary: str = ""
    categories: list[str] = field(default_factory=list)
    entry_id: str = ""

    def to_market_event(self, priority: int = 1) -> dict[str, Any]:
        """Convert RSS entry to MarketEvent-compatible dictionary.
        
        For RSS news without explicit price data, we use:
        - ticker: NEWS.RSS.<CATEGORY> to indicate news-based signal
        - price: derived from source priority (1-10 scale mapped to priority)
        """
        category = self._classify_category()
        importance_price = float(priority)  # Use priority as price proxy
        
        return {
            "ticker": f"NEWS.RSS.{category.upper()}",
            "price": importance_price,
            "headline": self.title,
            "source": self.source,
            "summary": self.summary[:500] if self.summary else "",
            "category": category,
            "occurred_at": self.published.isoformat() if self.published else None,
            "status": "active",
            "rss_metadata": {
                "link": self.link,
                "entry_id": self.entry_id,
                "original_categories": self.categories,
            }
        }

    def _classify_category(self) -> str:
        """Classify entry into market category based on content."""
        text = " ".join(
            part.lower()
            for part in [self.title, self.summary, " ".join(self.categories)]
            if part
        )

        # Category detection with priority order
        if any(k in text for k in ["regulation", "regulatory", "sfc", "sec", "compliance", "fine", "penalty", "probe"]):
            return "regulation"
        if any(k in text for k in ["crypto", "bitcoin", "ethereum", "btc", "eth", "blockchain", "token", "defi"]):
            return "crypto"
        if any(
            k in text
            for k in [
                "fed", "interest rate", "interest rates", "central bank", "inflation",
                "gdp", "recession", "economy", "economic", "monetary policy",
                "fiscal policy", "treasury", "tariff", "tariffs",
            ]
        ):
            return "macro"
        if any(k in text for k in ["earnings", "revenue", "profit", "loss", "dividend", "ipo"]):
            return "earnings"
        if any(k in text for k in ["oil", "gold", "commodity", "crude", "metals", "brent", "wti"]):
            return "commodity"
        if any(k in text for k in ["liquidity", "funding", "repo", "interbank"]):
            return "liquidity"
        if any(
            k in text
            for k in [
                "market", "markets", "stock", "stocks", "share", "shares", "equity",
                "equities", "bond", "bonds", "yield", "yields", "forex", "fx",
                "currency", "currencies", "hang seng", "s&p 500", "nasdaq", "dow jones",
            ]
        ):
            return "market"

        return "general"

    @property
    def content_hash(self) -> str:
        """Generate unique hash for deduplication."""
        content = f"{self.source}:{self.title}:{self.link}".encode("utf-8")
        return hashlib.sha256(content).hexdigest()[:16]


class RSSFetcher:
    """RSS feed fetcher and parser for ISE."""

    def __init__(
        self,
        config_path: Path,
        state_path: Path,
        keywords: list[str] | None = None,
        request_delay: float | None = None,
    ) -> None:
        self.config_path = config_path
        self.state_path = state_path
        self.config = self._load_config()
        rss_config = self.config.get("rss_config", {})
        self.keywords = keywords or rss_config.get("keywords") or DEFAULT_RSS_KEYWORDS
        self.request_delay = (
            request_delay if request_delay is not None
            else float(rss_config.get("request_delay_seconds", 1.0))
        )
        self.max_entries_per_fetch = int(rss_config.get("max_entries_per_fetch", 100))
        self.state = self._load_state()

    def _load_config(self) -> dict[str, Any]:
        """Load configuration from YAML file."""
        with self.config_path.open("r", encoding="utf-8") as handle:
            if yaml is not None:
                return yaml.safe_load(handle) or {}
            return json.load(handle)

    def _load_state(self) -> dict[str, Any]:
        """Load state file, initializing RSS section if needed."""
        if not self.state_path.exists():
            return {"rss": {"last_fetched": {}, "processed_entries": []}}
        
        with self.state_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        
        # Ensure RSS section exists
        if "rss" not in data:
            data["rss"] = {"last_fetched": {}, "processed_entries": []}
        else:
            data["rss"].setdefault("last_fetched", {})
            data["rss"].setdefault("processed_entries", [])
        
        return data

    def save_state(self) -> None:
        """Save state to JSON file."""
        with self.state_path.open("w", encoding="utf-8") as handle:
            json.dump(self.state, handle, indent=2, sort_keys=True)
            handle.write("\n")

    def get_feeds(self) -> list[dict[str, Any]]:
        """Get list of RSS feeds from config."""
        return self.config.get("rss_feeds", [])

    def fetch_feed(self, feed_config: dict[str, Any]) -> list[RSSEntry]:
        """Fetch and parse a single RSS feed.
        
        Args:
            feed_config: Feed configuration dict with 'name', 'url', 'priority'
            
        Returns:
            List of parsed RSSEntry objects
        """
        name = feed_config.get("name", "Unknown")
        url = feed_config.get("url", "")
        resolved_url = self._resolve_feed_url(name, url)

        if not resolved_url:
            logger.warning(f"No URL for feed: {name}")
            return []
        
        if not FEEDPARSER_AVAILABLE:
            logger.error("feedparser library not installed. Install with: pip install feedparser")
            return []
        
        logger.info(f"Fetching feed: {name} ({resolved_url})")
        
        try:
            # Parse feed with timeout handling
            parsed = feedparser.parse(resolved_url, request_headers={
                "User-Agent": "ISE-RSS-Fetcher/1.0 (Intelligent Signal Engine)"
            })
            
            if parsed.bozo and hasattr(parsed, 'bozo_exception'):
                logger.warning(f"Feed {name} has parse issues: {parsed.bozo_exception}")
            
            entries: list[RSSEntry] = []
            for entry in parsed.entries[:self.max_entries_per_fetch]:
                rss_entry = self._parse_entry(entry, name)
                if rss_entry:
                    entries.append(rss_entry)
            
            logger.info(f"Parsed {len(entries)} entries from {name}")
            return entries
            
        except Exception as e:
            logger.error(f"Error fetching feed {name}: {e}")
            return []

    def _resolve_feed_url(self, source: str, url: str) -> str:
        """Resolve landing pages to machine-readable feeds when needed."""
        resolved_url = KNOWN_FEED_URL_OVERRIDES.get(url, url)
        if resolved_url != url:
            logger.info(
                "Resolved %s feed URL from landing page %s to %s",
                source,
                url,
                resolved_url,
            )
        return resolved_url

    def _parse_entry(self, entry: Any, source: str) -> RSSEntry | None:
        """Parse a single feedparser entry into RSSEntry."""
        title = getattr(entry, 'title', '').strip()
        link = getattr(entry, 'link', '').strip()
        
        if not title or not link:
            return None
        
        # Parse publication date
        published = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            try:
                published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            except (TypeError, ValueError):
                pass
        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
            try:
                published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
            except (TypeError, ValueError):
                pass
        
        # Extract summary/description
        summary = ""
        if hasattr(entry, 'summary'):
            summary = entry.summary
        elif hasattr(entry, 'description'):
            summary = entry.description
        elif hasattr(entry, 'content') and entry.content:
            first_content = entry.content[0]
            summary = getattr(first_content, "value", "") if first_content else ""

        # Clean HTML from summary
        summary = self._clean_html(summary)
        
        # Extract categories
        categories = []
        if hasattr(entry, 'tags'):
            categories = [tag.term for tag in entry.tags if hasattr(tag, 'term')]
        elif hasattr(entry, 'category'):
            categories = [entry.category]
        
        # Extract entry ID
        entry_id = getattr(entry, 'id', link)
        
        return RSSEntry(
            title=title,
            link=link,
            source=source,
            published=published,
            summary=summary,
            categories=categories,
            entry_id=entry_id,
        )

    def _clean_html(self, text: str) -> str:
        """Remove HTML tags from text."""
        if not text:
            return ""
        # Simple regex-based HTML tag removal
        clean = re.sub(r'<[^>]+>', ' ', text)
        clean = html.unescape(clean)
        # Normalize whitespace
        clean = re.sub(r'\s+', ' ', clean).strip()
        return clean

    def is_duplicate(self, entry: RSSEntry) -> bool:
        """Check if entry has already been processed."""
        processed = self.state.get("rss", {}).get("processed_entries", [])
        return entry.content_hash in processed

    def mark_processed(self, entry: RSSEntry, max_history: int = 10000) -> None:
        """Mark entry as processed and maintain history limit."""
        processed = self.state.setdefault("rss", {}).setdefault("processed_entries", [])
        processed.append(entry.content_hash)
        
        # Maintain rolling window to prevent unbounded growth
        if len(processed) > max_history:
            self.state["rss"]["processed_entries"] = processed[-max_history:]

    def matches_keywords(self, entry: RSSEntry) -> bool:
        """Check if entry matches market-related keywords."""
        text = f"{entry.title} {entry.summary}".lower()
        
        for keyword in self.keywords:
            if keyword.lower() in text:
                return True
        
        return False

    def fetch_all(
        self,
        filter_keywords: bool = True,
        deduplicate: bool = True,
        since: datetime | None = None,
    ) -> list[RSSEntry]:
        """Fetch all feeds and return filtered entries.
        
        Args:
            filter_keywords: Only return entries matching keywords
            deduplicate: Skip already-processed entries
            since: Only return entries published after this time
            
        Returns:
            List of filtered RSSEntry objects
        """
        feeds = self.get_feeds()
        all_entries: list[RSSEntry] = []
        
        for feed_config in feeds:
            entries = self.fetch_feed(feed_config)
            
            for entry in entries:
                # Deduplication check
                if deduplicate and self.is_duplicate(entry):
                    continue
                
                # Time filter
                if since and entry.published and entry.published < since:
                    continue
                
                # Keyword filter
                if filter_keywords and not self.matches_keywords(entry):
                    continue
                
                all_entries.append(entry)
                self.mark_processed(entry)
            
            # Rate limiting between feeds
            time.sleep(self.request_delay)
        
        # Update last fetched timestamps
        now = datetime.now(timezone.utc).isoformat()
        for feed in feeds:
            name = feed.get("name", "")
            if name:
                self.state.setdefault("rss", {}).setdefault("last_fetched", {})[name] = now
        
        self.save_state()
        
        # Sort by publication date (newest first)
        all_entries.sort(key=lambda e: e.published or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        
        return all_entries

    def fetch_to_market_events(
        self,
        filter_keywords: bool = True,
        deduplicate: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch feeds and convert to MarketEvent-compatible dictionaries."""
        # Calculate 'since' based on last fetch
        last_fetched_map = self.state.get("rss", {}).get("last_fetched", {})
        if last_fetched_map:
            # Use oldest last_fetched as cutoff, but not older than 24h
            oldest = min(
                datetime.fromisoformat(ts.replace('Z', '+00:00'))
                for ts in last_fetched_map.values()
            )
            cutoff = max(oldest, datetime.now(timezone.utc) - __import__('datetime').timedelta(hours=24))
        else:
            cutoff = datetime.now(timezone.utc) - __import__('datetime').timedelta(hours=24)
        
        entries = self.fetch_all(
            filter_keywords=filter_keywords,
            deduplicate=deduplicate,
            since=cutoff,
        )
        
        # Build priority map
        priority_map = {
            f.get("name", ""): f.get("priority", 1)
            for f in self.get_feeds()
        }
        
        events = []
        for entry in entries:
            priority = priority_map.get(entry.source, 1)
            events.append(entry.to_market_event(priority))
        
        return events


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="RSS Feed Fetcher for ISE (Intelligent Signal Engine)"
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to config YAML",
    )
    parser.add_argument(
        "--state",
        default=str(DEFAULT_STATE_PATH),
        help="Path to JSON state store",
    )
    parser.add_argument(
        "--keywords",
        help="Comma-separated keywords to filter (overrides config)",
    )
    parser.add_argument(
        "--no-filter",
        action="store_true",
        help="Disable keyword filtering",
    )
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Disable deduplication (re-fetch all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch but don't save state or mark as processed",
    )
    parser.add_argument(
        "--delay",
        type=float,
        help="Delay between feed requests in seconds (overrides config.yaml)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of results",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check for feedparser
    if not FEEDPARSER_AVAILABLE:
        print(
            "Error: feedparser library required. Install with: pip install feedparser",
            file=sys.stderr,
        )
        return 1
    
    # Parse keywords if provided
    keywords = None
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    
    # Initialize fetcher
    fetcher = RSSFetcher(
        config_path=Path(args.config),
        state_path=Path(args.state),
        keywords=keywords,
        request_delay=args.delay,
    )
    
    # Fetch events
    logger.info("Starting RSS feed fetch...")
    events = fetcher.fetch_to_market_events(
        filter_keywords=not args.no_filter,
        deduplicate=not args.no_dedup,
    )
    
    # Apply limit if specified
    if args.limit and len(events) > args.limit:
        events = events[:args.limit]
    
    logger.info(f"Fetched {len(events)} events")
    
    # Output results
    if args.pretty:
        print(json.dumps(events, indent=2))
    else:
        for event in events:
            print(json.dumps(event, separators=(",", ":")))
    
    # Don't save state in dry-run mode
    if args.dry_run:
        logger.info("Dry run mode - state not saved")
        # Restore original state
        fetcher.state = fetcher._load_state()
        fetcher.save_state()
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
