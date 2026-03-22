#!/usr/bin/env python3
"""Targeted regression checks for RSS source integration."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from market_check_v2 import MarketEvent, SignalEngine
from rss_fetcher import RSSFetcher, RSSEntry


class RSSIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_path = Path(self.temp_dir.name) / "state.json"
        self.state_path.write_text(
            json.dumps({"signals": {}, "suppression_windows": {}, "rss": {"last_fetched": {}, "processed_entries": []}})
        )
        self.fetcher = RSSFetcher(Path("config.yaml"), self.state_path)
        self.engine = SignalEngine(Path("config.yaml"), self.state_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_rthk_landing_page_is_resolved_to_finance_feed(self) -> None:
        resolved = self.fetcher._resolve_feed_url("RTHK", "https://news.rthk.hk/rthk/en/rss.htm")
        self.assertEqual(resolved, "https://rthk.hk/rthk/news/rss/e_expressnews_efinance.xml")

    def test_regulatory_crypto_headline_prefers_regulation_category(self) -> None:
        entry = RSSEntry(
            title="Regulators probe crypto exchange over compliance failures",
            link="https://example.com/wsj",
            source="WSJ",
            summary="Bitcoin trading volume slips after the investigation",
            published=datetime.now(timezone.utc),
        )
        self.assertEqual(entry.to_market_event(priority=3)["category"], "regulation")

    def test_market_headline_gets_weighted_market_category(self) -> None:
        entry = RSSEntry(
            title="Hong Kong stocks rally as Hang Seng rebounds",
            link="https://example.com/rthk",
            source="RTHK",
            summary="Markets rose on renewed risk appetite",
            published=datetime.now(timezone.utc),
        )
        self.assertEqual(entry.to_market_event(priority=3)["category"], "market")

    def test_source_and_category_weights_produce_alertable_scores(self) -> None:
        samples = [
            RSSEntry(
                title="Hong Kong stocks rally as Hang Seng rebounds",
                link="https://example.com/rthk",
                source="RTHK",
                summary="Markets rose on renewed risk appetite",
                published=datetime.now(timezone.utc),
            ),
            RSSEntry(
                title="Company profit jumps after restructuring",
                link="https://example.com/bbc",
                source="BBC",
                summary="Revenue and earnings beat forecasts",
                published=datetime.now(timezone.utc),
            ),
            RSSEntry(
                title="Oil prices jump as supply fears return",
                link="https://example.com/wsj",
                source="WSJ",
                summary="Brent crude gains on geopolitical tension",
                published=datetime.now(timezone.utc),
            ),
        ]

        scores = {}
        for entry in samples:
            payload = entry.to_market_event({"RTHK": 3, "BBC": 2, "WSJ": 3}[entry.source])
            alert = self.engine.process_event(MarketEvent.from_dict(payload))
            self.assertIsNotNone(alert)
            scores[entry.source] = alert["importance_score"]

        self.assertGreaterEqual(scores["RTHK"], 6)
        self.assertGreaterEqual(scores["BBC"], 6)
        self.assertGreaterEqual(scores["WSJ"], 6)


if __name__ == "__main__":
    unittest.main()
