#!/usr/bin/env python3
"""ISE v1.0 core signal engine."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

from codex_bridge import maybe_trigger_codex
from feedback_handler import apply_feedback_adjustment, collect_feedback_adjustments
from news_correlator import correlate_news
from notion_archiver import archive_signal_to_notion


UTC = timezone.utc
LOGGER = logging.getLogger("ise.v1")
DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.yaml")
DEFAULT_STATE_PATH = Path(__file__).with_name("signals_state.json")


def utc_now() -> datetime:
    return datetime.now(UTC)


def isoformat_z(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


@dataclass
class WatchItem:
    ticker: str
    quote_symbol: str
    display_name: str
    keywords: list[str]


class SignalEngine:
    def __init__(self, config_path: Path, state_path: Path, dry_run: bool = False) -> None:
        self.config_path = config_path
        self.state_path = state_path
        self.dry_run = dry_run
        self.config = self._load_config()
        self.engine_config = self.config.get("engine", {})
        self.watchlist = [self._watch_item(entry) for entry in self.config.get("watchlist", [])]
        self.state = self._load_state()

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as handle:
            if yaml is not None:
                return yaml.safe_load(handle) or {}
            return json.load(handle)

    def _watch_item(self, entry: dict[str, Any]) -> WatchItem:
        return WatchItem(
            ticker=str(entry["ticker"]).upper(),
            quote_symbol=str(entry.get("quote_symbol", entry["ticker"])),
            display_name=str(entry.get("display_name", entry["ticker"])),
            keywords=[str(keyword).lower() for keyword in entry.get("keywords", [])],
        )

    def _default_signal_state(self) -> dict[str, Any]:
        return {
            "last_price": None,
            "last_alerted": None,
            "importance_score": 1,
            "alert_count": 0,
            "status": "resolved",
            "first_seen": None,
            "last_seen": None,
            "active_since": None,
            "last_delta_pct": None,
            "last_trigger_reason": None,
            "feedback": [],
        }

    def _load_state(self) -> dict[str, dict[str, Any]]:
        if not self.state_path.exists():
            return {item.ticker: self._default_signal_state() for item in self.watchlist}

        with self.state_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        # Accept both old nested and new flat state layouts.
        raw_state = payload.get("signals", payload) if isinstance(payload, dict) else {}
        state: dict[str, dict[str, Any]] = {}
        for item in self.watchlist:
            merged = self._default_signal_state()
            merged.update(raw_state.get(item.ticker, {}))
            state[item.ticker] = merged
        return state

    def save_state(self) -> None:
        if self.dry_run:
            return
        with self.state_path.open("w", encoding="utf-8") as handle:
            json.dump(self.state, handle, indent=2, sort_keys=True)
            handle.write("\n")

    def run(self) -> list[dict[str, Any]]:
        now = utc_now()
        signals: list[dict[str, Any]] = []
        for item in self.watchlist:
            signal = self.evaluate_item(item, now)
            if signal is not None:
                signals.append(signal)
        self.save_state()
        return signals

    def evaluate_item(self, item: WatchItem, now: datetime) -> dict[str, Any] | None:
        price = self.fetch_price(item.quote_symbol)
        signal_state = self.state.setdefault(item.ticker, self._default_signal_state())
        signal_state["last_seen"] = isoformat_z(now)
        signal_state["first_seen"] = signal_state.get("first_seen") or isoformat_z(now)

        if price is None:
            LOGGER.warning("No price available for %s", item.ticker)
            return None

        previous_price = self._to_float(signal_state.get("last_price"))
        delta_pct = self.calculate_delta_pct(previous_price, price)
        cooldown_hours = int(self.engine_config.get("alert_cooldown_hours", 4))
        threshold_pct = float(self.engine_config.get("price_shift_threshold_pct", 2.0))
        persistence_hours = int(self.engine_config.get("persistence_trigger_hours", 4))

        if signal_state.get("active_since") is None:
            signal_state["active_since"] = isoformat_z(now)
        active_since = parse_timestamp(signal_state.get("active_since")) or now
        last_alerted = parse_timestamp(signal_state.get("last_alerted"))

        price_trigger = delta_pct is not None and abs(delta_pct) >= threshold_pct
        persistence_trigger = (now - active_since) >= timedelta(hours=persistence_hours)
        in_cooldown = (
            last_alerted is not None
            and (now - last_alerted) < timedelta(hours=cooldown_hours)
            and (delta_pct is None or abs(delta_pct) < threshold_pct)
        )

        news_context = correlate_news(
            ticker=item.ticker,
            delta_pct=delta_pct,
            keywords=item.keywords,
            feeds=self.config.get("rss_feeds", []),
            brave_api_key=os.getenv(self.config.get("news", {}).get("brave_api_key_env", "BRAVE_API_KEY")),
            headline_limit=int(self.config.get("news", {}).get("headline_limit", 5)),
            lookback_hours=int(self.config.get("news", {}).get("lookback_hours", 12)),
        )

        base_importance = self.importance_score(
            delta_pct=delta_pct,
            price_trigger=price_trigger,
            persistence_trigger=persistence_trigger,
            news_context=news_context,
            alert_count=int(signal_state.get("alert_count", 0)),
        )
        adjusted_importance = apply_feedback_adjustment(
            base_importance,
            collect_feedback_adjustments(signal_state),
        )

        should_alert = not in_cooldown and (price_trigger or persistence_trigger)
        status = "active" if (price_trigger or persistence_trigger) else "resolved"
        trigger_reason = self.describe_trigger(price_trigger, persistence_trigger, delta_pct, threshold_pct)

        signal_state.update(
            {
                "last_price": round(price, 6),
                "last_delta_pct": round(delta_pct, 4) if delta_pct is not None else None,
                "importance_score": adjusted_importance,
                "status": status,
                "last_trigger_reason": trigger_reason,
            }
        )

        if status == "resolved":
            signal_state["active_since"] = isoformat_z(now)

        if not should_alert:
            return None

        signal_state["last_alerted"] = isoformat_z(now)
        signal_state["alert_count"] = int(signal_state.get("alert_count", 0)) + 1

        signal = {
            "timestamp": isoformat_z(now),
            "ticker": item.ticker,
            "display_name": item.display_name,
            "price": round(price, 6),
            "previous_price": previous_price,
            "delta_pct": round(delta_pct, 4) if delta_pct is not None else None,
            "trigger_reason": trigger_reason,
            "importance_score": adjusted_importance,
            "alert_count": signal_state["alert_count"],
            "status": status,
            "dedup_suppressed": False,
            "news_context": news_context,
            "summary": f"{item.display_name} moved {round(delta_pct, 2) if delta_pct is not None else 'N/A'}%",
        }
        signal["codex_deep_dive"] = maybe_trigger_codex(signal)
        signal["notion_archive"] = archive_signal_to_notion(signal)
        return signal

    def fetch_price(self, quote_symbol: str) -> float | None:
        env_key = f"ISE_PRICE_{quote_symbol.replace('^', '').replace('=', '_').replace('-', '_')}"
        if env_key in os.environ:
            return self._to_float(os.environ[env_key])
        return self.fetch_price_yahoo(quote_symbol)

    def fetch_price_yahoo(self, quote_symbol: str) -> float | None:
        if requests is None:
            return None
        try:
            response = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{quote_symbol}",
                params={"interval": "1d", "range": "5d"},
                timeout=15,
                headers={"User-Agent": "ISE/1.0"},
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Quote fetch failed for %s: %s", quote_symbol, exc)
            return None

        result = payload.get("chart", {}).get("result") or []
        if not result:
            return None
        meta = result[0].get("meta", {})
        price = meta.get("regularMarketPrice") or meta.get("previousClose")
        return self._to_float(price)

    def calculate_delta_pct(self, previous_price: float | None, current_price: float) -> float | None:
        if previous_price in (None, 0):
            return None
        return ((current_price - previous_price) / previous_price) * 100

    def importance_score(
        self,
        delta_pct: float | None,
        price_trigger: bool,
        persistence_trigger: bool,
        news_context: dict[str, Any],
        alert_count: int,
    ) -> int:
        score = 1.0
        move = abs(delta_pct or 0.0)
        score += min(5.0, move / 1.5)
        if price_trigger:
            score += 1.5
        if persistence_trigger:
            score += 1.0
        score += min(1.5, abs(float(news_context.get("aggregate_sentiment", 0.0))) * 0.5)
        if news_context.get("correlation") == "supportive":
            score += 1.0
        if alert_count >= 3:
            score += 0.5
        return max(1, min(10, int(math.ceil(score))))

    def describe_trigger(
        self,
        price_trigger: bool,
        persistence_trigger: bool,
        delta_pct: float | None,
        threshold_pct: float,
    ) -> str:
        reasons: list[str] = []
        if price_trigger:
            reasons.append(f"price shift >= {threshold_pct:.1f}%")
        if persistence_trigger:
            reasons.append("4h persistence")
        if delta_pct is not None:
            reasons.append(f"delta={delta_pct:.2f}%")
        return "; ".join(reasons) if reasons else "no trigger"

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the ISE v1.0 signal engine.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)
    engine = SignalEngine(config_path=args.config, state_path=args.state, dry_run=args.dry_run)

    try:
        signals = engine.run()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("ISE run failed: %s", exc)
        return 1

    if not signals:
        print("[]")
    else:
        print(json.dumps(signals, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
