#!/usr/bin/env python3
"""ISE Phase 1 market signal engine."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


DEFAULT_CONFIG_PATH = Path("config.yaml")
DEFAULT_STATE_PATH = Path("signals_state.json")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def clamp(value: float, lower: int = 1, upper: int = 10) -> int:
    return max(lower, min(upper, round(value)))


@dataclass
class MarketEvent:
    ticker: str
    price: float
    headline: str
    source: str
    summary: str = ""
    category: str = "general"
    occurred_at: datetime | None = None
    status: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MarketEvent":
        occurred_at = parse_timestamp(payload.get("occurred_at"))
        return cls(
            ticker=str(payload["ticker"]).upper(),
            price=float(payload["price"]),
            headline=str(payload["headline"]).strip(),
            source=str(payload["source"]).strip(),
            summary=str(payload.get("summary", "")).strip(),
            category=str(payload.get("category", "general")).strip().lower(),
            occurred_at=occurred_at,
            status=payload.get("status"),
        )


class SignalEngine:
    def __init__(self, config_path: Path, state_path: Path) -> None:
        self.config_path = config_path
        self.state_path = state_path
        self.config = self._load_config()
        self.state = self._load_state()

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as handle:
            if yaml is not None:
                return yaml.safe_load(handle) or {}
            return json.load(handle)

    def _default_state(self) -> dict[str, Any]:
        return {"signals": {}, "suppression_windows": {}}

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return self._default_state()
        with self.state_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        data.setdefault("signals", {})
        data.setdefault("suppression_windows", {})
        return data

    def save_state(self) -> None:
        with self.state_path.open("w", encoding="utf-8") as handle:
            json.dump(self.state, handle, indent=2, sort_keys=True)
            handle.write("\n")

    def process_events(self, events: list[MarketEvent]) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        for event in events:
            alert = self.process_event(event)
            if alert:
                alerts.append(alert)
        self.save_state()
        return alerts

    def process_event(self, event: MarketEvent) -> dict[str, Any] | None:
        signal_state = self.state["signals"].get(event.ticker, {})
        previous_price = signal_state.get("last_price")
        price_change_pct = self._calculate_price_change_pct(previous_price, event.price)
        importance_score = self._score_event(event, price_change_pct, signal_state)
        tier = self._classify_tier(importance_score)
        now = event.occurred_at or utc_now()

        resolved_status = (event.status or "").strip().lower() == "resolved"
        duplicate_suppressed = self._is_duplicate_suppressed(event.ticker, now)
        should_alert = resolved_status or self._should_alert(price_change_pct, importance_score, duplicate_suppressed)

        next_status = "resolved" if resolved_status else ("active" if should_alert else "monitoring")
        alert_count = int(signal_state.get("alert_count", 0)) + (1 if should_alert else 0)

        self.state["signals"][event.ticker] = {
            "last_price": round(event.price, 6),
            "last_alerted": isoformat_z(now) if should_alert else signal_state.get("last_alerted"),
            "importance_score": importance_score,
            "alert_count": alert_count,
            "status": next_status,
        }

        if should_alert:
            suppression_hours = int(self.config["alert_thresholds"]["suppression_window_hours"])
            self.state["suppression_windows"][event.ticker] = isoformat_z(now + timedelta(hours=suppression_hours))
        elif event.ticker not in self.state["suppression_windows"] and signal_state.get("last_alerted"):
            self.state["suppression_windows"][event.ticker] = signal_state["last_alerted"]

        if not should_alert:
            return None

        codex_analysis_needed = importance_score >= int(
            self.config["alert_thresholds"]["codex_analysis_min_score"]
        )
        return {
            "ticker": event.ticker,
            "price": event.price,
            "price_change_pct": price_change_pct,
            "headline": event.headline,
            "source": event.source,
            "summary": event.summary,
            "category": event.category,
            "importance_score": importance_score,
            "tier": tier,
            "status": next_status,
            "alert_count": alert_count,
            "duplicate_suppressed": duplicate_suppressed,
            "CODEX_ANALYSIS_NEEDED": codex_analysis_needed,
            "telegram_message": self._format_telegram_message(
                event=event,
                importance_score=importance_score,
                tier=tier,
                price_change_pct=price_change_pct,
                codex_analysis_needed=codex_analysis_needed,
                alert_count=alert_count,
                timestamp=now,
            ),
        }

    def _should_alert(
        self,
        price_change_pct: float | None,
        importance_score: int,
        duplicate_suppressed: bool,
    ) -> bool:
        if duplicate_suppressed:
            return False
        minimum_move = float(self.config["alert_thresholds"]["price_movement_pct"])
        minimum_score = int(self.config["alert_thresholds"]["minimum_importance_score"])
        move_trigger = price_change_pct is not None and abs(price_change_pct) > minimum_move
        score_trigger = importance_score >= minimum_score
        return move_trigger or score_trigger

    def _is_duplicate_suppressed(self, ticker: str, now: datetime) -> bool:
        expiry = parse_timestamp(self.state["suppression_windows"].get(ticker))
        return bool(expiry and expiry > now)

    def _calculate_price_change_pct(self, previous_price: float | None, current_price: float) -> float | None:
        if previous_price in (None, 0):
            return None
        return ((current_price - float(previous_price)) / float(previous_price)) * 100

    def _score_event(
        self,
        event: MarketEvent,
        price_change_pct: float | None,
        signal_state: dict[str, Any],
    ) -> int:
        weights = self.config["scoring_weights"]
        score = float(weights["base_score"])

        if price_change_pct is not None:
            move = abs(price_change_pct)
            if move >= 8:
                score += weights["major_price_move"]
            elif move >= 5:
                score += weights["strong_price_move"]
            elif move > float(self.config["alert_thresholds"]["price_movement_pct"]):
                score += weights["notable_price_move"]

        source_scores = self.config.get("source_weights", {})
        score += float(source_scores.get(event.source, source_scores.get("default", 1.0)))

        category_scores = self.config.get("category_weights", {})
        score += float(category_scores.get(event.category, category_scores.get("default", 0.5)))

        previous_alerts = int(signal_state.get("alert_count", 0))
        if previous_alerts >= 5:
            score += float(weights["persistent_signal"])
        elif previous_alerts >= 2:
            score += float(weights["repeat_signal"])

        if "downgrade" in event.headline.lower() or "probe" in event.headline.lower():
            score += float(weights["risk_language"])
        if "upgrade" in event.headline.lower() or "breakthrough" in event.headline.lower():
            score += float(weights["positive_catalyst"])
        if event.status == "resolved":
            score = min(score, 4)

        return clamp(score)

    def _classify_tier(self, importance_score: int) -> str:
        if importance_score >= 8:
            return "T3 Intelligence"
        if importance_score >= 5:
            return "T2 Momentum"
        return "T1 Tripwire"

    def _format_telegram_message(
        self,
        event: MarketEvent,
        importance_score: int,
        tier: str,
        price_change_pct: float | None,
        codex_analysis_needed: bool,
        alert_count: int,
        timestamp: datetime,
    ) -> str:
        move_display = "n/a" if price_change_pct is None else f"{price_change_pct:+.2f}%"
        lines = [
            "ISE ALERT",
            f"Tier: {tier}",
            f"Ticker: {event.ticker}",
            f"Price: {event.price:.4f}",
            f"Move: {move_display}",
            f"Importance: {importance_score}/10",
            f"Source: {event.source}",
            f"Headline: {event.headline}",
        ]
        if event.summary:
            lines.append(f"Summary: {event.summary}")
        lines.extend(
            [
                f"Status: {'CODEX_ANALYSIS_NEEDED' if codex_analysis_needed else 'AUTO_ROUTED'}",
                f"Alert Count: {alert_count}",
                f"Timestamp: {isoformat_z(timestamp)}",
            ]
        )
        return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ISE market signal engine")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to config YAML")
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH), help="Path to JSON state store")
    parser.add_argument("--input-file", help="JSON file containing one event object or a list of events")
    parser.add_argument("--ticker", help="Ticker symbol for a single event")
    parser.add_argument("--price", type=float, help="Latest price for a single event")
    parser.add_argument("--headline", help="Headline for a single event")
    parser.add_argument("--source", help="Source for a single event")
    parser.add_argument("--summary", default="", help="Optional event summary")
    parser.add_argument("--category", default="general", help="Optional event category")
    parser.add_argument("--status", choices=["active", "monitoring", "resolved"], help="Optional event status")
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output instead of compact JSON lines",
    )
    return parser.parse_args()


def load_events(args: argparse.Namespace) -> list[MarketEvent]:
    if args.input_file:
        with Path(args.input_file).open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    elif args.ticker and args.price is not None and args.headline and args.source:
        payload = {
            "ticker": args.ticker,
            "price": args.price,
            "headline": args.headline,
            "source": args.source,
            "summary": args.summary,
            "category": args.category,
            "status": args.status,
        }
    elif not sys.stdin.isatty():
        payload = json.load(sys.stdin)
    else:
        raise SystemExit(
            "Provide `--input-file`, pipe JSON on stdin, or pass --ticker/--price/--headline/--source."
        )

    if isinstance(payload, list):
        return [MarketEvent.from_dict(item) for item in payload]
    if isinstance(payload, dict):
        return [MarketEvent.from_dict(payload)]
    raise SystemExit("Input must be a JSON object or a list of JSON objects.")


def main() -> int:
    args = parse_args()
    engine = SignalEngine(config_path=Path(args.config), state_path=Path(args.state))
    alerts = engine.process_events(load_events(args))
    if args.pretty:
        print(json.dumps(alerts, indent=2))
    else:
        for alert in alerts:
            print(json.dumps(alert, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
