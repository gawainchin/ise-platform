#!/usr/bin/env python3
"""ISE Feedback Handler - Process Telegram reactions and adjust importance scores."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

LOGGER = logging.getLogger("ise.feedback")
TELEGRAM_API_BASE = "https://api.telegram.org"

# Feedback configuration
REACTION_SCORES = {
    "👍": 1,   # Increase importance by 1
    "👎": -1,  # Decrease importance by 1
    "❤️": 2,   # Strong positive - increase by 2
    "🚀": 2,   # Strong positive signal
    "⚠️": 2,   # Strong negative signal
    "🗑️": -2,  # Strong negative - decrease by 2
}

# Min/max importance bounds
MIN_SCORE = 1
MAX_SCORE = 10

# Feedback window (how long after alert to accept feedback)
FEEDBACK_WINDOW_HOURS = 48


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process ISE user feedback from Telegram reactions.")
    parser.add_argument("--state", type=Path, default=Path("signals_state.json"), help="Path to signals state JSON.")
    parser.add_argument("--dry-run", action="store_true", help="Process without updating state.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    parser.add_argument("--continuous", action="store_true", help="Run continuously polling for updates.")
    parser.add_argument("--poll-interval", type=int, default=60, help="Seconds between polls (default: 60).")
    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def load_state(path: Path) -> dict[str, Any]:
    """Load signals state from JSON."""
    with path.open("r", encoding="utf-8") as f:
        state = json.load(f)
    state.setdefault("signals", {})
    state.setdefault("tracked_alerts", [])
    state.setdefault("user_feedback", [])
    return state


def save_state(path: Path, state: dict[str, Any]) -> None:
    """Save signals state to JSON."""
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
        f.write("\n")


def get_telegram_updates(token: str, offset: int | None = None) -> list[dict[str, Any]]:
    """Fetch updates from Telegram Bot API."""
    url = f"{TELEGRAM_API_BASE}/bot{token}/getUpdates"
    if offset:
        url += f"?offset={offset}"

    try:
        request = Request(url, method="GET")
        with urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
            if data.get("ok"):
                return data.get("result", [])
    except Exception as e:
        LOGGER.error(f"Failed to fetch Telegram updates: {e}")

    return []


def get_message_reactions(token: str, chat_id: str, message_id: int) -> list[dict[str, Any]]:
    """Get reactions for a specific message."""
    # Note: Telegram Bot API requires message_reaction update type
    # This is a simplified version - actual implementation would track via updates
    url = f"{TELEGRAM_API_BASE}/bot{token}/getUpdates?allowed_updates=[\"message_reaction\"]"

    try:
        request = Request(url, method="GET")
        with urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
            if data.get("ok"):
                reactions = []
                for update in data.get("result", []):
                    reaction = update.get("message_reaction", {})
                    if reaction.get("message_id") == message_id:
                        reactions.append(reaction)
                return reactions
    except Exception as e:
        LOGGER.error(f"Failed to fetch reactions: {e}")

    return []


def find_tracked_alert(state: dict[str, Any], message_id: int) -> dict[str, Any] | None:
    """Find tracked alert by message ID."""
    for alert in state.get("tracked_alerts", []):
        if alert.get("message_id") == message_id:
            return alert
    return None


def is_feedback_window_open(alert: dict[str, Any]) -> bool:
    """Check if feedback is still accepted for this alert."""
    try:
        sent_at = datetime.fromisoformat(alert.get("sent_at", "").replace("Z", "+00:00"))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=FEEDBACK_WINDOW_HOURS)
        return sent_at > cutoff
    except:
        return False


def calculate_score_adjustment(reactions: list[str]) -> int:
    """Calculate total score adjustment from reactions."""
    adjustment = 0
    for reaction in reactions:
        adjustment += REACTION_SCORES.get(reaction, 0)
    return adjustment


def process_feedback(
    state: dict[str, Any],
    alert: dict[str, Any],
    reactions: list[str],
    user_id: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any] | None:
    """Process feedback and update signal importance score."""
    signal = alert.get("signal")
    if not signal:
        return None

    if not is_feedback_window_open(alert):
        LOGGER.debug(f"Feedback window closed for {signal}")
        return None

    # Calculate adjustment
    adjustment = calculate_score_adjustment(reactions)
    if adjustment == 0:
        return None

    # Get current score
    signal_state = state.get("signals", {}).get(signal, {})
    current_score = signal_state.get("adjusted_importance_score", signal_state.get("importance_score", 5))

    # Apply adjustment with bounds
    new_score = max(MIN_SCORE, min(MAX_SCORE, current_score + adjustment))

    if new_score == current_score:
        LOGGER.debug(f"Score unchanged for {signal} (at bounds)")
        return None

    # Record feedback
    feedback_entry = {
        "signal": signal,
        "message_id": alert.get("message_id"),
        "user_id": user_id,
        "reactions": reactions,
        "previous_score": current_score,
        "adjustment": adjustment,
        "new_score": new_score,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        LOGGER.info(f"[DRY RUN] Would update {signal}: {current_score} -> {new_score} (adjustment: {adjustment:+d})")
        return feedback_entry

    # Update state
    signal_state["adjusted_importance_score"] = new_score
    signal_state.setdefault("user_feedback", []).append(feedback_entry)
    state.setdefault("user_feedback", []).append(feedback_entry)

    # Mark alert as processed
    alert["feedback"] = feedback_entry
    alert["adjusted_score"] = new_score

    LOGGER.info(f"Updated {signal} importance: {current_score} -> {new_score} (feedback: {reactions})")

    return feedback_entry


def process_all_pending_feedback(
    state: dict[str, Any],
    token: str,
    chat_id: str,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Process all pending feedback from Telegram."""
    processed = []

    # Get recent updates with reactions
    updates = get_telegram_updates(token)

    for update in updates:
        # Check for message_reaction updates
        reaction_update = update.get("message_reaction")
        if not reaction_update:
            continue

        message_id = reaction_update.get("message_id")
        user = reaction_update.get("user", {})
        user_id = user.get("id")

        # Extract reactions
        new_reactions = reaction_update.get("new_reaction", [])
        reactions = [r.get("emoji") for r in new_reactions if r.get("type") == "emoji"]

        if not reactions:
            continue

        # Find matching tracked alert
        alert = find_tracked_alert(state, message_id)
        if not alert:
            LOGGER.debug(f"No tracked alert for message {message_id}")
            continue

        # Process the feedback
        result = process_feedback(state, alert, reactions, user_id, dry_run)
        if result:
            processed.append(result)

    return processed


def simulate_feedback(
    state: dict[str, Any],
    signal: str,
    reaction: str,
    dry_run: bool = False,
) -> dict[str, Any] | None:
    """Simulate feedback for testing purposes."""
    # Find most recent alert for signal
    for alert in reversed(state.get("tracked_alerts", [])):
        if alert.get("signal") == signal and not alert.get("feedback"):
            return process_feedback(state, alert, [reaction], user_id=12345, dry_run=dry_run)

    LOGGER.warning(f"No active alert found for {signal}")
    return None


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        LOGGER.error("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set")
        return 1

    LOGGER.info("ISE Feedback Handler starting...")
    LOGGER.info(f"State file: {args.state}")
    LOGGER.info(f"Feedback window: {FEEDBACK_WINDOW_HOURS} hours")

    if args.continuous:
        LOGGER.info(f"Running in continuous mode (poll interval: {args.poll_interval}s)")
        import time

        last_update_id = None
        while True:
            try:
                state = load_state(args.state)
                processed = process_all_pending_feedback(state, token, chat_id, args.dry_run)

                if processed and not args.dry_run:
                    save_state(args.state, state)
                    LOGGER.info(f"Processed {len(processed)} feedback items")

                time.sleep(args.poll_interval)
            except KeyboardInterrupt:
                LOGGER.info("Stopping feedback handler...")
                break
            except Exception as e:
                LOGGER.error(f"Error in feedback loop: {e}")
                time.sleep(args.poll_interval)
    else:
        # One-time run
        state = load_state(args.state)
        processed = process_all_pending_feedback(state, token, chat_id, args.dry_run)

        if processed and not args.dry_run:
            save_state(args.state, state)

        LOGGER.info(f"Processed {len(processed)} feedback items")

        # Display summary
        print("\nFeedback Summary")
        print("=" * 50)
        if processed:
            for item in processed:
                print(f"  {item['signal']}: {item['previous_score']} -> {item['new_score']} ({item['adjustment']:+d})")
        else:
            print("  No new feedback processed")

    return 0


if __name__ == "__main__":
    sys.exit(main())
