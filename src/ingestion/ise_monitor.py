#!/usr/bin/env python3
"""ISE Monitor - Wrapper script that orchestrates RSS fetching and signal processing.

Runs rss_fetcher to get new events, feeds them to SignalEngine,
triggers alerts when thresholds are met, and outputs formatted Telegram messages.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Import local modules
try:
    from rss_fetcher import RSSFeedFetcher, MarketEvent
    from market_check_v2 import SignalEngine, SignalState, ScoringCriteria
    MODULES_AVAILABLE = True
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    MODULES_AVAILABLE = False

DEFAULT_CONFIG_PATH = Path("config.yaml")
DEFAULT_STATE_PATH = Path("signals_state.json")


def format_telegram_message(event: MarketEvent, score: int) -> str:
    """Format a MarketEvent as a Telegram message."""
    emoji = "🔴" if score >= 8 else "🟡" if score >= 6 else "🟢"
    urgency = "URGENT" if score >= 8 else "MODERATE" if score >= 6 else "LOW"
    
    message = f"""
{emoji} <b>ISE Alert - {urgency}</b> {emoji}

📰 <b>{event.title}</b>

Source: {event.source}
Published: {event.published}
Relevance Score: {score}/10

{event.summary[:300]}{'...' if len(event.summary) > 300 else ''}

🔗 <a href="{event.url}">Read more</a>
"""
    return message.strip()


def run_monitor(config_path: Path, state_path: Path, dry_run: bool = False) -> dict[str, Any]:
    """Run the ISE monitoring loop.
    
    Args:
        config_path: Path to config.yaml
        state_path: Path to signals_state.json
        dry_run: If True, don't write state updates
        
    Returns:
        Dictionary with run results
    """
    if not MODULES_AVAILABLE:
        logger.error("Required modules not available. Cannot run monitor.")
        return {"error": "module_import_failed", "events_processed": 0}
    
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "events_fetched": 0,
        "events_processed": 0,
        "alerts_triggered": [],
        "errors": [],
    }
    
    try:
        # Initialize components
        logger.info(f"Loading config from {config_path}")
        fetcher = RSSFeedFetcher(config_path=config_path, state_path=state_path)
        
        logger.info(f"Loading signal engine from {state_path}")
        engine = SignalEngine(state_path=state_path, config_path=config_path)
        
        # Fetch new events
        logger.info("Fetching RSS feeds...")
        events = fetcher.fetch_all_feeds()
        results["events_fetched"] = len(events)
        logger.info(f"Fetched {len(events)} events from RSS feeds")
        
        # Process each event
        for event in events:
            try:
                # Check for duplicates
                if fetcher.is_duplicate(event):
                    logger.debug(f"Skipping duplicate event: {event.title[:50]}")
                    continue
                
                # Score the event
                score, reason = engine.score_event(event)
                logger.info(f"Event '{event.title[:50]}' scored {score}/10 ({reason})")
                
                # Process based on score
                if score >= 8:
                    # High importance - trigger alert
                    alert_msg = format_telegram_message(event, score)
                    results["alerts_triggered"].append({
                        "event_title": event.title,
                        "score": score,
                        "message": alert_msg,
                    })
                    logger.info(f"🚨 HIGH IMPORTANCE ALERT: {event.title[:60]}")
                    
                    # In production, this would send to Telegram
                    if not dry_run:
                        print(f"\n{'='*60}")
                        print("TELEGRAM ALERT (would be sent)")
                        print(f"{'='*60}")
                        print(alert_msg)
                        print(f"{'='*60}\n")
                        
                elif score >= 6:
                    # Moderate importance - log for review
                    logger.info(f"⚠️  Moderate importance event: {event.title[:60]}")
                    
                else:
                    # Low importance - suppress
                    logger.debug(f"Low importance event suppressed: {event.title[:50]}")
                
                results["events_processed"] += 1
                
                # Update state
                if not dry_run:
                    fetcher.mark_as_processed(event)
                    
            except Exception as e:
                logger.error(f"Error processing event '{event.title[:50]}': {e}")
                results["errors"].append({"event": event.title, "error": str(e)})
        
        # Save state
        if not dry_run:
            fetcher.save_state()
            engine.save_state()
            logger.info("State saved successfully")
        
        logger.info(f"Monitor run complete: {results['events_processed']} events processed, "
                   f"{len(results['alerts_triggered'])} alerts triggered")
        
    except Exception as e:
        logger.error(f"Monitor run failed: {e}")
        results["errors"].append({"fatal": True, "error": str(e)})
    
    return results


def main() -> int:
    """Main entry point for ISE Monitor."""
    parser = argparse.ArgumentParser(
        description="ISE Monitor - Intelligent Signal Engine orchestrator"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to config file (default: {DEFAULT_CONFIG_PATH})"
    )
    parser.add_argument(
        "--state",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help=f"Path to state file (default: {DEFAULT_STATE_PATH})"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without saving state changes"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output results to JSON file"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not MODULES_AVAILABLE:
        print("ERROR: Required modules (rss_fetcher, market_check_v2) not available.",
              file=sys.stderr)
        print("Make sure you're running from the projects/ise/ directory.", file=sys.stderr)
        return 1
    
    # Run monitor
    results = run_monitor(
        config_path=args.config,
        state_path=args.state,
        dry_run=args.dry_run
    )
    
    # Output results
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Results written to {args.output}")
    else:
        print(json.dumps(results, indent=2, default=str))
    
    return 0 if not results.get("errors") else 1


if __name__ == "__main__":
    sys.exit(main())
