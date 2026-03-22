#!/usr/bin/env python3
"""Prepare deep-dive analysis candidates from ISE signal state."""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STATE_PATH = Path("signals_state.json")
HIGH_IMPORTANCE_THRESHOLD = 8

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare ISE deep-dive analysis candidates.")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH, help="Path to signals state JSON.")
    parser.add_argument("--execute", action="store_true", help="Execute deep-dive analysis immediately after queuing.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be queued/analyzed without executing.")
    return parser.parse_args()


def to_iso8601(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_state(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        state = json.load(handle)
    state.setdefault("signals", {})
    state.setdefault("deep_dive_queue", [])
    return state


def save_state(path: Path, state: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)
        handle.write("\n")


def get_signal_score(signal_state: dict[str, Any]) -> int:
    return int(signal_state.get("adjusted_importance_score", signal_state.get("importance_score", 0)) or 0)


def execute_deep_dive_analysis(state_path: Path, dry_run: bool = False) -> bool:
    """Trigger execute_deep_dive.py to perform Codex analysis on pending signals."""
    script_path = Path(__file__).parent / "execute_deep_dive.py"
    
    if not script_path.exists():
        logger.error(f"execute_deep_dive.py not found at {script_path}")
        return False
    
    if dry_run:
        logger.info("[DRY RUN] Would execute deep-dive analysis")
        return True
    
    try:
        logger.info("Triggering deep-dive analysis via execute_deep_dive.py...")
        result = subprocess.run(
            [sys.executable, str(script_path), "--state", str(state_path)],
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout for full analysis
        )
        
        print(result.stdout)
        if result.stderr:
            logger.warning(f"Analysis stderr: {result.stderr}")
        
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        logger.error("Deep-dive analysis timed out")
        return False
    except Exception as e:
        logger.error(f"Failed to execute deep-dive analysis: {e}")
        return False


def prepare_deep_dive_analysis(
    state_path: Path,
    state: dict[str, Any] | None = None,
    now: datetime | None = None,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Queue high-importance signals for deep-dive analysis."""
    current_state = state if state is not None else load_state(state_path)
    current_state.setdefault("deep_dive_queue", [])
    queued_signals = {entry.get("signal") for entry in current_state["deep_dive_queue"]}
    observed_at = to_iso8601(now or datetime.now(timezone.utc))

    prepared: list[dict[str, Any]] = []
    for signal, signal_state in current_state.get("signals", {}).items():
        score = get_signal_score(signal_state)
        if score < HIGH_IMPORTANCE_THRESHOLD or signal in queued_signals:
            continue
        
        candidate = {
            "signal": signal,
            "score": score,
            "status": "pending_deep_dive",
            "observed_at": observed_at,
            "last_alerted": signal_state.get("last_alerted"),
        }
        
        if dry_run:
            logger.info(f"[DRY RUN] Would queue {signal} (score: {score})")
        else:
            current_state["deep_dive_queue"].append(candidate)
            logger.info(f"Queued {signal} for deep-dive analysis (score: {score})")
        
        prepared.append(candidate)

    if state is None and prepared and not dry_run:
        save_state(state_path, current_state)
    
    return prepared


def main() -> int:
    args = parse_args()
    state = load_state(args.state)
    
    logger.info("ISE Deep-Dive Analysis Engine - Queue Preparation")
    logger.info("=" * 50)
    
    prepared = prepare_deep_dive_analysis(args.state, state=state, dry_run=args.dry_run)
    
    if not args.dry_run:
        save_state(args.state, state)

    print("\nISE Deep-Dive Queue")
    print("===================")
    if not prepared:
        print("- No new high-importance signals ready for deep-dive analysis")
    else:
        for entry in prepared:
            print(f"- {entry['signal']}: score {entry['score']} status {entry['status']}")
    
    # Show current queue status
    pending = [e for e in state.get("deep_dive_queue", []) if e.get("status") == "pending_deep_dive"]
    completed = [e for e in state.get("deep_dive_queue", []) if e.get("status") == "completed"]
    failed = [e for e in state.get("deep_dive_queue", []) if e.get("status") == "failed"]
    
    print(f"\nQueue Status: {len(pending)} pending, {len(completed)} completed, {len(failed)} failed")
    
    # Execute analysis if requested
    if args.execute:
        print("\n" + "=" * 50)
        if pending or args.dry_run:
            execute_deep_dive_analysis(args.state, dry_run=args.dry_run)
        else:
            print("No pending signals to analyze")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
