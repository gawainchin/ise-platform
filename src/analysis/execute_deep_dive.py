#!/usr/bin/env python3
"""Execute deep-dive analysis for queued ISE signals using Codex CLI."""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_STATE_PATH = Path("/Users/openclaw/.openclaw/workspace/ise-phase1/signals_state.json")
LOG_PATH = Path("/Users/openclaw/.openclaw/workspace/ise-phase1/deep_dive.log")
CODEX_TIMEOUT = 180  # 3 minutes timeout for Codex analysis
NOTION_API_BASE_URL = "https://api.notion.com/v1"
NOTION_API_VERSION = "2022-06-28"
NOTION_API_KEY_PATH = Path.home() / ".config" / "notion" / "api_key"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute deep-dive analysis for queued ISE signals using Codex CLI."
    )
    parser.add_argument(
        "--state",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help="Path to signals state JSON.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be analyzed without executing.",
    )
    parser.add_argument(
        "--signal",
        type=str,
        default=None,
        help="Analyze a specific signal by name (bypasses queue status).",
    )
    parser.add_argument(
        "--skip-notion",
        action="store_true",
        help="Skip Notion archiving.",
    )
    return parser.parse_args()


def load_state(path: Path) -> dict[str, Any]:
    """Load signals state from JSON file."""
    if not path.exists():
        logger.error(f"State file not found: {path}")
        return {"signals": {}, "deep_dive_queue": []}
    
    with path.open("r", encoding="utf-8") as handle:
        state = json.load(handle)
    
    state.setdefault("signals", {})
    state.setdefault("deep_dive_queue", [])
    return state


def save_state(path: Path, state: dict[str, Any]) -> bool:
    """Save signals state to JSON file."""
    try:
        # Create backup
        if path.exists():
            backup_path = path.with_suffix(".json.bak")
            backup_path.write_text(path.read_text())
        
        with path.open("w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2)
            handle.write("\n")
        logger.info(f"State saved to {path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save state: {e}")
        return False


def generate_analysis_prompt(signal: str, signal_data: dict[str, Any]) -> str:
    """Generate a research prompt for Codex analysis."""
    price = signal_data.get("last_price", "N/A")
    importance = signal_data.get("adjusted_importance_score", signal_data.get("importance_score", "N/A"))
    
    # Determine asset type for context
    if signal in ["BTC", "ETH", "SOL", "XRP", "ADA"]:
        asset_type = "cryptocurrency"
        category = "crypto"
    elif "=F" in signal or signal in ["GC=F", "SI=F", "CL=F", "NG=F"]:
        asset_type = "commodity futures"
        category = "commodities"
    elif re.match(r'^[A-Z]{1,5}$', signal) and signal not in ["BTC", "ETH"]:
        asset_type = "stock/equity"
        category = "equities"
    else:
        asset_type = "financial instrument"
        category = "general"
    
    prompt = f"""You are a financial analyst conducting a deep-dive analysis on {signal} ({asset_type}).

SIGNAL CONTEXT:
- Symbol: {signal}
- Last Price: {price}
- Importance Score: {importance}/10
- Asset Category: {category}

RESEARCH TASK:
Conduct a comprehensive analysis covering:

1. MARKET CONTEXT (30%):
   - Current price action and trend
   - Key support/resistance levels
   - Trading volume and liquidity
   - Correlation with broader markets (S&P 500, Nasdaq, etc.)

2. RECENT NEWS & DEVELOPMENTS (40%):
   - Major news from the past 7-14 days
   - Earnings reports (if applicable)
   - Regulatory developments
   - Institutional activity (ETF flows, large holders)
   - Macro factors affecting this asset

3. RISK ASSESSMENT (30%):
   - Key risks (market, regulatory, technical)
   - Volatility analysis
   - Potential catalysts (upcoming events)
   - Bear case scenarios

OUTPUT FORMAT:
Provide a structured analysis with clear sections:

## Executive Summary
[2-3 sentence high-level takeaway]

## Market Context
[Detailed analysis]

## Recent News & Catalysts
[Key developments with dates]

## Risk Assessment
[Risk factors and mitigation]

## Key Levels to Watch
- Support: [price levels]
- Resistance: [price levels]

## Conclusion
[Bull/Bear/Neutral stance with confidence level]

Be thorough but concise. Focus on actionable insights."""

    return prompt


def run_codex_analysis(prompt: str, signal: str) -> dict[str, Any]:
    """Execute Codex CLI for deep-dive analysis using exec mode."""
    logger.info(f"Starting Codex analysis for {signal}...")
    
    try:
        # Use codex exec for non-interactive mode
        # Use '--' separator to ensure prompt is treated as positional argument
        # and not interpreted as CLI flags (e.g., if prompt contains "-q" or "--search")
        cmd = [
            "codex",
            "exec",
            "--full-auto",
            "--",
            prompt,
        ]
        
        logger.info(f"Executing codex exec for {signal}...")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=CODEX_TIMEOUT,
            cwd="/Users/openclaw/.openclaw/workspace/ise-phase1",
        )
        
        # Combine stdout and stderr for analysis output
        analysis_output = result.stdout.strip()
        stderr_output = result.stderr.strip()
        
        # Codex exec may return non-zero but still have useful output
        if result.returncode != 0 and not analysis_output:
            logger.error(f"Codex failed for {signal}: {stderr_output}")
            return {
                "success": False,
                "error": stderr_output or "Unknown error",
                "output": None,
            }
        
        if not analysis_output:
            logger.warning(f"Codex returned empty output for {signal}")
            return {
                "success": False,
                "error": "Empty analysis output",
                "output": None,
            }
        
        logger.info(f"Codex analysis completed for {signal} ({len(analysis_output)} chars)")
        
        return {
            "success": True,
            "error": None,
            "output": analysis_output,
        }
        
    except subprocess.TimeoutExpired:
        logger.error(f"Codex analysis timed out for {signal} after {CODEX_TIMEOUT}s")
        return {
            "success": False,
            "error": f"Analysis timeout ({CODEX_TIMEOUT}s)",
            "output": None,
        }
    except FileNotFoundError:
        logger.error("Codex CLI not found. Is it installed and in PATH?")
        return {
            "success": False,
            "error": "Codex CLI not found",
            "output": None,
        }
    except Exception as e:
        logger.error(f"Exception during Codex analysis for {signal}: {e}")
        return {
            "success": False,
            "error": str(e),
            "output": None,
        }


def analyze_signal(
    signal: str,
    signal_data: dict[str, Any],
    dry_run: bool = False,
) -> dict[str, Any]:
    """Perform deep-dive analysis on a single signal."""
    logger.info(f"Analyzing signal: {signal}")
    
    prompt = generate_analysis_prompt(signal, signal_data)
    
    if dry_run:
        logger.info(f"[DRY RUN] Would analyze {signal} with prompt length {len(prompt)}")
        return {
            "success": True,
            "error": None,
            "output": "[DRY RUN - Analysis skipped]",
            "dry_run": True,
        }
    
    # Run actual Codex analysis
    result = run_codex_analysis(prompt, signal)
    return result


def update_queue_status(
    state: dict[str, Any],
    signal: str,
    analysis_result: dict[str, Any],
) -> None:
    """Update the deep_dive_queue with analysis results."""
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    # Find and update the queue entry
    for entry in state.get("deep_dive_queue", []):
        if entry.get("signal") == signal:
            if analysis_result["success"]:
                notion_result = archive_analysis_to_notion(
                    signal=signal,
                    score=entry.get("score"),
                    completed_at=now,
                    analysis=analysis_result["output"],
                )
                entry["status"] = "completed"
                entry["analysis_completed_at"] = now
                entry["analysis_summary"] = analysis_result["output"][:500] + "..." if len(analysis_result["output"]) > 500 else analysis_result["output"]
                entry["analysis_full"] = analysis_result["output"]
                entry["notional_output"] = generate_notional_output(signal, analysis_result["output"])
                entry["notion_archive"] = notion_result
                analysis_result["notional_output"] = entry["notional_output"]
                analysis_result["notion_archive"] = notion_result
            else:
                entry["status"] = "failed"
                entry["analysis_attempted_at"] = now
                entry["error"] = analysis_result["error"]
            break
    
    # Also update the signal's state
    if signal in state.get("signals", {}):
        state["signals"][signal]["deep_dive_completed"] = analysis_result["success"]
        state["signals"][signal]["deep_dive_completed_at"] = now if analysis_result["success"] else None


def generate_notional_output(signal: str, analysis: str) -> str:
    """Generate a trading notional output from the analysis."""
    lines = analysis.split("\n")
    notional_parts = []
    
    # Extract key sections for notional output
    in_conclusion = False
    conclusion_lines = []
    
    for line in lines:
        if "## Conclusion" in line or "## conclusion" in line.lower():
            in_conclusion = True
            continue
        if in_conclusion:
            if line.startswith("##"):
                break
            if line.strip():
                conclusion_lines.append(line.strip())
    
    # Build notional output
    notional = f"ISE Deep-Dive: {signal}\n"
    notional += "=" * 40 + "\n\n"
    
    if conclusion_lines:
        notional += "📊 " + " ".join(conclusion_lines[:3]) + "\n\n"
    else:
        notional += "📊 Analysis completed. Review full report.\n\n"
    
    # Extract sentiment
    analysis_lower = analysis.lower()
    if "bull" in analysis_lower or "positive" in analysis_lower:
        sentiment = "🟢 Bullish"
    elif "bear" in analysis_lower or "negative" in analysis_lower:
        sentiment = "🔴 Bearish"
    else:
        sentiment = "🟡 Neutral"
    
    notional += f"Sentiment: {sentiment}\n"
    notional += f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    
    return notional


def read_notion_api_key(path: Path = NOTION_API_KEY_PATH) -> str:
    """Load the Notion API key from disk."""
    if not path.exists():
        raise RuntimeError(f"Notion API key not found at {path}")

    api_key = path.read_text(encoding="utf-8").strip()
    if not api_key:
        raise RuntimeError(f"Notion API key file is empty at {path}")
    return api_key


def notion_api_request(
    api_key: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Send an HTTP request to the Notion API."""
    import urllib.request
    import urllib.error
    
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        f"{NOTION_API_BASE_URL}{path}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Notion-Version": NOTION_API_VERSION,
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Notion API {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Notion request failed: {exc.reason}") from exc


def extract_notion_page_title(page: dict[str, Any]) -> str:
    """Extract the title from a Notion page object."""
    properties = page.get("properties", {})
    for prop in properties.values():
        if prop.get("type") != "title":
            continue
        return "".join(
            item.get("plain_text", "")
            for item in prop.get("title", [])
        ).strip()
    return ""


def find_ise_parent_page(api_key: str) -> dict[str, Any]:
    """Find the best matching ISE parent page in Notion."""
    best_match: dict[str, Any] | None = None
    best_score = -1

    for query in ("Intelligent Signal Engine", "ISE"):
        response = notion_api_request(
            api_key,
            "POST",
            "/search",
            {
                "query": query,
                "filter": {
                    "property": "object",
                    "value": "page",
                },
                "page_size": 10,
            },
        )

        for result in response.get("results", []):
            title = extract_notion_page_title(result)
            title_lower = title.lower()
            score = 0

            if title_lower == "intelligent signal engine":
                score = 100
            elif "intelligent signal engine" in title_lower:
                score = 90
            elif re.search(r"\bise\b", title, flags=re.IGNORECASE):
                score = 75
            elif query.lower() in title_lower:
                score = 50

            if score > best_score:
                best_score = score
                best_match = {
                    "id": result.get("id"),
                    "url": result.get("url"),
                    "title": title or "(untitled)",
                }

    if best_match is None:
        raise RuntimeError('Unable to find a Notion parent page matching "Intelligent Signal Engine" or "ISE"')

    logger.info(
        "Using Notion parent page '%s' (%s)",
        best_match["title"],
        best_match["id"],
    )
    return best_match


def split_notion_text(text: str, limit: int = 1800) -> list[str]:
    """Split text into chunks small enough for Notion rich_text blocks."""
    chunks: list[str] = []
    remaining = text.strip()

    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        split_at = remaining.rfind("\n", 0, limit)
        if split_at == -1:
            split_at = remaining.rfind(" ", 0, limit)
        if split_at == -1 or split_at < limit // 2:
            split_at = limit

        chunks.append(remaining[:split_at].strip())
        remaining = remaining[split_at:].strip()

    return [chunk for chunk in chunks if chunk]


def build_paragraph_block(text: str) -> dict[str, Any]:
    """Create a Notion paragraph block for plain text."""
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [
                {
                    "type": "text",
                    "text": {
                        "content": text,
                    },
                }
            ]
        },
    }


def build_analysis_blocks(
    signal: str,
    score: Any,
    completed_at: str,
    analysis: str,
) -> list[dict[str, Any]]:
    """Build the Notion block list for an archived analysis page."""
    blocks: list[dict[str, Any]] = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "Analysis Metadata",
                        },
                    }
                ]
            },
        },
        build_paragraph_block(f"Signal: {signal}"),
        build_paragraph_block(f"Completed At (UTC): {completed_at}"),
        build_paragraph_block(f"Importance Score: {score}"),
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "Full Analysis",
                        },
                    }
                ]
            },
        },
    ]

    for paragraph in analysis.split("\n\n"):
        cleaned = paragraph.strip()
        if not cleaned:
            continue
        for chunk in split_notion_text(cleaned):
            blocks.append(build_paragraph_block(chunk))

    return blocks


def archive_analysis_to_notion(
    signal: str,
    score: Any,
    completed_at: str,
    analysis: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Archive a completed analysis to Notion without breaking the pipeline on failure."""
    try:
        title = f"{signal} Deep Dive - {completed_at}"

        if dry_run:
            read_notion_api_key()
            logger.info(
                "[DRY RUN] Would archive %s to Notion with title '%s'",
                signal,
                title,
            )
            return {
                "success": True,
                "dry_run": True,
                "page_id": "dry-run",
                "page_url": None,
                "parent_page_id": None,
                "parent_page_title": None,
                "archived_at": completed_at,
                "error": None,
            }

        api_key = read_notion_api_key()
        parent_page = find_ise_parent_page(api_key)
        response = notion_api_request(
            api_key,
            "POST",
            "/pages",
            {
                "parent": {
                    "type": "page_id",
                    "page_id": parent_page["id"],
                },
                "properties": {
                    "title": {
                        "title": [
                            {
                                "type": "text",
                                "text": {
                                    "content": title,
                                },
                            }
                        ]
                    }
                },
                "children": build_analysis_blocks(signal, score, completed_at, analysis),
            },
        )

        logger.info("Archived %s analysis to Notion page %s", signal, response.get("id"))
        return {
            "success": True,
            "dry_run": False,
            "page_id": response.get("id"),
            "page_url": response.get("url"),
            "parent_page_id": parent_page["id"],
            "parent_page_title": parent_page["title"],
            "archived_at": completed_at,
            "error": None,
        }
    except Exception as exc:
        logger.error("Notion archive failed for %s: %s", signal, exc)
        return {
            "success": False,
            "dry_run": dry_run,
            "page_id": None,
            "page_url": None,
            "archived_at": None,
            "error": str(exc),
        }


def process_deep_dive_queue(
    state_path: Path,
    dry_run: bool = False,
    specific_signal: str | None = None,
) -> list[dict[str, Any]]:
    """Process all pending deep-dive signals in the queue."""
    state = load_state(state_path)
    results = []
    
    if specific_signal:
        # Analyze specific signal bypassing queue status
        if specific_signal not in state.get("signals", {}):
            logger.error(f"Signal {specific_signal} not found in state")
            return []
        
        signal_data = state["signals"][specific_signal]
        analysis_result = analyze_signal(specific_signal, signal_data, dry_run)
        
        # Add to queue if not present
        existing = next(
            (e for e in state.get("deep_dive_queue", []) if e.get("signal") == specific_signal),
            None,
        )
        if not existing:
            state.setdefault("deep_dive_queue", []).append({
                "signal": specific_signal,
                "score": signal_data.get("adjusted_importance_score", signal_data.get("importance_score", 0)),
                "status": "pending_deep_dive",
                "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            })
        
        if not dry_run:
            update_queue_status(state, specific_signal, analysis_result)
            save_state(state_path, state)
        
        results.append({
            "signal": specific_signal,
            "result": analysis_result,
        })
    else:
        # Process pending queue items
        pending = [
            entry for entry in state.get("deep_dive_queue", [])
            if entry.get("status") == "pending_deep_dive"
        ]
        
        if not pending:
            logger.info("No pending deep-dive signals in queue")
            return []
        
        logger.info(f"Found {len(pending)} pending deep-dive signal(s)")
        
        for entry in pending:
            signal = entry.get("signal")
            signal_data = state.get("signals", {}).get(signal, {})
            
            if not signal_data:
                logger.warning(f"No data found for signal {signal}, skipping")
                continue
            
            analysis_result = analyze_signal(signal, signal_data, dry_run)
            
            if not dry_run:
                update_queue_status(state, signal, analysis_result)
                save_state(state_path, state)
            
            results.append({
                "signal": signal,
                "result": analysis_result,
            })
    
    return results


def main() -> int:
    args = parse_args()
    
    logger.info("=" * 60)
    logger.info("ISE Deep-Dive Analysis Engine")
    logger.info("=" * 60)
    
    results = process_deep_dive_queue(
        args.state,
        dry_run=args.dry_run,
        specific_signal=args.signal,
    )
    
    # Print summary
    print("\n" + "=" * 60)
    print("DEEP-DIVE ANALYSIS SUMMARY")
    print("=" * 60)
    
    if not results:
        print("No signals analyzed")
        return 0
    
    for item in results:
        signal = item["signal"]
        result = item["result"]
        
        if result["success"]:
            status_icon = "✅"
            status_text = "COMPLETED"
        else:
            status_icon = "❌"
            status_text = f"FAILED: {result.get('error', 'Unknown error')}"
        
        print(f"\n{status_icon} {signal}: {status_text}")
        
        if result["success"] and "notional_output" in result:
            print(f"\n{result['notional_output']}")
    
    print("\n" + "=" * 60)
    print(f"Log file: {LOG_PATH}")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
