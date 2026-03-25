#!/usr/bin/env python3
"""
Portfolio Briefing Generator using Tavily Search
Replaces web_search with Tavily API for higher rate limits
"""

import subprocess
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add script path for tavily_search import
sys.path.insert(0, str(Path.home() / '.openclaw/workspace/.scripts'))
from tavily_search import tavily_search, format_results

PORTFOLIO = {
    'hk': [
        {'ticker': '9988.HK', 'name': 'Alibaba', 'shares': 500, 'price': 123.70},
        {'ticker': '02800.HK', 'name': 'Tracker Fund', 'shares': 1500, 'price': 25.56},
        {'ticker': '09698.HK', 'name': 'GDS Holdings', 'shares': 400, 'price': 42.80},
        {'ticker': '01816.HK', 'name': 'CGN Power', 'shares': 2000, 'price': 3.42},
    ],
    'us': [
        {'ticker': 'TSLA', 'name': 'Tesla', 'shares': 11, 'price': 367.96},
        {'ticker': 'SPY', 'name': 'S&P 500 ETF', 'shares': 4, 'price': 648.57},
        {'ticker': 'MU', 'name': 'Micron', 'shares': 5, 'price': 422.90},
        {'ticker': 'INTC', 'name': 'Intel', 'shares': 48, 'price': 43.87},
        {'ticker': 'NVDA', 'name': 'NVIDIA', 'shares': 4, 'price': 172.70},
    ]
}

def search_market_news():
    """Get overnight market news via Tavily"""
    queries = [
        "US stock market news today",
        "NVIDIA stock price news",
        "Alibaba 9988.HK stock news",
        "Fed interest rate news today",
        "crude oil price today"
    ]
    
    results = {}
    for query in queries:
        result = tavily_search(query, max_results=3, search_depth='basic')
        results[query] = result
    
    return results

def generate_briefing():
    """Generate portfolio briefing"""
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    
    briefing = []
    briefing.append("📊 PORTFOLIO BRIEFING")
    briefing.append(f"Generated: {now}")
    briefing.append("")
    
    # Portfolio summary
    briefing.append("=== PORTFOLIO SNAPSHOT ===")
    briefing.append("")
    briefing.append("HK Portfolio:")
    for stock in PORTFOLIO['hk']:
        value = stock['shares'] * stock['price']
        briefing.append(f"  {stock['ticker']}: {stock['shares']} sh @ HK${stock['price']} = HK${value:,.0f}")
    
    briefing.append("")
    briefing.append("US Portfolio:")
    for stock in PORTFOLIO['us']:
        value = stock['shares'] * stock['price']
        briefing.append(f"  {stock['ticker']}: {stock['shares']} sh @ ${stock['price']} = ${value:,.0f}")
    
    briefing.append("")
    briefing.append("=== MARKET NEWS (via Tavily) ===")
    briefing.append("")
    
    # Get news
    news = search_market_news()
    
    for query, result in news.items():
        briefing.append(f"📰 {query}")
        if result.get('success'):
            if result.get('answer'):
                briefing.append(f"  Summary: {result['answer'][:200]}...")
            for i, r in enumerate(result.get('results', [])[:2], 1):
                briefing.append(f"  {i}. {r.get('title', 'No title')}")
        else:
            briefing.append(f"  Error: {result.get('error', 'Unknown')}")
        briefing.append("")
    
    briefing.append("=== TRIGGERS ===")
    briefing.append("  9988.HK: Add at HK$100 (current ~HK$124)")
    briefing.append("  NVDA: Add at $170 (current ~$173)")
    briefing.append("  INTC: Add at $42 (current ~$44)")
    briefing.append("")
    briefing.append("Managed by: Rex (Head of Research)")
    
    return "\n".join(briefing)

if __name__ == '__main__':
    briefing = generate_briefing()
    print(briefing)
    
    # Save to file
    output_dir = Path.home() / '.openclaw/workspace/research'
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')
    output_file = output_dir / f'BRIEFING_{timestamp}.txt'
    
    with open(output_file, 'w') as f:
        f.write(briefing)
    
    print(f"\nSaved to: {output_file}")
