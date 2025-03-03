#!/usr/bin/env python3
"""
Polymarket Watcher - Local Testing Script

This script allows you to test the core functionality of the Polymarket Watcher system locally.
It fetches market data, detects significant changes, and simulates posting to X (Twitter).
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
import requests
from decimal import Decimal
from pprint import pprint

# Add serverless directory to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'serverless'))

# Import common utilities
from common.config import (
    POLYMARKET_API_URL,
    MARKETS_LIMIT,
    # CATEGORIES_OF_INTEREST,
    # LIQUIDITY_VOLATILITY_ADJUSTMENTS
)
from common.utils import (
    categorize_market,
    should_track_market,
    calculate_price_change,
    get_volatility_threshold,
    generate_post_text
)

# Create a cache to store previous market prices
market_cache = {}

def fetch_markets(limit=MARKETS_LIMIT, offset=0, active=True):
    """Fetch markets from Polymarket API"""
    params = {
        'limit': limit,
        'offset': offset,
        'active': active,
        'ascending': False,
        'end_date_min': (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d'),
        'liquidity_num_min': 1000
    }
    
    try:
        print(f"Fetching markets from {POLYMARKET_API_URL} with params: {params}")
        response = requests.get(POLYMARKET_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching markets: {e}")
        return None

def parse_outcomes_and_prices(market):
    """Parse outcomes and prices from market data"""
    outcomes = []
    prices = []
    
    try:
        # Parse outcomes
        if isinstance(market.get('outcomes'), str):
            outcomes = json.loads(market.get('outcomes', '[]'))
        elif isinstance(market.get('outcomes'), list):
            outcomes = market.get('outcomes')
            
        # Parse outcome prices
        if isinstance(market.get('outcomePrices'), str):
            prices = json.loads(market.get('outcomePrices', '[]'))
        elif isinstance(market.get('outcomePrices'), list):
            prices = market.get('outcomePrices')
            
        # Convert prices to float
        prices = [float(price) for price in prices]
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error parsing outcomes/prices for market {market.get('id')}: {e}")
        return [], []
        
    return outcomes, prices

def detect_high_volatility_markets(markets_data):
    """Detect markets with high volatility based on configurable thresholds"""
    high_volatility_markets = []
    
    if not markets_data:
        return high_volatility_markets
    
    for market in markets_data:
        # Check if this market should be tracked
        if not should_track_market(market):
            continue
        
        market_id = market.get('id')
        market_slug = market.get('slug')
        market_question = market.get('question')
        liquidity = float(market.get('liquidity', 0))
        volume = float(market.get('volume', 0))
        
        # Get outcomes and prices
        outcomes, prices = parse_outcomes_and_prices(market)
        
        if not outcomes or not prices or len(outcomes) != len(prices):
            continue
            
        # For binary markets, we'll track the YES price (first outcome)
        # For multi-outcome markets, we'll track the highest probability outcome
        current_price = None
        outcome_index = 0
        
        if len(outcomes) == 2 and "Yes" in outcomes and "No" in outcomes:
            # Binary market - track YES price
            yes_index = outcomes.index("Yes")
            current_price = prices[yes_index]
            outcome_index = yes_index
        else:
            # Multi-outcome market - track highest probability outcome
            outcome_index = prices.index(max(prices))
            current_price = prices[outcome_index]
        
        if market_id and current_price is not None:
            # Check if we have a previous price for this market
            if market_id in market_cache:
                previous_price = market_cache[market_id]['price']
                previous_outcome = market_cache[market_id]['outcome']
                
                # Only compare if we're tracking the same outcome
                if previous_outcome == outcome_index:
                    price_change = calculate_price_change(current_price, previous_price)
                    
                    # Get appropriate threshold based on liquidity
                    threshold = get_volatility_threshold(liquidity)
                    
                    # If price change exceeds threshold, add to high volatility list
                    if price_change >= threshold:
                        # Get market categories
                        categories = categorize_market(market)
                        
                        high_volatility_markets.append({
                            'id': market_id,
                            'slug': market_slug,
                            'question': market_question,
                            'current_price': current_price,
                            'previous_price': previous_price,
                            'price_change': price_change,
                            'liquidity': liquidity,
                            'volume': volume,
                            'categories': categories,
                            'threshold_used': threshold,
                            'outcomes': outcomes,
                            'prices': prices,
                            'tracked_outcome': outcomes[outcome_index],
                            'timestamp': datetime.utcnow().isoformat()
                        })
            
            # Update cache with current price
            market_cache[market_id] = {
                'price': current_price,
                'outcome': outcome_index,
                'timestamp': datetime.utcnow().isoformat()
            }
    
    # Sort by price change (descending)
    high_volatility_markets.sort(key=lambda x: x['price_change'], reverse=True)
    
    return high_volatility_markets

def simulate_twitter_post(market):
    """Simulate posting to Twitter"""
    post_text = generate_post_text(
        market, 
        market['price_change'],
        market['previous_price']
    )
    
    print("\n" + "=" * 80)
    print("SIMULATED TWITTER POST:")
    print("-" * 80)
    print(post_text)
    print("-" * 80)
    
    # Simulate reply with market link
    market_link = f"https://polymarket.com/market/{market['slug']}"
    print(f"REPLY: View and trade this market on Polymarket: {market_link}")
    print("=" * 80 + "\n")

def main():
    """Main function for local testing"""
    print("Polymarket Watcher - Local Testing")
    print("=" * 50)
    
    # Fetch markets from Polymarket
    print("Fetching markets from Polymarket...")
    markets_data = fetch_markets()
    
    if not markets_data:
        print("Failed to fetch markets data")
        return
    
    print(f"Fetched {len(markets_data)} markets")
    
    # For the first run, just cache the prices
    if not market_cache:
        print("First run - caching current prices")
        high_volatility_markets = detect_high_volatility_markets(markets_data)
        print("Prices cached. Will detect volatility on next run.")
        
        # Print sample of markets for verification
        print("\nSample of fetched markets:")
        for i, market in enumerate(markets_data[:5]):
            categories = categorize_market(market)
            liquidity = float(market.get('liquidity', 0))
            volume = float(market.get('volume', 0))
            threshold = get_volatility_threshold(liquidity)
            
            outcomes, prices = parse_outcomes_and_prices(market)
            
            print(f"{i+1}. {market.get('question')} (ID: {market.get('id')})")
            print(f"   Categories: {', '.join(categories) if categories else 'None'}")
            print(f"   Liquidity: ${liquidity:,.2f}")
            print(f"   Volume: ${volume:,.2f}")
            print(f"   Volatility threshold: {threshold*100:.1f}%")
            
            if outcomes and prices:
                print(f"   Outcomes: {outcomes}")
                print(f"   Prices: {[float(p) for p in prices]}")
            print()
        
        print("\nWaiting for next run to detect volatility...")
        print("In a real deployment, the Lambda functions would run every 5 minutes.")
        print("For testing purposes, we'll wait 10 seconds before the next run.")
        time.sleep(10)
        
        # Simulate some price changes for testing
        print("\nSimulating price changes for testing...")
        for market_id in list(market_cache.keys())[:3]:
            # Increase or decrease price by 10-20%
            current_price = market_cache[market_id]['price']
            new_price = current_price * 1.15  # 15% increase
            if new_price > 0.9:  # Avoid going over 100%
                new_price = current_price * 0.85  # 15% decrease instead
            market_cache[market_id]['price'] = current_price
            print(f"Simulated price change for market {market_id}: {current_price:.3f} -> {new_price:.3f}")
        
        # Fetch markets again
        print("\nFetching markets again...")
        markets_data = fetch_markets()
    
    # Detect high volatility markets
    high_volatility_markets = detect_high_volatility_markets(markets_data)
    
    if high_volatility_markets:
        print(f"\nDetected {len(high_volatility_markets)} high volatility markets:")
        for i, market in enumerate(high_volatility_markets):
            print(f"{i+1}. {market['question']}")
            print(f"   Categories: {', '.join(market['categories']) if market['categories'] else 'None'}")
            print(f"   Tracked outcome: {market['tracked_outcome']}")
            print(f"   Current price: {market['current_price']:.3f}")
            print(f"   Previous price: {market['previous_price']:.3f}")
            print(f"   Change: {market['price_change']*100:.2f}%")
            print(f"   Liquidity: ${market['liquidity']:,.2f}")
            print(f"   Volume: ${market['volume']:,.2f}")
            print(f"   Threshold used: {market['threshold_used']*100:.1f}%")
            print()
        
        # Simulate posting to Twitter for the most significant change
        most_significant = high_volatility_markets[0]
        simulate_twitter_post(most_significant)
    else:
        print("No high volatility markets detected")

if __name__ == "__main__":
    main()