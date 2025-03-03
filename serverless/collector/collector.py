"""
Polymarket Watcher - Collector Lambda

This Lambda function fetches market data from Polymarket API and stores it in DynamoDB.
It runs on a schedule (every 5 minutes) to keep the market data up to date.
"""

import json
import os
import time
from datetime import datetime, timedelta

import boto3
import requests

from common.config import (
    POLYMARKET_API_URL,
    MARKETS_LIMIT,
    MARKETS_TABLE,
    HISTORICAL_TABLE
)
from common.utils import (
    categorize_market,
    save_market_to_dynamodb,
    save_historical_price,
    get_tracked_outcome_and_price
)

def fetch_markets(limit=MARKETS_LIMIT, active=True):
    """Fetch markets from Polymarket API"""
    all_markets = []
    offset = 0
    
    while True:
        params = {
            'limit': limit,
            'offset': offset,
            'active': active,
            'ascending': False,
            'end_date_min': (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d'),
            'liquidity_num_min': 1_000,
            'volume_num_min': 10_000
        }
        
        try:
            print(f"Fetching markets with offset {offset}...")
            response = requests.get(POLYMARKET_API_URL, params=params)
            response.raise_for_status()
            
            markets_data = response.json()
            
            # Check if we got any markets back
            if not markets_data or len(markets_data) == 0:
                break
                
            # Add markets to our collection
            all_markets.extend(markets_data)
            
            # If we got fewer markets than the limit, we've reached the end
            if len(markets_data) < limit:
                break
                
            # Increment offset for next page
            offset += limit
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching markets: {e}")
            break
    
    print(f"Fetched a total of {len(all_markets)} markets")
    return all_markets

def process_markets(markets_data):
    """Process market data and store in DynamoDB"""
    if not markets_data:
        return 0
    
    processed_count = 0
    relevant_markets = []
    
    for market in markets_data:
        
        market_id = market.get('id')
        description = market.get('description')
        
        # Get the tracked outcome and price
        tracked_outcome, current_price, outcome_index = get_tracked_outcome_and_price(market)
        
        if tracked_outcome is None or current_price is None:
            continue
        
        # Save market to DynamoDB
        if save_market_to_dynamodb(market):
            # Save historical price point
            save_historical_price(
                market_id,
                tracked_outcome,
                current_price,
                outcome_index
            )
            
            # Add to relevant markets list
            relevant_markets.append({
                'id': market_id,
                'question': market.get('question'),
                'description': description,
                'slug': market.get('slug'),
                'categories': categorize_market(market),
            })
            
            processed_count += 1
    
    print(f"Processed {processed_count} relevant markets")
    return processed_count

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    print("Starting Polymarket Watcher Collector")
    
    start_time = time.time()
    
    # Fetch markets from Polymarket
    markets_data = fetch_markets()
    
    if not markets_data:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to fetch markets data from Polymarket API'
            })
        }
    
    # Process markets
    processed_count = process_markets(markets_data)
    
    execution_time = time.time() - start_time
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Successfully processed {processed_count} markets',
            'execution_time': f'{execution_time:.2f} seconds'
        })
    }

if __name__ == "__main__":
    # For local testing
    lambda_handler({}, None)
