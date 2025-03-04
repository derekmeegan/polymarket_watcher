"""
Polymarket Watcher - Collector Lambda

This Lambda function fetches market data from Polymarket API and stores it in DynamoDB.
It runs on a schedule (every 5 minutes) to keep the market data up to date.
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
import requests
from decimal import Decimal

from common.config import (
    POLYMARKET_API_URL,
    MARKETS_TABLE,
    HISTORICAL_TABLE,
    TTL_DAYS
)
from common.utils import (
    categorize_market,
    batch_write_to_dynamodb,
    get_tracked_outcome_and_price,
    calculate_ttl
)

def fetch_markets(limit=100, active=True):
    """Fetch markets from Polymarket API"""
    all_markets = []
    offset = 0
    
    while True:
        params = {
            'limit': limit,
            'offset': offset,
            'active': active,
            'ascending': False,
            'end_date_min': (datetime.now(timezone.utc) - timedelta(days=3)).strftime('%Y-%m-%d'),
            'liquidity_num_min': 1_000,
            'volume_num_min': 10_000
        }
        
        try:
            print(f"Fetching markets with offset {offset}...")
            response = requests.get(POLYMARKET_API_URL, params=params)
            response.raise_for_status()
            
            markets_data = response.json()
            
            # Check if we got any markets back
            if not markets_data:
                break
                
            # Add markets to our collection
            all_markets.extend(markets_data)
            
            # If we got fewer markets than the limit, we've reached the end
            if len(markets_data) < limit:
                break
                
            # Increment offset for next batch
            offset += len(markets_data)
            
        except Exception as e:
            print(f"Error fetching markets: {e}")
            break
    
    print(f"Fetched a total of {len(all_markets)} markets")
    return all_markets

def process_markets(markets_data):
    """Process market data and store in DynamoDB"""
    if not markets_data:
        return []
    
    processed_count = 0
    relevant_markets = []
    market_items = []
    historical_items = []
    
    for market in markets_data:
        
        market_id = market.get('id')
        description = market.get('description')
        
        # Get the tracked outcome and price
        tracked_outcome, current_price, outcome_index = get_tracked_outcome_and_price(market)
        
        if tracked_outcome is None or current_price is None:
            continue
        
        # Prepare market data for DynamoDB
        market_item = {
            'id': market_id,
            'question': market.get('question'),
            'description': description,
            'slug': market.get('slug'),
            'liquidity': Decimal(str(market.get('liquidity', 0))),
            'volume': Decimal(str(market.get('volume', 0))),
            'market_start_date': market.get('startDate'),
            'market_end_date': market.get('endDate'),
            'image': market.get('image'),
            'closed': market.get('closed'),
            'submitted_by': market.get('submitted_by'),
            'volume24hr': Decimal(str(market.get('volume24hr', 0))),
            'current_price': Decimal(str(current_price)),
            'tracked_outcome': tracked_outcome,
            'outcome_index': outcome_index,
            'categories': categorize_market(market),
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'ttl': calculate_ttl(TTL_DAYS['markets'])
        }
        
        # Add to batch of market items
        market_items.append(market_item)
        
        # Prepare historical data for DynamoDB
        historical_item = {
            'market_id': market_id,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'outcome': tracked_outcome,
            'outcome_index': outcome_index,
            'price': Decimal(str(current_price)),
            'ttl': calculate_ttl(TTL_DAYS['historical'])
        }
        
        # Add to batch of historical items
        historical_items.append(historical_item)
            
        relevant_markets.append({
            'id': market_id,
            'question': market.get('question'),
            'description': description,
            'slug': market.get('slug'),
            'categories': categorize_market(market),
        })
            
        processed_count += 1
    
    # Batch write market items
    if market_items:
        print(f"Batch writing {len(market_items)} market items to DynamoDB...")
        if batch_write_to_dynamodb(market_items, MARKETS_TABLE):
            print(f"Successfully wrote {len(market_items)} market items to DynamoDB")
        else:
            print("Failed to write market items to DynamoDB")
    
    # Batch write historical items
    if historical_items:
        print(f"Batch writing {len(historical_items)} historical items to DynamoDB...")
        if batch_write_to_dynamodb(historical_items, HISTORICAL_TABLE):
            print(f"Successfully wrote {len(historical_items)} historical items to DynamoDB")
        else:
            print("Failed to write historical items to DynamoDB")
    
    print(f"Processed {processed_count} markets")
    return relevant_markets

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
