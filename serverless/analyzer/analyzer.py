"""
Polymarket Watcher - Analyzer Lambda

This Lambda function analyzes market data to detect significant price changes.
It runs after the collector Lambda and triggers the publisher Lambda when significant changes are detected.
"""

import json
import os
import time
from datetime import datetime, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr

from common.config import (
    MARKETS_TABLE,
    HISTORICAL_TABLE,
    POSTS_TABLE,
    MIN_POST_INTERVAL,
    MAX_POSTS_PER_DAY
)
from common.utils import (
    get_dynamodb_client,
    calculate_price_change,
    get_volatility_threshold,
    get_last_post_time
)

def get_current_markets():
    """Get all markets from DynamoDB"""
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(MARKETS_TABLE)
        
        # Scan the table to get all markets
        response = table.scan()
        
        return response.get('Items', [])
    except Exception as e:
        print(f"Error getting markets from DynamoDB: {e}")
        return []

def get_historical_prices(market_id, hours=24):
    """Get historical prices for a market from the last 24 hours"""
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(HISTORICAL_TABLE)
        
        # Calculate timestamp for 24 hours ago
        timestamp_24h_ago = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        
        # Query historical prices
        response = table.query(
            KeyConditionExpression=Key('id').eq(market_id) & Key('timestamp').gt(timestamp_24h_ago)
        )
        
        return response.get('Items', [])
    except Exception as e:
        print(f"Error getting historical prices from DynamoDB: {e}")
        return []

def detect_significant_changes(markets):
    """Detect markets with significant price changes"""
    significant_changes = []
    
    for market in markets:
        market_id = market.get('id')
        current_price = float(market.get('current_price', 0))
        liquidity = float(market.get('liquidity', 0))
        outcome_index = market.get('outcome_index')
        tracked_outcome = market.get('tracked_outcome')
        
        # Get historical prices
        historical_prices = get_historical_prices(market_id)
        
        # Filter historical prices for the same outcome
        historical_prices = [
            p for p in historical_prices 
            if p.get('outcome_index') == outcome_index
        ]
        
        # Sort by timestamp (oldest first)
        historical_prices.sort(key=lambda x: x.get('timestamp', ''))
        
        # Need at least one historical price point
        if not historical_prices:
            continue
        
        # Get the oldest price in the time window
        oldest_price = float(historical_prices[0].get('price', 0))
        
        # Calculate price change
        price_change = calculate_price_change(current_price, oldest_price)
        
        # Get appropriate threshold based on liquidity
        threshold = get_volatility_threshold(liquidity)
        
        # If price change exceeds threshold, add to significant changes
        if price_change >= threshold:
            significant_changes.append({
                'id': market_id,
                'question': market.get('question'),
                'slug': market.get('slug'),
                'current_price': current_price,
                'previous_price': oldest_price,
                'price_change': price_change,
                'liquidity': liquidity,
                'categories': market.get('categories', []),
                'threshold_used': threshold,
                'tracked_outcome': tracked_outcome
            })
    
    # Sort by price change (descending)
    significant_changes.sort(key=lambda x: x['price_change'], reverse=True)
    
    return significant_changes

def can_post_update():
    """
    Check if we can post an update based on:
    1. Time since last post (MIN_POST_INTERVAL)
    2. Number of posts in the last 24 hours (MAX_POSTS_PER_DAY)
    """
    # Get the timestamp of the last post
    last_post_time = get_last_post_time()
    
    if last_post_time:
        # Convert to datetime
        last_post_dt = datetime.fromisoformat(last_post_time)
        
        # Check if enough time has passed since the last post
        time_since_last_post = datetime.utcnow() - last_post_dt
        if time_since_last_post.total_seconds() < MIN_POST_INTERVAL:
            print(f"Not enough time since last post ({time_since_last_post.total_seconds()} seconds)")
            return False
    
    # Check number of posts in the last 24 hours
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(POSTS_TABLE)
        
        # Calculate timestamp for 24 hours ago
        timestamp_24h_ago = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        
        # Scan for posts in the last 24 hours
        response = table.scan(
            FilterExpression=Attr('timestamp').gt(timestamp_24h_ago)
        )
        
        posts_24h = response.get('Items', [])
        
        if len(posts_24h) >= MAX_POSTS_PER_DAY:
            print(f"Reached maximum posts per day ({MAX_POSTS_PER_DAY})")
            return False
    except Exception as e:
        print(f"Error checking post count: {e}")
        # In case of error, allow posting
    
    return True

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    print("Starting Polymarket Watcher Analyzer")
    
    start_time = time.time()
    
    # Get current markets from DynamoDB
    markets = get_current_markets()
    
    if not markets:
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'No markets found in DynamoDB'
            })
        }
    
    print(f"Analyzing {len(markets)} markets")
    
    # Detect significant changes
    significant_changes = detect_significant_changes(markets)
    
    print(f"Detected {len(significant_changes)} markets with significant changes")
    
    # Check if we can post an update
    can_post = can_post_update()
    
    # Prepare response
    market_updates = []
    
    if significant_changes and can_post:
        # Take the top significant change
        market_updates = [significant_changes[0]]
        
        print(f"Selected market for posting: {market_updates[0]['question']}")
    
    execution_time = time.time() - start_time
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Analyzed {len(markets)} markets, found {len(significant_changes)} significant changes',
            'can_post': can_post,
            'market_updates': market_updates,
            'execution_time': f'{execution_time:.2f} seconds'
        })
    }

if __name__ == "__main__":
    # For local testing
    lambda_handler({}, None)
