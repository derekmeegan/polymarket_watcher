"""
Polymarket Watcher - Analyzer Lambda

This Lambda function analyzes market data to detect significant price changes.
It runs after the collector Lambda and triggers the publisher Lambda when significant changes are detected.
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr
import concurrent.futures

from common.config import (
    MARKETS_TABLE,
    HISTORICAL_TABLE,
    POSTS_TABLE
)

from common.utils import (
    get_dynamodb_client,
    calculate_price_change,
    get_volatility_threshold,
    get_last_post_time
)

# Get SNS topic ARN from environment
MARKET_MOVEMENTS_TOPIC_ARN = os.environ.get('MARKET_MOVEMENTS_TOPIC_ARN')

def get_current_markets():
    """Get all markets from DynamoDB"""
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(MARKETS_TABLE)
        
        # Scan the table to get all markets
        items = []
        response = table.scan()
        items.extend(response.get('Items', []))
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
        
        return items
    except Exception as e:
        print(f"Error getting markets from DynamoDB: {e}")
        return []

def get_all_historical_prices_batch(market_ids, hours=6):
    """
    Get historical prices for multiple markets in batch
    Returns a dictionary mapping market_id to its historical prices
    
    Args:
        market_ids: List of market IDs to get historical prices for
        hours: Number of hours to look back (default: 6)
    """
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(HISTORICAL_TABLE)
        
        # Calculate timestamp for 6 hours ago
        timestamp_six_hours_ago = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        # Store results for each market
        results = {}
        
        # Process in batches to avoid excessive memory usage
        batch_size = 25
        for i in range(0, len(market_ids), batch_size):
            batch_ids = market_ids[i:i+batch_size]
            
            # Use ThreadPoolExecutor for parallel queries
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_market = {
                    executor.submit(
                        table.query,
                        KeyConditionExpression=Key('market_id').eq(mid) & Key('timestamp').gt(timestamp_six_hours_ago)
                    ): mid for mid in batch_ids
                }
                
                for future in concurrent.futures.as_completed(future_to_market):
                    market_id = future_to_market[future]
                    try:
                        response = future.result()
                        results[market_id] = response.get('Items', [])
                    except Exception as e:
                        print(f"Error querying historical prices for market {market_id}: {e}")
                        results[market_id] = []
        
        return results
    except Exception as e:
        print(f"Error in batch historical price retrieval: {e}")
        return {}

def get_recently_posted_markets(hours=6):
    """Get list of market IDs that have been posted about recently"""
    try:
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(POSTS_TABLE)
        
        # Calculate timestamp for hours ago
        timestamp_hours_ago = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        # Scan for recent posts
        response = table.scan(
            FilterExpression=Attr('posted_at').gt(timestamp_hours_ago)
        )
        
        # Extract market IDs
        market_ids = [item.get('market_id') for item in response.get('Items', [])]
        
        # Handle pagination if needed
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=Attr('posted_at').gt(timestamp_hours_ago),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            market_ids.extend([item.get('market_id') for item in response.get('Items', [])])
        
        return market_ids
    except Exception as e:
        print(f"Error getting recently posted markets: {e}")
        return []

def detect_significant_changes(markets):
    """Detect markets with significant price changes"""
    significant_changes = []
    
    # Get recently posted markets to avoid duplicates
    recently_posted = get_recently_posted_markets()
    
    # Extract market IDs for batch processing
    market_ids = [market.get('id') for market in markets if market.get('id')]
    
    # Get historical prices for all markets in batch
    historical_prices_by_market = get_all_historical_prices_batch(market_ids)
    
    for market in markets:
        market_id = market.get('id')
        
        # Skip if this market was recently posted about
        if market_id in recently_posted:
            continue
            
        current_price = float(market.get('current_price', 0))
        liquidity = float(market.get('liquidity', 0))
        outcome_index = market.get('outcome_index')
        tracked_outcome = market.get('tracked_outcome')
        
        # Get historical prices for this market
        historical_prices = historical_prices_by_market.get(market_id, [])
        
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

def publish_top_movers_to_sns(significant_changes, max_markets=1):
    """Publish top market movers to SNS topic"""
    if not significant_changes:
        print("No significant changes to publish")
        return False
    
    # Take top N markets
    top_movers = significant_changes[:max_markets]
    
    try:
        # Initialize SNS client
        sns = boto3.client('sns')
        
        # Create message
        message = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'markets': top_movers
        }
        
        # Publish to SNS topic
        response = sns.publish(
            TopicArn=MARKET_MOVEMENTS_TOPIC_ARN,
            Message=json.dumps(message),
            Subject='Polymarket Top Movers'
        )
        
        print(f"Published top movers to SNS: {response['MessageId']}")
        return True
    except Exception as e:
        print(f"Error publishing to SNS: {e}")
        return False

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    print("Starting market analysis...")
    
    # Get all markets from DynamoDB
    markets = get_current_markets()
    print(f"Retrieved {len(markets)} markets from DynamoDB")
    
    # Detect significant changes
    significant_changes = detect_significant_changes(markets)
    print(f"Detected {len(significant_changes)} markets with significant changes")
    
    # Publish top movers to SNS
    if significant_changes:
        publish_top_movers_to_sns(significant_changes)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f"Analyzed {len(markets)} markets, found {len(significant_changes)} with significant changes"
        })
    }

if __name__ == "__main__":
    # For local testing
    lambda_handler({}, None)
