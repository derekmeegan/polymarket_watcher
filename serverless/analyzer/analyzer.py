"""
Polymarket Watcher - Analyzer Lambda

This Lambda function analyzes market data to detect significant price changes.
It runs after the collector Lambda and triggers the publisher Lambda when significant changes are detected.
It also integrates with the signal analyzer to provide enhanced market movement detection.
"""

import json
import os
from datetime import datetime, timedelta, timezone
import pytz
import uuid

import boto3
from boto3.dynamodb.conditions import Key, Attr
import concurrent.futures
from decimal import Decimal

from common.config import (
    MARKETS_TABLE,
    HISTORICAL_TABLE,
    POSTS_TABLE,
    SIGNALS_TABLE,
    CONFIDENCE_WEIGHTS
)

from common.utils import (
    get_dynamodb_client,
    calculate_price_change,
    get_volatility_threshold,
    calculate_signal_accuracy_metrics
)

# Get SNS topic ARN from environment
MARKET_MOVEMENTS_TOPIC_ARN = os.environ.get('MARKET_MOVEMENTS_TOPIC_ARN')

def is_within_active_hours():
    """Check if current time is within active hours (9 AM to 7 PM EST)"""
    # Get current time in EST
    est_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(est_tz)
    
    # Check if time is between 9 AM and 7 PM
    return 9 <= current_time.hour < 19

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

def get_recent_signals(hours=6):
    """Get list of market IDs that have had signals detected recently"""
    try:
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(SIGNALS_TABLE)
        
        # Calculate timestamp for hours ago
        timestamp_hours_ago = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        # Scan for recent signals
        response = table.scan(
            FilterExpression=Attr('detection_timestamp').gt(timestamp_hours_ago)
        )
        
        # Extract market IDs
        market_ids = [item.get('market_id') for item in response.get('Items', [])]
        
        # Handle pagination if needed
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=Attr('detection_timestamp').gt(timestamp_hours_ago),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            market_ids.extend([item.get('market_id') for item in response.get('Items', [])])
        
        return list(set(market_ids))  # Remove duplicates
    except Exception as e:
        print(f"Error getting recent signals: {e}")
        return []

def detect_significant_changes(markets):
    """Detect markets with significant price changes"""
    significant_changes = []
    
    # Get recently posted markets to avoid duplicates
    recently_posted = get_recently_posted_markets()
    
    # Get markets with recent signals
    recent_signals = get_recent_signals()
    
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
        
        # Calculate confidence score
        confidence_score = 0.5  # Default medium confidence
        
        # If this market has recent signals, increase confidence
        if market_id in recent_signals:
            confidence_score = 0.7  # Higher confidence for markets with signals
        
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
                'tracked_outcome': tracked_outcome,
                'confidence_score': confidence_score,
                'has_signals': market_id in recent_signals
            })
    
    # Sort by confidence score (descending) and then price change (descending)
    significant_changes.sort(key=lambda x: (x['confidence_score'], x['price_change']), reverse=True)
    
    return significant_changes

def save_significant_change_as_signal(market_change):
    """Save a significant price change as a signal"""
    try:
        # Generate a unique signal ID
        signal_id = f"signal_{uuid.uuid4()}"
        
        # Create signal data
        signal_data = {
            'market_id': market_change['id'],
            'signal_id': signal_id,
            'question': market_change['question'],
            'signal_type': 'PRICE_JUMP' if market_change['current_price'] > market_change['previous_price'] else 'PRICE_DROP',
            'signal_strength': 'STRONG' if market_change['price_change'] > 0.15 else 'MODERATE',
            'time_window': 6,  # Default 6-hour window
            'current_price': Decimal(str(market_change['current_price'])),
            'previous_price': Decimal(str(market_change['previous_price'])),
            'price_change': Decimal(str(market_change['price_change'])),
            'threshold_used': Decimal(str(market_change['threshold_used'])),
            'confidence_score': Decimal(str(market_change['confidence_score'])),
            'liquidity': Decimal(str(market_change['liquidity'])),
            'categories': market_change['categories'],
            'tracked_outcome': market_change['tracked_outcome'],
            'detection_timestamp': datetime.now(timezone.utc).isoformat(),
            'ttl': int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp())
        }
        
        # Predict outcome based on price movement
        if market_change['tracked_outcome'] == 'Yes':
            if market_change['current_price'] > market_change['previous_price']:
                signal_data['predicted_outcome'] = 'Yes'
            else:
                signal_data['predicted_outcome'] = 'No'
        
        # Write to DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(SIGNALS_TABLE)
        
        response = table.put_item(Item=signal_data)
        
        return True
    except Exception as e:
        print(f"Error saving significant change as signal: {e}")
        return False

def publish_top_movers_to_sns(significant_changes, max_markets=10):
    """Publish top market movers to SNS topic"""
    if not significant_changes:
        print("No significant changes to publish")
        return False
    
    # Take top N markets
    top_movers = significant_changes[:max_markets]
    
    # Save each significant change as a signal
    for market_change in top_movers:
        save_significant_change_as_signal(market_change)
    
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
    if not is_within_active_hours() and not event.get('ignore_time_filter', False):
        print("Current time is outside active hours (9 AM to 7 PM EST). Skipping execution.")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Execution skipped - outside active hours'
            })
        }
    
    # Get all markets from DynamoDB
    markets = get_current_markets()
    print(f"Retrieved {len(markets)} markets from DynamoDB")
    
    # Detect significant changes
    significant_changes = detect_significant_changes(markets)
    print(f"Detected {len(significant_changes)} markets with significant changes")
    
    # Get signal accuracy metrics
    accuracy_metrics = calculate_signal_accuracy_metrics()
    print(f"Signal accuracy: {accuracy_metrics['accuracy']:.2f} ({accuracy_metrics['correct_signals']}/{accuracy_metrics['total_signals']})")
    
    # Publish top movers to SNS
    if significant_changes:
        publish_top_movers_to_sns(significant_changes)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f"Analyzed {len(markets)} markets, found {len(significant_changes)} with significant changes",
            'signal_accuracy': accuracy_metrics
        })
    }

if __name__ == "__main__":
    # For local testing
    lambda_handler({}, None)
