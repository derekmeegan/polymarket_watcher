"""
Utility functions for the Polymarket Watcher
"""

import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import time

import boto3
from boto3.dynamodb.conditions import Key

from .config import (
    CATEGORIES_OF_INTEREST,
    VOLATILITY_THRESHOLDS,
    LOW_LIQUIDITY_THRESHOLD,
    MEDIUM_LIQUIDITY_THRESHOLD,
    HIGH_LIQUIDITY_THRESHOLD,
    LIQUIDITY_VOLATILITY_ADJUSTMENTS,
    MARKETS_TABLE,
    HISTORICAL_TABLE,
    POSTS_TABLE,
    TTL_DAYS,
    SIGNALS_TABLE,
    RESOLUTIONS_TABLE,
    SIGNAL_TYPES
)

def get_dynamodb_client():
    """Initialize DynamoDB client"""
    return boto3.resource('dynamodb')

def categorize_market(market):
    """
    Categorize a market based on its question and description
    Returns a list of categories that match
    """
    categories = []
    
    # Get the market question and description
    question = market.get('question', '').lower()
    description = market.get('description', '').lower()
    
    # Combine question and description for searching
    text = f"{question} {description}"
    
    # Check for each category of interest
    for category, keywords in CATEGORIES_OF_INTEREST.items():
        for keyword in keywords:
            # Use word boundary to avoid partial matches
            pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
            if re.search(pattern, text):
                categories.append(category)
                break  # Once we've matched a category, no need to check other keywords
    
    return list(set(categories))  # Remove duplicates

def get_liquidity_tier(liquidity):
    """
    Determine the liquidity tier of a market
    Returns one of: 'very_low', 'low', 'medium', 'high'
    """
    if liquidity < LOW_LIQUIDITY_THRESHOLD:
        return 'very_low'
    elif liquidity < MEDIUM_LIQUIDITY_THRESHOLD:
        return 'low'
    elif liquidity < HIGH_LIQUIDITY_THRESHOLD:
        return 'medium'
    else:
        return 'high'

def should_track_market(market):
    """
    Determine if a market should be tracked based on:
    1. Has sufficient liquidity
    2. Is not ignored based on liquidity tier
    3. Has at least one category of interest
    """
    # Check if market has sufficient liquidity
    liquidity = float(market.get('liquidity', 0))
    
    # Get liquidity tier and check if it should be ignored
    tier = get_liquidity_tier(liquidity)
    if LIQUIDITY_VOLATILITY_ADJUSTMENTS[tier]['ignore']:
        return False
    
    # Check if market has at least one category of interest
    categories = categorize_market(market)
    if not categories:
        return False
    
    return True

def calculate_price_change(current_price, previous_price):
    """
    Calculate the percentage change between current and previous price
    """
    if previous_price == 0:
        return 0
    
    return abs(float(current_price) - float(previous_price)) / float(previous_price)

def calculate_significant_price_change(current_price, previous_price, min_absolute_change=0.03):
    """
    Calculate a significant price change that considers both relative and absolute changes
    
    Args:
        current_price: Current price (0.01-1.00 range)
        previous_price: Previous price (0.01-1.00 range)
        min_absolute_change: Minimum absolute change to be considered significant (default: 0.03 or 3%)
        
    Returns:
        A tuple of (is_significant, change_value, change_type)
        - is_significant: Boolean indicating if the change is significant
        - change_value: The calculated change value (absolute or relative depending on which was used)
        - change_type: String indicating which type of change was used ('absolute' or 'relative')
    """
    current_price = float(current_price)
    previous_price = float(previous_price)
    
    # Calculate absolute change (in percentage points)
    absolute_change = abs(current_price - previous_price)
    
    # Calculate relative change
    if previous_price == 0:
        relative_change = 0
    else:
        relative_change = absolute_change / previous_price
    
    # For very low prices, prioritize absolute change
    if previous_price < 0.10:  # For prices below 10%
        if absolute_change >= min_absolute_change:
            return (True, absolute_change, 'absolute')
        return (False, absolute_change, 'absolute')
    
    # For mid to high prices, use relative change with a minimum absolute threshold
    if absolute_change >= min_absolute_change or relative_change >= 0.15:
        # Return the larger of the two changes
        if absolute_change > relative_change:
            return (True, absolute_change, 'absolute')
        return (True, relative_change, 'relative')
    
    return (False, max(absolute_change, relative_change), 'relative')

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
    except (json.JSONDecodeError, ValueError):
        return [], []
        
    return outcomes, prices

def get_tracked_outcome_and_price(market):
    """
    Get the outcome to track and its price
    For binary markets, track the YES outcome
    For multi-outcome markets, track the highest probability outcome
    """
    outcomes, prices = parse_outcomes_and_prices(market)
    
    if not outcomes or not prices or len(outcomes) != len(prices):
        return None, None, -1
    
    if len(outcomes) == 2 and "Yes" in outcomes and "No" in outcomes:
        # Binary market - track YES price
        yes_index = outcomes.index("Yes")
        return outcomes[yes_index], prices[yes_index], yes_index
    else:
        # Multi-outcome market - track highest probability outcome
        max_index = prices.index(max(prices))
        return outcomes[max_index], prices[max_index], max_index

def get_previous_price(market_id, outcome_index, table_name=MARKETS_TABLE):
    """
    Get the previous price for a market from DynamoDB
    """
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(table_name)
        
        # Get the item
        response = table.get_item(
            Key={'id': market_id}
        )
        
        if 'Item' in response:
            item = response['Item']
            # Check if we're tracking the same outcome
            if item.get('outcome_index') == outcome_index:
                return float(item.get('current_price', 0))
        
        return None
    except Exception as e:
        print(f"Error getting previous price from DynamoDB: {e}")
        return None

def save_post_to_dynamodb(market_id, post_content, idx):
    """
    Save a post record to DynamoDB
    
    Args:
        market_id: ID of the market that was posted about
        tweet_id: ID of the tweet that was posted
        
    Returns:
        The DynamoDB item that was created
    """
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(POSTS_TABLE)
        
        # Generate a unique post ID
        post_id = f"post_{int(time.time())}_{market_id}"
        
        # Current timestamp
        now = datetime.now(timezone.utc)
        timestamp = now.isoformat()
        sortable_timestamp = now.strftime("%Y%m%d%H%M%S")
        
        # Create post record
        post_item = {
            'id': post_id,
            'content': post_content,
            'market_id': str(market_id),
            'posted_at': timestamp,
            'sortable_timestamp': sortable_timestamp,
            'posted_automatically': idx == 0
        }
        
        # Save to DynamoDB
        table.put_item(Item=post_item)
        
        print(f"Saved post record to DynamoDB: {post_id}")
        
        return post_item
    except Exception as e:
        print(f"Error saving post to DynamoDB: {e}")
        return None

def get_last_post_time(market_id=None, table_name=POSTS_TABLE):
    """
    Get the timestamp of the last post for a market
    If market_id is None, get the timestamp of the last post for any market
    """
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(table_name)
        
        if market_id:
            # Get the last post for a specific market
            response = table.query(
                KeyConditionExpression=Key('id').eq(market_id),
                ScanIndexForward=False,  # descending order
                Limit=1
            )
        else:
            # Get the last post for any market
            response = table.scan(
                Limit=10,
                ScanIndexForward=False  # descending order
            )
            
            # Sort by timestamp
            if 'Items' in response:
                response['Items'].sort(
                    key=lambda x: x.get('timestamp', ''),
                    reverse=True
                )
        
        if 'Items' in response and response['Items']:
            return response['Items'][0].get('timestamp')
        
        return None
    except Exception as e:
        print(f"Error getting last post time from DynamoDB: {e}")
        return None

def generate_post_text(market, price_change, previous_price):
    """
    Generate text for a Twitter post based on market data
    """
    question = market.get('question', '')
    current_price = market.get('current_price', 0)
    tracked_outcome = market.get('tracked_outcome', '')
    
    # For binary markets with Yes/No outcomes
    if tracked_outcome == "Yes":
        change_direction = "increased" if current_price > previous_price else "decreased"
        post = f"Market Update: \"{question}\"\n\n"
        post += f"Probability has {change_direction} from {previous_price:.1%} to {current_price:.1%} ({abs(price_change) * 100:.1f}% swing)"
    else:
        # For multi-outcome markets
        change_direction = "increased" if current_price > previous_price else "decreased"
        post = f"Market Update: \"{question}\"\n\n"
        post += f"Probability for \"{tracked_outcome}\" has {change_direction} from {previous_price:.1%} to {current_price:.1%} ({abs(price_change) * 100:.1f}% swing)"
    
    return post

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def decimal_to_float(obj):
    """Convert Decimal values to float in a dictionary"""
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

def calculate_ttl(days):
    """Calculate TTL timestamp for DynamoDB"""
    return int((datetime.utcnow() + timedelta(days=days)).timestamp())

def prepare_for_dynamodb(item):
    """Prepare an item for DynamoDB by converting values to appropriate types"""
    if isinstance(item, dict):
        return {k: prepare_for_dynamodb(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [prepare_for_dynamodb(i) for i in item]
    elif isinstance(item, float):
        return Decimal(str(item))
    elif isinstance(item, bool):
        return bool(item)
    elif isinstance(item, (int, str)):
        return item
    else:
        return str(item)

def get_volatility_threshold(liquidity):
    """
    Get the appropriate volatility threshold based on liquidity
    """
    # Get liquidity tier
    tier = get_liquidity_tier(liquidity)
    
    # Return threshold based on tier
    return LIQUIDITY_VOLATILITY_ADJUSTMENTS[tier]['threshold']

def calculate_standard_deviation(values):
    """
    Calculate standard deviation of a list of values
    """
    if not values or len(values) < 2:
        return 0
    
    # Calculate mean
    mean = sum(values) / len(values)
    
    # Calculate sum of squared differences
    squared_diff_sum = sum((x - mean) ** 2 for x in values)
    
    # Calculate standard deviation
    return (squared_diff_sum / len(values)) ** 0.5

def calculate_z_score(value, mean, std_dev):
    """
    Calculate z-score (standard score) of a value
    """
    if std_dev == 0:
        return 0
    
    return (value - mean) / std_dev

def get_signal_by_id(signal_id, market_id):
    """
    Get a signal by its ID and market ID
    """
    try:
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(SIGNALS_TABLE)
        
        response = table.get_item(
            Key={
                'market_id': market_id,
                'signal_id': signal_id
            }
        )
        
        if 'Item' in response:
            return response['Item']
        
        return None
    except Exception as e:
        print(f"Error getting signal by ID: {e}")
        return None

def get_resolution_by_market_id(market_id):
    """
    Get resolution data for a market
    """
    try:
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(RESOLUTIONS_TABLE)
        
        response = table.get_item(
            Key={'market_id': market_id}
        )
        
        if 'Item' in response:
            return response['Item']
        
        return None
    except Exception as e:
        print(f"Error getting resolution by market ID: {e}")
        return None

def calculate_signal_accuracy_metrics(category=None, liquidity_tier=None):
    """
    Calculate accuracy metrics for signals
    
    Returns:
        dict: Dictionary with accuracy metrics
    """
    try:
        dynamodb = get_dynamodb_client()
        signals_table = dynamodb.Table(SIGNALS_TABLE)
        
        # Build filter expression
        filter_expression = Attr('actual_outcome').exists()
        
        if category:
            filter_expression = filter_expression & Attr('category').eq(category)
        
        if liquidity_tier:
            filter_expression = filter_expression & Attr('liquidity_tier').eq(liquidity_tier)
        
        # Query signals with resolutions
        response = signals_table.scan(
            FilterExpression=filter_expression
        )
        
        signals = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = signals_table.scan(
                FilterExpression=filter_expression,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            signals.extend(response.get('Items', []))
        
        # Calculate metrics
        if not signals:
            return {
                'total_signals': 0,
                'correct_signals': 0,
                'accuracy': 0,
                'by_signal_type': {}
            }
        
        total_signals = len(signals)
        correct_signals = sum(1 for signal in signals if signal.get('was_correct', False))
        
        # Calculate accuracy by signal type
        by_signal_type = {}
        for signal_type in SIGNAL_TYPES:
            type_signals = [s for s in signals if s.get('signal_type') == signal_type]
            if type_signals:
                type_correct = sum(1 for s in type_signals if s.get('was_correct', False))
                by_signal_type[signal_type] = {
                    'total': len(type_signals),
                    'correct': type_correct,
                    'accuracy': type_correct / len(type_signals)
                }
        
        return {
            'total_signals': total_signals,
            'correct_signals': correct_signals,
            'accuracy': correct_signals / total_signals if total_signals > 0 else 0,
            'by_signal_type': by_signal_type
        }
    except Exception as e:
        print(f"Error calculating signal accuracy metrics: {e}")
        return {
            'total_signals': 0,
            'correct_signals': 0,
            'accuracy': 0,
            'by_signal_type': {}
        }

def batch_write_to_dynamodb(items, table_name):
    """
    Batch write items to DynamoDB table
    
    Args:
        items: List of items to write
        table_name: Name of the table to write to
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not items:
        return True
        
    try:
        dynamodb = boto3.resource('dynamodb')
        
        # Process in batches of 25 (DynamoDB batch write limit)
        for i in range(0, len(items), 25):
            batch = {
                table_name: [
                    {'PutRequest': {'Item': item}} for item in items[i:i+25]
                ]
            }
            
            response = dynamodb.meta.client.batch_write_item(RequestItems=batch)
            unprocessed = response.get('UnprocessedItems', {})
            
            # Retry unprocessed items
            retry_count = 0
            max_retries = 3
            while unprocessed.get(table_name) and retry_count < max_retries:
                print(f"Retrying {len(unprocessed[table_name])} unprocessed items...")
                response = dynamodb.meta.client.batch_write_item(RequestItems=unprocessed)
                unprocessed = response.get('UnprocessedItems', {})
                retry_count += 1
                
            if unprocessed.get(table_name):
                print(f"Warning: {len(unprocessed[table_name])} items remained unprocessed after retries")
                
        return True
    except Exception as e:
        print(f"Error in batch_write_to_dynamodb: {e}")
        return False