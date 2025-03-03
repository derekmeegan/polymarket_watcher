"""
Utility functions for the Polymarket Watcher
"""

import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal

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
    TTL_DAYS
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
    for category in CATEGORIES_OF_INTEREST:
        # Use word boundary to avoid partial matches
        pattern = r'\b' + re.escape(category.lower()) + r'\b'
        if re.search(pattern, text):
            categories.append(category)
    
    return categories

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

def save_historical_price(market_id, outcome, price, outcome_index, table_name=HISTORICAL_TABLE):
    """
    Save a historical price point to DynamoDB
    """
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(table_name)
        
        timestamp = datetime.now(timezone.utc).isoformat()
        
        # Create item
        item = {
            'market_id': market_id,
            'timestamp': timestamp,
            'outcome': outcome,
            'outcome_index': outcome_index,
            'price': Decimal(str(price)),
            'ttl': int((datetime.now(timezone.utc) + timedelta(days=90)).timestamp())  # 90 day TTL
        }
        
        # Save to DynamoDB
        table.put_item(Item=item)
        return True
    except Exception as e:
        print(f"Error saving historical price to DynamoDB: {e}")
        return False

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

def save_post_to_dynamodb(market_id, post_id, post_text, table_name=POSTS_TABLE):
    """
    Save a post to DynamoDB
    """
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(table_name)
        
        timestamp = datetime.utcnow().isoformat()
        
        # Create item
        item = {
            'id': market_id,
            'post_id': post_id,
            'post_text': post_text,
            'timestamp': timestamp,
            'ttl': int((datetime.utcnow() + timedelta(days=90)).timestamp())  # 90 day TTL
        }
        
        # Save to DynamoDB
        table.put_item(Item=item)
        return True
    except Exception as e:
        print(f"Error saving post to DynamoDB: {e}")
        return False

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
    for threshold in VOLATILITY_THRESHOLDS:
        if liquidity >= threshold['min_liquidity']:
            return threshold['change_threshold']
    return VOLATILITY_THRESHOLDS[-1]['change_threshold']

def save_market_to_dynamodb(market):
    """Save market data to DynamoDB"""
    try:
        # Get tracked outcome and price
        tracked_outcome, current_price, outcome_index = get_tracked_outcome_and_price(market)
        
        if tracked_outcome is None or current_price is None:
            return False
        
        # Prepare market data for DynamoDB
        market_item = {
            'id': market.get('id'),
            'question': market.get('question'),
            'slug': market.get('slug'),
            'description': market.get('description', ''),
            'liquidity': Decimal(str(market.get('liquidity_num', 0))),
            'volume': Decimal(str(market.get('volume_num', 0))),
            'market_start_date':  market.get('startDate'),
            'market_end_date': market.get('endDate'),
            'image': market.get('image'),
            'closed': market.get('closed'),
            'submitted_by': market.get('submitted_by'),
            'volume24hr': Decimal(str(market.get('volume24hr'))),
            'current_price': Decimal(str(current_price)),
            'tracked_outcome': tracked_outcome,
            'outcome_index': outcome_index,
            'categories': categorize_market(market),
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'ttl': calculate_ttl(TTL_DAYS['markets'])
        }
        
        # Save to DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(MARKETS_TABLE)
        table.put_item(Item=market_item)
        
        return True
    except Exception as e:
        print(f"Error saving market to DynamoDB: {e}")
        return False