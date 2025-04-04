"""
Polymarket Watcher - Resolution Tracker Lambda

This Lambda function fetches resolved market data from Polymarket API and stores it in DynamoDB.
It tracks market resolutions and correlates them with previously detected signals.
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3
import requests
from boto3.dynamodb.conditions import Key, Attr

from common.config import (
    POLYMARKET_API_URL,
    MARKETS_TABLE,
    RESOLUTIONS_TABLE,
    SIGNALS_TABLE,
    TTL_DAYS,
    RESOLUTION_STATUS
)
from common.utils import (
    get_dynamodb_client,
    categorize_market,
    batch_write_to_dynamodb,
    get_tracked_outcome_and_price,
    calculate_ttl,
    parse_outcomes_and_prices
)

def fetch_resolved_markets(limit=100, days_lookback_max=1, days_lookback_min=14):
    """Fetch recently resolved markets from Polymarket API"""
    all_markets = []
    offset = 0
    
    # Calculate the date range for recently resolved markets
    end_date_max = (datetime.now(timezone.utc) - timedelta(days=days_lookback_max)).strftime('%Y-%m-%d')
    end_date_min = (datetime.now(timezone.utc) - timedelta(days=days_lookback_min)).strftime('%Y-%m-%d')
    
    while True:
        params = {
            'limit': limit,
            'offset': offset,
            'active': False,  # Get resolved markets
            'ascending': False,
            'end_date_max': end_date_max,
            "end_date_min": end_date_min
        }
        
        try:
            print(f"Fetching resolved markets with offset {offset}...")
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
            print(f"Error fetching resolved markets: {e}")
            break
    
    print(f"Fetched a total of {len(all_markets)} resolved markets")
    return all_markets

def get_market_from_dynamodb(market_id):
    """Get market data from DynamoDB"""
    try:
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(MARKETS_TABLE)
        
        response = table.get_item(
            Key={'id': market_id}
        )
        
        if 'Item' in response:
            return response['Item']
        
        return None
    except Exception as e:
        print(f"Error getting market from DynamoDB: {e}")
        return None

def get_signals_for_market(market_id):
    """Get all signals for a specific market"""
    try:
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(SIGNALS_TABLE)
        
        response = table.query(
            KeyConditionExpression=Key('market_id').eq(market_id)
        )
        
        return response.get('Items', [])
    except Exception as e:
        print(f"Error getting signals for market: {e}")
        return []

def determine_resolution_outcome(market):
    """Determine the resolution outcome from market data"""
    try:
        # Check if the market has a resolution field
        if 'resolution' in market:
            return market['resolution']
        
        # For binary markets, check if Yes or No won
        outcomes, prices = parse_outcomes_and_prices(market)
        
        if len(outcomes) == 2 and "Yes" in outcomes and "No" in outcomes:
            yes_index = outcomes.index("Yes")
            yes_price = prices[yes_index]
            
            if yes_price > 0.95:  # Assuming >0.95 means Yes won
                return "Yes"
            elif yes_price < 0.05:  # Assuming <0.05 means No won
                return "No"
        
        # For multi-outcome markets, find the winning outcome
        if prices:
            max_price = max(prices)
            if max_price > 0.95:  # Assuming >0.95 means this outcome won
                max_index = prices.index(max_price)
                return outcomes[max_index]
        
        # If we can't determine the outcome
        return "Unknown"
    except Exception as e:
        print(f"Error determining resolution outcome: {e}")
        return "Unknown"

def evaluate_signal_accuracy(signal, resolution_outcome):
    """Evaluate if a signal correctly predicted the market outcome"""
    try:
        signal_type = signal.get('signal_type')
        predicted_outcome = signal.get('predicted_outcome')
        
        # Direct prediction match
        if predicted_outcome and predicted_outcome == resolution_outcome:
            return True
        
        # For price movements, evaluate based on direction
        if signal_type == 'PRICE_JUMP' and resolution_outcome == 'Yes':
            return True
        if signal_type == 'PRICE_DROP' and resolution_outcome == 'No':
            return True
        
        # For more complex scenarios, implement additional logic
        
        return False
    except Exception as e:
        print(f"Error evaluating signal accuracy: {e}")
        return False

def update_signal_with_resolution(signal, resolution_outcome, was_correct):
    """Update a signal with resolution information"""
    try:
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(SIGNALS_TABLE)
        
        response = table.update_item(
            Key={
                'market_id': signal['market_id'],
                'signal_id': signal['signal_id']
            },
            UpdateExpression="set actual_outcome = :outcome, was_correct = :correct, resolution_date = :date",
            ExpressionAttributeValues={
                ':outcome': resolution_outcome,
                ':correct': was_correct,
                ':date': datetime.now(timezone.utc).isoformat()
            },
            ReturnValues="UPDATED_NEW"
        )
        
        return True
    except Exception as e:
        print(f"Error updating signal with resolution: {e}")
        return False

def save_resolution_to_dynamodb(market, resolution_outcome):
    """Save market resolution data to DynamoDB"""
    try:
        market_id = market.get('id')
        
        # Get outcomes and prices
        outcomes, prices = parse_outcomes_and_prices(market)
        
        # Convert prices to Decimal for DynamoDB
        decimal_prices = [Decimal(str(p)) for p in prices]
        
        # Create a map of outcome to price
        outcome_prices = {}
        for i, outcome in enumerate(outcomes):
            if i < len(decimal_prices):
                outcome_prices[outcome] = decimal_prices[i]
        
        # Prepare resolution item for DynamoDB
        resolution_item = {
            'market_id': market_id,
            'question': market.get('question'),
            'resolution_outcome': resolution_outcome,
            'resolution_timestamp': datetime.now(timezone.utc).isoformat(),
            'resolution_date': market.get('endDate'),
            'outcome_prices': outcome_prices,
            'categories': categorize_market(market),
            'liquidity': Decimal(str(market.get('liquidity', 0))),
            'volume': Decimal(str(market.get('volume', 0))),
            'ttl': calculate_ttl(TTL_DAYS['resolutions'])
        }
        
        # Write to DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(RESOLUTIONS_TABLE)
        
        response = table.put_item(Item=resolution_item)
        
        return True
    except Exception as e:
        print(f"Error saving resolution to DynamoDB: {e}")
        return False

def process_resolved_markets(markets_data):
    """Process resolved market data and update signals"""
    if not markets_data:
        return 0
    
    processed_count = 0
    
    for market in markets_data:
        try:
            market_id = market.get('id')
            
            # Check if we already processed this resolution
            dynamodb = get_dynamodb_client()
            resolutions_table = dynamodb.Table(RESOLUTIONS_TABLE)
            
            response = resolutions_table.get_item(
                Key={'market_id': market_id}
            )
            
            if 'Item' in response:
                print(f"Market {market_id} already processed. Skipping.")
                continue
            
            # Determine resolution outcome
            resolution_outcome = determine_resolution_outcome(market)
            
            if resolution_outcome == "Unknown":
                print(f"Could not determine resolution outcome for market {market_id}. Skipping.")
                continue
            
            # Save resolution to DynamoDB
            if save_resolution_to_dynamodb(market, resolution_outcome):
                print(f"Saved resolution for market {market_id}: {resolution_outcome}")
            
            # Get signals for this market
            signals = get_signals_for_market(market_id)
            
            # Update each signal with resolution information
            for signal in signals:
                was_correct = evaluate_signal_accuracy(signal, resolution_outcome)
                
                if update_signal_with_resolution(signal, resolution_outcome, was_correct):
                    print(f"Updated signal {signal.get('signal_id')} with resolution outcome")
            
            processed_count += 1
            
        except Exception as e:
            print(f"Error processing resolved market {market.get('id')}: {e}")
    
    return processed_count

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    print("Starting Polymarket Watcher Resolution Tracker")
    
    start_time = time.time()
    
    # Fetch resolved markets from Polymarket
    days_lookback = int(event.get('days_lookback_max', 1))
    markets_data = fetch_resolved_markets(days_lookback=days_lookback)
    
    if not markets_data:
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'No resolved markets found in the specified time period'
            })
        }
    
    # Process resolved markets
    processed_count = process_resolved_markets(markets_data)
    
    execution_time = time.time() - start_time
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Successfully processed {processed_count} resolved markets',
            'execution_time': f'{execution_time:.2f} seconds'
        })
    }

if __name__ == "__main__":
    # For local testing
    lambda_handler({'days_lookback': 7}, None)
