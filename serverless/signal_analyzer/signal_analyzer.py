"""
Polymarket Watcher - Signal Analyzer Lambda

This Lambda function analyzes market data to detect signals using adaptive thresholds.
It identifies various types of market movements and stores them as signals.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import statistics

import boto3
from boto3.dynamodb.conditions import Key, Attr

from common.config import (
    MARKETS_TABLE,
    HISTORICAL_TABLE,
    SIGNALS_TABLE,
    THRESHOLDS_TABLE,
    RESOLUTIONS_TABLE,
    SIGNAL_TYPES,
    SIGNAL_STRENGTH,
    TIME_WINDOWS,
    CONFIDENCE_WEIGHTS
)

from common.utils import (
    get_dynamodb_client,
    calculate_price_change,
    calculate_significant_price_change,
    get_volatility_threshold,
    get_liquidity_tier
)

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

def get_historical_prices_for_time_windows(market_id, outcome_index, time_windows):
    """
    Get historical prices for a market for multiple time windows
    Returns a dictionary mapping time window to price data
    
    Args:
        market_id: ID of the market
        outcome_index: Index of the outcome to get prices for
        time_windows: List of time windows in hours
    """
    try:
        # Initialize DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(HISTORICAL_TABLE)
        
        # Get current time
        now = datetime.now(timezone.utc)
        
        # Initialize results
        results = {}
        
        # Get historical prices for each time window
        for window in time_windows:
            # Calculate timestamp for window hours ago
            timestamp_window_ago = (now - timedelta(hours=window)).isoformat()
            
            # Query historical prices
            response = table.query(
                KeyConditionExpression=Key('market_id').eq(market_id) & Key('timestamp').gt(timestamp_window_ago)
            )
            
            # Filter for the specific outcome
            prices = [
                item for item in response.get('Items', [])
                if item.get('outcome_index') == outcome_index
            ]
            
            # Sort by timestamp
            prices.sort(key=lambda x: x.get('timestamp', ''))
            
            # Store in results
            results[window] = prices
        
        return results
    except Exception as e:
        print(f"Error getting historical prices for time windows: {e}")
        return {window: [] for window in time_windows}

def calculate_volatility(prices):
    """
    Calculate price volatility (standard deviation of price changes)
    """
    if len(prices) < 2:
        return 0
    
    # Extract price values
    price_values = [float(item.get('price', 0)) for item in prices]
    
    # Calculate price changes between consecutive points
    price_changes = [
        abs(price_values[i] - price_values[i-1]) / price_values[i-1] if price_values[i-1] > 0 else 0
        for i in range(1, len(price_values))
    ]
    
    # Calculate standard deviation of price changes
    if not price_changes:
        return 0
    
    try:
        return statistics.stdev(price_changes)
    except statistics.StatisticsError:
        return 0

def calculate_price_momentum(prices, window_size=3):
    """
    Calculate price momentum (rate of change of price)
    """
    if len(prices) < window_size:
        return 0
    
    # Extract price values and timestamps
    price_data = [
        (
            datetime.fromisoformat(item.get('timestamp')), 
            float(item.get('price', 0))
        )
        for item in prices
    ]
    
    # Calculate momentum for each window
    momentum_values = []
    for i in range(len(price_data) - window_size):
        start_time, start_price = price_data[i]
        end_time, end_price = price_data[i + window_size]
        
        # Calculate time difference in hours
        time_diff = (end_time - start_time).total_seconds() / 3600
        
        if time_diff > 0 and start_price > 0:
            # Calculate price change per hour
            price_change = (end_price - start_price) / start_price
            momentum = price_change / time_diff
            momentum_values.append(momentum)
    
    # Return average momentum
    if momentum_values:
        return sum(momentum_values) / len(momentum_values)
    
    return 0

def get_adaptive_threshold(market):
    """
    Get adaptive threshold for a market based on its characteristics
    and historical performance
    """
    try:
        # Get market properties
        liquidity = float(market.get('liquidity', 0))
        categories = market.get('categories', [])
        
        # Get liquidity tier
        tier = get_liquidity_tier(liquidity)
        
        # Try to get custom threshold from thresholds table
        dynamodb = get_dynamodb_client()
        thresholds_table = dynamodb.Table(THRESHOLDS_TABLE)
        
        # Check for category-specific thresholds
        category_thresholds = []
        for category in categories:
            response = thresholds_table.get_item(
                Key={
                    'category': category,
                    'liquidity_tier': tier
                }
            )
            
            if 'Item' in response:
                category_thresholds.append(float(response['Item'].get('base_threshold', 0)))
        
        # If we have category-specific thresholds, use the average
        if category_thresholds:
            return sum(category_thresholds) / len(category_thresholds)
        
        # Otherwise, fall back to default threshold
        return get_volatility_threshold(liquidity)
    except Exception as e:
        print(f"Error getting adaptive threshold: {e}")
        # Fall back to default threshold
        return get_volatility_threshold(float(market.get('liquidity', 0)))

def get_signal_strength_category(price_change):
    """
    Determine the signal strength category based on price change
    """
    for category, range_values in SIGNAL_STRENGTH.items():
        if range_values['min'] <= price_change < range_values['max']:
            return category
    
    # Default to strongest category if above all ranges
    return 'VERY_STRONG'

def calculate_confidence_score(market, price_change, historical_accuracy=0.5):
    """
    Calculate a confidence score for a signal
    """
    try:
        # Get market properties
        liquidity = float(market.get('liquidity', 0))
        volume = float(market.get('volume24hr', 0))
        
        # Normalize values to 0-1 range
        normalized_magnitude = min(price_change / 0.5, 1.0)  # Cap at 1.0 for changes > 50%
        normalized_volume = min(volume / 1000000, 1.0)  # Cap at 1.0 for volume > $1M
        normalized_liquidity = min(liquidity / 1000000, 1.0)  # Cap at 1.0 for liquidity > $1M
        
        # Calculate time to resolution (if available)
        time_to_resolution = 0.5  # Default value
        if market.get('market_end_date'):
            try:
                end_date = datetime.fromisoformat(market.get('market_end_date').replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                days_to_resolution = (end_date - now).days
                
                # Normalize: 1.0 for markets resolving soon, 0.0 for markets far in the future
                time_to_resolution = max(0, min(1.0, 1.0 - (days_to_resolution / 30)))
            except:
                pass
        
        # Calculate weighted score
        score = (
            CONFIDENCE_WEIGHTS['magnitude'] * normalized_magnitude +
            CONFIDENCE_WEIGHTS['volume'] * normalized_volume +
            CONFIDENCE_WEIGHTS['liquidity'] * normalized_liquidity +
            CONFIDENCE_WEIGHTS['historical_accuracy'] * historical_accuracy +
            CONFIDENCE_WEIGHTS['time_to_resolution'] * time_to_resolution
        )
        
        return min(score, 1.0)  # Cap at 1.0
    except Exception as e:
        print(f"Error calculating confidence score: {e}")
        return 0.5  # Default to medium confidence

def determine_signal_type(current_price, historical_prices, volatility):
    """
    Determine the type of signal based on price movement patterns
    """
    if not historical_prices:
        return None
    
    # Get oldest and most recent prices
    oldest_price = float(historical_prices[0].get('price', 0))
    current_price = float(current_price)
    
    # Calculate significant price change using the new function
    # Minimum absolute change of 0.05 (5 percentage points) for a signal
    is_significant, change_value, change_type = calculate_significant_price_change(
        current_price, oldest_price, min_absolute_change=0.05
    )
    
    # Also calculate traditional price change for backwards compatibility
    traditional_price_change = calculate_price_change(current_price, oldest_price)
    
    # Only proceed if the change is significant
    if is_significant:
        # Determine if it's a jump or drop
        if current_price > oldest_price and change_value >= 0.15:  # Significant price jump
            return 'PRICE_JUMP'
        elif current_price < oldest_price and change_value >= 0.15:  # Significant price drop
            return 'PRICE_DROP'
    
    # Check for volatility spike regardless of significant price change
    if volatility >= 0.1:  # High volatility
        return 'VOLATILITY_SPIKE'
    
    # Check for sustained trend if we have enough data points
    if len(historical_prices) >= 5:
        price_values = [float(item.get('price', 0)) for item in historical_prices]
        increasing = all(price_values[i] <= price_values[i+1] for i in range(len(price_values)-1))
        decreasing = all(price_values[i] >= price_values[i+1] for i in range(len(price_values)-1))
        
        # For sustained trends, we use a lower threshold but still require a minimum absolute change
        is_trend_significant, trend_change, _ = calculate_significant_price_change(
            current_price, oldest_price, min_absolute_change=0.03
        )
        
        if is_trend_significant:
            if increasing and current_price > oldest_price:
                return 'SUSTAINED_TREND'
            elif decreasing and current_price < oldest_price:
                return 'SUSTAINED_TREND'
    
    return None

def predict_outcome_from_signal(signal_type, current_price, market):
    """
    Predict the likely market outcome based on the signal type
    """
    # For binary markets, predict Yes/No
    if market.get('tracked_outcome') == 'Yes':
        if signal_type == 'PRICE_JUMP' or (signal_type == 'SUSTAINED_TREND' and current_price > 0.5):
            return 'Yes'
        elif signal_type == 'PRICE_DROP' or (signal_type == 'SUSTAINED_TREND' and current_price < 0.5):
            return 'No'
    
    # For multi-outcome markets, predict the tracked outcome if price is high
    if current_price > 0.7:
        return market.get('tracked_outcome')
    
    return None  # Cannot make a confident prediction

def save_signal_to_dynamodb(market, signal_data):
    """Save signal data to DynamoDB"""
    try:
        # Generate a unique signal ID
        signal_id = f"signal_{uuid.uuid4()}"
        
        # Add metadata
        signal_data['signal_id'] = signal_id
        signal_data['detection_timestamp'] = datetime.now(timezone.utc).isoformat()
        signal_data['ttl'] = int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp())
        
        # Write to DynamoDB
        dynamodb = get_dynamodb_client()
        table = dynamodb.Table(SIGNALS_TABLE)
        
        response = table.put_item(Item=signal_data)
        
        return True
    except Exception as e:
        print(f"Error saving signal to DynamoDB: {e}")
        return False

def get_historical_signal_accuracy(category=None, liquidity_tier=None):
    """
    Get historical accuracy of signals for a category and liquidity tier
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
        
        # Calculate accuracy
        if not signals:
            return 0.5  # Default to 50% if no data
        
        correct_signals = sum(1 for signal in signals if signal.get('was_correct', False))
        
        return correct_signals / len(signals)
    except Exception as e:
        print(f"Error getting historical signal accuracy: {e}")
        return 0.5  # Default to 50%

def update_threshold_based_on_performance(category, liquidity_tier, accuracy):
    """
    Update threshold based on historical performance
    """
    try:
        dynamodb = get_dynamodb_client()
        thresholds_table = dynamodb.Table(THRESHOLDS_TABLE)
        
        # Get current threshold
        response = thresholds_table.get_item(
            Key={
                'category': category,
                'liquidity_tier': liquidity_tier
            }
        )
        
        # Initialize with default values if not found
        if 'Item' not in response:
            base_threshold = get_volatility_threshold(
                100000 if liquidity_tier == 'high' else
                50000 if liquidity_tier == 'medium' else
                10000 if liquidity_tier == 'low' else 5000
            )
            
            current_item = {
                'category': category,
                'liquidity_tier': liquidity_tier,
                'base_threshold': Decimal(str(base_threshold)),
                'performance_metrics': {
                    'true_positives': 0,
                    'false_positives': 0,
                    'missed_signals': 0,
                    'accuracy': Decimal('0.5')
                },
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
        else:
            current_item = response['Item']
        
        # Adjust threshold based on accuracy
        current_threshold = float(current_item.get('base_threshold', 0.1))
        
        # If accuracy is low, increase threshold to reduce false positives
        # If accuracy is high, decrease threshold to catch more signals
        if accuracy < 0.4:
            new_threshold = current_threshold * 1.1  # Increase by 10%
        elif accuracy > 0.7:
            new_threshold = current_threshold * 0.95  # Decrease by 5%
        else:
            new_threshold = current_threshold  # Keep the same
        
        # Cap threshold within reasonable bounds
        new_threshold = max(0.03, min(0.3, new_threshold))
        
        # Update performance metrics
        performance_metrics = current_item.get('performance_metrics', {})
        performance_metrics['accuracy'] = Decimal(str(accuracy))
        
        # Update threshold in DynamoDB
        thresholds_table.put_item(
            Item={
                'category': category,
                'liquidity_tier': liquidity_tier,
                'base_threshold': Decimal(str(new_threshold)),
                'performance_metrics': performance_metrics,
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'ttl': int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp())
            }
        )
        
        return True
    except Exception as e:
        print(f"Error updating threshold: {e}")
        return False

def detect_signals(markets):
    """Detect signals in market data using adaptive thresholds"""
    signals_detected = []
    
    for market in markets:
        try:
            market_id = market.get('id')
            current_price = float(market.get('current_price', 0))
            outcome_index = market.get('outcome_index')
            tracked_outcome = market.get('tracked_outcome')
            liquidity = float(market.get('liquidity', 0))
            categories = market.get('categories', [])
            
            # Skip markets with very low liquidity
            if liquidity < 5000:
                continue
            
            # Get liquidity tier
            liquidity_tier = get_liquidity_tier(liquidity)
            
            # Get historical prices for different time windows
            historical_prices_by_window = get_historical_prices_for_time_windows(
                market_id, outcome_index, TIME_WINDOWS
            )
            
            # For each time window, analyze for signals
            for window, historical_prices in historical_prices_by_window.items():
                # Need at least a few data points
                if len(historical_prices) < 3:
                    continue
                
                # Calculate volatility
                volatility = calculate_volatility(historical_prices)
                
                # Calculate momentum
                momentum = calculate_price_momentum(historical_prices)
                
                # Determine signal type
                signal_type = determine_signal_type(current_price, historical_prices, volatility)
                
                if not signal_type:
                    continue
                
                # Get oldest price in the window
                oldest_price = float(historical_prices[0].get('price', 0))
                
                # Calculate price change
                price_change = calculate_price_change(current_price, oldest_price)
                
                # Get adaptive threshold
                threshold = get_adaptive_threshold(market)
                
                # If price change exceeds threshold, create a signal
                if price_change >= threshold:
                    # Get signal strength category
                    strength = get_signal_strength_category(price_change)
                    
                    # Get historical accuracy for this category and liquidity tier
                    historical_accuracy = 0.5
                    if categories:
                        category_accuracies = []
                        for category in categories:
                            accuracy = get_historical_signal_accuracy(category, liquidity_tier)
                            category_accuracies.append(accuracy)
                        
                        if category_accuracies:
                            historical_accuracy = sum(category_accuracies) / len(category_accuracies)
                    
                    # Calculate confidence score
                    confidence = calculate_confidence_score(market, price_change, historical_accuracy)
                    
                    # Predict likely outcome
                    predicted_outcome = predict_outcome_from_signal(signal_type, current_price, market)
                    
                    # Create signal data
                    signal_data = {
                        'market_id': market_id,
                        'question': market.get('question'),
                        'signal_type': signal_type,
                        'signal_strength': strength,
                        'time_window': window,
                        'current_price': Decimal(str(current_price)),
                        'previous_price': Decimal(str(oldest_price)),
                        'price_change': Decimal(str(price_change)),
                        'volatility': Decimal(str(volatility)),
                        'momentum': Decimal(str(momentum)),
                        'threshold_used': Decimal(str(threshold)),
                        'confidence_score': Decimal(str(confidence)),
                        'liquidity': Decimal(str(liquidity)),
                        'liquidity_tier': liquidity_tier,
                        'categories': categories,
                        'tracked_outcome': tracked_outcome,
                        'predicted_outcome': predicted_outcome
                    }
                    
                    # Save signal to DynamoDB
                    if save_signal_to_dynamodb(market, signal_data):
                        print(f"Saved signal for market {market_id}: {signal_type} with {strength} strength")
                        signals_detected.append(signal_data)
                    
                    # Update thresholds based on performance
                    for category in categories:
                        update_threshold_based_on_performance(category, liquidity_tier, historical_accuracy)
        
        except Exception as e:
            print(f"Error detecting signals for market {market.get('id')}: {e}")
    
    return signals_detected

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    print("Starting Polymarket Signal Analyzer")
    
    start_time = time.time()
    
    # Get all markets from DynamoDB
    markets = get_current_markets()
    print(f"Retrieved {len(markets)} markets from DynamoDB")
    
    # Detect signals
    signals = detect_signals(markets)
    print(f"Detected {len(signals)} signals")
    
    execution_time = time.time() - start_time
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f"Analyzed {len(markets)} markets, detected {len(signals)} signals",
            'execution_time': f'{execution_time:.2f} seconds'
        })
    }

if __name__ == "__main__":
    # For local testing
    lambda_handler({}, None)
