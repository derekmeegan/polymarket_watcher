"""
Polymarket Watcher - Publisher Lambda

This Lambda function publishes market updates to Twitter.
It is triggered by SNS messages from the analyzer Lambda.
"""

import json
import os
import time
from datetime import datetime, timezone
import tweepy
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from common.config import (
    POSTS_TABLE,
    POLYMARKET_URL,
    SIGNALS_TABLE
)
from common.utils import save_post_to_dynamodb, get_dynamodb_client

# Get Twitter API Secret ARNs from environment
# Get Twitter API Secret Names from environment
X_ACCESS_TOKEN_SECRET_NAME = os.environ.get('X_ACCESS_TOKEN_SECRET_NAME', 'polymarket/x-access-token')
X_ACCESS_TOKEN_SECRET_SECRET_NAME = os.environ.get('X_ACCESS_TOKEN_SECRET_SECRET_NAME', 'polymarket/x-access-token-secret')
X_CONSUMER_KEY_SECRET_NAME = os.environ.get('X_CONSUMER_KEY_SECRET_NAME', 'polymarket/x-consumer-key')
X_CONSUMER_SECRET_SECRET_NAME = os.environ.get('X_CONSUMER_SECRET_SECRET_NAME', 'polymarket/x-consumer-secret')

def get_secret_value(secret_name):
    """Retrieve a secret value from AWS Secrets Manager"""
    try:
        # Create a Secrets Manager client
        client = boto3.client(service_name='secretsmanager', region_name = 'us-east-1')
        
        # Get the secret value
        response = client.get_secret_value(SecretId=secret_name)
        
        # Return the secret string directly (not as JSON)
        return response['SecretString']
    except ClientError as e:
        print(f"Error retrieving secret from Secrets Manager: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error retrieving secret: {e}")
        return None

def get_twitter_credentials():
    """Retrieve Twitter API credentials from AWS Secrets Manager"""
    try:
        # Get credentials from Secrets Manager
        access_token = get_secret_value(X_ACCESS_TOKEN_SECRET_NAME)
        access_token_secret = get_secret_value(X_ACCESS_TOKEN_SECRET_SECRET_NAME)
        consumer_key = get_secret_value(X_CONSUMER_KEY_SECRET_NAME)
        consumer_secret = get_secret_value(X_CONSUMER_SECRET_SECRET_NAME)
        
        # Check if all credentials were retrieved successfully
        if not all([access_token, access_token_secret, consumer_key, consumer_secret]):
            print("Failed to retrieve Twitter API credentials from Secrets Manager")
            return None
        
        # Return credentials
        return {
            'access_token': access_token,
            'access_token_secret': access_token_secret,
            'consumer_key': consumer_key,
            'consumer_secret': consumer_secret
        }
    except Exception as e:
        print(f"Error retrieving Twitter credentials: {e}")
        return None

def get_twitter_client():
    """Initialize and return a Twitter API client"""
    try:
        # Get Twitter API credentials
        credentials = get_twitter_credentials()
        
        if not credentials:
            print("Failed to retrieve Twitter API credentials")
            return None
        
        # Initialize Twitter client
        client = tweepy.Client(
            consumer_key=credentials['consumer_key'],
            consumer_secret=credentials['consumer_secret'],
            access_token=credentials['access_token'],
            access_token_secret=credentials['access_token_secret']
        )
        
        print("Twitter API client initialized successfully")
        
        return client
    except Exception as e:
        print(f"Error initializing Twitter client: {e}")
        return None

def get_confidence_emoji(confidence_score):
    """Get emoji representation of confidence score"""
    if confidence_score >= 0.8:
        return "🔥" # Very high confidence
    elif confidence_score >= 0.7:
        return "✅" # High confidence
    elif confidence_score >= 0.5:
        return "⚠️" # Medium confidence
    else:
        return "❓" # Low confidence

def generate_post_text(market_update, price_change, previous_price, max_length=280):
    """Generate post text for a market update"""
    question = market_update['question']
    current_price = market_update['current_price']
    outcome = market_update['tracked_outcome']
    confidence_score = market_update.get('confidence_score', 0.5)
    has_signals = market_update.get('has_signals', False)
    
    # Format price as percentage
    current_pct = f"{current_price * 100:.1f}%"
    previous_pct = f"{previous_price * 100:.1f}%"
    change_direction = "↑" if current_price > previous_price else "↓"
    change_pct = f"{abs(price_change) * 100:.1f}%"
    
    # Get confidence emoji
    confidence_emoji = get_confidence_emoji(confidence_score)
    
    # Truncate question if too long
    max_question_length = 180
    if len(question) > max_question_length:
        question = question[:max_question_length-3] + "..."
    
    # Create post text
    post_text = f"{confidence_emoji} {question}\n\n"
    post_text += f"Outcome: {outcome}\n"
    post_text += f"Price: {current_pct} ({change_direction}{change_pct} from {previous_pct})"
    
    # Add signal info if available
    if has_signals:
        post_text += f"\nSignal Confidence: {confidence_score:.2f}"
    
    return post_text

def post_to_twitter(market_update, idx):
    """Post a market update to Twitter"""
    try:
        # Generate post text
        post_text = generate_post_text(
            market_update, 
            market_update['price_change'], 
            market_update['previous_price']
        )
        
        # Add market URL
        market_url = f"{POLYMARKET_URL}/{market_update['slug']}"
        post_text += f"\n\n{market_url}"
        
        # Initialize Twitter client
        if idx == 0:
            twitter = get_twitter_client()
            
            if not twitter:
                print("Failed to initialize Twitter client")
                return None
            
            # Post to Twitter
            tweet = twitter.create_tweet(text = post_text)
            
            print(f"Posted to Twitter: {post_text}")
        
        # Return tweet ID
        return True, post_text
    except Exception as e:
        if 'duplicate content' in str(e):
            return True, post_text
        print(f"Error posting to Twitter: {e}")
        return None, None

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    print("Starting Polymarket Watcher Publisher")
    
    start_time = time.time()
    
    # Parse event body
    market_updates = []
    for record in event.get('Records', []):
        # Extract the SNS message
        sns_message_str = record.get('Sns', {}).get('Message', '{}')
        try:
            message = json.loads(sns_message_str)
            market_updates.extend(message.get('markets', []))
        except Exception as e:
            print(f"Error parsing SNS message: {e}")

    print(f'Captured {len(market_updates)} markets.')
    
    if not market_updates:
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'No market updates to publish'
            })
        }
    
    # Process each market update
    posts_made = []
    
    for idx, market_update in enumerate(market_updates):
        # Post to Twitter
        post_successful, post_content = post_to_twitter(market_update, idx)
        
        if post_successful:
            # Save post to DynamoDB
            post_record = save_post_to_dynamodb(market_update['id'], post_content, idx)
            
            posts_made.append({
                'market_id': market_update['id'],
                'question': market_update['question'],
                'confidence_score': market_update.get('confidence_score', 0.5)
            })
    
    execution_time = time.time() - start_time
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Published {len(posts_made)} market updates',
            'posts_made': posts_made,
            'execution_time': f'{execution_time:.2f} seconds'
        })
    }

if __name__ == "__main__":
    # For local testing
    # Create a mock event
    mock_event = {
        'body': json.dumps({
            'market_updates': [
                {
                    'id': '123456',
                    'question': 'Will Bitcoin reach $100,000 by the end of 2023?',
                    'slug': 'bitcoin-100k-2023',
                    'current_price': 0.75,
                    'previous_price': 0.5,
                    'price_change': 0.5,
                    'liquidity': 50000,
                    'categories': ['Crypto'],
                    'tracked_outcome': 'Yes',
                    'confidence_score': 0.85,
                    'has_signals': True
                }
            ]
        })
    }
    
    lambda_handler(mock_event, None)
