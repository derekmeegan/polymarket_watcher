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
    POLYMARKET_URL
)
from common.utils import save_post_to_dynamodb

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
        # Get each credential from its own secret
        access_token = get_secret_value(X_ACCESS_TOKEN_SECRET_NAME)
        access_token_secret = get_secret_value(X_ACCESS_TOKEN_SECRET_SECRET_NAME)
        consumer_key = get_secret_value(X_CONSUMER_KEY_SECRET_NAME)
        consumer_secret = get_secret_value(X_CONSUMER_SECRET_SECRET_NAME)
        
        # Check if all credentials were retrieved successfully
        if not all([access_token, access_token_secret, consumer_key, consumer_secret]):
            print("Failed to retrieve one or more Twitter credentials")
            return None
        
        # Return the credentials directly
        return {
            'consumer_key': consumer_key,
            'consumer_secret': consumer_secret,
            'access_token': access_token,
            'access_token_secret': access_token_secret
        }
    except Exception as e:
        print(f"Error retrieving Twitter credentials: {e}")
        return None

def get_twitter_client():
    """Initialize and return a Twitter API client"""
    try:
        # Get Twitter credentials from Secrets Manager
        print(f"Retrieving Twitter credentials from Secrets Manager...")
        print(f"X_ACCESS_TOKEN_SECRET_NAME: {X_ACCESS_TOKEN_SECRET_NAME}")
        print(f"X_ACCESS_TOKEN_SECRET_SECRET_NAME: {X_ACCESS_TOKEN_SECRET_SECRET_NAME}")
        print(f"X_CONSUMER_KEY_SECRET_NAME: {X_CONSUMER_KEY_SECRET_NAME}")
        print(f"X_CONSUMER_SECRET_SECRET_NAME: {X_CONSUMER_SECRET_SECRET_NAME}")
        
        credentials = get_twitter_credentials()
        
        if not credentials:
            print("Failed to retrieve Twitter credentials")
            return None
        
        # Log successful credential retrieval (without exposing the actual values)
        print("Successfully retrieved all Twitter API credentials")
        
        # Initialize Twitter API client
        print("Initializing Twitter API client...")
        client = tweepy.Client(
            access_token=credentials['access_token'], 
            access_token_secret=credentials['access_token_secret'], 
            consumer_key=credentials['consumer_key'], 
            consumer_secret=credentials['consumer_secret']
        )
        print("Twitter API client initialized successfully")
        
        return client
    except Exception as e:
        print(f"Error initializing Twitter client: {e}")
        return None

def generate_post_text(market_update, price_change, previous_price, max_length=280):
    """Generate post text for a market update"""
    question = market_update['question']
    current_price = market_update['current_price']
    outcome = market_update['tracked_outcome']
    
    # Format price as percentage
    current_pct = f"{current_price * 100:.1f}%"
    previous_pct = f"{previous_price * 100:.1f}%"
    change_direction = "↑" if current_price > previous_price else "↓"
    change_pct = f"{abs(price_change) * 100:.1f}%"
    
    # Truncate question if too long
    max_question_length = 180
    if len(question) > max_question_length:
        question = question[:max_question_length-3] + "..."
    
    # Create post text
    post_text = f"📊 {question}\n\n"
    post_text += f"Outcome: {outcome}\n"
    post_text += f"Price: {current_pct} ({change_direction}{change_pct} from {previous_pct})"
    
    return post_text

def post_to_twitter(market_update):
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
        twitter = get_twitter_client()
        
        if not twitter:
            print("Failed to initialize Twitter client")
            return None
        
        # Post to Twitter
        tweet = twitter.create_tweet(text = post_text)
        
        print(f"Posted to Twitter: {post_text}")
        
        # Return tweet ID
        return True
    except Exception as e:
        if 'duplicate content' in str(e):
            return True
        print(f"Error posting to Twitter: {e}")
        return None

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
    
    for market_update in market_updates:
        # Post to Twitter
        tweet_id = post_to_twitter(market_update)
        
        if tweet_id:
            # Save post to DynamoDB
            post_record = save_post_to_dynamodb(market_update['id'])
            
            posts_made.append({
                'market_id': market_update['id'],
                'question': market_update['question'],
                'tweet_id': tweet_id
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
                    'tracked_outcome': 'Yes'
                }
            ]
        })
    }
    
    lambda_handler(mock_event, None)
