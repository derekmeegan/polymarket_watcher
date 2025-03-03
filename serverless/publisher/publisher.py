"""
Polymarket Watcher - Publisher Lambda

This Lambda function publishes market updates to Twitter.
It runs after the analyzer Lambda when significant changes are detected.
"""

import json
import os
import time
from datetime import datetime
import tweepy

from common.config import (
    POSTS_TABLE,
    TWITTER_API_KEY,
    TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_SECRET,
    POLYMARKET_URL
)
from common.utils import (
    get_dynamodb_client,
    generate_post_text,
    save_post_to_dynamodb
)

def get_twitter_client():
    """Initialize and return a Twitter API client"""
    try:
        # Initialize Twitter API client
        auth = tweepy.OAuthHandler(TWITTER_API_KEY, TWITTER_API_SECRET)
        auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
        
        return tweepy.API(auth)
    except Exception as e:
        print(f"Error initializing Twitter client: {e}")
        return None

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
        tweet = twitter.update_status(post_text)
        
        print(f"Posted to Twitter: {post_text}")
        
        # Return tweet ID
        return tweet.id_str
    except Exception as e:
        print(f"Error posting to Twitter: {e}")
        return None

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    print("Starting Polymarket Watcher Publisher")
    
    start_time = time.time()
    
    # Parse event body
    try:
        body = json.loads(event.get('body', '{}'))
        market_updates = body.get('market_updates', [])
    except Exception as e:
        print(f"Error parsing event body: {e}")
        market_updates = []
    
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
            post_record = save_post_to_dynamodb(
                market_update,
                tweet_id
            )
            
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
