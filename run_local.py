#!/usr/bin/env python3
"""
Polymarket Watcher - Local Runner

This script runs all three Lambda functions in sequence for local testing.
It simulates the AWS Lambda environment by setting environment variables
and calling the handler functions directly.
"""

import os
import sys
import json
import time
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()

# Add serverless directory to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'serverless'))

# Import Lambda handlers
from collector.collector import lambda_handler as collector_handler
from analyzer.analyzer import lambda_handler as analyzer_handler
from publisher.publisher import lambda_handler as publisher_handler

def setup_local_dynamodb():
    """Set up local DynamoDB tables if they don't exist"""
    try:
        # Initialize DynamoDB client
        dynamodb = boto3.resource(
            'dynamodb',
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
        )
        
        # Create tables if they don't exist
        tables = [
            {
                'TableName': 'PolymarketMarkets',
                'KeySchema': [
                    {'AttributeName': 'id', 'KeyType': 'HASH'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'id', 'AttributeType': 'S'}
                ],
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            },
            {
                'TableName': 'PolymarketHistorical',
                'KeySchema': [
                    {'AttributeName': 'id', 'KeyType': 'HASH'},
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'id', 'AttributeType': 'S'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'S'}
                ],
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            },
            {
                'TableName': 'PolymarketPosts',
                'KeySchema': [
                    {'AttributeName': 'id', 'KeyType': 'HASH'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'id', 'AttributeType': 'S'}
                ],
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            }
        ]
        
        for table_def in tables:
            table_name = table_def['TableName']
            try:
                # Check if table exists
                dynamodb.Table(table_name).table_status
                print(f"Table {table_name} already exists")
            except ClientError:
                # Create table if it doesn't exist
                table = dynamodb.create_table(**table_def)
                print(f"Created table {table_name}")
                # Wait for table to be created
                table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        
        return True
    except Exception as e:
        print(f"Error setting up DynamoDB: {e}")
        return False

def run_collector():
    """Run the collector Lambda function"""
    print("\n" + "=" * 80)
    print("RUNNING COLLECTOR FUNCTION")
    print("=" * 80)
    
    event = {}
    context = {}
    
    result = collector_handler(event, context)
    
    print(f"Collector result: {json.dumps(result, indent=2)}")
    return result

def run_analyzer():
    """Run the analyzer Lambda function"""
    print("\n" + "=" * 80)
    print("RUNNING ANALYZER FUNCTION")
    print("=" * 80)
    
    event = {}
    context = {}
    
    result = analyzer_handler(event, context)
    
    print(f"Analyzer result: {json.dumps(result, indent=2)}")
    return result

def run_publisher(market_updates=None):
    """Run the publisher Lambda function"""
    print("\n" + "=" * 80)
    print("RUNNING PUBLISHER FUNCTION")
    print("=" * 80)
    
    # If market_updates provided, use them in the event
    event = {}
    if market_updates:
        event = {
            'market_updates': market_updates
        }
    
    context = {}
    
    # Check if Twitter credentials are available
    twitter_creds = all([
        os.environ.get('TWITTER_API_KEY'),
        os.environ.get('TWITTER_API_SECRET'),
        os.environ.get('TWITTER_ACCESS_TOKEN'),
        os.environ.get('TWITTER_ACCESS_SECRET')
    ])
    
    if not twitter_creds:
        print("WARNING: Twitter API credentials not found in environment variables.")
        print("The publisher will run in simulation mode only.")
    
    result = publisher_handler(event, context)
    
    print(f"Publisher result: {json.dumps(result, indent=2)}")
    return result

def main():
    """Main function to run all Lambda functions in sequence"""
    print("Polymarket Watcher - Local Runner")
    print("=" * 50)
    
    # Check for AWS credentials
    if not os.environ.get('AWS_ACCESS_KEY') or not os.environ.get('AWS_SECRET_ACCESS_KEY'):
        print("ERROR: AWS credentials not found in environment variables.")
        print("Please set AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY in your .env file.")
        return
    
    # Set up DynamoDB tables
    print("Setting up DynamoDB tables...")
    if not setup_local_dynamodb():
        print("Failed to set up DynamoDB tables. Exiting.")
        return
    
    # Run collector
    collector_result = run_collector()
    
    # Wait a moment to ensure data is available
    print("Waiting for DynamoDB to update...")
    time.sleep(2)
    
    # Run analyzer
    analyzer_result = run_analyzer()
    
    # Extract market updates from analyzer result
    market_updates = None
    if analyzer_result and analyzer_result.get('statusCode') == 200:
        try:
            body = json.loads(analyzer_result.get('body', '{}'))
            market_updates = body.get('market_updates')
        except:
            pass
    
    # Run publisher with market updates if available
    publisher_result = run_publisher(market_updates)
    
    print("\n" + "=" * 80)
    print("LOCAL RUN COMPLETE")
    print("=" * 80)
    
    # Summary of results
    print("\nSummary:")
    
    if collector_result and collector_result.get('statusCode') == 200:
        collector_body = json.loads(collector_result.get('body', '{}'))
        print(f"Collector: {collector_body.get('message', 'No message')}")
    else:
        print("Collector: Failed")
    
    if analyzer_result and analyzer_result.get('statusCode') == 200:
        analyzer_body = json.loads(analyzer_result.get('body', '{}'))
        print(f"Analyzer: {analyzer_body.get('message', 'No message')}")
    else:
        print("Analyzer: Failed")
    
    if publisher_result and publisher_result.get('statusCode') == 200:
        publisher_body = json.loads(publisher_result.get('body', '{}'))
        print(f"Publisher: {publisher_body.get('message', 'No message')}")
    else:
        print("Publisher: Failed")

if __name__ == "__main__":
    main()
