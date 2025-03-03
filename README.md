# Polymarket Watcher

A serverless application that monitors Polymarket prediction markets, detects significant price changes, and posts updates to Twitter.

## Overview

Polymarket Watcher continuously monitors prediction markets on Polymarket, identifies markets with significant price changes, and posts updates about these changes to Twitter. It's designed to run as a set of AWS Lambda functions that execute on a schedule.

## Features

- **Market Monitoring**: Fetches data from the Polymarket API and stores it in DynamoDB
- **Price Change Detection**: Analyzes market data to detect significant price changes
- **Multi-outcome Support**: Handles both binary (Yes/No) and multi-outcome markets
- **Volatility Thresholds**: Adjusts significance thresholds based on market liquidity
- **Rate Limiting**: Controls post frequency to avoid spamming
- **Market Categorization**: Categorizes markets by topic (Politics, Crypto, Tech, etc.)
- **Historical Data**: Maintains historical price data for trend analysis

## Architecture

The application consists of three main Lambda functions:

1. **Collector**: Fetches market data from Polymarket API and stores it in DynamoDB
2. **Analyzer**: Analyzes market data to detect significant price changes
3. **Publisher**: Posts updates about significant price changes to Twitter

## Data Flow

1. The Collector Lambda runs every 5 minutes to fetch market data from Polymarket
2. Market data is stored in the `markets` DynamoDB table
3. Historical price points are stored in the `historical` DynamoDB table
4. The Analyzer Lambda runs after the Collector to detect significant price changes
5. When significant changes are detected, the Publisher Lambda is triggered
6. The Publisher posts updates to Twitter and records the posts in the `posts` DynamoDB table

## Configuration

Configuration settings are stored in `serverless/common/config.py`:

- API URLs
- DynamoDB table names
- Twitter API credentials
- Market fetching parameters
- Volatility thresholds
- Post rate limiting
- Categories of interest

## Environment Variables

The following environment variables are required:

- `TWITTER_API_KEY`: Twitter API key
- `TWITTER_API_SECRET`: Twitter API secret
- `TWITTER_ACCESS_TOKEN`: Twitter access token
- `TWITTER_ACCESS_SECRET`: Twitter access token secret
- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `AWS_REGION`: AWS region

## Development

### Prerequisites

- Python 3.8+
- AWS account
- Twitter Developer account

### Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure environment variables
4. Deploy to AWS using the Serverless Framework: `serverless deploy`

### Local Testing

You can test the Lambda functions locally:

```bash
python serverless/collector/collector.py
python serverless/analyzer/analyzer.py
python serverless/publisher/publisher.py
```

## License

MIT
