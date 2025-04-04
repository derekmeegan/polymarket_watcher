"""
Polymarket Watcher - Configuration

This module contains configuration values for the Polymarket Watcher.
"""

import os

# API URLs
POLYMARKET_API_URL = "https://gamma-api.polymarket.com/markets"
POLYMARKET_URL = "https://polymarket.com/market"

# DynamoDB Tables
MARKETS_TABLE = os.environ.get('MARKETS_TABLE', 'polymarket-markets')
HISTORICAL_TABLE = os.environ.get('HISTORICAL_TABLE', 'polymarket-historical')
POSTS_TABLE = os.environ.get('POSTS_TABLE', 'polymarket-posts')
RESOLUTIONS_TABLE = os.environ.get('RESOLUTIONS_TABLE', 'polymarket-resolutions')
SIGNALS_TABLE = os.environ.get('SIGNALS_TABLE', 'polymarket-signals')
THRESHOLDS_TABLE = os.environ.get('THRESHOLDS_TABLE', 'polymarket-thresholds')

# Twitter API Credentials
# Now handled via environment variables in the publisher Lambda

# Posting Configuration
MAX_POSTS_PER_DAY = 100  # Maximum number of posts per 24 hours
MIN_POST_INTERVAL = 15 * 60  # Minimum seconds between posts (15 minutes)

# Market Tracking
MIN_LIQUIDITY = 1000  # Minimum liquidity for a market to be tracked
LOW_LIQUIDITY_THRESHOLD = 5000
MEDIUM_LIQUIDITY_THRESHOLD = 100000
HIGH_LIQUIDITY_THRESHOLD = 500000

# Volatility Thresholds
VOLATILITY_THRESHOLDS = [
    {'min_liquidity': 1000000, 'change_threshold': 0.05},  # 5% for markets with $1M+ liquidity
    {'min_liquidity': 500000, 'change_threshold': 0.07},   # 7% for markets with $500K+ liquidity
    {'min_liquidity': 100000, 'change_threshold': 0.10},   # 10% for markets with $100K+ liquidity
    {'min_liquidity': 50000, 'change_threshold': 0.15},    # 15% for markets with $50K+ liquidity
    {'min_liquidity': 10000, 'change_threshold': 0.20},    # 20% for markets with $10K+ liquidity
    {'min_liquidity': 0, 'change_threshold': 0.25}         # 25% for all other markets
]

# Adjust volatility thresholds based on liquidity
LIQUIDITY_VOLATILITY_ADJUSTMENTS = {
    'very_low': {  # Below LOW_LIQUIDITY_THRESHOLD
        'threshold': 0.20,  # 20% change required for very low liquidity markets
        'ignore': True  # Ignore very low liquidity markets completely
    },
    'low': {  # Between LOW and MEDIUM
        'threshold': 0.15,  # 15% change required for low liquidity markets
        'ignore': False
    },
    'medium': {  # Between MEDIUM and HIGH
        'threshold': 0.08,  # 8% change required for medium liquidity markets
        'ignore': False
    },
    'high': {  # Above HIGH_LIQUIDITY_THRESHOLD
        'threshold': 0.05,  # 5% change required for high liquidity markets
        'ignore': False
    }
}

# TTL for DynamoDB items (in days)
TTL_DAYS = {
    'markets': 30,
    'historical': 90,
    'posts': 365,
    'resolutions': 365,
    'signals': 365,
    'thresholds': 365
}

# Categories of Interest
CATEGORIES_OF_INTEREST = {
    'Politics': [
        'election', 'president', 'senate', 'congress', 'democrat', 'republican',
        'biden', 'trump', 'harris', 'political', 'government', 'vote', 'ballot'
    ],
    'Crypto': [
        'bitcoin', 'ethereum', 'crypto', 'blockchain', 'token', 'defi', 'nft',
        'btc', 'eth', 'sol', 'solana', 'coinbase', 'binance', 'exchange'
    ],
    'Tech': [
        'ai', 'artificial intelligence', 'openai', 'chatgpt', 'gpt', 'llm',
        'tech', 'technology', 'google', 'microsoft', 'apple', 'meta', 'facebook',
        'twitter', 'x', 'amazon', 'tesla'
    ],
    'Finance': [
        'stock', 'finance', 'economy', 'recession', 'inflation',
        'fed', 'federal reserve', 'interest rate', 'gdp', 'dow', 'nasdaq', 's&p'
    ],
    'Sports': [
        'nfl', 'football', 'nba', 'basketball', 'mlb', 'baseball', 'nhl', 'hockey',
        'soccer', 'tennis', 'golf', 'olympics', 'world cup', 'super bowl'
    ],
    'Entertainment': [
        'movie', 'film', 'tv', 'television', 'streaming', 'netflix', 'disney',
        'hbo', 'award', 'oscar', 'emmy', 'grammy', 'actor', 'actress', 'celebrity'
    ]
}

# Signal Types
SIGNAL_TYPES = {
    'PRICE_JUMP': 'Sudden increase in price',
    'PRICE_DROP': 'Sudden decrease in price',
    'SUSTAINED_TREND': 'Consistent price movement in one direction',
    'VOLATILITY_SPIKE': 'Increased price volatility',
    'VOLUME_SPIKE': 'Unusual trading volume'
}

# Signal Strength Categories
SIGNAL_STRENGTH = {
    'WEAK': {'min': 0.03, 'max': 0.08},
    'MODERATE': {'min': 0.08, 'max': 0.15},
    'STRONG': {'min': 0.15, 'max': 0.25},
    'VERY_STRONG': {'min': 0.25, 'max': 1.0}
}

# Time Windows for Analysis (in hours)
TIME_WINDOWS = [1, 6, 24, 168]  # 1h, 6h, 24h, 7d

# Initial Confidence Score Weights
CONFIDENCE_WEIGHTS = {
    'magnitude': 0.3,
    'volume': 0.2,
    'liquidity': 0.15,
    'historical_accuracy': 0.25,
    'time_to_resolution': 0.1
}

# Resolution Status
RESOLUTION_STATUS = {
    'PENDING': 'Market not yet resolved',
    'RESOLVED': 'Market has been resolved',
    'CANCELED': 'Market was canceled'
}
