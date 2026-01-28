# ============================================
# FILE: config.py (UPDATED)
# ============================================

"""
Configuration management for Polymarket data collector
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Configuration settings"""
    
    # Data storage directory
    DATA_DIR = os.getenv("DATA_DIR", "data")
    
    # Polymarket WebSocket
    POLYMARKET_WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    
    # Markets to track (condition IDs)
    MARKETS = os.getenv("POLYMARKET_MARKETS", "").split(",") if os.getenv("POLYMARKET_MARKETS") else []
    
    # Asset IDs to track (token IDs)
    ASSET_IDS = os.getenv("POLYMARKET_ASSET_IDS", "").split(",") if os.getenv("POLYMARKET_ASSET_IDS") else []
    
    # Enable custom features (needed for best_bid_ask messages)
    CUSTOM_FEATURES_ENABLED = True
    
    # Reconnection settings
    RECONNECT_DELAY = 5  # seconds
    MAX_RECONNECT_ATTEMPTS = None  # None = infinite
    
    # Parquet settings
    BUFFER_SIZE = 100  # Records to buffer before writing
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        if not cls.ASSET_IDS and not cls.MARKETS:
            raise ValueError("At least one of POLYMARKET_ASSET_IDS or POLYMARKET_MARKETS is required")