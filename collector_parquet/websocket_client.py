# ============================================
# FILE: websocket_client.py (UPDATED)
# ============================================

"""
WebSocket client for Polymarket orderbook data
"""
import json
import asyncio
import logging
from typing import List
import websockets
from datetime import datetime

from config import Config
from database import Database

logger = logging.getLogger(__name__)


class PolymarketWebSocketClient:
    """WebSocket client for Polymarket CLOB"""
    
    def __init__(self, asset_ids: List[str] = None, markets: List[str] = None):
        self.asset_ids = asset_ids or []
        self.markets = markets or []
        self.ws_url = Config.POLYMARKET_WSS_URL
        self.db = Database(Config.DATA_DIR, flush_interval=60)
        
        # Stats
        self.messages_received = 0
        self.book_messages = 0
        self.price_change_messages = 0
        self.last_message_time = None
        
        # Connection state
        self.running = True
        self.ws = None
    
    async def run_once(self):
        """Single connection attempt - returns when disconnected"""
        if not self.running:
            return
        
        logger.info(f"Connecting to {self.ws_url}")
        
        try:
            async with websockets.connect(
                self.ws_url,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5
            ) as ws:
                self.ws = ws
                logger.info("Connected to Polymarket WebSocket")
                
                # Subscribe
                if self.asset_ids:
                    await self._subscribe_assets(ws)
                if self.markets:
                    await self._subscribe_markets(ws)
                
                # Listen
                async for message in ws:
                    if not self.running:
                        break
                    await self._handle_message(message)
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
        except asyncio.CancelledError:
            logger.info("WebSocket task cancelled")
            raise
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            self.ws = None
    
    async def run(self):
        """Main run loop with reconnection (legacy method)"""
        while self.running:
            await self.run_once()
            
            if not self.running:
                break
            
            logger.info(f"Reconnecting in {Config.RECONNECT_DELAY}s...")
            await asyncio.sleep(Config.RECONNECT_DELAY)
        
        self.db.flush()
        logger.info("WebSocket client stopped")
    
    async def _subscribe_assets(self, ws):
        """Subscribe to asset orderbooks"""
        subscribe_msg = {
            "type": "subscribe",
            "channel": "book",
            "assets_ids": self.asset_ids,
        }
        
        if Config.CUSTOM_FEATURES_ENABLED:
            subscribe_msg["custom_features"] = {"best_bid_ask": True}
        
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to {len(self.asset_ids)} assets")
    
    async def _subscribe_markets(self, ws):
        """Subscribe to market orderbooks"""
        subscribe_msg = {
            "type": "subscribe",
            "channel": "book",
            "markets": self.markets,
        }
        
        if Config.CUSTOM_FEATURES_ENABLED:
            subscribe_msg["custom_features"] = {"best_bid_ask": True}
        
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to {len(self.markets)} markets")
    
    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message"""
        try:
            self.messages_received += 1
            self.last_message_time = datetime.now()
            
            data = json.loads(message)

            # Guard: some messages arrive as JSON arrays
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        self._process_event(item, message)
                return

            if isinstance(data, dict):
                self._process_event(data, message)
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    def _process_event(self, data: dict, raw_message: str):
        """Route a single parsed event dict."""
        event_type = data.get('event_type')

        if event_type == 'book':
            self.book_messages += 1
            self.db.save_book_snapshot(data, raw_message=raw_message)

            if self.book_messages % 100 == 0:
                logger.debug(f"Received {self.book_messages} book snapshots")

        elif event_type == 'price_change':
            self.price_change_messages += 1
            self.db.save_price_change(data, raw_message=raw_message)
    
    def stop(self):
        """Stop the WebSocket client"""
        self.running = False
        if self.ws:
            asyncio.create_task(self.ws.close())
    
    def get_stats(self) -> dict:
        """Get client statistics"""
        return {
            'messages_received': self.messages_received,
            'book_messages': self.book_messages,
            'price_change_messages': self.price_change_messages,
            'last_message_time': self.last_message_time,
            'snapshot_count': self.db.get_snapshot_count(),
        }