"""
Live WebSocket orderbook feed for Polymarket.

Subscribes to orderbook updates for configured markets and
dispatches tick data to the strategy engine in real time.
"""

import json
import asyncio
import logging
from typing import Dict, List, Callable, Optional, Set
from datetime import datetime

import websockets

logger = logging.getLogger(__name__)


class OrderbookTick:
    """A single orderbook update tick."""

    __slots__ = [
        "timestamp", "asset_id", "market_id",
        "best_bid", "best_ask", "mid_price", "spread",
        "bids", "asks", "raw",
    ]

    def __init__(self, data: dict, event_type: str = "book"):
        self.timestamp = datetime.utcnow()
        self.raw = data

        if event_type == "book":
            self.asset_id = data.get("asset_id", "")
            self.market_id = data.get("market", "")
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            self.bids = bids
            self.asks = asks
            self.best_bid = max((self._parse_price(b["price"]) for b in bids), default=0.0) if bids else 0.0
            self.best_ask = min((self._parse_price(a["price"]) for a in asks), default=0.0) if asks else 0.0
        elif event_type == "price_change":
            # price_change events have a different structure
            self.asset_id = data.get("asset_id", "")
            self.market_id = data.get("market", "")
            self.best_bid = self._parse_price(data.get("best_bid", 0))
            self.best_ask = self._parse_price(data.get("best_ask", 0))
            self.bids = []
            self.asks = []
        else:
            self.asset_id = ""
            self.market_id = ""
            self.best_bid = 0.0
            self.best_ask = 0.0
            self.bids = []
            self.asks = []

        if self.best_bid > 0 and self.best_ask > 0:
            self.mid_price = (self.best_bid + self.best_ask) / 2
            self.spread = self.best_ask - self.best_bid
        else:
            self.mid_price = 0.0
            self.spread = 0.0

    @staticmethod
    def _parse_price(price_str) -> float:
        if not price_str:
            return 0.0
        s = str(price_str)
        if s.startswith("."):
            return float("0" + s)
        return float(s)


class OrderbookFeed:
    """
    Connects to Polymarket WebSocket and delivers orderbook ticks.

    Usage:
        feed = OrderbookFeed(asset_ids=[...], on_tick=my_callback)
        await feed.run()
    """

    def __init__(
        self,
        asset_ids: List[str] = None,
        market_ids: List[str] = None,
        wss_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        on_tick: Optional[Callable] = None,
        reconnect_delay: int = 5,
    ):
        self.asset_ids = asset_ids or []
        self.market_ids = market_ids or []
        self.wss_url = wss_url
        self.on_tick = on_tick
        self.reconnect_delay = reconnect_delay
        self.running = True
        self.ws = None

        # Stats
        self.messages_received = 0
        self.book_messages = 0
        self.price_change_messages = 0
        self.last_message_time: Optional[datetime] = None

        # Track which assets we've seen (for discovery)
        self.known_assets: Set[str] = set()
        self.asset_to_market: Dict[str, str] = {}
        
        # Track book message count per asset (need 2nd book before trading)
        self.asset_book_count: Dict[str, int] = {}  # asset_id -> count
        self.buffered_price_changes: Dict[str, List] = {}  # asset_id -> list of buffered deltas
        
        # Maintain live orderbook per asset (updated by price_change deltas)
        self.live_orderbook: Dict[str, Dict] = {}  # asset_id -> {bids: [], asks: [], market: str}

    async def run(self):
        """Main run loop with automatic reconnection."""
        while self.running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                logger.info("Feed cancelled")
                break
            except Exception as e:
                logger.error(f"Feed error: {e}")

            if not self.running:
                break

            logger.info(f"Reconnecting in {self.reconnect_delay}s...")
            await asyncio.sleep(self.reconnect_delay)

    async def _connect_and_listen(self):
        """Single connection lifecycle."""
        logger.info(f"Connecting to {self.wss_url}")

        async with websockets.connect(
            self.wss_url,
            ping_interval=30,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self.ws = ws
            logger.info("Connected to Polymarket WebSocket")

            # Subscribe
            if self.asset_ids:
                await self._subscribe_assets(ws)
            if self.market_ids:
                await self._subscribe_markets(ws)

            async for message in ws:
                if not self.running:
                    break
                await self._handle_message(message)

    async def _subscribe_assets(self, ws):
        """Subscribe to asset-level orderbook data."""
        msg = {
            "type": "subscribe",
            "channel": "book",
            "assets_ids": self.asset_ids,
            "custom_features": {"best_bid_ask": True},
        }
        await ws.send(json.dumps(msg))
        logger.info(f"Subscribed to {len(self.asset_ids)} assets")

    async def _subscribe_markets(self, ws):
        """Subscribe to market-level orderbook data."""
        msg = {
            "type": "subscribe",
            "channel": "book",
            "markets": self.market_ids,
            "custom_features": {"best_bid_ask": True},
        }
        await ws.send(json.dumps(msg))
        logger.info(f"Subscribed to {len(self.market_ids)} markets")

    async def _handle_message(self, message: str):
        """Parse message and dispatch tick."""
        try:
            self.messages_received += 1
            self.last_message_time = datetime.utcnow()
            data = json.loads(message)
            event_type = data.get("event_type", "")


            if event_type == "book":
                self.book_messages += 1
                aid = data.get("asset_id", "")
                
                # Track book count for this asset
                self.asset_book_count[aid] = self.asset_book_count.get(aid, 0) + 1
                book_num = self.asset_book_count[aid]
                
                # First book: ignore (stale snapshot)
                if book_num == 1:
                    logger.info(f"📘 First book snapshot for {aid[:20]}... (IGNORING)")
                    # Initialize buffer for price changes
                    self.buffered_price_changes[aid] = []
                    return  # Don't process or dispatch
                
                # Second+ book: Update live orderbook and dispatch
                logger.info(f"📗 Book #{book_num} for {aid[:20]}... - Updating live orderbook")
                
                # Store full orderbook snapshot with sorted levels
                bids = data.get("bids", []).copy()
                asks = data.get("asks", []).copy()
                
                # Sort: bids descending (highest first), asks ascending (lowest first)
                bids.sort(key=lambda x: float(x.get("price", 0)), reverse=True)
                asks.sort(key=lambda x: float(x.get("price", 0)))
                
                self.live_orderbook[aid] = {
                    "bids": bids,
                    "asks": asks,
                    "market": data.get("market", "")
                }
                
                # Create tick from live orderbook
                tick = OrderbookTick(data, event_type="book")
                self._track_asset(tick)
                
                # Second book: apply buffered price changes
                if book_num == 2:
                    logger.info(f"✅ Second book received for {aid[:20]}... - NOW ACTIVE")
                    
                    # Apply buffered price change deltas
                    buffered = self.buffered_price_changes.get(aid, [])
                    if buffered:
                        logger.info(f"📦 Applying {len(buffered)} buffered price changes...")
                        for delta in buffered:
                            self._apply_price_change_delta(aid, delta)
                    
                    # Clear buffer
                    self.buffered_price_changes.pop(aid, None)
                    
                    # Create tick from updated orderbook (already sorted)
                    book_data = self.live_orderbook[aid]
                    tick_data = {
                        "asset_id": aid,
                        "market": book_data["market"],
                        "bids": book_data["bids"],
                        "asks": book_data["asks"]
                    }
                    tick = OrderbookTick(tick_data, event_type="book")
                
                # Dispatch the tick
                if self.on_tick:
                    await self._dispatch_tick(tick)

            elif event_type == "price_change":
                self.price_change_messages += 1
                price_changes = data.get("price_changes", [])
                
                for change in price_changes:
                    if not isinstance(change, dict):
                        logger.warning(f"Skipping non-dict price_change: {type(change)}")
                        continue
                    
                    aid = change.get("asset_id", "")
                    if not aid:
                        continue
                    
                    self._track_asset_id(aid, data.get("market", ""))
                    
                    # Check if we've received 2nd book for this asset yet
                    book_count = self.asset_book_count.get(aid, 0)
                    
                    if book_count < 2:
                        # Buffer this delta until 2nd book arrives
                        if aid not in self.buffered_price_changes:
                            self.buffered_price_changes[aid] = []
                        self.buffered_price_changes[aid].append(change)
                    else:
                        # Asset is active - apply delta to live orderbook
                        self._apply_price_change_delta(aid, change)
                        
                        # Sort and dispatch tick from updated orderbook
                        if aid in self.live_orderbook:
                            book = self.live_orderbook[aid]
                            
                            # Sort before dispatching: bids desc, asks asc
                            sorted_bids = sorted(book["bids"], key=lambda x: float(x.get("price", 0)), reverse=True)
                            sorted_asks = sorted(book["asks"], key=lambda x: float(x.get("price", 0)))
                            
                            tick_data = {
                                "asset_id": aid,
                                "market": book["market"],
                                "bids": sorted_bids,
                                "asks": sorted_asks
                            }
                            tick = OrderbookTick(tick_data, event_type="book")
                            if self.on_tick:
                                await self._dispatch_tick(tick)

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
        except Exception as e:
            logger.error(f"Message handling error: {e}", exc_info=True)

    async def _dispatch_tick(self, tick: OrderbookTick):
        """Dispatch tick to callback (async or sync)."""
        if asyncio.iscoroutinefunction(self.on_tick):
            await self.on_tick(tick)
        else:
            self.on_tick(tick)

    def _track_asset(self, tick: OrderbookTick):
        if tick.asset_id:
            self.known_assets.add(tick.asset_id)
        if tick.asset_id and tick.market_id:
            self.asset_to_market[tick.asset_id] = tick.market_id
    
    def _track_asset_id(self, asset_id: str, market_id: str):
        """Track asset without creating a full tick."""
        if asset_id:
            self.known_assets.add(asset_id)
        if asset_id and market_id:
            self.asset_to_market[asset_id] = market_id
    
    def _apply_price_change_delta(self, asset_id: str, change: Dict):
        """Apply a price change delta to the live orderbook."""
        if asset_id not in self.live_orderbook:
            logger.warning(f"Cannot apply price change - no live orderbook for {asset_id[:20]}...")
            return
        
        book = self.live_orderbook[asset_id]
        price = change.get("price", "")
        size = change.get("size", "")
        side = change.get("side", "")
        
        try:
            price_float = float(price)
            size_float = float(size)
        except (ValueError, TypeError):
            logger.warning(f"Invalid price/size in delta: price={price}, size={size}")
            return
        
        # Update the appropriate side
        if side == "BUY":
            # BUY order updates the bid side
            self._update_orderbook_level(book["bids"], price_float, size_float)
        elif side == "SELL":
            # SELL order updates the ask side
            self._update_orderbook_level(book["asks"], price_float, size_float)
    
    def _update_orderbook_level(self, levels: List[Dict], price: float, size: float):
        """Update or insert a price level in the orderbook.
        Note: Caller is responsible for sorting after updates."""
        # Find existing level with this price
        for i, level in enumerate(levels):
            level_price = float(level.get("price", 0))
            if abs(level_price - price) < 0.0001:  # Same price
                if size > 0:
                    # Update size
                    levels[i]["size"] = str(size)
                else:
                    # Remove level (size 0)
                    levels.pop(i)
                return
        
        # Price not found - add new level if size > 0
        if size > 0:
            levels.append({"price": str(price), "size": str(size)})

    def stop(self):
        """Stop the feed."""
        self.running = False
        if self.ws:
            asyncio.get_event_loop().call_soon_threadsafe(
                lambda: asyncio.ensure_future(self.ws.close()) if self.ws else None
            )

    def get_stats(self) -> dict:
        active_assets = sum(1 for count in self.asset_book_count.values() if count >= 2)
        buffered_total = sum(len(changes) for changes in self.buffered_price_changes.values())
        
        return {
            "messages_received": self.messages_received,
            "book_messages": self.book_messages,
            "price_change_messages": self.price_change_messages,
            "last_message_time": self.last_message_time,
            "known_assets": len(self.known_assets),
            "active_assets": active_assets,
            "buffered_price_changes": buffered_total,
        }
