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
        "timestamp",
        "asset_id",
        "market_id",
        "best_bid",
        "best_ask",
        "mid_price",
        "spread",
        "bids",
        "asks",
        "raw",
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
            self.best_bid = (
                max((self._parse_price(b["price"]) for b in bids), default=0.0)
                if bids
                else 0.0
            )
            self.best_ask = (
                min((self._parse_price(a["price"]) for a in asks), default=0.0)
                if asks
                else 0.0
            )

        elif event_type == "price_change":
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

    @classmethod
    def synthetic(
        cls,
        asset_id: str,
        market_id: str,
        timestamp,
        best_bid: float,
        best_ask: float,
        bids=None,
        asks=None,
        mid_price: float = None,
        spread: float = None,
    ):
        """Build a tick without going through the WebSocket parsing path.

        Used by parquet replay / backtest_main.
        """
        obj = object.__new__(cls)
        obj.asset_id = asset_id
        obj.market_id = market_id
        obj.timestamp = timestamp
        obj.best_bid = best_bid
        obj.best_ask = best_ask
        obj.bids = bids or []
        obj.asks = asks or []
        obj.raw = {}
        if mid_price is not None:
            obj.mid_price = mid_price
        elif best_bid > 0 and best_ask > 0:
            obj.mid_price = (best_bid + best_ask) / 2
        else:
            obj.mid_price = 0.0
        if spread is not None:
            obj.spread = spread
        elif best_bid > 0 and best_ask > 0:
            obj.spread = best_ask - best_bid
        else:
            obj.spread = 0.0
        return obj

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

    Usage::

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

        # Track which assets we've seen
        self.known_assets: Set[str] = set()
        self.asset_to_market: Dict[str, str] = {}

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

            if self.asset_ids:
                await self._subscribe_assets(ws)
            if self.market_ids:
                await self._subscribe_markets(ws)

            async for message in ws:
                if not self.running:
                    break
                await self._handle_message(message)

    async def _subscribe_assets(self, ws):
        msg = {
            "type": "subscribe",
            "channel": "book",
            "assets_ids": self.asset_ids,
            "custom_features": {"best_bid_ask": True},
        }
        await ws.send(json.dumps(msg))
        logger.info(f"Subscribed to {len(self.asset_ids)} assets")

    async def _subscribe_markets(self, ws):
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
                tick = OrderbookTick(data, event_type="book")
                self._track_asset(tick)
                if self.on_tick:
                    await self._dispatch_tick(tick)

            elif event_type == "price_change":
                self.price_change_messages += 1
                for change in data.get("price_changes", []):
                    change["market"] = data.get("market", "")
                    tick = OrderbookTick(change, event_type="price_change")
                    self._track_asset(tick)
                    if self.on_tick:
                        await self._dispatch_tick(tick)

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
        except Exception as e:
            logger.error(f"Message handling error: {e}")

    async def _dispatch_tick(self, tick: OrderbookTick):
        if asyncio.iscoroutinefunction(self.on_tick):
            await self.on_tick(tick)
        else:
            self.on_tick(tick)

    def _track_asset(self, tick: OrderbookTick):
        if tick.asset_id:
            self.known_assets.add(tick.asset_id)
        if tick.asset_id and tick.market_id:
            self.asset_to_market[tick.asset_id] = tick.market_id

    async def stream(self):
        """Async generator that yields ticks. Bridges callback → async-for.

        Usage::

            async for tick in feed.stream():
                process(tick)
        """
        queue: asyncio.Queue = asyncio.Queue()

        # Wire the internal callback to push ticks into the queue
        self.on_tick = queue.put

        # Run the WebSocket listener as a background task
        self._run_task = asyncio.create_task(self.run())

        try:
            while self.running or not queue.empty():
                try:
                    tick = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield tick
                except asyncio.TimeoutError:
                    # Check if the background task died unexpectedly
                    if self._run_task.done():
                        exc = self._run_task.exception()
                        if exc:
                            logger.error(f"Feed task crashed: {exc}")
                        break
                    continue
        finally:
            self.stop()

    async def close(self):
        """Gracefully shut down the feed and wait for the background task."""
        self.running = False
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
        if hasattr(self, '_run_task') and not self._run_task.done():
            self._run_task.cancel()
            try:
                await self._run_task
            except (asyncio.CancelledError, Exception):
                pass

    def stop(self):
        self.running = False

    def get_stats(self) -> dict:
        return {
            "messages_received": self.messages_received,
            "book_messages": self.book_messages,
            "price_change_messages": self.price_change_messages,
            "last_message_time": self.last_message_time,
            "known_assets": len(self.known_assets),
        }
