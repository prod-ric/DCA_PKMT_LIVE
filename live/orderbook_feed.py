"""
Live WebSocket orderbook feed for Polymarket.

Subscribes to orderbook updates for configured markets and
dispatches tick data to the strategy engine in real time.

Maintains full reconstructed orderbook state per asset so that
every emitted tick carries the complete bids/asks levels —
matching the logic in ``collector_parquet/reconstruct_orderbooks.py``.

Message formats from the Polymarket WebSocket
----------------------------------------------
**book** (full snapshot)::

    {
      "event_type": "book",
      "asset_id": "...",
      "market": "...",
      "bids": [{"price": "0.55", "size": "100"}, ...],
      "asks": [{"price": "0.56", "size": "200"}, ...]
    }

**price_change** (individual order delta)::

    {
      "event_type": "price_change",
      "market": "...",
      "price_changes": [
          {
              "asset_id": "...",
              "side": "BUY",      # or "SELL"
              "price": "0.55",
              "size": "120",      # new total size at level; 0 = remove
              "best_bid": "0.55",
              "best_ask": "0.56"
          },
          ...
      ]
    }
"""

import json
import asyncio
import logging
from typing import Dict, List, Callable, Optional, Set
from datetime import datetime

import websockets

logger = logging.getLogger(__name__)

MAX_BOOK_LEVELS = 50  # max levels kept per side


# ────────────────────────────────────────────────────────────────
# OrderbookTick
# ────────────────────────────────────────────────────────────────

class OrderbookTick:
    """A single orderbook update tick with full reconstructed book."""

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

    def __init__(self, asset_id: str, market_id: str,
                 best_bid: float, best_ask: float,
                 bids: list, asks: list,
                 raw: dict = None):
        self.timestamp = datetime.utcnow()
        self.asset_id = asset_id
        self.market_id = market_id
        self.best_bid = best_bid
        self.best_ask = best_ask
        self.bids = bids
        self.asks = asks
        self.raw = raw or {}
        if best_bid > 0 and best_ask > 0:
            self.mid_price = (best_bid + best_ask) / 2
            self.spread = best_ask - best_bid
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


# ────────────────────────────────────────────────────────────────
# Per-asset reconstructed book state
# ────────────────────────────────────────────────────────────────

class _BookState:
    """Maintains a live orderbook for one asset.

    Bids/asks are stored as ``{price_float: size_float}`` dicts.
    A ``book`` event replaces the entire state; a ``price_change``
    delta updates a single level (or removes it when size == 0).
    """

    __slots__ = ("bids", "asks")

    def __init__(self):
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

    # -- mutators -------------------------------------------------

    def reset(self, raw_bids: list, raw_asks: list):
        """Full book reset from a ``book`` snapshot."""
        self.bids = {
            float(b["price"]): float(b["size"])
            for b in raw_bids
            if float(b.get("size", 0)) > 0
        }
        self.asks = {
            float(a["price"]): float(a["size"])
            for a in raw_asks
            if float(a.get("size", 0)) > 0
        }

    def apply_delta(self, side: str, price: float, size: float):
        """Apply a single ``price_change`` delta.

        ``side`` is ``"BUY"`` (bid) or ``"SELL"`` (ask).
        ``size == 0`` means the level is removed.
        """
        book = self.bids if side == "BUY" else self.asks
        if size > 0:
            book[price] = size
        else:
            book.pop(price, None)

    # -- read-only snapshots --------------------------------------

    def sorted_bids(self, max_levels: int = MAX_BOOK_LEVELS) -> List[dict]:
        """Return bids sorted highest-first, capped to *max_levels*."""
        return [
            {"price": p, "size": s}
            for p, s in sorted(self.bids.items(), key=lambda x: -x[0])[:max_levels]
        ]

    def sorted_asks(self, max_levels: int = MAX_BOOK_LEVELS) -> List[dict]:
        """Return asks sorted lowest-first, capped to *max_levels*."""
        return [
            {"price": p, "size": s}
            for p, s in sorted(self.asks.items(), key=lambda x: x[0])[:max_levels]
        ]

    @property
    def best_bid(self) -> float:
        return max(self.bids, default=0.0) if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return min(self.asks, default=0.0) if self.asks else 0.0


# ────────────────────────────────────────────────────────────────
# OrderbookFeed
# ────────────────────────────────────────────────────────────────

class OrderbookFeed:
    """
    Connects to Polymarket WebSocket and delivers orderbook ticks.

    Maintains a *reconstructed* full orderbook per asset so that
    every tick includes the complete bids/asks levels, regardless
    of whether the underlying event was a ``book`` snapshot or a
    ``price_change`` delta.

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

        # ── Per-asset reconstructed book ──
        self._books: Dict[str, _BookState] = {}

        # Stats
        self.messages_received = 0
        self.book_messages = 0
        self.price_change_messages = 0
        self.last_message_time: Optional[datetime] = None

        # Track which assets we've seen
        self.known_assets: Set[str] = set()
        self.asset_to_market: Dict[str, str] = {}

    # ── helpers ──────────────────────────────────────────────────

    def _get_book(self, asset_id: str) -> _BookState:
        """Return (or create) the book state for *asset_id*."""
        if asset_id not in self._books:
            self._books[asset_id] = _BookState()
        return self._books[asset_id]

    def _make_tick(self, asset_id: str, market_id: str,
                   raw: dict = None) -> OrderbookTick:
        """Build a tick from the current reconstructed book state."""
        book = self._get_book(asset_id)
        return OrderbookTick(
            asset_id=asset_id,
            market_id=market_id,
            best_bid=book.best_bid,
            best_ask=book.best_ask,
            bids=book.sorted_bids(),
            asks=book.sorted_asks(),
            raw=raw,
        )

    # ── connection lifecycle ────────────────────────────────────

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

            # Clear book state on reconnect — we'll get fresh snapshots
            self._books.clear()
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

    # ── message handling ────────────────────────────────────────

    async def _handle_message(self, message: str):
        """Parse a WebSocket message, update book state, emit tick(s)."""
        try:
            self.messages_received += 1
            self.last_message_time = datetime.utcnow()
            data = json.loads(message)

            # Guard: some messages arrive as lists (e.g. batch ack)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        await self._dispatch_event(item)
                return

            if isinstance(data, dict):
                await self._dispatch_event(data)

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
        except Exception as e:
            logger.error(f"Message handling error: {e}", exc_info=True)

    async def _dispatch_event(self, data: dict):
        """Route a single event dict (``book`` or ``price_change``)."""
        event_type = data.get("event_type", "")

        if event_type == "book":
            await self._on_book(data)

        elif event_type == "price_change":
            await self._on_price_change(data)

        # Silently ignore unknown / control messages (subscribe ack, etc.)

    # ── book snapshot ───────────────────────────────────────────

    async def _on_book(self, data: dict):
        """Handle a full ``book`` snapshot — replaces the entire book."""
        self.book_messages += 1
        asset_id = data.get("asset_id", "")
        market_id = data.get("market", "")
        if not asset_id:
            return

        book = self._get_book(asset_id)
        book.reset(data.get("bids", []), data.get("asks", []))

        tick = self._make_tick(asset_id, market_id, raw=data)
        self._track_asset(tick)
        if self.on_tick:
            await self._dispatch_tick(tick)

    # ── price_change deltas ─────────────────────────────────────

    async def _on_price_change(self, data: dict):
        """Handle a ``price_change`` — apply delta(s), emit tick(s)."""
        self.price_change_messages += 1
        market_id = data.get("market", "")

        changes = data.get("price_changes", [])

        # Group changes by asset so we emit one tick per asset per msg
        assets_changed: Dict[str, list] = {}
        for change in changes:
            aid = change.get("asset_id", "")
            if not aid:
                continue
            assets_changed.setdefault(aid, []).append(change)

        for aid, deltas in assets_changed.items():
            book = self._get_book(aid)
            for ch in deltas:
                side = str(ch.get("side", "")).upper()
                price = float(ch.get("price", 0))
                size = float(ch.get("size", 0))
                if side in ("BUY", "SELL") and price > 0:
                    book.apply_delta(side, price, size)

            tick = self._make_tick(aid, market_id, raw=data)
            self._track_asset(tick)
            if self.on_tick:
                await self._dispatch_tick(tick)

    # ── dispatch / tracking ─────────────────────────────────────

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

    # ── async generator interface ───────────────────────────────

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

    # ── shutdown ────────────────────────────────────────────────

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
            "books_tracked": len(self._books),
        }
