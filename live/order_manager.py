"""
Order manager for DCA Polymarket Live Trading.

Handles both paper and real order execution.
- Paper mode: simulates fills against live orderbook data
- Real mode: places orders via Polymarket CLOB API (py-clob-client)
"""

import json
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def parse_orderbook_levels(json_str) -> List[Dict]:
    """Parse orderbook levels from JSON string or list."""
    if not json_str:
        return []
    if isinstance(json_str, list):
        return json_str
    if isinstance(json_str, str):
        try:
            return json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return []
    return []


class PaperOrderManager:
    """
    Simulates order execution against live orderbook data.
    No real money is at risk.
    """

    def __init__(self):
        self.order_count = 0

    def execute_buy(self, asks, amount_usd: float) -> Tuple[float, float, float]:
        """
        Simulate buy order against asks.

        Args:
            asks: Ask levels (list of dicts or JSON string)
            amount_usd: Amount to spend in USD

        Returns:
            (shares_bought, total_cost, avg_price)
        """
        levels = parse_orderbook_levels(asks)
        
        # Debug logging
        logger.info(f"[ORDER_MGR BUY] Raw asks type: {type(asks)}, has data: {bool(asks)}")
        logger.info(f"[ORDER_MGR BUY] Parsed {len(levels)} levels, amount: ${amount_usd:.2f}")
        if levels:
            logger.info(f"[ORDER_MGR BUY] First 3 levels: {levels[:3]}")
        else:
            logger.warning(f"[ORDER_MGR BUY] ⚠️  EMPTY LEVELS! Raw: {str(asks)[:200]}")
        
        if not levels or amount_usd <= 0:
            logger.warning(f"[ORDER_MGR BUY] Returning 0 (no levels or invalid amount)")
            return 0.0, 0.0, 0.0

        shares, cost, remaining = 0.0, 0.0, amount_usd
        levels_used = 0
        for level in levels:
            if remaining <= 0:
                break
            p = float(level.get("price", 0))
            s = float(level.get("size", 0))
            if p <= 0 or s <= 0:
                continue
            take = min(s, remaining / p)
            shares += take
            cost += take * p
            remaining -= take * p
            levels_used += 1

        avg_price = cost / shares if shares > 0 else 0.0
        self.order_count += 1
        logger.info(f"[PAPER BUY] {shares:.4f} shares @ avg ${avg_price:.4f} = ${cost:.2f} (used {levels_used} levels)")
        return shares, cost, avg_price

    def execute_sell(self, bids, shares_to_sell: float) -> Tuple[float, float, float]:
        """
        Simulate sell order against bids.

        Args:
            bids: Bid levels (list of dicts or JSON string)
            shares_to_sell: Number of shares to sell

        Returns:
            (shares_sold, total_proceeds, avg_price)
        """
        levels = parse_orderbook_levels(bids)
        if not levels or shares_to_sell <= 0:
            return 0.0, 0.0, 0.0

        sold, proceeds, remaining = 0.0, 0.0, shares_to_sell
        for level in levels:
            if remaining <= 0:
                break
            p = float(level.get("price", 0))
            s = float(level.get("size", 0))
            if p <= 0 or s <= 0:
                continue
            take = min(s, remaining)
            sold += take
            proceeds += take * p
            remaining -= take

        avg_price = proceeds / sold if sold > 0 else 0.0
        self.order_count += 1
        logger.info(f"[PAPER SELL] {sold:.4f} shares @ avg ${avg_price:.4f} = ${proceeds:.2f}")
        return sold, proceeds, avg_price


class RealOrderManager:
    """
    Executes real orders on Polymarket via the CLOB API.

    Uses py-clob-client for authenticated order placement.
    Orders are placed as limit orders at the current best price
    to ensure immediate fill (marketable limit).
    """

    def __init__(self, private_key: str, clob_url: str = "https://clob.polymarket.com",
                 chain_id: int = 137, api_key: str = "", api_secret: str = "",
                 api_passphrase: str = ""):
        self.order_count = 0
        self.private_key = private_key
        self.clob_url = clob_url
        self.chain_id = chain_id
        self.client = None

        self._init_client(api_key, api_secret, api_passphrase)

    def _init_client(self, api_key: str, api_secret: str, api_passphrase: str):
        """Initialize the py-clob-client."""
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            if api_key and api_secret and api_passphrase:
                creds = ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase,
                )
                self.client = ClobClient(
                    self.clob_url,
                    key=self.private_key,
                    chain_id=self.chain_id,
                    creds=creds,
                )
            else:
                self.client = ClobClient(
                    self.clob_url,
                    key=self.private_key,
                    chain_id=self.chain_id,
                )
                # Derive API creds
                self.client.set_api_creds(self.client.create_or_derive_api_creds())

            logger.info("Real order client initialized")

        except ImportError:
            raise ImportError(
                "py-clob-client required for real trading. "
                "Install with: pip install py-clob-client"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to init CLOB client: {e}")

    def execute_buy(self, token_id: str, amount_usd: float,
                    best_ask: float = None, asks=None) -> Tuple[float, float, float]:
        """
        Place a real buy order on Polymarket.

        Places a limit buy at or slightly above best ask for immediate fill.

        Args:
            token_id: The token/asset ID to buy
            amount_usd: Amount in USD to spend
            best_ask: Current best ask price
            asks: Ask levels for price estimation

        Returns:
            (shares_bought, total_cost, avg_price)
        """
        if not self.client:
            logger.error("CLOB client not initialized")
            return 0.0, 0.0, 0.0

        if amount_usd <= 0 or not best_ask or best_ask <= 0:
            return 0.0, 0.0, 0.0

        try:
            from py_clob_client.clob_types import OrderArgs
            from py_clob_client.order_builder.constants import BUY

            # Calculate shares at best ask price
            shares = amount_usd / best_ask

            order_args = OrderArgs(
                price=round(best_ask, 2),
                size=round(shares, 2),
                side=BUY,
                token_id=token_id,
            )

            signed_order = self.client.create_order(order_args)
            resp = self.client.post_order(signed_order)

            self.order_count += 1
            logger.info(f"[REAL BUY] {shares:.2f} shares @ ${best_ask:.4f} = ${amount_usd:.2f}")
            logger.info(f"  Order response: {resp}")

            # Estimate fill (actual fill may differ)
            cost = shares * best_ask
            return shares, cost, best_ask

        except Exception as e:
            logger.error(f"REAL BUY FAILED: {e}")
            return 0.0, 0.0, 0.0

    def execute_sell(self, token_id: str, shares_to_sell: float,
                     best_bid: float = None, bids=None) -> Tuple[float, float, float]:
        """
        Place a real sell order on Polymarket.

        Args:
            token_id: The token/asset ID to sell
            shares_to_sell: Number of shares
            best_bid: Current best bid price
            bids: Bid levels for price estimation

        Returns:
            (shares_sold, total_proceeds, avg_price)
        """
        if not self.client:
            logger.error("CLOB client not initialized")
            return 0.0, 0.0, 0.0

        if shares_to_sell <= 0 or not best_bid or best_bid <= 0:
            return 0.0, 0.0, 0.0

        try:
            from py_clob_client.clob_types import OrderArgs
            from py_clob_client.order_builder.constants import SELL

            order_args = OrderArgs(
                price=round(best_bid, 2),
                size=round(shares_to_sell, 2),
                side=SELL,
                token_id=token_id,
            )

            signed_order = self.client.create_order(order_args)
            resp = self.client.post_order(signed_order)

            self.order_count += 1
            proceeds = shares_to_sell * best_bid
            logger.info(f"[REAL SELL] {shares_to_sell:.2f} shares @ ${best_bid:.4f} = ${proceeds:.2f}")
            logger.info(f"  Order response: {resp}")

            return shares_to_sell, proceeds, best_bid

        except Exception as e:
            logger.error(f"REAL SELL FAILED: {e}")
            return 0.0, 0.0, 0.0


def create_order_manager(mode: str = "paper", **kwargs):
    """
    Factory function to create the appropriate order manager.

    Args:
        mode: "paper" or "real"
        **kwargs: For real mode, pass private_key, clob_url, etc.

    Returns:
        PaperOrderManager or RealOrderManager
    """
    if mode == "real":
        required = ["private_key"]
        missing = [k for k in required if k not in kwargs or not kwargs[k]]
        if missing:
            raise ValueError(f"Missing required params for real mode: {missing}")
        return RealOrderManager(**kwargs)
    else:
        return PaperOrderManager()
