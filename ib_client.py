import json
import logging
import asyncio
from pathlib import Path
from typing import Any
from ib_async import IB, Stock, Contract

from config import Config

logger = logging.getLogger(__name__)


class IBClientManager:
    """Manages IBKR API connection and stock contract qualification."""

    def __init__(self, config: Config):
        self.config = config
        self.ib = IB()
        self._connect_lock = asyncio.Lock()

    async def connect(self) -> bool:
        async with self._connect_lock:
            if self.ib.isConnected():
                return True
            try:
                logger.info("Connecting to IBKR TWS/Gateway at %s:%d...", self.config.ib.host, self.config.ib.port)
                await self.ib.connectAsync(
                    host=self.config.ib.host,
                    port=self.config.ib.port,
                    clientId=self.config.ib.client_id,
                    timeout=10,
                )
                self.ib.reqMarketDataType(1)  # Request IBKR Real-Time Live Market Data
                logger.info("Connected to IBKR successfully.")
                return True
            except Exception as e:
                logger.error("Failed to connect to IBKR: %s", e)
                return False

    def is_connected(self) -> bool:
        return self.ib.isConnected()

    async def load_or_qualify_contracts(self) -> list[Contract]:
        """Loads qualified contracts from JSON cache or qualifies tickers from tickers.txt via IBKR."""
        if not self.config.tickers_file.exists():
            logger.error("Tickers file not found at %s", self.config.tickers_file)
            return []

        # Read tickers from tickers.txt (supports comma-separated or line-separated)
        raw_text = self.config.tickers_file.read_text(encoding="utf-8")
        raw_tokens = [t.strip().upper().replace(".", " ") for item in raw_text.split(",") for t in item.split() if t.strip()]
        unique_symbols = sorted(list(set(raw_tokens)))
        logger.info("Loaded %d unique ticker symbols from %s", len(unique_symbols), self.config.tickers_file.name)

        cache_file = self.config.contracts_cache_file
        cached_contracts: dict[str, dict[str, Any]] = {}
        if cache_file.exists():
            try:
                cached_contracts = json.loads(cache_file.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning("Failed to parse contracts cache %s: %s", cache_file, e)

        contracts: list[Contract] = []
        unqualified_symbols: list[str] = []

        for sym in unique_symbols:
            if sym in cached_contracts:
                c_data = cached_contracts[sym]
                contract = Stock(
                    symbol=c_data.get("symbol", sym),
                    exchange=c_data.get("exchange", "SMART"),
                    currency=c_data.get("currency", "USD"),
                )
                contract.conId = c_data["conId"]
                contract.primaryExchange = c_data.get("primaryExchange", "")
                contract.localSymbol = c_data.get("localSymbol", sym)
                contracts.append(contract)
            else:
                unqualified_symbols.append(sym)

        if unqualified_symbols:
            logger.info("Qualifying %d new stock contracts with IBKR...", len(unqualified_symbols))
            if not self.ib.isConnected():
                await self.connect()

            new_contracts = [Stock(sym, "SMART", "USD") for sym in unqualified_symbols]
            qualified = await self.ib.qualifyContractsAsync(*new_contracts)

            for c in qualified:
                if c and getattr(c, "conId", 0) > 0:
                    contracts.append(c)
                    cached_contracts[c.symbol] = {
                        "conId": c.conId,
                        "symbol": c.symbol,
                        "primaryExchange": c.primaryExchange,
                        "exchange": c.exchange,
                        "currency": c.currency,
                        "localSymbol": c.localSymbol,
                    }

            try:
                cache_file.write_text(json.dumps(cached_contracts, indent=2), encoding="utf-8")
                logger.info("Saved %d qualified contracts to %s", len(cached_contracts), cache_file.name)
            except Exception as e:
                logger.warning("Failed to save contracts cache: %s", e)

        return contracts
