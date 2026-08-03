import json
import logging
import asyncio
from typing import Any, Optional
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
                self.ib.reqMarketDataType(self.config.ib.market_data_type)  # Request IBKR Market Data Type from config
                
                # Attach error event handler to auto-requalify contracts on Error 162 or 200
                self.ib.errorEvent += self._on_ib_error
                
                logger.info("Connected to IBKR successfully.")
                return True
            except Exception as e:
                logger.error("Failed to connect to IBKR: %s", e)
                return False

    def _on_ib_error(self, reqId: int, errorCode: int, errorString: str, contract: Optional[Contract]):
        """Handler for IBKR API error messages. Automatically requalifies contracts on Error 162 or 200."""
        if errorCode in (162, 200) and contract and getattr(contract, "symbol", None):
            sym = contract.symbol
            logger.warning(
                "IBKR Error %d received for %s: %s. Scheduling contract re-qualification.",
                errorCode,
                sym,
                errorString,
            )
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.requalify_symbol(sym))
            except Exception as e:
                logger.debug("Could not schedule re-qualification task for %s: %s", sym, e)

    def load_deactivated_tickers(self) -> set[str]:
        cache_file = self.config.deactivated_tickers_file
        if cache_file.exists():
            try:
                data = json.loads(cache_file.read_text(encoding="utf-8"))
                return set(data)
            except Exception as e:
                logger.warning("Failed to load deactivated tickers %s: %s", cache_file, e)
        return set()

    def save_deactivated_tickers(self, deactivated: set[str]):
        cache_file = self.config.deactivated_tickers_file
        try:
            cache_file.write_text(json.dumps(sorted(list(deactivated)), indent=2), encoding="utf-8")
        except Exception as e:
            logger.warning("Failed to save deactivated tickers: %s", e)

    def is_deactivated(self, symbol: str) -> bool:
        return symbol.upper().replace(".", " ") in self.load_deactivated_tickers()

    async def requalify_symbol(self, symbol: str) -> Optional[Contract]:
        """Invalidates cached contract for a symbol and re-qualifies it fresh via IBKR.
        If qualification fails, deactivates the ticker so it is not retried.
        """
        symbol = symbol.upper().replace(".", " ")

        deactivated = self.load_deactivated_tickers()
        if symbol in deactivated:
            logger.info("Symbol '%s' is deactivated. Skipping re-qualification.", symbol)
            return None

        cache_file = self.config.contracts_cache_file
        cached_contracts: dict[str, dict[str, Any]] = {}
        if cache_file.exists():
            try:
                cached_contracts = json.loads(cache_file.read_text(encoding="utf-8"))
            except Exception:
                pass

        # Invalidate stale cache entry
        cached_contracts.pop(symbol, None)
        try:
            cache_file.write_text(json.dumps(cached_contracts, indent=2), encoding="utf-8")
        except Exception:
            pass

        logger.info("Re-qualifying contract for symbol '%s' with IBKR...", symbol)
        raw_stock = Stock(symbol, "SMART", "USD")
        try:
            qualified = await self.ib.qualifyContractsAsync(raw_stock)
            if qualified and len(qualified) > 0 and getattr(qualified[0], "conId", 0) > 0:
                c = qualified[0]
                cached_contracts[symbol] = {
                    "conId": c.conId,
                    "symbol": c.symbol,
                    "primaryExchange": getattr(c, "primaryExchange", ""),
                    "exchange": getattr(c, "exchange", "SMART"),
                    "currency": getattr(c, "currency", "USD"),
                    "localSymbol": getattr(c, "localSymbol", c.symbol),
                }
                cache_file.write_text(json.dumps(cached_contracts, indent=2), encoding="utf-8")
                logger.info("Successfully re-qualified contract for '%s' (conId=%d).", symbol, c.conId)
                return c
        except Exception as e:
            logger.warning("Failed to re-qualify contract for '%s': %s", symbol, e)

        # Qualification failed -> Deactivate ticker
        logger.warning("Deactivating ticker '%s': Qualification failed with IBKR. Will not retry.", symbol)
        deactivated.add(symbol)
        self.save_deactivated_tickers(deactivated)
        return None

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

        # Exclude deactivated tickers
        deactivated = self.load_deactivated_tickers()
        active_symbols = [s for s in unique_symbols if s not in deactivated]
        if len(deactivated) > 0:
            logger.info("Skipping %d deactivated tickers from market universe.", len(deactivated))

        logger.info("Loaded %d active ticker symbols from %s", len(active_symbols), self.config.tickers_file.name)

        cache_file = self.config.contracts_cache_file
        cached_contracts: dict[str, dict[str, Any]] = {}
        if cache_file.exists():
            try:
                cached_contracts = json.loads(cache_file.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning("Failed to parse contracts cache %s: %s", cache_file, e)

        contracts: list[Contract] = []
        unqualified_symbols: list[str] = []

        for sym in active_symbols:
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

            for orig_stock in new_contracts:
                sym = orig_stock.symbol
                matched = [c for c in qualified if c and getattr(c, "conId", 0) > 0 and c.symbol == sym]
                if matched:
                    c = matched[0]
                    contracts.append(c)
                    cached_contracts[sym] = {
                        "conId": c.conId,
                        "symbol": c.symbol,
                        "primaryExchange": c.primaryExchange,
                        "exchange": c.exchange,
                        "currency": c.currency,
                        "localSymbol": c.localSymbol,
                    }
                else:
                    logger.warning("Deactivating ticker '%s': Qualification failed with IBKR.", sym)
                    deactivated.add(sym)

            try:
                cache_file.write_text(json.dumps(cached_contracts, indent=2), encoding="utf-8")
                self.save_deactivated_tickers(deactivated)
                logger.info("Saved %d qualified contracts to %s", len(cached_contracts), cache_file.name)
            except Exception as e:
                logger.warning("Failed to save contracts cache: %s", e)

        return contracts
