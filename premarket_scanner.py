import logging
import math
import asyncio
from datetime import datetime, time
from typing import Any, Optional
import zoneinfo

from ib_async import IB, Contract, util
from config import Config, ET_TZ, PT_TZ
from ib_client import IBClientManager
from briefing_news import BriefingNewsClient

logger = logging.getLogger(__name__)


class ScanLogBuffer(logging.Handler):
    """Custom logging handler to buffer WARNING and ERROR logs for Dashboard inspection."""

    def __init__(self, capacity: int = 150):
        super().__init__()
        self.capacity = capacity
        self.logs: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord):
        try:
            now_pt = datetime.now(PT_TZ)
            ts_str = now_pt.strftime("%I:%M:%S %p PT")
            msg = record.getMessage()
            entry = {
                "timestamp": ts_str,
                "level": record.levelname,
                "logger": record.name,
                "message": msg,
            }
            self.logs.append(entry)
            if len(self.logs) > self.capacity:
                self.logs.pop(0)
        except Exception:
            self.handleError(record)

    def get_logs(self) -> list[dict[str, Any]]:
        return list(reversed(self.logs))

    def clear(self):
        self.logs.clear()


scan_log_buffer = ScanLogBuffer()
scan_log_buffer.setLevel(logging.WARNING)
logging.getLogger().addHandler(scan_log_buffer)


class PremarketScanner:
    """Ultra-fast, clean Pre/Post Market Mover Scanner complying strictly with specification."""

    def __init__(self, config: Config, ib_manager: IBClientManager, briefing_client: BriefingNewsClient):
        self.config = config
        self.ib_manager = ib_manager
        self.briefing_client = briefing_client

        # Cached state across session runs
        self.adv20_cache: dict[str, float] = {}          # symbol -> 20-day ADV
        self.last_adv20_session_key: Optional[str] = None

        # Scan metadata
        self.is_scanning: bool = False
        self.is_paused: bool = False
        self.user_resumed: bool = False
        self.last_scan_start_time: Optional[datetime] = None
        self.last_scan_end_time: Optional[datetime] = None
        self.last_scan_duration_sec: float = 0.0
        self.last_scan_results: list[dict[str, Any]] = []

    def get_market_status(self, now_pt: datetime | None = None) -> tuple[str, bool, bool]:
        """Determines market status string, whether controls are active, and whether auto-scan should run.
        Returns: (session_name, is_active_session, should_auto_scan)
        """
        if now_pt is None:
            now_pt = datetime.now(PT_TZ)

        # Check weekday (0 = Mon, 4 = Fri)
        if now_pt.weekday() >= 5:
            return "Market Closed", False, False

        t = now_pt.time()
        pm_display_start = time(*self.config.pm_display_start_pt)  # (1, 0)
        pm_auto_start = time(*self.config.pm_auto_start_pt)        # (5, 30)
        pm_end = time(*self.config.pm_end_pt)                      # (6, 30)

        post_start = time(*self.config.post_start_pt)              # (13, 0)
        post_end = time(*self.config.post_end_pt)                  # (17, 0)

        if pm_display_start <= t < pm_end:
            # Controls are active (can press Resume)
            is_active = True
            # Auto-scanning runs automatically from 5:30 to 6:30 AM
            should_auto_scan = (pm_auto_start <= t < pm_end)
            return "Premarket", is_active, should_auto_scan
        elif post_start <= t < post_end:
            return "Postmarket", True, True
        elif pm_end <= t < time(13, 0):
            return "Market Open", False, False
        else:
            return "Market Closed", False, False

    def _is_postmarket_bar(self, bar_date: Any) -> bool:
        """Helper to determine if a bar's timestamp is at or after 16:00 ET (4:00 PM Eastern Time)."""
        if isinstance(bar_date, str):
            bar_date = util.parseIBDatetime(bar_date)
        if isinstance(bar_date, datetime):
            if bar_date.tzinfo is not None:
                dt_et = bar_date.astimezone(ET_TZ)
            else:
                dt_et = bar_date.replace(tzinfo=ET_TZ)
            return dt_et.time() >= time(16, 0)
        return False

    async def _fetch_adv20_batch(self, contracts: list[Contract]):
        """Fetches 20 daily RTH bars in parallel to calculate ADV20 over the last 20 market days for all contracts."""
        logger.info("Initializing ADV20 calculation for %d contracts...", len(contracts))
        ib = self.ib_manager.ib
        semaphore = asyncio.Semaphore(40)

        async def _fetch_one(c: Contract):
            async with semaphore:
                try:
                    bars = await ib.reqHistoricalDataAsync(
                        c,
                        endDateTime="",
                        durationStr="20 D",
                        barSizeSetting="1 day",
                        whatToShow="TRADES",
                        useRTH=True,
                        formatDate=1,
                    )
                    if bars and len(bars) > 0:
                        vols = [b.volume for b in bars if b.volume is not None and b.volume >= 0]
                        if len(vols) < 20:
                            logger.warning(
                                "ADV20 notice for %s: Received only %d market bars (fewer than 20 market days required).",
                                c.symbol,
                                len(vols),
                            )
                        recent_20_vols = vols[-20:]
                        if recent_20_vols:
                            adv = sum(recent_20_vols) / float(len(recent_20_vols))
                            if adv > 0:
                                self.adv20_cache[c.symbol] = adv
                                return
                    logger.warning("ADV20 unavailable for ticker %s - disqualifying.", c.symbol)
                except Exception as e:
                    logger.warning("Failed to fetch ADV20 for %s: %s - disqualifying.", c.symbol, e)

        await asyncio.gather(*[_fetch_one(c) for c in contracts])
        logger.info("ADV20 calculation complete. Successfully cached ADV20 for %d tickers.", len(self.adv20_cache))

    async def scan(self, force: bool = False) -> list[dict[str, Any]]:
        """Executes a complete scan over all universe contracts following spec rules."""
        if self.is_scanning:
            logger.info("Scan already in progress. Skipping.")
            return self.last_scan_results

        self.is_scanning = True
        scan_log_buffer.clear()  # Clear log buffer so warnings & errors reflect only the current scan run
        now_pt = datetime.now(PT_TZ)
        start_dt = datetime.now(ET_TZ)
        self.last_scan_start_time = start_dt

        try:
            session_name, is_active, should_auto_scan = self.get_market_status(now_pt)
            if not is_active and not force:
                logger.info("Current market status: %s (Outside active scan hours). Skipping auto-scan.", session_name)
                return self.last_scan_results

            if not self.ib_manager.is_connected():
                connected = await self.ib_manager.connect()
                if not connected:
                    logger.error("Cannot execute scan: IBKR disconnected.")
                    return self.last_scan_results

            contracts = await self.ib_manager.load_or_qualify_contracts()
            if not contracts:
                logger.warning("No contracts loaded for scanning.")
                return []

            today_date_str = now_pt.strftime("%Y-%m-%d")
            adv_session_key = f"{today_date_str}_{session_name}"

            # 1. Initialize ADV20 cache on the first scan of Premarket / Postmarket session
            if self.last_adv20_session_key != adv_session_key or not self.adv20_cache:
                await self._fetch_adv20_batch(contracts)
                self.last_adv20_session_key = adv_session_key

            is_postmarket = (session_name == "Postmarket") or (session_name != "Premarket" and now_pt.hour >= 13)

            # 2. Snapshot market data for all universe tickers in bulk using IBKR reqTickers
            logger.info("Fetching live market data snapshots for %d contracts...", len(contracts))
            ib = self.ib_manager.ib

            chunk_size = 50
            chunks = [contracts[i : i + chunk_size] for i in range(0, len(contracts), chunk_size)]

            async def fetch_chunk(c_list):
                try:
                    return await asyncio.wait_for(
                        ib.reqTickersAsync(*c_list), timeout=self.config.ib.req_tickers_timeout_sec
                    )
                except asyncio.TimeoutError:
                    symbols = [c.symbol for c in c_list]
                    logger.warning(
                        "reqTickersAsync timed out after %.1fs for batch of %d symbols: %s. Falling back to local ticker snapshots.",
                        self.config.ib.req_tickers_timeout_sec,
                        len(c_list),
                        symbols,
                    )
                    return [ib.ticker(c) for c in c_list]
                except Exception as e:
                    symbols = [c.symbol for c in c_list]
                    logger.warning(
                        "reqTickersAsync failed with error for batch of %d symbols (%s): %s",
                        len(c_list),
                        symbols,
                        e,
                    )
                    return []

            chunk_results = await asyncio.gather(*[fetch_chunk(c) for c in chunks])
            all_tickers = [t for sublist in chunk_results for t in sublist if t]

            logger.info("Received market data snapshots for %d tickers. Evaluating mover criteria...", len(all_tickers))

            # Pass 1: Filter candidates meeting |% Change| >= 2.0%
            candidates: list[dict[str, Any]] = []

            for t in all_tickers:
                symbol = t.contract.symbol.upper()

                adv20 = self.adv20_cache.get(symbol)
                if not adv20 or adv20 <= 0:
                    continue

                price = 0.0
                if t.last and not math.isnan(t.last) and t.last > 0:
                    price = t.last
                elif t.marketPrice() and not math.isnan(t.marketPrice()) and t.marketPrice() > 0:
                    price = t.marketPrice()
                elif t.bid and not math.isnan(t.bid) and t.ask and not math.isnan(t.ask) and t.bid > 0 and t.ask > 0:
                    price = (t.bid + t.ask) / 2.0
                elif t.close and not math.isnan(t.close) and t.close > 0:
                    price = t.close

                base_close = t.close if (t.close and not math.isnan(t.close) and t.close > 0) else None

                if price <= 0 or not base_close or base_close <= 0:
                    logger.warning(
                        "Price or close is 0 or not found for ticker %s (t.last=%s, t.close=%s) - skipping.",
                        symbol,
                        t.last,
                        t.close,
                    )
                    continue

                price_change = price - base_close
                price_change_pct = (price_change / base_close) * 100.0
                abs_change_pct = abs(price_change_pct)

                if abs_change_pct >= self.config.scan.min_abs_price_change_pct:
                    candidates.append({
                        "symbol": symbol,
                        "contract": t.contract,
                        "price": price,
                        "base_close": base_close,
                        "price_change": price_change,
                        "price_change_pct": price_change_pct,
                        "abs_change_pct": abs_change_pct,
                        "adv20": adv20,
                        "snap_volume": t.volume if (t.volume and not math.isnan(t.volume) and t.volume >= 0) else 0.0,
                    })

            # Pass 2: Volume calculation
            # In premarket: use t.volume directly (snap_volume) - no historical API call needed.
            # In postmarket: query 1-hour bars (1 D) with useRTH=False for candidate movers and aggregate bars >= 16:00 ET.
            if candidates:
                if is_postmarket:
                    logger.info("Fetching 1-hour postmarket volume bars for %d candidate movers...", len(candidates))

                    async def fetch_candidate_postmarket_volume(c: dict[str, Any]) -> float:
                        try:
                            bars = await ib.reqHistoricalDataAsync(
                                c["contract"],
                                endDateTime="",
                                durationStr="1 D",
                                barSizeSetting="1 hour",
                                whatToShow="TRADES",
                                useRTH=False,
                                formatDate=1,
                            )
                            if bars:
                                pm_vol = sum(
                                    b.volume
                                    for b in bars
                                    if b.volume is not None and b.volume >= 0 and self._is_postmarket_bar(b.date)
                                )
                                return float(pm_vol)
                        except Exception as e:
                            logger.warning("Failed to fetch postmarket 1-hour bars for %s: %s", c["symbol"], e)
                        return c["snap_volume"]

                    pm_volumes = await asyncio.gather(*[fetch_candidate_postmarket_volume(c) for c in candidates])
                    for c, pm_vol in zip(candidates, pm_volumes):
                        c["session_volume"] = pm_vol
                else:
                    for c in candidates:
                        c["session_volume"] = c["snap_volume"]

            # Filter candidates by Rel Vol % >= min_rel_volume_pct
            matches: list[dict[str, Any]] = []
            for c in candidates:
                rel_volume_pct = (c["session_volume"] / c["adv20"]) * 100.0
                if rel_volume_pct >= self.config.scan.min_rel_volume_pct:
                    matches.append({
                        "symbol": c["symbol"],
                        "price": round(c["price"], 2),
                        "prev_close": round(c["base_close"], 2),
                        "price_change": round(c["price_change"], 2),
                        "price_change_pct": round(c["price_change_pct"], 2),
                        "abs_change_pct": round(c["abs_change_pct"], 2),
                        "volume": int(c["session_volume"]),
                        "adv": int(c["adv20"]),
                        "rel_volume_pct": round(rel_volume_pct, 2),
                        "briefing_news": None,
                    })

            # Sort default by abs % change descending
            matches.sort(key=lambda x: x["abs_change_pct"], reverse=True)

            logger.info("Found %d movers matching criteria. Checking Briefing.com email news...", len(matches))

            # Enrich matching tickers with Briefing.com emails from Gmail Inbox
            if matches:
                mover_symbols = [m["symbol"] for m in matches]
                news_map = await self.briefing_client.get_news_for_symbols_batch(mover_symbols)
                for m in matches:
                    m["briefing_news"] = news_map.get(m["symbol"])

            end_dt = datetime.now(ET_TZ)
            self.last_scan_end_time = end_dt
            self.last_scan_duration_sec = round((end_dt - start_dt).total_seconds(), 1)
            self.last_scan_results = matches
            logger.info("Scan completed in %.1f seconds. Matches: %d", self.last_scan_duration_sec, len(matches))
            return matches

        except Exception as e:
            logger.error("Error executing scan: %s", e, exc_info=True)
            return self.last_scan_results
        finally:
            self.is_scanning = False
