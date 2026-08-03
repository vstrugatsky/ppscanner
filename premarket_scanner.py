import logging
import math
import asyncio
from datetime import datetime, date, time, timedelta
from typing import Any, Optional

from ib_async import Stock, Contract

from config import Config, PT_TZ, ET_TZ
from ib_client import IBClientManager
from briefing_news import BriefingNewsClient
from cache_manager import CacheManager

logger = logging.getLogger(__name__)


class ScanLogBufferHandler(logging.Handler):
    """Custom logging handler to record WARNING and ERROR logs for the Dashboard W/E card."""

    def __init__(self):
        super().__init__()
        self.logs: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord):
        if record.levelno >= logging.WARNING:
            log_time = datetime.now(PT_TZ).strftime("%I:%M:%S %p PT")
            self.logs.append({
                "timestamp": log_time,
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
            })

    def clear(self):
        self.logs.clear()

    def get_logs(self) -> list[dict[str, Any]]:
        return list(self.logs)


# Global log buffer for UI W/E card
scan_log_buffer = ScanLogBufferHandler()
scan_log_buffer.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(scan_log_buffer)


def get_prev_market_day(d: date) -> date:
    """Helper to return the previous weekday market date."""
    cur = d - timedelta(days=1)
    while cur.weekday() >= 5:  # Skip Sat (5) and Sun (6)
        cur -= timedelta(days=1)
    return cur


class PremarketScanner:
    """
    Core Scanner implementing the exact specification matrix for
    Premarket Live, Premarket Historical, Postmarket Live, and Postmarket Historical scans.
    """

    def __init__(
        self,
        config: Config,
        ib_manager: IBClientManager,
        briefing_client: Optional[BriefingNewsClient] = None,
    ):
        self.config = config
        self.ib_manager = ib_manager
        self.briefing_client = briefing_client
        self.cache_manager = CacheManager(config.scan_cache_file)

        # Scanning State & Metrics
        self.is_scanning = False
        self.is_auto_scan_enabled = False
        self.last_scan_results: dict[str, list[dict[str, Any]]] = {
            "premarket": [],
            "postmarket": [],
        }
        self.last_scan_start_time: Optional[datetime] = None
        self.last_scan_end_time: Optional[datetime] = None
        self.first_scan_duration_sec: Optional[float] = None
        self.last_scan_duration_sec: Optional[float] = None

        # Diagnostics & Last Scan Summary per session
        self.last_scan_summary: dict[str, dict[str, Any]] = {
            "premarket": {
                "missing_price": {"count": 0, "list": []},
                "missing_close": {"count": 0, "list": []},
                "missing_volume": {"count": 0, "list": []},
                "missing_adv20": {"count": 0, "list": []},
            },
            "postmarket": {
                "missing_price": {"count": 0, "list": []},
                "missing_close": {"count": 0, "list": []},
                "missing_volume": {"count": 0, "list": []},
                "missing_adv20": {"count": 0, "list": []},
            },
        }

        # Test Scan Result Isolation State
        self.test_scan_results: dict[str, list[dict[str, Any]]] = {
            "premarket": [],
            "postmarket": [],
        }
        self.is_test_view_active: dict[str, bool] = {
            "premarket": False,
            "postmarket": False,
        }

        # Baseline End Times per session
        self.baseline_end_time: dict[str, Optional[datetime]] = {
            "premarket": None,
            "postmarket": None,
        }

    def get_results_for_session(self, session_type: str = "premarket") -> list[dict[str, Any]]:
        sess = session_type if session_type in ("premarket", "postmarket") else "premarket"
        if self.is_test_view_active.get(sess, False):
            return self.test_scan_results.get(sess, [])
        return self.last_scan_results.get(sess, [])

    def restore_full_results(self, session_type: str = "premarket"):
        sess = session_type if session_type in ("premarket", "postmarket") else "premarket"
        self.is_test_view_active[sess] = False

    def get_summary_for_session(self, session_type: str = "premarket") -> dict[str, Any]:
        sess = session_type if session_type in ("premarket", "postmarket") else "premarket"
        raw_summary = self.last_scan_summary.get(
            sess,
            {
                "missing_price": {"count": 0, "list": []},
                "missing_close": {"count": 0, "list": []},
                "missing_volume": {"count": 0, "list": []},
                "missing_adv20": {"count": 0, "list": []},
            },
        )
        res = {}
        for k, v in raw_summary.items():
            lst = v.get("list", [])
            res[k] = {"count": len(lst), "list": lst}
        return res

    def get_market_status(self, now_pt: Optional[datetime] = None) -> tuple[str, bool, str]:
        """
        Determines current market status string, live status, and active session type.
        Returns: (status_display_str, is_active_live_session, session_type)
        """
        if now_pt is None:
            now_pt = datetime.now(PT_TZ)

        weekday = now_pt.weekday()
        t = now_pt.time()

        if weekday < 5:  # Monday - Friday
            if time(1, 0) <= t < time(6, 29):
                return "Premarket Session", True, "premarket"
            elif time(6, 30) <= t < time(13, 0):
                return "Regular Trading Hours", False, "rth"
            elif time(13, 0) <= t < time(16, 59):
                return "Postmarket Session", True, "postmarket"
            else:
                return "Market Closed", False, "closed"
        else:
            return "Market Closed", False, "closed"

    def resolve_target_session(self, selected_session: str = "auto", now_pt: Optional[datetime] = None):
        """
        Resolves whether scan is Live or Historical, the target session date D, and the previous close date.
        - selected_session: 'auto' | 'premarket' | 'postmarket'
        Returns: (session_type, is_live, target_date, prev_close_date)
        """
        if now_pt is None:
            now_pt = datetime.now(PT_TZ)

        status_str, is_live_now, active_session = self.get_market_status(now_pt)
        dt_date = now_pt.date()
        weekday = now_pt.weekday()
        t = now_pt.time()

        if selected_session == "auto":
            if active_session in ("premarket", "postmarket") and is_live_now:
                session_type = active_session
            else:
                # Default off-hours scan to last premarket if before 13:00 PT, else last postmarket
                session_type = "premarket" if t < time(13, 0) else "postmarket"
        else:
            session_type = selected_session

        # Check if requested session is live right now
        is_live = (weekday < 5) and (
            (session_type == "premarket" and time(1, 0) <= t < time(6, 30))
            or (session_type == "postmarket" and time(13, 0) <= t < time(17, 0))
        )

        # Resolve target session date D and previous close date
        if session_type == "premarket":
            if is_live:
                target_date = dt_date
            elif weekday < 5 and t >= time(6, 30):
                target_date = dt_date
            else:
                target_date = get_prev_market_day(dt_date)
            prev_close_date = get_prev_market_day(target_date)

        else:  # postmarket
            if is_live:
                target_date = dt_date
                prev_close_date = dt_date  # 16:00 ET RTH close of today
            elif weekday < 5 and t >= time(17, 0):
                target_date = dt_date
                prev_close_date = dt_date  # 16:00 ET RTH close of today
            else:
                target_date = get_prev_market_day(dt_date)
                prev_close_date = target_date  # 16:00 ET RTH close of target date

        return session_type, is_live, target_date, prev_close_date

    async def scan(
        self,
        selected_session: str = "auto",
        custom_tickers: Optional[list[str]] = None,
        is_scheduled: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Executes a scan according to the specification matrix.
        """
        if self.is_scanning:
            logger.info("Scan already in progress. Skipping concurrent scan call.")
            return self.last_scan_results

        self.is_scanning = True
        scan_log_buffer.clear()
        start_dt = datetime.now(PT_TZ)
        self.last_scan_start_time = start_dt

        try:
            session_type, is_live, target_date, prev_close_date = self.resolve_target_session(
                selected_session=selected_session, now_pt=start_dt
            )

            # Reset diagnostic summary lists for current session_type
            self.last_scan_summary[session_type] = {
                "missing_price": {"count": 0, "list": []},
                "missing_close": {"count": 0, "list": []},
                "missing_volume": {"count": 0, "list": []},
                "missing_adv20": {"count": 0, "list": []},
            }
            current_summary = self.last_scan_summary[session_type]
            mode_name = f"{session_type.capitalize()} {'Live' if is_live else 'Historical'}"
            logger.info(
                "Starting %s scan (Target Date: %s, Prev Close Date: %s)",
                mode_name,
                target_date,
                prev_close_date,
            )

            if not self.ib_manager.is_connected():
                connected = await self.ib_manager.connect()
                if not connected:
                    logger.error("Cannot execute scan: IBKR disconnected.")
                    return self.get_results_for_session(selected_session)

            # Load contracts
            if custom_tickers:
                clean_custom = [t.strip().upper().replace(".", " ") for t in custom_tickers if t.strip()]
                all_contracts = [c for c in await self.ib_manager.load_or_qualify_contracts() if c.symbol in clean_custom]
            else:
                all_contracts = await self.ib_manager.load_or_qualify_contracts()

            is_test_scan = custom_tickers is not None and len(custom_tickers) > 0

            if not all_contracts:
                logger.warning("No contracts available for scanning.")
                return []

            ib = self.ib_manager.ib
            hist_sem = asyncio.Semaphore(self.config.ib.hist_concurrency_limit)
            target_date_str = target_date.strftime("%Y%m%d")
            prev_close_date_str = prev_close_date.strftime("%Y%m%d")

            # Helper for clean contract in historical requests
            def _clean_contract(c: Contract) -> Contract:
                return Stock(c.symbol, "SMART", "USD", conId=c.conId)

            # Data collections
            prices: dict[str, float] = {}
            prev_closes: dict[str, float] = {}
            volumes: dict[str, float] = {}
            adv20s: dict[str, float] = {}

            # =========================================================================
            # STEP 1: PREVIOUS CLOSE & ADV20 FETCHING (Cached per date)
            # =========================================================================
            async def _get_prev_close_and_adv20(contract):
                sym = contract.symbol
                hist_c = _clean_contract(contract)

                pc = self.cache_manager.get_prev_close(session_type, prev_close_date_str, sym)
                adv = self.cache_manager.get_adv20(session_type, prev_close_date_str, sym)

                if (pc is None or pc <= 0) or (adv is None or adv <= 0):
                    try:
                        async with hist_sem:
                            end_time_str = f"{prev_close_date_str} 16:00:00 US/Eastern" if session_type == "postmarket" else f"{target_date_str} 00:00:00 US/Eastern"
                            bars = await asyncio.wait_for(
                                ib.reqHistoricalDataAsync(
                                    hist_c,
                                    endDateTime=end_time_str,
                                    durationStr="25 D",
                                    barSizeSetting="1 day",
                                    whatToShow="TRADES",
                                    useRTH=True,
                                    formatDate=1,
                                ),
                                timeout=self.config.ib.hist_rth_timeout_sec,
                            )
                            if bars and len(bars) > 0:
                                if (pc is None or pc <= 0) and bars[-1].close and bars[-1].close > 0:
                                    pc = float(bars[-1].close)
                                    if not is_test_scan:
                                        self.cache_manager.set_prev_close(session_type, prev_close_date_str, sym, pc)

                                if adv is None or adv <= 0:
                                    vols = [b.volume for b in bars if b.volume is not None and b.volume >= 0]
                                    if len(vols) < 20:
                                        logger.warning(
                                            "Fewer than 20 daily volume bars (%d) returned for %s ADV20 calculation",
                                            len(vols),
                                            sym,
                                        )
                                    vols20 = vols[-20:] if len(vols) >= 20 else vols
                                    if vols20:
                                        adv = sum(vols20) / float(len(vols20))
                                        if adv > 0 and not is_test_scan:
                                            self.cache_manager.set_adv20(session_type, prev_close_date_str, sym, adv)
                    except asyncio.TimeoutError:
                        logger.warning("⏱️ Timeout (%.1fs) fetching daily bars for %s", self.config.ib.hist_rth_timeout_sec, sym)
                    except Exception as e:
                        logger.warning("Error fetching daily bars for %s: %s", sym, e)

                if pc and pc > 0:
                    prev_closes[sym] = pc
                else:
                    current_summary["missing_close"]["list"].append(sym)

                if adv and adv > 0:
                    adv20s[sym] = adv
                else:
                    current_summary["missing_adv20"]["list"].append(sym)

            # =========================================================================
            # STEP 2: PRICE & VOLUME FETCHING ACCORDING TO SPEC MATRIX
            # =========================================================================
            if is_live:
                # Live Scan: Bulk snapshot using reqTickersAsync
                logger.info("Fetching real-time market data snapshots for %d contracts...", len(all_contracts))
                chunk_size = 50
                chunks = [all_contracts[i : i + chunk_size] for i in range(0, len(all_contracts), chunk_size)]

                async def fetch_chunk(c_list):
                    try:
                        return await asyncio.wait_for(
                            ib.reqTickersAsync(*c_list), timeout=self.config.ib.req_tickers_timeout_sec
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            "⏱️ Timeout (%.1fs) requesting live market data snapshot chunk of %d contracts",
                            self.config.ib.req_tickers_timeout_sec,
                            len(c_list),
                        )
                        return [ib.ticker(c) for c in c_list]
                    except Exception as e:
                        logger.warning("Error requesting live market data snapshot chunk: %s", e)
                        return [ib.ticker(c) for c in c_list]

                chunk_results = await asyncio.gather(*[fetch_chunk(c) for c in chunks])
                all_tickers = [t for sublist in chunk_results for t in sublist if t]

                for t in all_tickers:
                    sym = t.contract.symbol.upper()
                    # Live Price: t.marketPrice()
                    price = 0.0
                    try:
                        mp = t.marketPrice()
                        if mp and not math.isnan(mp) and mp > 0:
                            price = float(mp)
                        elif t.last and not math.isnan(t.last) and t.last > 0:
                            price = float(t.last)
                    except Exception:
                        pass

                    if price > 0:
                        prices[sym] = price
                    else:
                        logger.warning("Price is 0 or NaN for ticker %s (t.last=%s, t.marketPrice=%s)", sym, t.last, getattr(t, "marketPrice", None))
                        current_summary["missing_price"]["list"].append(sym)

                    # Live Volume: Premarket Live uses t.volume attribute; Postmarket Live uses 15-min bars sum
                    if session_type == "premarket":
                        vol = float(t.volume) if (t.volume and not math.isnan(t.volume) and t.volume >= 0) else 0.0
                        volumes[sym] = vol
                        if vol <= 0:
                            current_summary["missing_volume"]["list"].append(sym)

                # Fetch Previous Close & ADV20 for candidates with price > 0
                candidate_contracts = [c for c in all_contracts if c.symbol in prices]
                await asyncio.gather(*[_get_prev_close_and_adv20(c) for c in candidate_contracts])

                # Postmarket Live Volume: 15-min bars from 16:00 to 20:00 ET (14400 S)
                if session_type == "postmarket" and candidate_contracts:
                    if is_test_scan:
                        vol_contracts = candidate_contracts
                    else:
                        min_pct = self.config.scan.min_abs_price_change_pct
                        vol_contracts = [
                            c for c in candidate_contracts
                            if (
                                prices.get(c.symbol, 0.0) > 0
                                and prev_closes.get(c.symbol, 0.0) > 0
                                and (abs(prices[c.symbol] - prev_closes[c.symbol]) / prev_closes[c.symbol]) * 100.0 >= min_pct
                            )
                        ]

                    if vol_contracts:
                        logger.info(
                            "Fetching Postmarket Live 15-min volume bars for %d price-qualifying tickers (out of %d)...",
                            len(vol_contracts),
                            len(candidate_contracts),
                        )

                        async def _fetch_postmarket_live_vol(contract):
                            sym = contract.symbol
                            hist_c = _clean_contract(contract)
                            try:
                                async with hist_sem:
                                    end_time_str = f"{target_date_str} 20:00:00 US/Eastern"
                                    bars = await asyncio.wait_for(
                                        ib.reqHistoricalDataAsync(
                                            hist_c,
                                            endDateTime=end_time_str,
                                            durationStr="14400 S",
                                            barSizeSetting="15 mins",
                                            whatToShow="TRADES",
                                            useRTH=False,
                                            formatDate=1,
                                        ),
                                        timeout=self.config.ib.hist_eth_timeout_sec,
                                    )
                                    if bars:
                                        pm_vol = sum(
                                            b.volume for b in bars if b.volume is not None and b.volume >= 0 and b.date.hour >= 16
                                        )
                                        volumes[sym] = float(pm_vol)
                                        if pm_vol <= 0:
                                            current_summary["missing_volume"]["list"].append(sym)
                                        return
                            except asyncio.TimeoutError:
                                logger.warning("⏱️ Timeout (%.1fs) fetching postmarket live volume for %s", self.config.ib.hist_eth_timeout_sec, sym)
                            except Exception as e:
                                logger.error("Failed to fetch postmarket live volume for %s: %s", sym, e)
                            volumes[sym] = 0.0
                            current_summary["missing_volume"]["list"].append(sym)

                        await asyncio.gather(*[_fetch_postmarket_live_vol(c) for c in vol_contracts])

            else:
                # Off-Hours / Historical Scan (Premarket or Postmarket)
                logger.info("Running Off-Hours %s scan for date %s...", session_type.capitalize(), target_date_str)
                await asyncio.gather(*[_get_prev_close_and_adv20(c) for c in all_contracts])

                # Read session prices & volumes directly from persistent cache (no slow historical bar queries)
                candidate_contracts = [c for c in all_contracts if c.symbol in prev_closes]
                for c in candidate_contracts:
                    sym = c.symbol
                    hp = self.cache_manager.get_hist_price(session_type, target_date_str, sym)
                    hv = self.cache_manager.get_hist_vol(session_type, target_date_str, sym)

                    if hp and hp > 0:
                        prices[sym] = hp
                    else:
                        current_summary["missing_price"]["list"].append(sym)

                    if hv is not None and hv >= 0:
                        volumes[sym] = float(hv)
                        if hv == 0:
                            current_summary["missing_volume"]["list"].append(sym)
                    else:
                        volumes[sym] = 0.0
                        current_summary["missing_volume"]["list"].append(sym)

            # Save updated cache and mark session as warmed if it is a full universe scan
            if not is_test_scan:
                if is_live:
                    for sym, p in prices.items():
                        if p > 0:
                            self.cache_manager.set_hist_price(session_type, target_date_str, sym, p)
                    for sym, v in volumes.items():
                        if v >= 0:
                            self.cache_manager.set_hist_vol(session_type, target_date_str, sym, v)

                self.cache_manager.mark_warmed(
                    session_type=session_type,
                    target_date_str=target_date_str,
                    prev_close_date_str=prev_close_date_str,
                    count=len(all_contracts),
                    is_scheduled=is_scheduled,
                )

            # Update summary counts
            for key in current_summary:
                current_summary[key]["count"] = len(current_summary[key]["list"])

            # =========================================================================
            # STEP 3: EVALUATE MOVER CRITERIA
            # =========================================================================
            matches: list[dict[str, Any]] = []
            symbols_eval = (
                [c.symbol for c in all_contracts]
                if is_test_scan
                else set(prices.keys()).intersection(prev_closes.keys()).intersection(adv20s.keys())
            )

            for sym in symbols_eval:
                price = prices.get(sym, 0.0)
                base_close = prev_closes.get(sym, 0.0)
                vol = volumes.get(sym, 0.0)
                adv = adv20s.get(sym, 0.0)

                if not is_test_scan and (price <= 0 or base_close <= 0 or adv <= 0):
                    continue

                price_change = price - base_close if base_close > 0 else 0.0
                price_change_pct = (price_change / base_close) * 100.0 if base_close > 0 else 0.0
                abs_change_pct = abs(price_change_pct)
                rel_volume_pct = (vol / adv) * 100.0 if adv > 0 else 0.0

                meets_criteria = (
                    abs_change_pct >= self.config.scan.min_abs_price_change_pct
                    and rel_volume_pct >= self.config.scan.min_rel_volume_pct
                )

                if is_test_scan or meets_criteria:
                    matches.append({
                        "symbol": sym,
                        "price": round(price, 2),
                        "prev_close": round(base_close, 2),
                        "price_change": round(price_change, 2),
                        "price_change_pct": round(price_change_pct, 2),
                        "abs_change_pct": round(abs_change_pct, 2),
                        "volume": int(vol),
                        "adv": int(adv),
                        "rel_volume_pct": round(rel_volume_pct, 2),
                        "meets_criteria": meets_criteria,
                        "briefing_news": None,
                    })

            # Sort default by Percent Change descending (by magnitude)
            matches.sort(key=lambda x: x["abs_change_pct"], reverse=True)

            logger.info("Scan identified %d movers meeting criteria. Fetching Briefing.com news...", len(matches))

            # Fetch Briefing news for movers if Briefing client is available
            if matches and self.briefing_client:
                mover_symbols = [m["symbol"] for m in matches]
                news_map = await self.briefing_client.get_news_for_symbols_batch(
                    mover_symbols, session_type=session_type, session_date=target_date
                )
                for m in matches:
                    m["briefing_news"] = news_map.get(m["symbol"])

            end_dt = datetime.now(PT_TZ)
            duration_sec = round((end_dt - start_dt).total_seconds(), 1)
            self.last_scan_end_time = end_dt
            self.last_scan_duration_sec = duration_sec
            if self.first_scan_duration_sec is None:
                self.first_scan_duration_sec = duration_sec

            if is_scheduled and end_dt.time() < time(14, 0) and (end_dt.time().hour in (1, 13)):
                self.baseline_end_time[session_type] = end_dt

            if is_test_scan:
                self.test_scan_results[session_type] = matches
                self.is_test_view_active[session_type] = True
            else:
                self.last_scan_results[session_type] = matches
                self.is_test_view_active[session_type] = False

            logger.info("Scan completed in %.1f seconds. Total matches: %d", duration_sec, len(matches))
            return matches

        except Exception as e:
            logger.error("Unhandled error during scan execution: %s", e, exc_info=True)
            return self.get_results_for_session(selected_session)
        finally:
            self.is_scanning = False
