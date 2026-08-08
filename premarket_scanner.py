import asyncio
import json
import logging
import math
from datetime import date, datetime, time, timedelta
from typing import Any

from ib_async import Contract, Stock

from briefing_news import BriefingNewsClient
from cache_manager import CacheManager
from config import PT_TZ, Config
from ib_client import IBClientManager

logger = logging.getLogger("premarket_scanner")
logger.setLevel(logging.INFO)


class ScanLogBufferHandler(logging.Handler):
    """Custom logging handler to record WARNING and ERROR logs for the Dashboard W/E card."""

    def __init__(self):
        super().__init__()
        self.logs: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord):
        if record.levelno >= logging.INFO:
            log_time = datetime.now(PT_TZ).strftime("%I:%M:%S %p PT")
            self.logs.append(
                {
                    "timestamp": log_time,
                    "level": record.levelname,
                    "logger": record.name,
                    "message": self.format(record),
                }
            )

    def clear(self):
        self.logs.clear()

    def get_logs(self) -> list[dict[str, Any]]:
        return list(self.logs)


# Global log buffer for UI W/E card
scan_log_buffer = ScanLogBufferHandler()
scan_log_buffer.setFormatter(logging.Formatter("%(message)s"))
scan_log_buffer.setLevel(logging.INFO)
logger.addHandler(scan_log_buffer)


def get_prev_market_day(d: date) -> date:
    """Helper to return the previous weekday market date."""
    cur = d - timedelta(days=1)
    while cur.weekday() >= 5:  # Skip Sat (5) and Sun (6)
        cur -= timedelta(days=1)
    return cur


def get_previous_trading_day(ref_dt: datetime) -> date:
    d = ref_dt.date() - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d


async def _warmup_ibkr_connection(ib, contracts: list[Contract]):
    """Sends a quick 5-ticker snapshot probe to wake up TWS market data farm routing."""
    if not contracts or not ib.isConnected():
        return
    probe_contracts = contracts[: min(5, len(contracts))]
    try:
        logger.info(
            "⚡ Issuing TWS connection warmup probe for %d tickers...",
            len(probe_contracts),
        )
        await asyncio.wait_for(ib.reqTickersAsync(*probe_contracts), timeout=2.0)
        await asyncio.sleep(0.5)
    except Exception as e:
        logger.debug("Warmup probe exception (safe to ignore): %s", e)


class PremarketScanner:
    """
    Core scanning engine supporting:
    Premarket Live, Premarket Historical, Postmarket Live, and Postmarket Historical scans.
    """

    def __init__(
        self,
        config: Config,
        ib_manager: IBClientManager,
        briefing_client: BriefingNewsClient | None = None,
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
        self.last_scan_start_time: datetime | None = None
        self.last_scan_end_time: datetime | None = None
        self.first_scan_duration_sec: float | None = None
        self.last_scan_duration_sec: float | None = None

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

        # Durable Scan Results Persistence File & Store
        self.scan_results_file = config.scan_results_file
        self.session_store: dict[str, dict[str, Any]] = self._load_scan_results_store()

    def _load_scan_results_store(self) -> dict[str, dict[str, Any]]:
        default_store = {
            "premarket": {
                "target_date": None,
                "prev_close_date": None,
                "baseline_end_time_pt": None,
                "last_scan_end_time_pt": None,
                "last_scan_duration_sec": None,
                "prev_closes_count": 0,
                "adv20s_count": 0,
                "session_prices_count": 0,
                "session_volumes_count": 0,
                "matches": [],
                "last_scan_summary": {
                    "missing_price": {"count": 0, "list": []},
                    "missing_close": {"count": 0, "list": []},
                    "missing_volume": {"count": 0, "list": []},
                    "missing_adv20": {"count": 0, "list": []},
                },
                "logs": [],
            },
            "postmarket": {
                "target_date": None,
                "prev_close_date": None,
                "baseline_end_time_pt": None,
                "last_scan_end_time_pt": None,
                "last_scan_duration_sec": None,
                "prev_closes_count": 0,
                "adv20s_count": 0,
                "session_prices_count": 0,
                "session_volumes_count": 0,
                "matches": [],
                "last_scan_summary": {
                    "missing_price": {"count": 0, "list": []},
                    "missing_close": {"count": 0, "list": []},
                    "missing_volume": {"count": 0, "list": []},
                    "missing_adv20": {"count": 0, "list": []},
                },
                "logs": [],
            },
        }
        if self.scan_results_file.exists():
            try:
                with open(self.scan_results_file, encoding="utf-8") as f:
                    data = json.load(f)
                    for sess in ("premarket", "postmarket"):
                        if sess in data:
                            default_store[sess].update(data[sess])
            except Exception as e:
                logger.error("Failed to load scan_results.json: %s", e)
        return default_store

    def _save_scan_results_store(self):
        try:
            with open(self.scan_results_file, "w", encoding="utf-8") as f:
                json.dump(self.session_store, f, indent=2)
        except Exception as e:
            logger.error("Failed to save scan_results.json: %s", e)

    def restore_full_results(self, session_type: str = "premarket"):
        sess = (
            session_type if session_type in ("premarket", "postmarket") else "premarket"
        )
        self.is_test_view_active[sess] = False

    async def reload_baseline_cache(
        self, selected_session: str = "premarket"
    ) -> dict[str, Any]:
        """Reloads ONLY the Baseline data (Previous Close and ADV20) for all contracts.
        Does NOT fetch live prices or 15-min volume bars, and does NOT overwrite mover results in scan_results.json.
        """
        self.is_scanning = True
        scan_log_buffer.clear()
        sess = (
            selected_session
            if selected_session in ("premarket", "postmarket")
            else "premarket"
        )

        empty_summary = {
            "missing_price": {"count": 0, "list": []},
            "missing_close": {"count": 0, "list": []},
            "missing_volume": {"count": 0, "list": []},
            "missing_adv20": {"count": 0, "list": []},
        }
        self.last_scan_summary[sess] = empty_summary
        if sess in self.session_store:
            self.session_store[sess]["last_scan_summary"] = empty_summary
            self.session_store[sess]["logs"] = []

        try:
            now_pt = datetime.now(PT_TZ)
            target_date_str = now_pt.strftime("%Y%m%d")
            prev_close_date_str = get_previous_trading_day(now_pt).strftime("%Y%m%d")

            logger.info("Starting Baseline-only reload for %s...", sess.capitalize())
            self.cache_manager.clear_baseline_cache(sess)

            if not self.ib_manager.is_connected():
                connected = await self.ib_manager.connect()
                if not connected:
                    logger.error("Cannot reload baseline cache: IBKR disconnected.")
                    return {"status": "error", "message": "IBKR connection failed"}

            all_contracts = await self.ib_manager.load_or_qualify_contracts()
            hist_sem = asyncio.Semaphore(5)
            ib = self.ib_manager.ib

            if len(all_contracts) > 20:
                await _warmup_ibkr_connection(ib, all_contracts)

            prev_closes: dict[str, float] = {}
            adv20s: dict[str, float] = {}

            def _clean_contract(c: Contract) -> Contract:
                return Stock(c.symbol, "SMART", "USD", conId=c.conId)

            async def _get_prev_close_and_adv20(contract):
                sym = contract.symbol
                end_time_str = (
                    f"{prev_close_date_str} 16:00:00 US/Eastern"
                    if sess == "postmarket"
                    else f"{target_date_str} 00:00:00 US/Eastern"
                )
                hist_c = _clean_contract(contract)
                for attempt in range(1, 4):
                    try:
                        async with hist_sem:
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
                                if bars[-1].close and bars[-1].close > 0:
                                    pc = float(bars[-1].close)
                                    self.cache_manager.set_prev_close(
                                        sess, prev_close_date_str, sym, pc
                                    )
                                    prev_closes[sym] = pc

                                vols = [
                                    b.volume
                                    for b in bars
                                    if b.volume is not None and b.volume >= 0
                                ]
                                vols20 = vols[-20:] if len(vols) >= 20 else vols
                                if vols20:
                                    adv = sum(vols20) / float(len(vols20))
                                    if adv > 0:
                                        self.cache_manager.set_adv20(
                                            sess, prev_close_date_str, sym, adv
                                        )
                                        adv20s[sym] = adv
                                break
                    except TimeoutError:
                        logger.warning(
                            "⏱️ Timeout fetching baseline bars for %s (Attempt %d/3)",
                            sym,
                            attempt,
                        )
                    except Exception as e:
                        logger.warning(
                            "Error fetching baseline bars for %s (Attempt %d/3): %s",
                            sym,
                            attempt,
                            e,
                        )
                    if attempt < 3:
                        sleep_sec = min(2.0 + (attempt - 1) * 1.0, 5.0)
                        await asyncio.sleep(sleep_sec)

            await asyncio.gather(*[_get_prev_close_and_adv20(c) for c in all_contracts])

            end_dt = datetime.now(PT_TZ)
            b_time = end_dt.strftime("%I:%M:%S %p PT")

            self.cache_manager.mark_warmed(
                session_type=sess,
                target_date_str=target_date_str,
                prev_close_date_str=prev_close_date_str,
                count=len(all_contracts),
                is_scheduled=False,
            )

            # Update baseline metrics in session store
            cs = self.cache_manager.cache.get(sess, {})
            pc_cnt = len(cs.get("prev_closes", {}))
            adv_cnt = len(cs.get("adv20s", {}))

            self.session_store[sess]["baseline_end_time_pt"] = b_time
            self.session_store[sess]["prev_closes_count"] = pc_cnt
            self.session_store[sess]["adv20s_count"] = adv_cnt
            self.session_store[sess]["logs"] = scan_log_buffer.get_logs()
            self._save_scan_results_store()

            logger.info(
                "Baseline reload complete for %s. PC: %d | ADV: %d",
                sess,
                pc_cnt,
                adv_cnt,
            )
            return {
                "status": "success",
                "message": f"Baseline reload complete for {sess.capitalize()}",
                "baseline_end_time_pt": b_time,
                "prev_closes_count": pc_cnt,
                "adv20s_count": adv_cnt,
            }
        finally:
            self.is_scanning = False

    def check_and_reset_obsolete_session_data(
        self, session_type: str = "premarket"
    ) -> bool:
        """
        Checks if the stored session data belongs to a previous market session.
        If the current target_date is newer than stored target_date, clears obsolete results & metrics.
        Returns True if a reset occurred.
        """
        sess = (
            session_type if session_type in ("premarket", "postmarket") else "premarket"
        )
        if self.is_scanning:
            return False

        start_dt = datetime.now(PT_TZ)
        _, _, target_date, _ = self.resolve_target_session(
            selected_session=sess, now_pt=start_dt
        )
        curr_target_date_str = target_date.strftime("%Y%m%d")

        stored_data = self.session_store.get(sess, {})
        stored_target_date_str = stored_data.get("target_date")

        # If stored_target_date exists and is older than current target date, perform rollover reset
        if stored_target_date_str and stored_target_date_str < curr_target_date_str:
            logger.info(
                "New %s session detected (Current: %s vs Stored: %s). Clearing obsolete session results.",
                sess.capitalize(),
                curr_target_date_str,
                stored_target_date_str,
            )
            self.session_store[sess] = {
                "target_date": curr_target_date_str,
                "prev_close_date": None,
                "baseline_end_time_pt": None,
                "last_scan_end_time_pt": None,
                "last_scan_duration_sec": None,
                "prev_closes_count": 0,
                "adv20s_count": 0,
                "session_prices_count": 0,
                "session_volumes_count": 0,
                "matches": [],
                "last_scan_summary": {
                    "missing_price": {"count": 0, "list": []},
                    "missing_close": {"count": 0, "list": []},
                    "missing_volume": {"count": 0, "list": []},
                    "missing_adv20": {"count": 0, "list": []},
                },
                "logs": [],
            }
            self.last_scan_summary[sess] = {
                "missing_price": {"count": 0, "list": []},
                "missing_close": {"count": 0, "list": []},
                "missing_volume": {"count": 0, "list": []},
                "missing_adv20": {"count": 0, "list": []},
            }
            self.last_scan_results[sess] = []
            scan_log_buffer.clear()
            self._save_scan_results_store()
            return True

        return False

    def get_session_data(self, session_type: str = "premarket") -> dict[str, Any]:
        sess = (
            session_type if session_type in ("premarket", "postmarket") else "premarket"
        )
        self.check_and_reset_obsolete_session_data(sess)
        data = dict(self.session_store.get(sess, {}))

        # Check warmth status and counts dynamically from cache_manager
        cs = self.cache_manager.cache.get(sess, {})
        is_warmed = cs.get("is_warmed", False)

        data["prev_closes_count"] = len(cs.get("prev_closes", {}))
        data["adv20s_count"] = len(cs.get("adv20s", {}))
        data["session_prices_count"] = data.get("session_prices_count", 0)
        data["session_volumes_count"] = data.get("session_volumes_count", 0)

        if is_warmed:
            if data.get("baseline_end_time_pt") is None:
                data["baseline_end_time_pt"] = cs.get("last_warmed_pt")
        else:
            data["baseline_end_time_pt"] = None

        # Override matches if Test View is active
        if self.is_test_view_active.get(sess, False):
            data["matches"] = self.test_scan_results.get(sess, [])
            data["is_test_view_active"] = True
        else:
            data["is_test_view_active"] = False

        # Expose live logs and diagnostics if a scan is currently active
        all_logs = (
            scan_log_buffer.get_logs() if self.is_scanning else data.get("logs", [])
        )
        data["info_logs"] = [log for log in all_logs if log.get("level") == "INFO"]
        data["we_logs"] = [
            log for log in all_logs if log.get("level") in ("WARNING", "ERROR")
        ]
        data["errors_count"] = len(
            [log for log in data["we_logs"] if log.get("level") == "ERROR"]
        )
        data["warnings_count"] = len(
            [log for log in data["we_logs"] if log.get("level") == "WARNING"]
        )
        data["logs"] = all_logs

        if self.is_scanning:
            raw_summary = self.last_scan_summary.get(
                sess,
                {
                    "missing_price": {"count": 0, "list": []},
                    "missing_close": {"count": 0, "list": []},
                    "missing_volume": {"count": 0, "list": []},
                    "missing_adv20": {"count": 0, "list": []},
                },
            )
        else:
            raw_summary = data.get(
                "last_scan_summary",
                {
                    "missing_price": {"count": 0, "list": []},
                    "missing_close": {"count": 0, "list": []},
                    "missing_volume": {"count": 0, "list": []},
                    "missing_adv20": {"count": 0, "list": []},
                },
            )

        processed_summary = {}
        for k, v in raw_summary.items():
            lst = v.get("list", []) if isinstance(v, dict) else []
            processed_summary[k] = {"count": len(lst), "list": lst}
        data["last_scan_summary"] = processed_summary

        return data

    def get_market_status(
        self, now_pt: datetime | None = None
    ) -> tuple[str, bool, str]:
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

    def resolve_target_session(
        self, selected_session: str = "auto", now_pt: datetime | None = None
    ):
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
        custom_tickers: list[str] | None = None,
        is_scheduled: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Executes a scan according to the specification matrix.
        """
        if self.is_scanning:
            logger.info("Scan already in progress. Skipping concurrent scan call.")
            return self.last_scan_results

        self.is_scanning = True
        is_test_scan = custom_tickers is not None and len(custom_tickers) > 0
        scan_log_buffer.clear()
        start_dt = datetime.now(PT_TZ)
        self.last_scan_start_time = start_dt

        try:
            session_type, is_live, target_date, prev_close_date = (
                self.resolve_target_session(
                    selected_session=selected_session, now_pt=start_dt
                )
            )

            # Reset diagnostic summary lists for current session_type
            self.last_scan_summary[session_type] = {
                "missing_price": {"count": 0, "list": []},
                "missing_close": {"count": 0, "list": []},
                "missing_volume": {"count": 0, "list": []},
                "missing_adv20": {"count": 0, "list": []},
            }
            current_summary = self.last_scan_summary[session_type]
            mode_name = (
                f"{session_type.capitalize()} {'Live' if is_live else 'Historical'}"
            )
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
                clean_custom = [
                    t.strip().upper().replace(".", " ")
                    for t in custom_tickers
                    if t.strip()
                ]
                all_contracts = [
                    c
                    for c in await self.ib_manager.load_or_qualify_contracts()
                    if c.symbol in clean_custom
                ]
            else:
                all_contracts = await self.ib_manager.load_or_qualify_contracts()

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

            # Warmup connection for large contract universes
            if not is_test_scan and len(all_contracts) > 20:
                await _warmup_ibkr_connection(ib, all_contracts)

            async def _req_historical_data_with_retry(
                contract: Contract,
                endDateTime: str,
                durationStr: str,
                barSizeSetting: str,
                whatToShow: str,
                useRTH: bool,
                timeout_sec: float,
                max_attempts: int = 3,
            ):
                hist_c = _clean_contract(contract)
                for attempt in range(1, max_attempts + 1):
                    try:
                        async with hist_sem:
                            bars = await asyncio.wait_for(
                                ib.reqHistoricalDataAsync(
                                    hist_c,
                                    endDateTime=endDateTime,
                                    durationStr=durationStr,
                                    barSizeSetting=barSizeSetting,
                                    whatToShow=whatToShow,
                                    useRTH=useRTH,
                                    formatDate=1,
                                ),
                                timeout=timeout_sec,
                            )
                            if bars and len(bars) > 0:
                                return bars
                    except TimeoutError:
                        logger.warning(
                            "⏱️ Timeout (%.1fs) fetching historical bars for %s (Attempt %d/%d)",
                            timeout_sec,
                            contract.symbol,
                            attempt,
                            max_attempts,
                        )
                    except Exception as e:
                        logger.warning(
                            "Error fetching historical bars for %s (Attempt %d/%d): %s",
                            contract.symbol,
                            attempt,
                            max_attempts,
                            e,
                        )

                    if attempt < max_attempts:
                        sleep_sec = min(2.0 + (attempt - 1) * 1.0, 5.0)
                        await asyncio.sleep(sleep_sec)

                return []

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

                pc = self.cache_manager.get_prev_close(
                    session_type, prev_close_date_str, sym
                )
                adv = self.cache_manager.get_adv20(
                    session_type, prev_close_date_str, sym
                )

                if (pc is None or pc <= 0) or (adv is None or adv <= 0):
                    end_time_str = (
                        f"{prev_close_date_str} 16:00:00 US/Eastern"
                        if session_type == "postmarket"
                        else f"{target_date_str} 00:00:00 US/Eastern"
                    )
                    bars = await _req_historical_data_with_retry(
                        contract,
                        endDateTime=end_time_str,
                        durationStr="25 D",
                        barSizeSetting="1 day",
                        whatToShow="TRADES",
                        useRTH=True,
                        timeout_sec=self.config.ib.hist_rth_timeout_sec,
                        max_attempts=3,
                    )
                    if bars:
                        if (
                            (pc is None or pc <= 0)
                            and bars[-1].close
                            and bars[-1].close > 0
                        ):
                            pc = float(bars[-1].close)
                            if not is_test_scan:
                                self.cache_manager.set_prev_close(
                                    session_type, prev_close_date_str, sym, pc
                                )

                        if adv is None or adv <= 0:
                            vols = [
                                b.volume
                                for b in bars
                                if b.volume is not None and b.volume >= 0
                            ]
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
                                    self.cache_manager.set_adv20(
                                        session_type,
                                        prev_close_date_str,
                                        sym,
                                        adv,
                                    )

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
                # Live Scan: Dynamic progress-driven retry loop
                max_passes = 8 if (is_scheduled or not is_live) else 4
                remaining_contracts = list(all_contracts)
                prev_price_count = 0

                for pass_num in range(1, max_passes + 1):
                    chunk_size = 50 if pass_num == 1 else 20
                    chunks = [
                        remaining_contracts[i : i + chunk_size]
                        for i in range(0, len(remaining_contracts), chunk_size)
                    ]
                    logger.info(
                        "Live Snapshot Pass %d/%d: Requesting market data for %d contracts (%d chunks, size %d)...",
                        pass_num,
                        max_passes,
                        len(remaining_contracts),
                        len(chunks),
                        chunk_size,
                    )

                    async def fetch_chunk(c_list):
                        try:
                            return await asyncio.wait_for(
                                ib.reqTickersAsync(*c_list),
                                timeout=self.config.ib.req_tickers_timeout_sec,
                            )
                        except TimeoutError:
                            logger.warning(
                                "⏱️ Timeout (%.1fs) requesting live market data chunk of %d contracts",
                                self.config.ib.req_tickers_timeout_sec,
                                len(c_list),
                            )
                            return [ib.ticker(c) for c in c_list]
                        except Exception as e:
                            logger.warning(
                                "Error requesting live market data chunk: %s",
                                e,
                            )
                            return [ib.ticker(c) for c in c_list]

                    chunk_results = await asyncio.gather(
                        *[fetch_chunk(c) for c in chunks]
                    )
                    all_tickers = [t for sublist in chunk_results for t in sublist if t]

                    for t in all_tickers:
                        sym = t.contract.symbol.upper()
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

                    current_price_count = len(prices)
                    new_gains = current_price_count - prev_price_count
                    missing_contracts = [
                        c for c in all_contracts if c.symbol not in prices
                    ]

                    if not missing_contracts:
                        logger.info(
                            "✅ Live Snapshot Pass %d: 100%% price coverage achieved (%d/%d).",
                            pass_num,
                            current_price_count,
                            len(all_contracts),
                        )
                        break

                    if pass_num > 1 and new_gains <= 0:
                        logger.warning(
                            "Live Snapshot Pass %d: No new prices received (stalled at %d/%d). Stopping retries.",
                            pass_num,
                            current_price_count,
                            len(all_contracts),
                        )
                        break

                    prev_price_count = current_price_count

                    if pass_num < max_passes:
                        sleep_sec = min(2.0 + (pass_num - 1) * 1.0, 5.0)
                        logger.info(
                            "Live Snapshot Pass %d: Received %d/%d prices (+%d gained, %d missing). Waiting %.1fs before Pass %d...",
                            pass_num,
                            current_price_count,
                            len(all_contracts),
                            new_gains,
                            len(missing_contracts),
                            sleep_sec,
                            pass_num + 1,
                        )
                        await asyncio.sleep(sleep_sec)
                        remaining_contracts = missing_contracts
                    else:
                        logger.info(
                            "Live Snapshot Pass %d complete: Final price count %d/%d (%d missing).",
                            pass_num,
                            current_price_count,
                            len(all_contracts),
                            len(missing_contracts),
                        )

                for c in all_contracts:
                    if c.symbol not in prices:
                        current_summary["missing_price"]["list"].append(c.symbol)

                # Fetch Previous Close & ADV20 for candidates with price > 0
                candidate_contracts = [c for c in all_contracts if c.symbol in prices]
                await asyncio.gather(
                    *[_get_prev_close_and_adv20(c) for c in candidate_contracts]
                )

                # Live Volume Fetching via 15-min bars for price-qualifying tickers
                if candidate_contracts:
                    if is_test_scan:
                        vol_contracts = candidate_contracts
                    else:
                        min_pct = self.config.scan.min_abs_price_change_pct
                        vol_contracts = [
                            c
                            for c in candidate_contracts
                            if (
                                prices.get(c.symbol, 0.0) > 0
                                and prev_closes.get(c.symbol, 0.0) > 0
                                and (
                                    abs(prices[c.symbol] - prev_closes[c.symbol])
                                    / prev_closes[c.symbol]
                                )
                                * 100.0
                                >= min_pct
                            )
                        ]

                    if vol_contracts:
                        logger.info(
                            "Fetching %s Live 15-min volume bars for %d price-qualifying tickers (out of %d)...",
                            session_type.capitalize(),
                            len(vol_contracts),
                            len(candidate_contracts),
                        )

                        async def _fetch_live_eth_vol(contract):
                            sym = contract.symbol
                            if session_type == "premarket":
                                end_time_str = f"{target_date_str} 09:30:00 US/Eastern"
                                dur_str = "19800 S"  # 5.5 hours (04:00 to 09:30 ET)
                            else:
                                end_time_str = f"{target_date_str} 20:00:00 US/Eastern"
                                dur_str = "14400 S"  # 4.0 hours (16:00 to 20:00 ET)

                            bars = await _req_historical_data_with_retry(
                                contract,
                                endDateTime=end_time_str,
                                durationStr=dur_str,
                                barSizeSetting="15 mins",
                                whatToShow="TRADES",
                                useRTH=False,
                                timeout_sec=self.config.ib.hist_eth_timeout_sec,
                                max_attempts=3,
                            )
                            if bars:
                                if session_type == "premarket":
                                    session_vol = sum(
                                        b.volume
                                        for b in bars
                                        if b.volume is not None
                                        and b.volume >= 0
                                        and b.date.date() == target_date
                                        and (
                                            b.date.hour < 9
                                            or (b.date.hour == 9 and b.date.minute < 30)
                                        )
                                    )
                                else:
                                    session_vol = sum(
                                        b.volume
                                        for b in bars
                                        if b.volume is not None
                                        and b.volume >= 0
                                        and b.date.date() == target_date
                                        and b.date.hour >= 16
                                    )

                                volumes[sym] = float(session_vol)
                                if session_vol <= 0:
                                    current_summary["missing_volume"]["list"].append(
                                        sym
                                    )
                                return

                            volumes[sym] = 0.0
                            current_summary["missing_volume"]["list"].append(sym)

                        await asyncio.gather(
                            *[_fetch_live_eth_vol(c) for c in vol_contracts]
                        )

            else:
                # Off-Hours / Historical Scan (Premarket or Postmarket)
                logger.info(
                    "Running Off-Hours %s scan for date %s...",
                    session_type.capitalize(),
                    target_date_str,
                )
                await asyncio.gather(
                    *[_get_prev_close_and_adv20(c) for c in all_contracts]
                )

                # Read session prices & volumes directly from persistent cache (no slow historical bar queries)
                candidate_contracts = [
                    c for c in all_contracts if c.symbol in prev_closes
                ]
                for c in candidate_contracts:
                    sym = c.symbol
                    hp = self.cache_manager.get_hist_price(
                        session_type, target_date_str, sym
                    )
                    hv = self.cache_manager.get_hist_vol(
                        session_type, target_date_str, sym
                    )

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
                            self.cache_manager.set_hist_price(
                                session_type, target_date_str, sym, p
                            )
                    for sym, v in volumes.items():
                        if v >= 0:
                            self.cache_manager.set_hist_vol(
                                session_type, target_date_str, sym, v
                            )

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
                else set(prices.keys())
                .intersection(prev_closes.keys())
                .intersection(adv20s.keys())
            )

            for sym in symbols_eval:
                price = prices.get(sym, 0.0)
                base_close = prev_closes.get(sym, 0.0)
                vol = volumes.get(sym, 0.0)
                adv = adv20s.get(sym, 0.0)

                if not is_test_scan and (price <= 0 or base_close <= 0 or adv <= 0):
                    continue

                price_change = price - base_close if base_close > 0 else 0.0
                price_change_pct = (
                    (price_change / base_close) * 100.0 if base_close > 0 else 0.0
                )
                abs_change_pct = abs(price_change_pct)
                rel_volume_pct = (vol / adv) * 100.0 if adv > 0 else 0.0

                meets_criteria = (
                    abs_change_pct >= self.config.scan.min_abs_price_change_pct
                    and rel_volume_pct >= self.config.scan.min_rel_volume_pct
                )

                if is_test_scan or meets_criteria:
                    matches.append(
                        {
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
                        }
                    )

            # Sort default by Percent Change descending (by magnitude)
            matches.sort(key=lambda x: x["abs_change_pct"], reverse=True)

            logger.info(
                "Scan identified %d movers meeting criteria. Fetching Briefing.com news...",
                len(matches),
            )

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

            if is_test_scan:
                self.test_scan_results[session_type] = matches
                self.is_test_view_active[session_type] = True
            else:
                self.last_scan_results[session_type] = matches
                self.is_test_view_active[session_type] = False

                sess_store = self.session_store.get(session_type, {})
                b_time = sess_store.get("baseline_end_time_pt")
                if (
                    is_scheduled
                    and end_dt.time() < time(14, 0)
                    and (end_dt.time().hour in (1, 13))
                ):
                    b_time = end_dt.strftime("%I:%M:%S %p PT")

                cs = self.cache_manager.cache.get(session_type, {})
                pc_cnt = len(cs.get("prev_closes", {}))
                adv_cnt = len(cs.get("adv20s", {}))
                sp_cnt = len(prices)
                sv_cnt = len(volumes)

                self.session_store[session_type] = {
                    "target_date": target_date_str,
                    "prev_close_date": prev_close_date_str,
                    "baseline_end_time_pt": b_time,
                    "last_scan_end_time_pt": end_dt.strftime("%I:%M:%S %p PT"),
                    "last_scan_duration_sec": duration_sec,
                    "prev_closes_count": pc_cnt,
                    "adv20s_count": adv_cnt,
                    "session_prices_count": sp_cnt,
                    "session_volumes_count": sv_cnt,
                    "matches": matches,
                    "last_scan_summary": current_summary,
                    "logs": scan_log_buffer.get_logs(),
                }
                self._save_scan_results_store()

            logger.info(
                "Scan completed in %.1f seconds. Total matches: %d",
                duration_sec,
                len(matches),
            )
            return matches

        except Exception as e:
            logger.error("Unhandled error during scan execution: %s", e, exc_info=True)
            return self.get_results_for_session(selected_session)
        finally:
            self.is_scanning = False
