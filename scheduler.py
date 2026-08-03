from __future__ import annotations

import asyncio
import logging
from datetime import datetime, date
from typing import Optional

from config import Config, PT_TZ
from premarket_scanner import PremarketScanner

logger = logging.getLogger(__name__)


class ScanScheduler:
    """
    Automated baseline and live snapshot scheduler.
    Triggers scheduled cache pre-warming at key trading day times in Pacific Time:
    - 01:05 PT: Premarket Cache Warm
    - 06:29 PT: Premarket Live Snapshot
    - 13:05 PT: Postmarket Cache Warm
    - 16:59 PT: Postmarket Live Snapshot
    """

    def __init__(self, scanner: PremarketScanner, config: Config | None = None):
        self.scanner = scanner
        self.config = config or Config()
        self.running = False
        self._task: Optional[asyncio.Task] = None
        # Track last date triggered for each slot to prevent duplicate runs
        self._last_triggered: dict[str, Optional[date]] = {
            "01:05": None,
            "06:29": None,
            "13:05": None,
            "16:59": None,
        }

    async def start(self):
        """Starts the background pre-warmer loop."""
        self.running = True
        logger.info("Starting Automatic Scheduler...")
        while self.running:
            try:
                await self._check_and_trigger()
            except Exception as e:
                logger.error("Error in ScanScheduler loop: %s", e, exc_info=True)
            await asyncio.sleep(30)  # Check every 30 seconds

    def stop(self):
        """Stops the scheduler."""
        self.running = False

    async def _check_and_trigger(self):
        now_pt = datetime.now(PT_TZ)

        # Only run on trading weekdays (Monday=0 through Friday=4) TODO: implement awareness of holidays
        if now_pt.weekday() >= 5:
            return

        today = now_pt.date()
        time_str = now_pt.strftime("%H:%M")

        # Map scheduled times to session selection
        schedule_map = {
            "01:05": ("premarket", "Premarket Baseline Pre-warm"),
            "06:29": ("premarket", "Premarket Final Live Snapshot"),
            "13:05": ("postmarket", "Postmarket Baseline Pre-warm"),
            "16:59": ("postmarket", "Postmarket Final Live Snapshot"),
        }

        if time_str in schedule_map:
            if self._last_triggered.get(time_str) != today:
                if self.scanner.is_scanning:
                    logger.info("Scanner is currently busy. Skipping scheduled pre-warm at %s PT.", time_str)
                    return

                if not self.scanner.ib_manager.is_connected():
                    logger.warning("IBKR is disconnected. Skipping scheduled pre-warm at %s PT.", time_str)
                    return

                session_mode, mode_label = schedule_map[time_str]
                self._last_triggered[time_str] = today

                logger.info("⚡ Automatic Scan triggered for %s at %s PT", mode_label, time_str)
                # Run scan in background to pre-warm cache
                asyncio.create_task(self._run_prewarm(session_mode, mode_label, time_str))

    async def _run_prewarm(self, session_mode: str, mode_label: str, time_str: str):
        try:
            start_t = datetime.now(PT_TZ)
            await self.scanner.scan(selected_session=session_mode, is_scheduled=True)
            dur = round((datetime.now(PT_TZ) - start_t).total_seconds(), 1)
            logger.info("✅ Automatic Scan completed for %s (%s PT) in %.1fs", mode_label, time_str, dur)
        except Exception as e:
            logger.error("Failed automatic scan for %s: %s", mode_label, e)
