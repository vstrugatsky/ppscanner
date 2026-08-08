from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime

from config import PT_TZ, Config
from premarket_scanner import PremarketScanner

logger = logging.getLogger(__name__)


class ScanScheduler:
    """
    Automated baseline and live snapshot scheduler.
    Triggers scheduled cache pre-warming at key trading day times in Pacific Time:
    - 01:05 PT: Premarket Cache Warm
    - 06:28 PT: Premarket Live Snapshot
    - 13:20 PT: Postmarket Cache Warm
    - 16:58 PT: Postmarket Live Snapshot
    """

    def __init__(self, scanner: PremarketScanner, config: Config | None = None):
        self.scanner = scanner
        self.config = config or Config()
        self.running = False
        self._task: asyncio.Task | None = None
        # Track last date triggered for each slot to prevent duplicate runs
        self._last_triggered: dict[str, date | None] = {
            "01:05": None,
            "06:28": None,
            "13:20": None,
            "16:58": None,
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

        # Map scheduled times to session selection and run type
        schedule_map = {
            "01:05": ("premarket", "Premarket Baseline Pre-warm", "baseline"),
            "06:28": ("premarket", "Premarket Final Live Snapshot", "live"),
            "13:20": ("postmarket", "Postmarket Baseline Pre-warm", "baseline"),
            "16:58": ("postmarket", "Postmarket Final Live Snapshot", "live"),
        }

        if time_str in schedule_map:
            if self._last_triggered.get(time_str) != today:
                if self.scanner.is_scanning:
                    logger.info(
                        "Scanner is currently busy. Skipping scheduled trigger at %s PT.",
                        time_str,
                    )
                    return

                if not self.scanner.ib_manager.is_connected():
                    logger.warning(
                        "IBKR is disconnected. Skipping scheduled trigger at %s PT.",
                        time_str,
                    )
                    return

                session_mode, mode_label, run_type = schedule_map[time_str]
                self._last_triggered[time_str] = today

                logger.info(
                    "⚡ Automatic Scheduler triggered for %s at %s PT",
                    mode_label,
                    time_str,
                )
                # Run scheduled task in background
                asyncio.create_task(
                    self._run_scheduled_task(
                        session_mode, mode_label, time_str, run_type
                    )
                )

    async def _run_scheduled_task(
        self, session_mode: str, mode_label: str, time_str: str, run_type: str
    ):
        try:
            start_t = datetime.now(PT_TZ)
            if run_type == "baseline":
                await self.scanner.reload_baseline_cache(session_mode)
            else:
                await self.scanner.scan(
                    selected_session=session_mode, is_scheduled=True
                )
            dur = round((datetime.now(PT_TZ) - start_t).total_seconds(), 1)
            logger.info(
                "✅ Scheduled %s completed for %s (%s PT) in %.1fs",
                run_type.capitalize(),
                mode_label,
                time_str,
                dur,
            )
        except Exception as e:
            logger.error("Failed scheduled task for %s: %s", mode_label, e)
