import logging
import asyncio
import uvicorn

from config import Config
from ib_client import IBClientManager
from gmail_client import GmailClientManager
from briefing_news import BriefingNewsClient
from premarket_scanner import PremarketScanner
from scheduler import ScanScheduler
from web_server import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


async def scanner_auto_loop(scanner: PremarketScanner):
    """
    Continuous background scanner loop.
    During Live Scan windows (01:00-06:30 PT or 13:00-17:00 PT), if continuous auto-scan is enabled,
    runs scans with 15-second intervals as required by the spec.
    """
    logger.info("Starting PP Scanner Background Auto-Loop...")

    while True:
        try:
            status_str, is_live_session, active_session = scanner.get_market_status()
            if is_live_session and scanner.is_auto_scan_enabled:
                if not scanner.is_scanning:
                    await scanner.scan(selected_session=active_session)
                await asyncio.sleep(15)  # Spec: 15-second intervals during Live Scan windows
            else:
                await asyncio.sleep(5)
        except Exception as e:
            logger.error("Unhandled error in background scanner loop: %s", e)
            await asyncio.sleep(10)


def main():
    logger.info("=== Starting PP Scanner Service ===")
    config = Config()

    ib_manager = IBClientManager(config)
    gmail_manager = GmailClientManager(config.gmail)
    briefing_client = BriefingNewsClient(gmail_manager)

    scanner = PremarketScanner(config, ib_manager, briefing_client)
    prewarmer = ScanScheduler(scanner, config)

    app = create_app(config, ib_manager, scanner)

    @app.on_event("startup")
    async def startup_event():
        async def _init_services():
            # Authenticate Gmail asynchronously
            try:
                await gmail_manager.authenticate_async()
            except Exception as e:
                logger.warning("Gmail authentication error: %s", e)

            # Connect to IBKR
            try:
                await ib_manager.connect()
            except Exception as e:
                logger.warning("Initial IBKR connection error: %s", e)

            # Launch continuous scanner loop & prewarmer scheduler in background
            asyncio.create_task(scanner_auto_loop(scanner))
            asyncio.create_task(prewarmer.start())

        asyncio.create_task(_init_services())

    logger.info("=== Service Ready! Access Live Dashboard at http://localhost:8000 ===")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    main()
