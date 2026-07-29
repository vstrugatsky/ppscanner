import logging
import asyncio
import uvicorn

from config import Config
from ib_client import IBClientManager
from gmail_client import GmailClientManager
from briefing_news import BriefingNewsClient
from premarket_scanner import PremarketScanner
from web_server import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


async def scanner_loop(scanner: PremarketScanner):
    """Continuous scanner loop running scans with only a few seconds pause during active hours."""
    logger.info("Starting PP Scanner Loop...")
    
    # Run initial scan at boot
    try:
        await scanner.scan(force=True)
    except Exception as e:
        logger.error("Error during initial scan: %s", e)

    while True:
        try:
            session_name, is_active, should_auto_scan = scanner.get_market_status()
            if is_active:
                if not scanner.is_paused and (should_auto_scan or scanner.user_resumed):
                    await scanner.scan()
                    await asyncio.sleep(3)  # Brief pause between scans during active hours
                else:
                    await asyncio.sleep(5)
            else:
                scanner.user_resumed = False
                await asyncio.sleep(5)  # Re-check status during off-hours
        except Exception as e:
            logger.error("Unhandled error in scanner loop: %s", e)
            await asyncio.sleep(10)


def main():
    logger.info("=== Starting PP Scanner Service ===")
    config = Config()

    ib_manager = IBClientManager(config)
    gmail_manager = GmailClientManager(config.gmail)
    briefing_client = BriefingNewsClient(gmail_manager)

    scanner = PremarketScanner(config, ib_manager, briefing_client)

    app = create_app(config, ib_manager, scanner)

    @app.on_event("startup")
    async def startup_event():
        # Authenticate Gmail
        try:
            await gmail_manager.authenticate_async()
        except Exception as e:
            logger.warning("Gmail authentication error: %s", e)

        # Launch continuous scanner loop in background
        asyncio.create_task(scanner_loop(scanner))

    logger.info("=== Service Ready! Access Live Dashboard at http://localhost:8000 ===")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    main()
