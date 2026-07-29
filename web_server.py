import logging
from datetime import datetime
from typing import Any
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from config import Config, PT_TZ
from ib_client import IBClientManager
from premarket_scanner import PremarketScanner, scan_log_buffer

logger = logging.getLogger(__name__)


def create_app(config: Config, ib_manager: IBClientManager, scanner: PremarketScanner) -> FastAPI:
    app = FastAPI(title="PP Scanner - Pre & Post Market Movers")
    templates = Jinja2Templates(directory=str(config.root_dir / "templates"))

    @app.get("/", response_class=HTMLResponse)
    async def get_dashboard(request: Request):
        return templates.TemplateResponse(request=request, name="index.html")

    @app.get("/api/scan-results")
    async def get_scan_results() -> dict[str, Any]:
        now_pt = datetime.now(PT_TZ)
        market_status, is_active, should_auto_scan = scanner.get_market_status(now_pt)
        ib_connected = ib_manager.is_connected()

        end_time_pt_str = (
            scanner.last_scan_end_time.astimezone(PT_TZ).strftime("%I:%M:%S %p PT")
            if scanner.last_scan_end_time
            else None
        )

        is_paused_effective = scanner.is_paused or (is_active and not should_auto_scan and not scanner.user_resumed)

        logs = scan_log_buffer.get_logs()
        warning_count = sum(1 for l in logs if l["level"] == "WARNING")
        error_count = sum(1 for l in logs if l["level"] == "ERROR")

        return {
            "status": "success",
            "market_status": market_status,
            "is_active_session": is_active,
            "should_auto_scan": should_auto_scan,
            "ib_connected": ib_connected,
            "total_matches": len(scanner.last_scan_results),
            "last_scan_end_time_pt": end_time_pt_str,
            "last_scan_duration_sec": scanner.last_scan_duration_sec,
            "is_scanning": scanner.is_scanning,
            "is_paused": is_paused_effective,
            "warning_count": warning_count,
            "error_count": error_count,
            "scan_logs": logs,
            "results": scanner.last_scan_results,
        }

    @app.post("/api/toggle-pause")
    async def toggle_pause() -> dict[str, Any]:
        now_pt = datetime.now(PT_TZ)
        session_name, is_active, should_auto_scan = scanner.get_market_status(now_pt)
        if is_active:
            if not should_auto_scan:
                scanner.user_resumed = not scanner.user_resumed
                scanner.is_paused = not scanner.user_resumed
            else:
                scanner.is_paused = not scanner.is_paused
            logger.info("Scanner pause toggled: is_paused=%s, user_resumed=%s", scanner.is_paused, scanner.user_resumed)

        is_paused_effective = scanner.is_paused or (is_active and not should_auto_scan and not scanner.user_resumed)

        return {
            "status": "success",
            "is_paused": is_paused_effective,
            "is_active_session": is_active,
        }

    return app
