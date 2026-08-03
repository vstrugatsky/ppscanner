import logging
import asyncio
from datetime import datetime
from typing import Any, Optional
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from config import Config, PT_TZ
from ib_client import IBClientManager
from premarket_scanner import PremarketScanner, scan_log_buffer

logger = logging.getLogger(__name__)


class RunScanRequest(BaseModel):
    selected_session: str = "auto"  # 'auto' | 'premarket' | 'postmarket'
    custom_tickers: Optional[list[str]] = None


class ToggleAutoScanRequest(BaseModel):
    enabled: bool


class ClearCacheRequest(BaseModel):
    session: str = "all"  # 'premarket' | 'postmarket' | 'all'


def create_app(config: Config, ib_manager: IBClientManager, scanner: PremarketScanner) -> FastAPI:
    app = FastAPI(title="PP Scanner - Pre & Post Market Movers")
    templates = Jinja2Templates(directory=str(config.root_dir / "templates"))

    @app.get("/", response_class=HTMLResponse)
    async def get_dashboard(request: Request):
        return templates.TemplateResponse(request=request, name="index.html")

    @app.get("/api/scan-results")
    async def get_scan_results(session: str = "premarket") -> dict[str, Any]:
        now_pt = datetime.now(PT_TZ)
        market_status, is_active_live, active_session = scanner.get_market_status(now_pt)
        ib_connected = ib_manager.is_connected()

        sess = session.lower() if session.lower() in ("premarket", "postmarket") else "premarket"
        session_results = scanner.get_results_for_session(sess)
        session_summary = scanner.get_summary_for_session(sess)

        end_time_pt_str = (
            scanner.last_scan_end_time.strftime("%I:%M:%S %p PT")
            if scanner.last_scan_end_time
            else None
        )

        b_end = scanner.baseline_end_time.get(sess)
        baseline_end_pt_str = b_end.strftime("%I:%M:%S %p PT") if b_end else None

        logs = scan_log_buffer.get_logs()

        return {
            "status": "success",
            "market_status": market_status,
            "is_active_live_session": is_active_live,
            "session_type": active_session,
            "ib_connected": ib_connected,
            "is_auto_scan_enabled": scanner.is_auto_scan_enabled,
            "total_matches": len(session_results),
            "baseline_end_time_pt": baseline_end_pt_str,
            "first_scan_duration_sec": scanner.first_scan_duration_sec,
            "last_scan_duration_sec": scanner.last_scan_duration_sec,
            "last_scan_end_time_pt": end_time_pt_str,
            "is_scanning": scanner.is_scanning,
            "is_test_view_active": scanner.is_test_view_active.get(sess, False),
            "cache_status": scanner.cache_manager.get_warm_status(),
            "results": session_results,
            "last_scan_summary": session_summary,
            "logs": logs,
        }

    @app.post("/api/run-scan")
    async def run_scan_endpoint(req: RunScanRequest) -> dict[str, Any]:
        if scanner.is_scanning:
            return {"status": "busy", "message": "Scan is already in progress."}

        results = await scanner.scan(
            selected_session=req.selected_session,
            custom_tickers=req.custom_tickers,
            force=True,
        )
        return {"status": "success", "matches_count": len(results)}

    @app.post("/api/toggle-autoscan")
    async def toggle_autoscan_endpoint(req: ToggleAutoScanRequest) -> dict[str, Any]:
        scanner.is_auto_scan_enabled = req.enabled
        logger.info("Continuous 15-second auto-scan %s by user.", "enabled" if req.enabled else "disabled")
        return {"status": "success", "is_auto_scan_enabled": scanner.is_auto_scan_enabled}

    @app.post("/api/clear-cache")
    async def clear_cache_endpoint(req: ClearCacheRequest) -> dict[str, Any]:
        scanner.cache_manager.clear_session_cache(req.session)
        return {"status": "success", "message": f"Cache cleared for {req.session}."}

    @app.post("/api/reload-cache")
    async def reload_cache_endpoint(req: ClearCacheRequest) -> dict[str, Any]:
        scanner.cache_manager.clear_session_cache(req.session)
        if not scanner.is_scanning:
            asyncio.create_task(scanner.scan(selected_session=req.session, force=True))
        return {"status": "success", "message": f"Reloading cache for {req.session}."}

    @app.post("/api/restore-full-results")
    async def restore_full_results_endpoint(req: ClearCacheRequest) -> dict[str, Any]:
        scanner.restore_full_results(req.session)
        return {"status": "success", "message": f"Restored full scan results for {req.session}."}

    return app
