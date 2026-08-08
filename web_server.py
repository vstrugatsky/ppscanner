import asyncio
import logging
from datetime import datetime
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from config import PT_TZ, Config
from ib_client import IBClientManager
from premarket_scanner import PremarketScanner

logger = logging.getLogger(__name__)


class RunScanRequest(BaseModel):
    selected_session: str = "auto"  # 'auto' | 'premarket' | 'postmarket'
    custom_tickers: list[str] | None = None


class ToggleAutoScanRequest(BaseModel):
    enabled: bool


class ClearCacheRequest(BaseModel):
    session: str = "all"  # 'premarket' | 'postmarket' | 'all'


def create_app(
    config: Config, ib_manager: IBClientManager, scanner: PremarketScanner
) -> FastAPI:
    app = FastAPI(title="PP Scanner - Pre & Post Market Movers")
    templates = Jinja2Templates(directory=str(config.root_dir / "templates"))

    @app.get("/", response_class=HTMLResponse)
    async def get_dashboard(request: Request):
        return templates.TemplateResponse(request=request, name="index.html")

    @app.get("/api/scan-results")
    async def get_scan_results(session: str = "premarket") -> dict[str, Any]:
        now_pt = datetime.now(PT_TZ)
        market_status, is_active_live, active_session = scanner.get_market_status(
            now_pt
        )
        ib_connected = ib_manager.is_connected()

        sess = (
            session.lower()
            if session.lower() in ("premarket", "postmarket")
            else "premarket"
        )
        sess_data = scanner.get_session_data(sess)

        return {
            "status": "success",
            "market_status": market_status,
            "is_active_live_session": is_active_live,
            "session_type": active_session,
            "requested_session": sess,
            "ib_connected": ib_connected,
            "is_auto_scan_enabled": scanner.is_auto_scan_enabled,
            "total_matches": len(sess_data.get("matches", [])),
            "baseline_end_time_pt": sess_data.get("baseline_end_time_pt"),
            "first_scan_duration_sec": scanner.first_scan_duration_sec,
            "last_scan_duration_sec": sess_data.get("last_scan_duration_sec"),
            "last_scan_end_time_pt": sess_data.get("last_scan_end_time_pt"),
            "is_scanning": scanner.is_scanning,
            "is_test_view_active": sess_data.get("is_test_view_active", False),
            "session_metrics": {
                "prev_closes_count": sess_data.get("prev_closes_count", 0),
                "adv20s_count": sess_data.get("adv20s_count", 0),
                "session_prices_count": sess_data.get("session_prices_count", 0),
                "session_volumes_count": sess_data.get("session_volumes_count", 0),
            },
            "cache_status": scanner.cache_manager.get_warm_status(),
            "results": sess_data.get("matches", []),
            "last_scan_summary": sess_data.get("last_scan_summary", {}),
            "logs": sess_data.get("logs", []),
            "info_logs": sess_data.get("info_logs", []),
            "we_logs": sess_data.get("we_logs", []),
            "errors_count": sess_data.get("errors_count", 0),
            "warnings_count": sess_data.get("warnings_count", 0),
        }

    @app.post("/api/run-scan")
    async def run_scan_endpoint(req: RunScanRequest) -> dict[str, Any]:
        if scanner.is_scanning:
            return {"status": "busy", "message": "Scan is already in progress."}

        results = await scanner.scan(
            selected_session=req.selected_session,
            custom_tickers=req.custom_tickers,
        )
        return {"status": "success", "matches_count": len(results)}

    @app.post("/api/toggle-autoscan")
    async def toggle_autoscan_endpoint(req: ToggleAutoScanRequest) -> dict[str, Any]:
        scanner.is_auto_scan_enabled = req.enabled
        logger.info(
            "Continuous 15-second auto-scan %s by user.",
            "enabled" if req.enabled else "disabled",
        )
        return {
            "status": "success",
            "is_auto_scan_enabled": scanner.is_auto_scan_enabled,
        }

    @app.post("/api/clear-cache")
    async def clear_cache_endpoint(req: ClearCacheRequest) -> dict[str, Any]:
        scanner.cache_manager.clear_session_cache(req.session)
        return {"status": "success", "message": f"Cache cleared for {req.session}."}

    @app.post("/api/reload-cache")
    async def reload_cache_endpoint(req: ClearCacheRequest) -> dict[str, Any]:
        if not scanner.is_scanning:
            asyncio.create_task(
                scanner.reload_baseline_cache(selected_session=req.session)
            )
        return {
            "status": "success",
            "message": f"Reloading baseline data for {req.session}.",
        }

    @app.post("/api/restore-full-results")
    async def restore_full_results_endpoint(req: ClearCacheRequest) -> dict[str, Any]:
        scanner.restore_full_results(req.session)
        return {
            "status": "success",
            "message": f"Restored full scan results for {req.session}.",
        }

    return app
