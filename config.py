from __future__ import annotations

import zoneinfo
from dataclasses import dataclass
from pathlib import Path

# Timezones
ET_TZ = zoneinfo.ZoneInfo("America/New_York")
PT_TZ = zoneinfo.ZoneInfo("America/Los_Angeles")


@dataclass
class IBConfig:
    host: str = "127.0.0.1"
    port: int = 4001
    client_id: int = 1
    market_data_type: int = (
        1  # 1 = Real-Time Live, 2 = Frozen, 3 = Delayed, 4 = Delayed Frozen
    )
    req_tickers_timeout_sec: float = 5.0
    hist_concurrency_limit: int = (
        10  # IBKR rate limit semaphore for historical data calls
    )
    hist_rth_timeout_sec: float = 10.0  # Timeout for RTH daily bars (useRTH=True)
    hist_eth_timeout_sec: float = (
        60.0  # Timeout for extended hours 15-min bars (useRTH=False)
    )


@dataclass
class GmailConfig:
    credentials_file: str = "credentials.json"
    token_file: str = "token.json"
    scopes: tuple[str, ...] = ("https://www.googleapis.com/auth/gmail.readonly",)


@dataclass
class ScanCriteria:
    min_rel_volume_pct: float = 5.0
    min_abs_price_change_pct: float = 2.0


class Config:
    def __init__(self, root_dir: Path | None = None):
        self.root_dir = root_dir or Path(__file__).parent.resolve()
        self.tickers_file = self.root_dir / "tickers.txt"
        self.contracts_cache_file = self.root_dir / "qualified_contracts.json"
        self.deactivated_tickers_file = self.root_dir / "deactivated_tickers.json"
        self.scan_cache_file = self.root_dir / "scan_cache.json"
        self.scan_results_file = self.root_dir / "scan_results.json"

        self.ib = IBConfig()
        self.gmail = GmailConfig()
        self.scan = ScanCriteria()

        # Session Windows (Pacific Time)
        # Premarket: 01:00 to 06:30 PT
        self.pm_start_pt = (1, 0)
        self.pm_end_pt = (6, 30)

        # RTH: 06:30 to 13:00 PT
        self.rth_start_pt = (6, 30)
        self.rth_end_pt = (13, 0)

        # Postmarket: 13:00 to 17:00 PT
        self.post_start_pt = (13, 0)
        self.post_end_pt = (17, 0)
