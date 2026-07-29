import os
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


@dataclass
class GmailConfig:
    credentials_file: str = "credentials.json"
    token_file: str = "token.json"
    scopes: tuple[str, ...] = ("https://www.googleapis.com/auth/gmail.readonly",)


@dataclass
class ScanCriteria:
    min_rel_volume_pct: float = 5.0
    min_abs_price_change_pct: float = 2.0
    max_briefing_emails: int = 300


class Config:
    def __init__(self, root_dir: Path | None = None):
        self.root_dir = root_dir or Path(__file__).parent.resolve()
        self.tickers_file = self.root_dir / "tickers.txt"
        self.contracts_cache_file = self.root_dir / "qualified_contracts.json"

        self.ib = IBConfig()
        self.gmail = GmailConfig()
        self.scan = ScanCriteria()

        # Session Windows (Pacific Time)
        # Premarket: Status reported 01:00 to 06:30 PT; Automatic scanning starts at 05:30 PT
        self.pm_display_start_pt = (1, 0)
        self.pm_auto_start_pt = (5, 30)
        self.pm_end_pt = (6, 30)

        # Postmarket: 13:00 to 17:00 PT
        self.post_start_pt = (13, 0)
        self.post_end_pt = (17, 0)
