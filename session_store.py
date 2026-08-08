from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def create_empty_summary() -> dict[str, dict[str, Any]]:
    """Creates an empty diagnostic summary dictionary."""
    return {
        "missing_price": {"count": 0, "list": []},
        "missing_close": {"count": 0, "list": []},
        "missing_volume": {"count": 0, "list": []},
        "missing_adv20": {"count": 0, "list": []},
    }


def create_empty_session_data(target_date: str | None = None) -> dict[str, Any]:
    """Creates initial default session state dictionary."""
    return {
        "target_date": target_date,
        "prev_close_date": None,
        "baseline_end_time_pt": None,
        "last_scan_end_time_pt": None,
        "last_scan_duration_sec": None,
        "prev_closes_count": 0,
        "adv20s_count": 0,
        "session_prices_count": 0,
        "session_volumes_count": 0,
        "matches": [],
        "last_scan_summary": create_empty_summary(),
        "logs": [],
    }


class SessionStoreManager:
    """
    Manages persistent session results storage (scan_results.json).
    Provides methods for reading, updating, resetting, and persisting session states.
    """

    def __init__(self, filepath: Path | str):
        self.filepath = Path(filepath)
        self.store: dict[str, dict[str, Any]] = self._load()

    def _load(self) -> dict[str, dict[str, Any]]:
        data = {
            "premarket": create_empty_session_data(),
            "postmarket": create_empty_session_data(),
        }
        if self.filepath.exists():
            try:
                with open(self.filepath, encoding="utf-8") as f:
                    content = json.load(f)
                    for sess in ("premarket", "postmarket"):
                        if sess in content and isinstance(content[sess], dict):
                            data[sess].update(content[sess])
            except Exception as e:
                logger.error("Failed to load %s: %s", self.filepath.name, e)
        return data

    def save(self) -> None:
        """Persists the session store to JSON file."""
        try:
            with open(self.filepath, "w", encoding="utf-8") as f:
                json.dump(self.store, f, indent=2)
        except Exception as e:
            logger.error("Failed to save %s: %s", self.filepath.name, e)

    def get_session(self, session_type: str = "premarket") -> dict[str, Any]:
        """Gets stored data for a session type."""
        sess = (
            session_type if session_type in ("premarket", "postmarket") else "premarket"
        )
        return self.store.setdefault(sess, create_empty_session_data())

    def reset_session(self, session_type: str, target_date_str: str) -> None:
        """Clears and resets a session to initial state for a new target date."""
        sess = (
            session_type if session_type in ("premarket", "postmarket") else "premarket"
        )
        self.store[sess] = create_empty_session_data(target_date_str)
        self.save()

    def update_session(self, session_type: str, updates: dict[str, Any]) -> None:
        """Updates specific fields for a session and persists."""
        sess = (
            session_type if session_type in ("premarket", "postmarket") else "premarket"
        )
        self.get_session(sess).update(updates)
        self.save()
