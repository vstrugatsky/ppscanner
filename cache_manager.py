import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from config import PT_TZ

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Manages 100% isolated persistent disk caching for Premarket and Postmarket sessions.
    Each session block stores its own target_date, prev_close_date, prev_closes, adv20s,
    session_prices, session_volumes, and warmth status.
    """

    def __init__(self, cache_file_path: Path):
        self.cache_file_path = cache_file_path
        self.cache: dict[str, dict[str, Any]] = {
            "premarket": {
                "is_warmed": False,
                "last_warmed_pt": None,
                "target_date": None,
                "prev_close_date": None,
                "prev_closes": {},
                "adv20s": {},
                "session_prices": {},
                "session_volumes": {},
            },
            "postmarket": {
                "is_warmed": False,
                "last_warmed_pt": None,
                "target_date": None,
                "prev_close_date": None,
                "prev_closes": {},
                "adv20s": {},
                "session_prices": {},
                "session_volumes": {},
            },
        }
        self.load()

    def _normalize_session(self, session_type: str) -> str:
        s = session_type.lower()
        return s if s in ("premarket", "postmarket") else "premarket"

    def load(self):
        """Loads persistent cache from JSON file."""
        if not self.cache_file_path.exists():
            logger.info(
                "Cache file %s does not exist yet. Initializing empty cache.",
                self.cache_file_path.name,
            )
            return

        try:
            data = json.loads(self.cache_file_path.read_text(encoding="utf-8"))
            if "premarket" in data and "postmarket" in data:
                self.cache = data
                logger.info(
                    "Loaded scan cache (Pmarket: %d items, Postmarket: %d items)",
                    len(self.cache["premarket"].get("prev_closes", {})),
                    len(self.cache["postmarket"].get("prev_closes", {})),
                )
        except Exception as e:
            logger.warning(
                "Failed to load scan cache file %s: %s", self.cache_file_path, e
            )

    def save(self):
        """Saves persistent cache to JSON file atomically."""
        try:
            temp_file = self.cache_file_path.with_suffix(".tmp")
            temp_file.write_text(json.dumps(self.cache, indent=2), encoding="utf-8")
            temp_file.replace(self.cache_file_path)
            logger.debug("Successfully saved scan cache to disk.")
        except Exception as e:
            logger.warning(
                "Failed to save scan cache to %s: %s", self.cache_file_path, e
            )

    def mark_warmed(
        self,
        session_type: str,
        target_date_str: str,
        prev_close_date_str: str,
        count: int,
        is_scheduled: bool = False,
    ):
        """Updates warmth status and timestamp for the specified session type."""
        sess = self._normalize_session(session_type)
        current_cache = self.cache[sess]
        date_changed = current_cache.get("target_date") != target_date_str
        was_cold = not current_cache.get("is_warmed", False)

        if was_cold or date_changed or is_scheduled:
            now_pt_str = datetime.now(PT_TZ).strftime("%I:%M:%S %p PT")
            current_cache["last_warmed_pt"] = now_pt_str

        current_cache["is_warmed"] = True
        current_cache["target_date"] = target_date_str
        current_cache["prev_close_date"] = prev_close_date_str
        current_cache["cached_count"] = count
        self.save()

    def is_warmed(self, session_type: str) -> bool:
        """Returns True if the baseline cache for the specified session is warmed."""
        sess = self._normalize_session(session_type)
        return self.cache.get(sess, {}).get("is_warmed", False)

    def clear_session_cache(self, session_type: str = "all"):
        """Invalidates and clears cache for premarket, postmarket, or all sessions."""
        sessions = (
            ["premarket", "postmarket"]
            if session_type == "all"
            else [self._normalize_session(session_type)]
        )
        for s in sessions:
            self.cache[s] = {
                "is_warmed": False,
                "last_warmed_pt": None,
                "target_date": None,
                "prev_close_date": None,
                "prev_closes": {},
                "adv20s": {},
                "session_prices": {},
                "session_volumes": {},
                "cached_count": 0,
            }
        self.save()
        logger.info("Cleared scan cache for session(s): %s", session_type)

    def clear_baseline_cache(self, session_type: str = "premarket"):
        """Clears prev_closes and adv20s while leaving prices/volumes intact."""
        s = self._normalize_session(session_type)
        self.cache[s]["prev_closes"] = {}
        self.cache[s]["adv20s"] = {}
        self.cache[s]["is_warmed"] = False
        self.save()
        logger.info("Cleared baseline cache (prev_closes & adv20s) for %s", s)

    def get_warm_status(self) -> dict[str, Any]:
        """Returns warmth status for both premarket and postmarket sessions."""
        pm = self.cache.get("premarket", {})
        post = self.cache.get("postmarket", {})
        return {
            "premarket": {
                "is_warmed": pm.get("is_warmed", False),
                "last_warmed_pt": pm.get("last_warmed_pt"),
                "target_date": pm.get("target_date"),
                "prev_close_date": pm.get("prev_close_date"),
                "prev_closes_count": len(pm.get("prev_closes", {})),
                "adv20s_count": len(pm.get("adv20s", {})),
                "session_prices_count": len(pm.get("session_prices", {})),
                "session_volumes_count": len(pm.get("session_volumes", {})),
                "count": pm.get("cached_count", len(pm.get("prev_closes", {}))),
            },
            "postmarket": {
                "is_warmed": post.get("is_warmed", False),
                "last_warmed_pt": post.get("last_warmed_pt"),
                "target_date": post.get("target_date"),
                "prev_close_date": post.get("prev_close_date"),
                "prev_closes_count": len(post.get("prev_closes", {})),
                "adv20s_count": len(post.get("adv20s", {})),
                "session_prices_count": len(post.get("session_prices", {})),
                "session_volumes_count": len(post.get("session_volumes", {})),
                "count": post.get("cached_count", len(post.get("prev_closes", {}))),
            },
        }

    # Premarket vs Postmarket Getters & Setters
    def get_prev_close(
        self, session_type: str, date_str: str, symbol: str
    ) -> float | None:
        sess = self._normalize_session(session_type)
        if self.cache[sess].get("prev_close_date") == date_str:
            return self.cache[sess].get("prev_closes", {}).get(symbol.upper())
        return None

    def set_prev_close(self, session_type: str, date_str: str, symbol: str, val: float):
        sess = self._normalize_session(session_type)
        self.cache[sess]["prev_close_date"] = date_str
        self.cache[sess].setdefault("prev_closes", {})[symbol.upper()] = float(val)

    def get_adv20(self, session_type: str, date_str: str, symbol: str) -> float | None:
        sess = self._normalize_session(session_type)
        if self.cache[sess].get("prev_close_date") == date_str:
            return self.cache[sess].get("adv20s", {}).get(symbol.upper())
        return None

    def set_adv20(self, session_type: str, date_str: str, symbol: str, val: float):
        sess = self._normalize_session(session_type)
        self.cache[sess]["prev_close_date"] = date_str
        self.cache[sess].setdefault("adv20s", {})[symbol.upper()] = float(val)

    def get_hist_price(
        self, session_type: str, date_str: str, symbol: str
    ) -> float | None:
        sess = self._normalize_session(session_type)
        if self.cache[sess].get("target_date") == date_str:
            return self.cache[sess].get("session_prices", {}).get(symbol.upper())
        return None

    def set_hist_price(self, session_type: str, date_str: str, symbol: str, val: float):
        sess = self._normalize_session(session_type)
        self.cache[sess]["target_date"] = date_str
        self.cache[sess].setdefault("session_prices", {})[symbol.upper()] = float(val)

    def get_hist_vol(
        self, session_type: str, date_str: str, symbol: str
    ) -> float | None:
        sess = self._normalize_session(session_type)
        if self.cache[sess].get("target_date") == date_str:
            return self.cache[sess].get("session_volumes", {}).get(symbol.upper())
        return None

    def set_hist_vol(self, session_type: str, date_str: str, symbol: str, val: float):
        sess = self._normalize_session(session_type)
        self.cache[sess]["target_date"] = date_str
        self.cache[sess].setdefault("session_volumes", {})[symbol.upper()] = float(val)
