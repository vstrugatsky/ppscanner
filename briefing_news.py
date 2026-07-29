import logging
import re
import asyncio
from typing import Any, Optional
from gmail_client import GmailClientManager

logger = logging.getLogger(__name__)


def count_tickers_in_subject(subject: str) -> int:
    """Counts the number of stock ticker symbols listed in a Briefing.com email subject line."""
    parts = subject.split(";")
    if len(parts) >= 2:
        ticker_part = parts[1].strip()
        tokens = [t for t in ticker_part.split() if t.isupper() and t.isalpha()]
        if tokens:
            return len(tokens)

    # Fallback: count all standalone uppercase words of length 1-5 that are not common acronyms
    ignored = {"Q1", "Q2", "Q3", "Q4", "AI", "FY26", "FY27", "FY28", "USA", "PPA", "EST", "ET"}
    matches = re.findall(r"\b[A-Z]{1,5}\b", subject)
    valid_tickers = [m for m in matches if m not in ignored]
    return len(valid_tickers) if valid_tickers else 999


import time


class BriefingNewsClient:
    """Retrieves Briefing.com emails from Inbox, caches them in memory, and matches tickers."""

    def __init__(self, gmail_manager: GmailClientManager):
        self.gmail_manager = gmail_manager
        self.email_cache: list[dict[str, Any]] = []
        self.seen_msg_ids: set[str] = set()

    async def sync_emails(self, max_initial_emails: int = 300) -> int:
        """Syncs Briefing.com emails into memory cache.
        If cache is empty, loads up to max_initial_emails.
        If cache exists, fetches only new/unseen messages (up to 50).
        Returns the number of new emails added.
        """
        service = self.gmail_manager.service
        if not service:
            await self.gmail_manager.authenticate_async()
            service = self.gmail_manager.service

        if not service:
            return 0

        try:
            fetch_limit = max_initial_emails if not self.email_cache else 50
            res = await self.gmail_manager.list_messages_async(max_results=fetch_limit, query="from:briefing.com")
            messages = res.get("messages", [])

            new_msgs = [m for m in messages if m.get("id") and m["id"] not in self.seen_msg_ids]
            if not new_msgs:
                return 0

            logger.info("Syncing %d new Briefing.com emails into memory cache...", len(new_msgs))

            new_parsed: list[dict[str, Any]] = []
            for msg_meta in new_msgs:
                msg_id = msg_meta["id"]
                self.seen_msg_ids.add(msg_id)

                def _fetch_msg(m_id=msg_id):
                    return (
                        service.users()
                        .messages()
                        .get(userId="me", id=m_id, format="metadata", metadataHeaders=["Subject", "From", "Date"])
                        .execute()
                    )

                msg = await asyncio.to_thread(_fetch_msg)
                payload = msg.get("payload", {})
                headers = payload.get("headers", [])

                subject = ""
                sender = ""
                date_str = ""
                for h in headers:
                    h_name = h.get("name", "").lower()
                    if h_name == "subject":
                        subject = h.get("value", "")
                    elif h_name == "from":
                        sender = h.get("value", "")
                    elif h_name == "date":
                        date_str = h.get("value", "")

                snippet = msg.get("snippet", "")
                ticker_count = count_tickers_in_subject(subject)

                new_parsed.append({
                    "id": msg_id,
                    "subject": subject,
                    "snippet": snippet,
                    "from": sender,
                    "date": date_str,
                    "ticker_count": ticker_count,
                })

            # Prepend new emails so most recent are first
            self.email_cache = new_parsed + self.email_cache
            if len(self.email_cache) > 500:
                self.email_cache = self.email_cache[:500]

            return len(new_parsed)

        except Exception as e:
            logger.warning("Error syncing Briefing.com emails: %s", e)
            return 0

    def match_symbols_from_cache(self, symbols: list[str]) -> dict[str, Optional[dict[str, Any]]]:
        """Matches target mover symbols against cached Briefing.com emails instantly in memory."""
        results: dict[str, Optional[dict[str, Any]]] = {s: None for s in symbols}
        if not symbols or not self.email_cache:
            return results

        clean_symbols = [s.strip().upper() for s in symbols if s.strip()]

        for sym in clean_symbols:
            pattern = re.compile(rf"(?<![A-Za-z0-9\.-]){re.escape(sym)}(?![A-Za-z0-9\.-])")

            best_match: Optional[dict[str, Any]] = None
            best_ticker_count = 999

            for email in self.email_cache:
                if pattern.search(email["subject"]):
                    t_count = email["ticker_count"]
                    if t_count < best_ticker_count:
                        best_ticker_count = t_count
                        best_match = {
                            "id": email["id"],
                            "subject": email["subject"],
                            "snippet": email["snippet"],
                            "from": email["from"],
                            "date": email["date"],
                        }

            results[sym] = best_match

        return results

    async def get_news_for_symbols_batch(
        self, symbols: list[str], max_emails: int = 300
    ) -> dict[str, Optional[dict[str, Any]]]:
        """Backward-compatible wrapper: syncs emails and returns instant matches from cache."""
        await self.sync_emails(max_initial_emails=max_emails)
        return self.match_symbols_from_cache(symbols)
