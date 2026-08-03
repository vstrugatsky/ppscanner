from __future__ import annotations

import logging
import re
import asyncio
from datetime import datetime, date, time, timedelta
from typing import Any, Optional
from gmail_client import GmailClientManager
from config import PT_TZ

logger = logging.getLogger(__name__)


def count_tickers_in_subject(subject: str) -> int:
    """
    Counts the number of stock ticker symbols listed in a Briefing.com email subject line.
    Rule: Fewer tickers in subject line indicate higher specific relevance for the mover.
    """
    parts = subject.split(";")
    if len(parts) >= 2:
        ticker_part = parts[1].strip()
        tokens = [t for t in ticker_part.split() if t.isupper() and t.isalpha()]
        if tokens:
            return len(tokens)

    # Fallback: count all standalone uppercase words of length 1-5 that are not common acronyms
    ignored = {"Q1", "Q2", "Q3", "Q4", "AI", "FY26", "FY27", "FY28", "USA", "PPA", "EST", "ET", "USD", "PT"}
    matches = re.findall(r"\b[A-Z]{1,5}\b", subject)
    valid_tickers = [m for m in matches if m not in ignored]
    return len(valid_tickers) if valid_tickers else 999


class BriefingNewsClient:
    """
    Retrieves Briefing.com emails from Gmail under the 'briefing' label,
    filtered by exact session time windows, and matches tickers by highest relevance.
    """

    def __init__(self, gmail_manager: GmailClientManager):
        self.gmail_manager = gmail_manager
        self.email_cache: list[dict[str, Any]] = []

    async def fetch_session_emails(self, session_type: str, session_date: date) -> list[dict[str, Any]]:
        """
        Fetches all Briefing.com emails within the spec-defined time window:
        - Premarket: 13:00 PT day before to 06:30 PT session day
        - Postmarket: 13:00 PT to 17:00 PT session day
        """
        service = self.gmail_manager.service
        if not service:
            await self.gmail_manager.authenticate_async()
            service = self.gmail_manager.service

        if not service:
            logger.warning("Gmail service unavailable. Skipping Briefing news fetch.")
            return []

        try:
            # Determine time window in Pacific Time
            if session_type == "premarket":
                prev_date = session_date - timedelta(days=1)
                window_start = datetime.combine(prev_date, time(13, 0), tzinfo=PT_TZ)
                window_end = datetime.combine(session_date, time(6, 30), tzinfo=PT_TZ)
            else: # postmarket
                window_start = datetime.combine(session_date, time(13, 0), tzinfo=PT_TZ)
                window_end = datetime.combine(session_date, time(17, 0), tzinfo=PT_TZ)

            # Convert to UNIX timestamp for Gmail query
            start_ts = int(window_start.timestamp())
            end_ts = int(window_end.timestamp())

            query = f"label:briefing after:{start_ts} before:{end_ts}"
            logger.info("Querying Gmail Briefing emails with: '%s'", query)

            res = await self.gmail_manager.list_messages_async(max_results=500, query=query)
            messages = res.get("messages", [])

            if not messages:
                # Fallback to query by from:briefing.com if label:briefing isn't set
                fallback_query = f"from:briefing.com after:{start_ts} before:{end_ts}"
                res = await self.gmail_manager.list_messages_async(max_results=500, query=fallback_query)
                messages = res.get("messages", [])

            logger.info("Retrieved %d Briefing.com emails for session date %s (%s)", len(messages), session_date, session_type)

            parsed_emails: list[dict[str, Any]] = []
            for msg_meta in messages:
                msg_id = msg_meta["id"]

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
                internal_date = int(msg.get("internalDate", 0))

                parsed_emails.append({
                    "id": msg_id,
                    "subject": subject,
                    "snippet": snippet,
                    "from": sender,
                    "date": date_str,
                    "internal_date": internal_date,
                    "ticker_count": ticker_count,
                })

            # Sort by internal_date descending so tiebreaker chooses most recent email
            parsed_emails.sort(key=lambda x: x["internal_date"], reverse=True)
            self.email_cache = parsed_emails
            return parsed_emails

        except Exception as e:
            logger.warning("Error fetching Briefing.com emails: %s", e)
            return []

    def match_symbols(self, symbols: list[str]) -> dict[str, Optional[dict[str, Any]]]:
        """
        Matches target mover symbols against cached Briefing.com emails.
        Selects email with fewest uppercase tickers in subject; tiebreaks by most recent email.
        """
        results: dict[str, Optional[dict[str, Any]]] = {s: None for s in symbols}
        if not symbols or not self.email_cache:
            return results

        for sym in symbols:
            clean_sym = sym.strip().upper()
            variants = [clean_sym]
            if " " in clean_sym:
                variants.append(clean_sym.replace(" ", "."))
            if "." in clean_sym:
                variants.append(clean_sym.replace(".", " "))

            pattern_str = "|".join(re.escape(v) for v in set(variants))
            pattern = re.compile(rf"(?<![A-Za-z0-9\.-])(?:{pattern_str})(?![A-Za-z0-9\.-])")

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
        self, symbols: list[str], session_type: str = "premarket", session_date: date | None = None
    ) -> dict[str, Optional[dict[str, Any]]]:
        """Fetches session emails and returns best news match for each mover symbol."""
        if session_date is None:
            session_date = datetime.now(PT_TZ).date()
        await self.fetch_session_emails(session_type=session_type, session_date=session_date)
        return self.match_symbols(symbols)
