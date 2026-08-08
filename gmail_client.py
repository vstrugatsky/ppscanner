import asyncio
import logging
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build

from config import GmailConfig

logger = logging.getLogger(__name__)


class GmailClientManager:
    """OAuth2 authentication wrapper and API manager for Gmail API integration."""

    def __init__(self, config: GmailConfig):
        self.config = config
        self.creds: Credentials | None = None
        self.service: Resource | None = None

    def authenticate(self) -> Credentials:
        """Synchronous authentication workflow: checks token.json, refreshes if expired, or runs local server auth flow."""
        token_path = Path(self.config.token_file)
        creds_path = Path(self.config.credentials_file)

        # 1. Check for existing token.json
        if token_path.exists():
            logger.info("Loading existing Gmail OAuth2 token from %s", token_path)
            try:
                self.creds = Credentials.from_authorized_user_file(
                    str(token_path), self.config.scopes
                )
            except Exception as e:
                logger.warning("Failed to parse token file %s: %s", token_path, e)
                self.creds = None

        # 2. Refresh or prompt for local browser login if token is invalid
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                logger.info(
                    "Gmail OAuth2 token expired. Refreshing using refresh token..."
                )
                try:
                    self.creds.refresh(Request())
                except Exception as e:
                    logger.warning(
                        "Failed to refresh token: %s. Re-authenticating...", e
                    )
                    self.creds = None

            if not self.creds:
                if not creds_path.exists():
                    logger.error(
                        "Google Client Secrets file not found at '%s'. "
                        "Please download OAuth 2.0 Credentials (client_secret.json) from Google Cloud Console.",
                        creds_path,
                    )
                    raise FileNotFoundError(
                        f"Missing Gmail credentials file: '{creds_path.resolve()}'"
                    )

                logger.info("Initiating local browser authentication flow...")
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(creds_path), self.config.scopes
                )
                self.creds = flow.run_local_server(port=0)

            # Save newly acquired / refreshed credentials
            logger.info("Saving updated token to %s", token_path)
            with open(token_path, "w", encoding="utf-8") as token_file:
                token_file.write(self.creds.to_json())

        logger.info("Gmail OAuth2 authentication successful.")
        self.service = build("gmail", "v1", credentials=self.creds)
        return self.creds

    async def authenticate_async(self) -> Credentials:
        """Asynchronously run the authentication workflow without blocking the event loop."""
        return await asyncio.to_thread(self.authenticate)

    async def get_profile_async(self, user_id: str = "me") -> dict[str, Any]:
        """Fetch user profile asynchronously."""
        if not self.service:
            await self.authenticate_async()

        def _get_profile():
            return self.service.users().getProfile(userId=user_id).execute()

        return await asyncio.to_thread(_get_profile)

    async def list_messages_async(
        self, user_id: str = "me", max_results: int = 10, query: str = ""
    ) -> dict[str, Any]:
        """Asynchronously query Gmail messages."""
        if not self.service:
            await self.authenticate_async()

        def _list_messages():
            return (
                self.service.users()
                .messages()
                .list(userId=user_id, maxResults=max_results, q=query)
                .execute()
            )

        return await asyncio.to_thread(_list_messages)
