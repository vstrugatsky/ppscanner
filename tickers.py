import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_tickers_from_file(file_path: Path) -> list[str]:
    """
    Loads comma-separated tickers from tickers.txt,
    replacing any periods with spaces (e.g. 'MOG.A' -> 'MOG A') for IBKR compatibility.
    Returns a sorted list of unique clean ticker symbols.
    """
    if not file_path.exists():
        logger.error("Tickers file not found at %s", file_path)
        return []

    try:
        content = file_path.read_text(encoding="utf-8")
        raw_items = [t.strip() for t in content.split(",") if t.strip()]
        
        cleaned_tickers = []
        for symbol in raw_items:
            # Spec requirement: Replace periods with spaces for IBKR (e.g., MOG.A -> MOG A)
            clean_sym = symbol.replace(".", " ").upper()
            if clean_sym and clean_sym not in cleaned_tickers:
                cleaned_tickers.append(clean_sym)

        logger.info("Loaded %d unique clean tickers from %s", len(cleaned_tickers), file_path.name)
        return cleaned_tickers
    except Exception as e:
        logger.error("Error reading tickers file %s: %s", file_path, e)
        return []
