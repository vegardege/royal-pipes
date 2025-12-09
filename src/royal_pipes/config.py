"""Configuration and XDG directory management for royal-pipes."""

import os
from pathlib import Path

# XDG Base Directory specification
XDG_DATA = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))

# Application data directory
DATA_ROOT = XDG_DATA / "royal-pipes"

# Create directories
DATA_ROOT.mkdir(parents=True, exist_ok=True)
(DATA_ROOT / "speeches").mkdir(parents=True, exist_ok=True)


def speeches_dir() -> Path:
    """Get the directory where speech text files are stored.

    Returns:
        Path to speeches directory (e.g., ~/.local/share/royal-pipes/speeches/)
    """
    return DATA_ROOT / "speeches"


def analytics_db_path() -> Path:
    """Get the path to the analytics SQLite database.

    Returns:
        Path to analytics database (e.g., ~/.local/share/royal-pipes/analytics.db)
    """
    return DATA_ROOT / "analytics.db"
