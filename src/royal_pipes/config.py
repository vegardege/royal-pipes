import os
from pathlib import Path

# We store files relative to the XDG Base Directory specification
XDG_DATA = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))

# The root is the data directory, which contains the output database and
# subdirectories for files we want to keep in the local file system.
DATA_ROOT = XDG_DATA / "royal-pipes"
DATA_ROOT.mkdir(parents=True, exist_ok=True)

SPEECHES_DIR = DATA_ROOT / "speeches"
SPEECHES_DIR.mkdir(parents=True, exist_ok=True)

NER_DIR = DATA_ROOT / "ner"
NER_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_ROOT / "analytics.db"
