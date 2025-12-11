from pathlib import Path

from dagster import Definitions, load_from_defs_folder

from royal_pipes.config import DB_PATH, SPEECHES_DIR
from royal_pipes.defs.io_managers import SpeechTextIOManager
from royal_pipes.defs.resources import AnalyticsDB


def _load_definitions() -> Definitions:
    """Load definitions with custom I/O manager."""
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)

    return Definitions(
        assets=loaded.assets,
        asset_checks=loaded.asset_checks,
        schedules=loaded.schedules,
        sensors=loaded.sensors,
        jobs=loaded.jobs,
        resources={
            **(loaded.resources or {}),
            "speech_text_io": SpeechTextIOManager(storage_dir=str(SPEECHES_DIR)),
            "analytics_db": AnalyticsDB(db_path=str(DB_PATH)),
        },
    )


defs = _load_definitions()
