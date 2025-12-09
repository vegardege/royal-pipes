from pathlib import Path

from dagster import Definitions, load_from_defs_folder

from royal_pipes.defs.io_managers import SpeechTextIOManager


def _load_definitions() -> Definitions:
    """Load definitions with custom I/O manager."""
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)

    return Definitions(
        assets=loaded.assets,
        schedules=loaded.schedules,
        sensors=loaded.sensors,
        jobs=loaded.jobs,
        resources={
            **(loaded.resources or {}),
            "speech_text_io": SpeechTextIOManager(storage_dir="data/speeches"),
        },
    )


defs = _load_definitions()
