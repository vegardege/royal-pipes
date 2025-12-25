import json
from pathlib import Path

import dagster as dg


class SpeechTextIOManager(dg.ConfigurableIOManager):
    """I/O Manager that stores speeches as local text files.

    Each partitioned speech is stored as {storage_dir}/YYYY.txt.
    Supports both manual files (dropped in by user) and scraped files.
    Defaults to XDG_DATA_HOME/royal-pipes/speeches.
    """

    storage_dir: str

    def _get_path(self, context: dg.OutputContext | dg.InputContext) -> Path:
        """Get file path for a speech partition."""
        if not context.has_partition_key:
            raise ValueError("SpeechTextIOManager requires partitioned assets")

        year = context.partition_key
        base_path = Path(self.storage_dir)
        base_path.mkdir(parents=True, exist_ok=True)
        return base_path / f"{year}.txt"

    def handle_output(self, context: dg.OutputContext, obj: str):
        """Save speech text to file."""
        path = self._get_path(context)
        path.write_text(obj, encoding="utf-8")
        context.log.info(f"Stored speech at {path}")

    def load_input(self, context: dg.InputContext) -> str:
        """Load speech text from file."""
        path = self._get_path(context)

        if not path.exists():
            raise FileNotFoundError(
                f"Speech file not found: {path}. "
                "Materialize the asset or add the file manually."
            )

        context.log.info(f"Loaded speech from {path}")
        return path.read_text(encoding="utf-8")


class SpeechNerJsonIOManager(dg.ConfigurableIOManager):
    """I/O Manager that stores NER results as local JSON files.

    Each partitioned NER result is stored as {storage_dir}/YYYY.json.
    Defaults to XDG_DATA_HOME/royal-pipes/ner.
    """

    storage_dir: str

    def _get_path(self, context: dg.OutputContext | dg.InputContext) -> Path:
        """Get file path for a NER partition."""
        if not context.has_partition_key:
            raise ValueError("SpeechNerJsonIOManager requires partitioned assets")

        year = context.partition_key
        base_path = Path(self.storage_dir)
        base_path.mkdir(parents=True, exist_ok=True)
        return base_path / f"{year}.json"

    def handle_output(self, context: dg.OutputContext, obj: dict):
        """Save NER results to JSON file."""
        path = self._get_path(context)
        path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
        context.log.info(f"Stored NER results at {path}")

    def load_input(self, context: dg.InputContext) -> dict:
        """Load NER results from JSON file."""
        path = self._get_path(context)

        if not path.exists():
            raise FileNotFoundError(
                f"NER file not found: {path}. "
                "Materialize the asset first."
            )

        context.log.info(f"Loaded NER results from {path}")
        return json.loads(path.read_text(encoding="utf-8"))
