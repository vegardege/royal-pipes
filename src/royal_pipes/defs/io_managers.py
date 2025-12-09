from pathlib import Path

import dagster as dg


class SpeechTextIOManager(dg.ConfigurableIOManager):
    """I/O Manager that stores speeches as local text files.

    Each partitioned speech is stored as data/speeches/YYYY.txt.
    Supports both manual files (dropped in by user) and scraped files.
    """

    storage_dir: str = "data/speeches"

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
