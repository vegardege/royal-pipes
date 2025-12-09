import re
from collections import Counter
from pathlib import Path


def compute_word_counts(speeches_dir: str | Path) -> list[tuple[int, str, int]]:
    """Compute word counts across all speech files.

    Args:
        speeches_dir: Directory containing YYYY.txt speech files

    Returns:
        List of (year, word, count) tuples with lowercase cleaned words
    """
    speeches_path = Path(speeches_dir)
    word_counts: list[tuple[int, str, int]] = []

    for speech_file in sorted(speeches_path.glob("*.txt")):
        year = int(speech_file.stem)

        text = speech_file.read_text(encoding="utf-8")
        text_lower = text.lower()

        words = re.findall(r"\b[\w]+\b", text_lower)
        word_counter = Counter(words)

        for word, count in word_counter.items():
            word_counts.append((year, word, count))

    return word_counts
