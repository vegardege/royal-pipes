import logging
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Official source of yearly speeches
OFFICIAL_URL = "https://www.kongehuset.dk/monarkiet-i-danmark/nytaarstaler/"

# Starts of non-textual paragraphs
SKIP_PARAGRAPH_PREFIXES = [
    "* * *",
    "Læs Dronningens",
    "Læs H.M. Dronningens",
    "Læs om nytårstalens",
    "Læs pressemeddelelse",
    "Se H.M. Dronningens",
    "Hent H.M. Dronningens",
    "Yderligere oplysninger",
    "FACEBOOK",
    "FØLG KONGEHUSET",
    "Del",
    "Copyright",
    "KONGEHUSET",
    "©",
]


async def download(url: str) -> BeautifulSoup:
    """Download the content of a web page and return a soup object.

    Raises:
        aiohttp.ClientError: If the request fails or returns non-200 status.
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return BeautifulSoup(await response.text(), "html.parser")


async def load_official_speeches(url: str = OFFICIAL_URL) -> dict[int, str]:
    """Download the most recent speeches from the official source.

    This is where new speeches will appear every year.

    Returns:
        A list of (year, url) tuples containing all links to speeches.

    Raises:
        ValueError: If link text or year parsing fails (indicates site structure changed).
        KeyError: If expected link attributes are missing (indicates site structure changed).
        aiohttp.ClientError: If the request fails or returns non-200 status.
    """
    logger.info(f"Downloading speeches from {url}")

    speeches: dict[int, str] = {}
    soup = await download(url)

    for link in soup.select("main a"):
        link_text = link.get_text(strip=True)

        if not link_text.startswith("Nytårstalen "):
            continue  # Not a speech link

        parts = link_text.split()
        if len(parts) != 2:
            raise ValueError(
                f"Unexpected link format: '{link_text}' - expected 'Nytårstalen YYYY'"
            )

        try:
            year = int(parts[1])
        except ValueError as e:
            raise ValueError(f"Could not parse year from '{link_text}': {e}") from e

        href = link.get("href")
        if not href:
            raise KeyError(f"Link for '{link_text}' has no href attribute")

        speech_link = urljoin(url, str(href))

        logger.info(f"Found {year} at {speech_link}")
        speeches[year] = speech_link

    logger.info(f"Found {len(speeches)} speeches")
    return speeches


async def load_official_speech(url: str) -> str:
    """Download the text of a single yearly speech.

    Returns:
        The stripped text content of the speech.

    Raises:
        ValueError: If the text could not reliably be extracted from the site.
        aiohttp.ClientError: If the request fails or returns non-200 status.
    """
    logger.info(f"Downloading speech from {url}")

    paragraphs: list[str] = []

    soup = await download(url)

    for p in soup.select("main .rich-text p"):
        text = p.get_text(separator=" ", strip=True)

        if not text:
            continue

        if any(
            text.lower().startswith(prefix.lower())
            for prefix in SKIP_PARAGRAPH_PREFIXES
        ):
            continue

        paragraphs.append(text)

    logger.info(f"Found {len(paragraphs)} paragraphs")
    return "\n\n".join(paragraphs)


if __name__ == "__main__":
    import asyncio
    import sys

    logging.basicConfig(level=logging.INFO)
    asyncio.run(load_official_speech(sys.argv[1]))
