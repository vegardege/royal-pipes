import logging
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Official source of yearly speeches
OFFICIAL_URL = "https://www.kongehuset.dk/monarkiet-i-danmark/nytaarstaler/"


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


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)
    asyncio.run(load_official_speeches(OFFICIAL_URL))
