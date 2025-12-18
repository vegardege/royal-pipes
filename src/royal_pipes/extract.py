import io
import logging
import re
import tarfile
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from royal_pipes.models import CorpusWord, Monarch

logger = logging.getLogger(__name__)

# Headers for well behaved requests
HEADERS = {"User-Agent": "RoyalPipesBot/1.0"}

# Official source of yearly speeches from Kongehuset
OFFICIAL_URL = "https://www.kongehuset.dk/monarkiet-i-danmark/nytaarstaler/"

# Danske Spil odds
BETTING_URL = "https://danskespil.dk/oddset/sports/competition/25652/underholdning/danmark/danmark-kongens-nytarstale/matches?eventsSort=outrights"

# Wikipedia monarchs list
MONARCHS_URL = "https://en.wikipedia.org/wiki/List_of_monarchs_of_Denmark"

# Leipzig Corpora Collection - Danish Mixed 1M dataset
LEIPZIG_URL = "https://downloads.wortschatz-leipzig.de/corpora/dan_mixed_2014_1M.tar.gz"


# Starts of non-textual paragraphs. These are observed values that are used
# in the official transcripts, but which does not represent a part of the
# actual speech being held.
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


async def download_bytes(url: str) -> bytes:
    """Download the content of a web page and return the raw bytes.

    Raises:
        aiohttp.ClientError: If the request fails or returns non-200 status.
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as response:
            response.raise_for_status()
            return await response.read()


async def download_soup(url: str) -> BeautifulSoup:
    """Download the content of a web page and return a soup object.

    Raises:
        aiohttp.ClientError: If the request fails or returns non-200 status.
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as response:
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
    soup = await download_soup(url)

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

    soup = await download_soup(url)

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


async def load_danskespil_odds(
    url: str = BETTING_URL, section_index: int = 0
) -> dict[str, float]:
    """Scrape betting odds for which words will appear in the King's New Year speech.

    Uses Playwright to handle JavaScript-rendered content.

    Args:
        url: The betting page URL.
        section_index: Which market section to scrape (0=word list, 1=over/under, 2=combinations).

    Returns:
        A dictionary mapping word/phrase to odds (e.g., {"AI": 4.00, "Grønland": 1.03})

    Raises:
        ValueError: If no betting options could be found (indicates site structure changed).
    """
    logger.info(f"Scraping betting odds from {url} (section {section_index})")

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        try:
            await page.goto(url, wait_until="networkidle")

            # Handle cookie banner - try to reject/close it
            reject_button = page.locator('button:has-text("Afvis")')
            if await reject_button.count() > 0:
                await reject_button.click(timeout=2000)
                logger.info("Rejected cookies")
            else:
                raise ValueError("No button found to reject cookies")
            await page.wait_for_timeout(500)

            # Click all "Vis mere" (Show more) buttons to load all betting options
            max_clicks = 20
            for i in range(max_clicks):
                try:
                    vis_mere = page.locator('button:has-text("Vis mere")')
                    count = await vis_mere.count()
                    if count == 0:
                        logger.info(f"All items loaded after {i} clicks")
                        break
                    await vis_mere.first.click()
                    await page.wait_for_timeout(500)
                except Exception:
                    break

            # Get the specific market section
            market_sections = page.locator('[class*="eventMarketWrapper"]')
            section_count = await market_sections.count()

            if section_index >= section_count:
                raise ValueError(
                    f"Section index {section_index} out of range (found {section_count} sections)"
                )

            target_section = market_sections.nth(section_index)

            # Extract all betting options from the target section using data-testid
            outcomes = target_section.locator('[data-testid="outcome-button"]')
            total_count = await outcomes.count()
            logger.info(
                f"Found {total_count} betting options in section {section_index}/{section_count}"
            )

            betting_odds = {}
            for i in range(total_count):
                outcome = outcomes.nth(i)

                # Extract label from the outcome description span
                label_elem = outcome.locator('[data-testid="outcome-odds-description"]')
                if await label_elem.count() == 0:
                    logger.warning(f"No label found for outcome {i}")
                    continue

                label = await label_elem.text_content()
                if not label:
                    continue

                label = label.strip()

                # Extract odds - look for the price span within outcome-odds
                odds_elem = outcome.locator('[data-testid="outcome-odds"]').locator(
                    '[class*="outcomePriceCommon"]'
                )
                if await odds_elem.count() == 0:
                    logger.warning(f"No odds found for '{label}'")
                    continue

                odds_text = await odds_elem.text_content()
                if not odds_text:
                    continue

                try:
                    # Convert comma to dot for float parsing
                    odds = float(odds_text.strip().replace(",", "."))
                    betting_odds[label] = odds
                except ValueError as e:
                    logger.warning(
                        f"Could not parse odds '{odds_text}' for '{label}': {e}"
                    )

        finally:
            await browser.close()

    if not betting_odds:
        raise ValueError("No betting odds found - site structure may have changed")

    logger.info(f"Extracted {len(betting_odds)} betting options")
    return betting_odds


async def load_monarchs(url: str = MONARCHS_URL) -> list[Monarch]:
    """Download and parse the list of Danish monarchs from Wikipedia.

    Returns a list of monarchs from 1941 onwards, with their names and reign periods.

    Returns:
        List of Monarch objects, where end_year is None if still reigning.
        Example: [Monarch("Christian X", 1912, 1947), Monarch("Frederick IX", 1948, 1971), ...]

    Raises:
        ValueError: If the table structure is unexpected or parsing fails.
        aiohttp.ClientError: If the request fails or returns non-200 status.
    """
    logger.info(f"Downloading monarchs list from {url}")

    soup = await download_soup(url)

    # We only need the Schleswig-Holstein-Sonderburg-Glücksburg branch
    tables = soup.find_all("table", class_="wikitable")
    if not tables:
        raise ValueError("No wikitable found on Wikipedia page")

    table = tables[-1]
    logger.info("Found monarchs table")

    monarchs: list[Monarch] = []

    rows = table.find_all("tr")
    for row in rows:
        # First cell contains the info we need
        first_td = row.find("td")
        if not first_td:
            continue

        # Name is in the first <span>
        name_span = first_td.find("span")
        if not name_span:
            continue

        name = name_span.get_text(strip=True)

        # Find year patterns (4-digit years)
        cell_text = first_td.get_text(separator=" ", strip=True)
        years = re.findall(r"\b(1\d{3}|20\d{2})\b", cell_text)
        if len(years) < 1:
            logger.warning(f"Could not find years for {name}, skipping")
            continue

        start_year = int(years[0])
        end_year = int(years[1]) if len(years) >= 2 else None

        # Ignore monarchs who never held a New Year's Eve speech
        if end_year is not None and end_year < 1941:
            continue

        logger.info(f"Found monarch: {name} ({start_year}–{end_year or 'present'})")
        monarchs.append(Monarch(name=name, start_year=start_year, end_year=end_year))

    if not monarchs:
        raise ValueError(
            "No monarchs found - Wikipedia table structure may have changed"
        )

    logger.info(f"Found {len(monarchs)} monarchs from 1941 onwards")
    return monarchs


async def load_leipzig_corpus(url: str = LEIPZIG_URL) -> list[CorpusWord]:
    """Download and parse the Leipzig Corpora Collection Danish dataset.

    Downloads the tar.gz file, extracts the words file, and parses the word frequencies.

    Args:
        url: URL of the Leipzig Corpora tar.gz file

    Returns:
        List of CorpusWord objects from the corpus

    Raises:
        ValueError: If the words file cannot be found or parsed
        aiohttp.ClientError: If the download fails
    """
    logger.info(f"Downloading Leipzig corpus from {url}")
    content = await download_bytes(url)
    logger.info(f"Downloaded {len(content)} bytes")

    # The corpus is downloaded as a .tar.gz file. We are looking for a specific
    # file in that archie, named `*-words.txt`. This file is formatted as:
    #  <lineno>\t<word>\t<count>
    try:
        with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as tar:
            words_file = None
            for member in tar.getmembers():
                if member.name.endswith("-words.txt"):
                    words_file = member
                    break

            if not words_file:
                raise ValueError("Could not find words.txt file in archive.")

            logger.info(f"Found words file: {words_file.name}")

            # Extract and parse the words file
            words_data = tar.extractfile(words_file)
            if not words_data:
                raise ValueError(f"Could not extract {words_file.name}")

            # Parse tab-separated format: <lineno>\t<word>\t<count>
            # Use a dict to aggregate words by lowercase form (matches word_counts logic)
            word_counts: dict[str, int] = {}
            for line_num, line_bytes in enumerate(words_data, start=1):
                line = line_bytes.decode("utf-8").strip()
                if not line:
                    continue

                parts = line.split("\t")
                if len(parts) != 3:
                    logger.warning(
                        f"Line {line_num} has {len(parts)} columns (expected 3): {line[:100]}"
                    )
                    continue

                try:
                    # Corpus is mixed case, we only want to evaluate lower case words.
                    word = parts[1].strip().lower()

                    if not word:
                        continue

                    # The corpus contains repeats of the same word with different
                    # capitalization. We aggregate these duplicate words by summing.
                    count = int(parts[2].strip())
                    word_counts[word] = word_counts.get(word, 0) + count
                except (IndexError, ValueError) as e:
                    logger.warning(
                        f"Could not parse line {line_num}: {line[:100]} - {e}"
                    )

    except tarfile.TarError as e:
        raise ValueError(f"Could not extract tar.gz file: {e}") from e

    if not word_counts:
        raise ValueError("No word data found in corpus file")

    # Convert dict to list of CorpusWord objects, sorted by frequency (descending)
    corpus = [
        CorpusWord(word=word, count=count)
        for word, count in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
    ]

    logger.info(f"Parsed {len(corpus)} unique words from Leipzig corpus")
    return corpus
