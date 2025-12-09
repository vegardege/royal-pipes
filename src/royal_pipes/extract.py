import logging
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

# Official source of yearly speeches
OFFICIAL_URL = "https://www.kongehuset.dk/monarkiet-i-danmark/nytaarstaler/"

# Danske Spil odds
BETTING_URL = "https://danskespil.dk/oddset/sports/competition/25652/underholdning/danmark/danmark-kongens-nytarstale/outrights"

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
            try:
                # Try "Afvis" (Reject) button first
                reject_button = page.locator('button:has-text("Afvis")')
                if await reject_button.count() > 0:
                    await reject_button.click(timeout=2000)
                    logger.info("Rejected cookies")
                else:
                    # Fall back to accepting if no reject option
                    await page.click('button:has-text("Accepter")', timeout=2000)
                    logger.info("Accepted cookies")
                await page.wait_for_timeout(500)
            except Exception:
                logger.info("No cookie banner found")

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
