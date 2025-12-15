# Royal Pipes

Have you ever wanted a massively over-engineered set of data pipelines to analyse the Danish monarch's New Year's Eve speeches? Well this is your lucky day!

This project scrapes the speeches from the official source, as well as odds from Danske Spil, a corpus of the Danish language from Wortschatz Leipzig, and a list of monarch's from Wikipedia. The pipelines are orchestrated with [Dagster](https://dagster.io) and finally output a [SQLite](https://sqlite.org/) database with raw data and pre-analysed numbers.

The data is used by [The King's Speech](https://kingsspeech.pebblepatch.dev/). Both the page and pipelines are under active development and will be improved over the next few weeks.

![Pipeline Architecture](assets/pipelines.png)

## Features

- **Historical Speech Scraping**: Automatically downloads all historical New Year speeches from the official royal website
- **Word Frequency Analysis**: Computes word counts across all speeches and stores them in a SQLite database
- **Betting Odds Tracking**: Scrapes current betting odds from Danske Spil for which words will appear in the upcoming speech
- **Danish Language Corpus**: Downloads general Danish word frequencies from Leipzig Corpora Collection for comparison
- **Data Quality Checks**: Automated checks to ensure data integrity and completeness

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Installing Playwright browsers

This project uses Playwright to scrape betting odds from Danske Spil. After installing dependencies, you need to install the Playwright browser:

```bash
uv run playwright install chromium
```

Or if you're using pip:

```bash
playwright install chromium
```

This downloads the Chromium browser that Playwright uses for web scraping.

### Setting up persistent storage

To ensure Dagster remembers materializations across restarts, set the `DAGSTER_HOME` environment variable.

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Data Storage

All data is stored locally following the [XDG Base Directory Specification](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html):

```
~/.local/share/royal-pipes/
├── speeches/            # Scraped speech text files (YYYY.txt)
└── analytics.db         # SQLite database with word counts and analysis
```

The storage location respects the `XDG_DATA_HOME` environment variable if set.

### Database Schema

The `analytics.db` SQLite database contains the following tables:

- **`speeches`**: Metadata about each speech (year, word count, monarch)
- **`word_counts`**: Word frequencies per year (year, word, count)
- **`odds`**: Current betting odds from Danske Spil (word, odds)
- **`odds_count`**: Historical frequency of betting words (year, word, count)
- **`corpus`**: General Danish word frequencies from Leipzig Corpora (word, count)

## Data Sources

This project uses the following data sources:

- **Royal Speeches**: Official New Year speeches from [Kongehuset.dk](https://www.kongehuset.dk/)
- **Betting Odds**: Betting markets from [Danske Spil](https://danskespil.dk/)
- **Danish Language Corpus**: Word frequency data from the [Leipzig Corpora Collection](https://wortschatz.uni-leipzig.de/en/download/)
  - Dataset: Danish Mixed 1M (2014)
  - Source: Wortschatz Leipzig, University of Leipzig
  - License: Creative Commons Attribution-NonCommercial 4.0 International License
  - Citation: Dirk Goldhahn, Thomas Eckart and Uwe Quasthoff (2012): "Building Large Monolingual Dictionaries at the Leipzig Corpora Collection: From 100 to 200 Languages." *Proceedings of LREC'12*.
