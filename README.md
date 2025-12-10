# Royal Pipes

Beheading royalty has fallen out of fashion. The Danes, however, have found different use for their royalty: betting on which words will be used in the yearly Nytårstale (new year's eve speech).

This project contains the [Dagster](https://dagster.io) pipelines needed to scrape the historical speeches and analyze their content. The pipelines output a [SQLite](https://sqlite.org/) database described below.

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
- **Betting Odds**: Current betting markets from [Danske Spil](https://danskespil.dk/)
- **Danish Language Corpus**: Word frequency data from the [Leipzig Corpora Collection](https://wortschatz.uni-leipzig.de/en/download/)
  - Dataset: Danish Mixed 1M (2014)
  - Source: Wortschatz Leipzig, University of Leipzig
  - License: Creative Commons Attribution-NonCommercial 4.0 International License
  - Citation: D. Goldhahn, T. Eckart & U. Quasthoff: Building Large Monolingual Dictionaries at the Leipzig Corpora Collection: From 100 to 200 Languages. In: *Proceedings of the 8th International Language Resources and Evaluation (LREC'12)*, 2012

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
