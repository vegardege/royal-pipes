# Royal Pipes

Beheading royalty has fallen out of fashion. The Danes, however, have found different use for their royalty: betting on which words will be used in the yearly Nytårstale (new year's eve speech).

This project contains the [Dagster](https://dagster.io) pipelines needed to scrape the historical speeches and analyze their content. The pipelines output a [SQLite](https://sqlite.org/) database described below.

## Features

- **Historical Speech Scraping**: Automatically downloads all historical New Year speeches from the official royal website
- **Word Frequency Analysis**: Computes word counts across all speeches and stores them in a SQLite database
- **Betting Odds Tracking**: Scrapes current betting odds from Danske Spil for which words will appear in the upcoming speech
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

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)

