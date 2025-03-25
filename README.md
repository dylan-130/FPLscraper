# Fantasy Premier League (FPL) Data Scraper

This project is a Python-based asynchronous scraper designed to fetch player data from the Fantasy Premier League (FPL) API. Leveraging the `aiohttp` library, the scraper efficiently handles large-scale data requests while managing rate limits and connection issues. The scraped data includes details about players, their team names, and unique player IDs, which can be used for analysis, or integration with a further piece of software (as in my case).

---

## Features

- **Asynchronous Requests**: Efficiently fetches large volumes of data using `aiohttp` for concurrent API requests.
- **Rate Limit Handling**: Implements retry logic and exponential backoff when rate limits are encountered.
- **Error Logging**: Logs detailed errors, including connection issues, rate limits, and unexpected responses.
- **Data Output**: Saves player data into a JSON file (`player_data.json`), with each record containing:
  - Full Name
  - Team Name
  - Player ID
- **Resiliency**: Automatically retries failed pages up to a set maximum, tracking failures in a separate file (`failed_attempts.json`).

---

## Prerequisites

Before running the scraper, ensure you have the following installed:

- Python 3.7 or newer
- Required Python libraries:
  - `aiohttp`
  - `certifi`
  - `logging`

To install the dependencies, run:

```bash
pip install aiohttp certifi
