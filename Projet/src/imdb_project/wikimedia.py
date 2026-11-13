"""Polite HTTP access to the Wikimedia APIs (Wikipedia, Wikidata).

Used by ``scripts/fetch_wikipedia.py`` (enrichment).

Politeness: one request at a time, a fixed pause between two requests, a
descriptive User-Agent (Wikimedia User-Agent policy), and retries with a
growing pause on network errors and on HTTP 429 / 5xx, honouring
``Retry-After``; MediaWiki ``maxlag`` errors are retried as well.
"""

import json
import time
from datetime import datetime, timezone

import requests

USER_AGENT = (
    "ECE-BigData-IMDb-Project/1.0 "
    "(student project; https://github.com/ChaseX9/Lab-Project-Big-Data-processing)"
)
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_PAGE = "https://en.wikipedia.org/wiki/"
WIKIDATA_PAGE = "https://www.wikidata.org/wiki/"


class Client:
    """HTTP client with a fixed pause between requests and polite retries."""

    def __init__(self, delay, max_retries=5):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.delay = delay
        self.max_retries = max_retries
        self._last = 0.0

    def get(self, url, params):
        for attempt in range(self.max_retries):
            wait = self.delay - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            try:
                response = self.session.get(url, params=params, timeout=60)
            except (requests.ConnectionError, requests.Timeout,
                    requests.exceptions.ChunkedEncodingError) as error:
                backoff = 5 * 2 ** attempt
                print(f"  network error ({type(error).__name__}), retrying in {backoff}s")
                time.sleep(backoff)
                continue
            if response.status_code in (429, 500, 502, 503, 504):
                retry_after = int(response.headers.get("Retry-After", 5 * 2 ** attempt))
                print(f"  server busy ({response.status_code}), waiting {retry_after}s")
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and data.get("error", {}).get("code") == "maxlag":
                time.sleep(int(response.headers.get("Retry-After", 5)))
                continue
            return data
        raise RuntimeError(f"giving up on {url} after {self.max_retries} attempts")


def chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def already_fetched(path):
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as f:
        return {json.loads(line)["imdb_id"] for line in f if line.strip()}
