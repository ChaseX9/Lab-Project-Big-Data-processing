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
from email.utils import parsedate_to_datetime

import requests

USER_AGENT = (
    "ECE-BigData-IMDb-Project/1.0 "
    "(student project; https://github.com/ChaseX9/Lab-Project-Big-Data-processing)"
)
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_PAGE = "https://en.wikipedia.org/wiki/"
WIKIDATA_PAGE = "https://www.wikidata.org/wiki/"


def retry_after(response, default):
    """Seconds to wait from a ``Retry-After`` header, else ``default``.

    The header is either a number of seconds or an HTTP date (RFC 9110).
    """
    value = response.headers.get("Retry-After")
    if value is None:
        return default
    try:
        return max(0, int(value))
    except ValueError:
        pass
    try:
        delta = parsedate_to_datetime(value) - datetime.now(timezone.utc)
        return max(0, int(delta.total_seconds()))
    except (TypeError, ValueError):
        return default


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
                wait = retry_after(response, 5 * 2 ** attempt)
                print(f"  server busy ({response.status_code}), waiting {wait}s")
                time.sleep(wait)
                continue
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and data.get("error", {}).get("code") == "maxlag":
                time.sleep(retry_after(response, 5))
                continue
            return data
        raise RuntimeError(f"giving up on {url} after {self.max_retries} attempts")


def chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def already_fetched(path):
    """Ids already saved in the JSON lines file ``path``.

    Lines that cannot be read are skipped (for instance the last line, cut
    when the script was stopped while writing): their ids are fetched again.
    """
    if not path.exists():
        return set()
    done = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                done.add(json.loads(line)["imdb_id"])
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    return done


def end_with_newline(path):
    """Add a line break if ``path`` ends with a cut line, so the next record
    appended to the file starts on its own line."""
    if path.exists() and path.stat().st_size > 0:
        with open(path, "rb+") as f:
            f.seek(-1, 2)
            if f.read(1) != b"\n":
                f.write(b"\n")
