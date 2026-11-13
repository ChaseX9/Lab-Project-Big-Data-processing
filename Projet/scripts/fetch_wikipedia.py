"""Download the Wikipedia articles of the enrichment targets.

Reads the target lists written by ``jobs/enrichment.py targets``
(``data/enrichment/targets/{films,people}.txt``, one IMDb id per line) and
appends one JSON record per id to ``data/enrichment/raw/{films,people}.jsonl``.

How an IMDb id is mapped to Wikipedia: Wikidata stores IMDb ids (property
P345) and links each item to its English Wikipedia article, so no title
search (and no guess) is needed.

- Films: the full plain text of the article (the synopsis is extracted later
  in Spark).
- People: the lead section of the article (the biography summary).

Politeness: one request at a time, a pause between requests (``--delay``),
a descriptive User-Agent, ``maxlag`` on the MediaWiki API and retries that
honour ``Retry-After`` on HTTP 429 / 5xx and network errors. Ids already present in the output
are skipped, so the script can be stopped and restarted.

Usage (inside the Spark container):

    python scripts/fetch_wikipedia.py                  # films and people
    python scripts/fetch_wikipedia.py --kind films --limit 50
"""

import argparse
import json
import sys
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from imdb_project.config import ENRICHMENT_DIR  # noqa: E402
from imdb_project.wikimedia import (  # noqa: E402
    WIKIDATA_PAGE,
    WIKIDATA_SPARQL,
    WIKIPEDIA_API,
    WIKIPEDIA_PAGE,
    Client,
    already_fetched,
    chunks,
    now,
)

SPARQL_BATCH = 200       # IMDb ids per Wikidata query
EXTRACT_BATCH = 20       # MediaWiki limit for lead-section extracts


def wikidata_articles(client, imdb_ids):
    """Map IMDb ids to ``(wikidata id, English Wikipedia title)``."""
    values = " ".join(f'"{i}"' for i in imdb_ids)
    query = f"""
        SELECT ?imdb ?item ?article WHERE {{
          VALUES ?imdb {{ {values} }}
          ?item wdt:P345 ?imdb .
          OPTIONAL {{
            ?article schema:about ?item ;
                     schema:isPartOf <https://en.wikipedia.org/> .
          }}
        }}"""
    data = client.get(WIKIDATA_SPARQL, {"query": query, "format": "json"})
    result = {}
    for row in data["results"]["bindings"]:
        imdb = row["imdb"]["value"]
        qid = row["item"]["value"].rsplit("/", 1)[-1]
        title = None
        if "article" in row:
            title = urllib.parse.unquote(row["article"]["value"].rsplit("/wiki/", 1)[-1])
            title = title.replace("_", " ")
        # Keep the item that has an article when several items share an id.
        if imdb not in result or (title and not result[imdb][1]):
            result[imdb] = (qid, title)
    return result


def page_extracts(client, titles, intro_only):
    """Plain-text extracts of Wikipedia pages, keyed by requested title."""
    params = {
        "action": "query",
        "prop": "extracts",
        "explaintext": 1,
        "exsectionformat": "wiki",
        "redirects": 1,
        "titles": "|".join(titles),
        "format": "json",
        "formatversion": 2,
        "maxlag": 5,
    }
    if intro_only:
        params["exintro"] = 1
        params["exlimit"] = "max"
    data = client.get(WIKIPEDIA_API, params)
    query = data.get("query", {})
    # Follow normalisation and redirects back to the requested title.
    alias = {t: t for t in titles}
    for step in query.get("normalized", []) + query.get("redirects", []):
        for requested, current in list(alias.items()):
            if current == step["from"]:
                alias[requested] = step["to"]
    pages = {p["title"]: p for p in query.get("pages", [])}
    out = {}
    for requested, final in alias.items():
        page = pages.get(final)
        if page and not page.get("missing"):
            out[requested] = (page["title"], page.get("extract", ""))
    return out


def record(imdb_id, kind, status, qid=None, title=None, text=None):
    return {
        "imdb_id": imdb_id,
        "kind": kind,
        "wikidata_id": qid,
        "wikipedia_title": title,
        "source_url": WIKIPEDIA_PAGE + urllib.parse.quote(title.replace(" ", "_"))
        if title else None,
        "wikidata_url": WIKIDATA_PAGE + qid if qid else None,
        "retrieved_at": now(),
        "status": status,
        "text": text,
    }


def fetch(client, kind, limit):
    targets_path = ENRICHMENT_DIR / "targets" / f"{kind}.txt"
    output = ENRICHMENT_DIR / "raw" / f"{kind}.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)

    ids = [line.strip() for line in open(targets_path, encoding="utf-8") if line.strip()]
    done = already_fetched(output)
    todo = [i for i in ids if i not in done][: limit or None]
    print(f"[{kind}] {len(ids):,} targets, {len(done):,} already fetched, "
          f"{len(todo):,} to fetch")

    intro_only = kind == "people"
    page_batch = EXTRACT_BATCH if intro_only else 1
    with open(output, "a", encoding="utf-8") as out:
        for batch_number, batch in enumerate(chunks(todo, SPARQL_BATCH), start=1):
            mapping = wikidata_articles(client, batch)
            with_article = [i for i in batch if mapping.get(i, (None, None))[1]]
            for i in batch:
                if i not in with_article:
                    qid = mapping.get(i, (None, None))[0]
                    out.write(json.dumps(record(i, kind, "no_article", qid)) + "\n")

            for group in chunks(with_article, page_batch):
                titles = {i: mapping[i][1] for i in group}
                extracts = page_extracts(client, list(set(titles.values())), intro_only)
                for i in group:
                    qid, title = mapping[i]
                    if title in extracts:
                        final_title, text = extracts[title]
                        rec = record(i, kind, "ok", qid, final_title, text)
                    else:
                        rec = record(i, kind, "missing_page", qid, title)
                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                out.flush()
            print(f"[{kind}] batch {batch_number}: {len(batch)} ids, "
                  f"{len(with_article)} with an article")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--kind", choices=["films", "people", "all"], default="all")
    parser.add_argument("--limit", type=int, default=0,
                        help="maximum number of new ids per kind (0 = no limit)")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="seconds between two requests (default: 1)")
    args = parser.parse_args()

    client = Client(args.delay)
    kinds = ["films", "people"] if args.kind == "all" else [args.kind]
    for kind in kinds:
        fetch(client, kind, args.limit)


if __name__ == "__main__":
    main()
