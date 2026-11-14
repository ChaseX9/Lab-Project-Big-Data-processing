"""Download the original language of films from Wikidata (exploration Q6).

IMDb has no original language. Wikidata stores it in the property
"original language of film or TV show" (P364), and links its items to IMDb
ids through the property P345.

Reads the target list written by ``jobs/exploration.py q6``
(``data/enrichment/targets/language_films.txt``, one IMDb id per line) and
appends one JSON record per id to ``data/enrichment/raw/languages.jsonl``:

- ``languages``: the P364 values in the order of the Wikidata item
  (statements with the "preferred" rank first, "deprecated" ones skipped),
  as ``{"id": "Q1860", "label": "English"}``;
- ``status``: ``ok``, ``no_item`` (no Wikidata item has this IMDb id) or
  ``no_language`` (the item has no P364 value);
- ``source_url``: the Wikidata page the languages come from.

Politeness: same client as ``fetch_wikipedia.py``, in
``imdb_project/wikimedia.py`` (one request at a time,
a pause between requests, descriptive User-Agent, ``maxlag``, retries).
Ids already present in the output are skipped, so the script can be stopped
and restarted.

Usage (from the ``projet`` folder):

    python scripts/fetch_languages.py
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from imdb_project.wikimedia import (  # noqa: E402
    WIKIDATA_PAGE,
    WIKIDATA_SPARQL,
    Client,
    already_fetched,
    chunks,
    now,
)
from imdb_project.config import ENRICHMENT_DIR  # noqa: E402

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
SPARQL_BATCH = 200     # IMDb ids per SPARQL query
ENTITY_BATCH = 50      # Wikidata API limit for wbgetentities
LANGUAGE_PROPERTY = "P364"


def wikidata_items(client, imdb_ids):
    """Map IMDb ids to Wikidata item ids (the first one when several)."""
    values = " ".join(f'"{i}"' for i in imdb_ids)
    query = f"""
        SELECT ?imdb ?item WHERE {{
          VALUES ?imdb {{ {values} }}
          ?item wdt:P345 ?imdb .
        }}"""
    data = client.get(WIKIDATA_SPARQL, {"query": query, "format": "json"})
    result = {}
    for row in data["results"]["bindings"]:
        imdb = row["imdb"]["value"]
        qid = row["item"]["value"].rsplit("/", 1)[-1]
        result.setdefault(imdb, qid)
    return result


def get_entities(client, ids, props):
    params = {
        "action": "wbgetentities",
        "ids": "|".join(ids),
        "props": props,
        "languages": "en",
        "format": "json",
        "maxlag": 5,
    }
    return client.get(WIKIDATA_API, params).get("entities", {})


def ordered_languages(entity):
    """P364 values of an item: preferred rank first, then the item order."""
    statements = entity.get("claims", {}).get(LANGUAGE_PROPERTY, [])
    values = []
    for rank in ("preferred", "normal"):
        for statement in statements:
            snak = statement.get("mainsnak", {})
            if statement.get("rank") == rank and snak.get("snaktype") == "value":
                qid = snak["datavalue"]["value"]["id"]
                if qid not in values:
                    values.append(qid)
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--delay", type=float, default=1.0,
                        help="seconds between two requests (default: 1)")
    args = parser.parse_args()
    client = Client(args.delay)

    targets_path = ENRICHMENT_DIR / "targets" / "language_films.txt"
    output = ENRICHMENT_DIR / "raw" / "languages.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)
    ids = [line.strip() for line in open(targets_path, encoding="utf-8") if line.strip()]
    done = already_fetched(output)
    todo = [i for i in ids if i not in done]
    print(f"[languages] {len(ids):,} targets, {len(done):,} already fetched, "
          f"{len(todo):,} to fetch")

    labels = {}
    with open(output, "a", encoding="utf-8") as out:
        for batch_number, batch in enumerate(chunks(todo, SPARQL_BATCH), start=1):
            items = wikidata_items(client, batch)
            languages = {}
            for group in chunks(sorted(set(items.values())), ENTITY_BATCH):
                entities = get_entities(client, group, "claims")
                for qid in group:
                    languages[qid] = ordered_languages(entities.get(qid, {}))

            unknown = sorted({q for v in languages.values() for q in v} - set(labels))
            for group in chunks(unknown, ENTITY_BATCH):
                for qid, entity in get_entities(client, group, "labels").items():
                    labels[qid] = entity.get("labels", {}).get("en", {}).get("value", qid)

            for imdb_id in batch:
                qid = items.get(imdb_id)
                langs = languages.get(qid, []) if qid else []
                status = "ok" if langs else ("no_language" if qid else "no_item")
                out.write(json.dumps({
                    "imdb_id": imdb_id,
                    "wikidata_id": qid,
                    "languages": [{"id": q, "label": labels.get(q, q)} for q in langs],
                    "status": status,
                    "source_url": WIKIDATA_PAGE + qid if qid else None,
                    "retrieved_at": now(),
                }, ensure_ascii=False) + "\n")
            out.flush()
            found = sum(1 for i in batch if languages.get(items.get(i)))
            print(f"[languages] batch {batch_number}: {len(batch)} ids, "
                  f"{len(items)} on Wikidata, {found} with a language")


if __name__ == "__main__":
    main()
