"""Download the IMDb non-commercial datasets into ``data/raw``.

Usage (from the ``projet`` folder, inside or outside the Spark container):

    python scripts/download_imdb.py                 # all tables
    python scripts/download_imdb.py title.basics    # some tables only
    python scripts/download_imdb.py --force         # download again

Only the Python standard library is used, so no installation is needed.
"""

import argparse
import shutil
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from imdb_project.config import IMDB_BASE_URL, IMDB_TABLES, RAW_DIR, raw_path  # noqa: E402

CHUNK_SIZE = 1024 * 1024


def download(table, force=False):
    target = raw_path(table)
    if target.exists() and not force:
        print(f"[skip] {target.name} already present")
        return
    url = f"{IMDB_BASE_URL}/{table}.tsv.gz"
    partial = target.with_suffix(target.suffix + ".part")
    print(f"[get ] {url}")
    request = urllib.request.Request(url, headers={"User-Agent": "ece-imdb-project"})
    with urllib.request.urlopen(request) as response, open(partial, "wb") as out:
        total = int(response.headers.get("Content-Length", 0))
        done = 0
        while True:
            chunk = response.read(CHUNK_SIZE)
            if not chunk:
                break
            out.write(chunk)
            done += len(chunk)
            if total:
                print(f"\r       {done / total:6.1%} of {total / 1e6:.0f} MB", end="")
    print()
    shutil.move(partial, target)
    print(f"[done] {target}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tables", nargs="*", help="tables to download (default: all)")
    parser.add_argument("--force", action="store_true", help="download even if present")
    args = parser.parse_args()

    tables = args.tables or IMDB_TABLES
    unknown = sorted(set(tables) - set(IMDB_TABLES))
    if unknown:
        parser.error(f"unknown tables {unknown}, choose from {IMDB_TABLES}")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for table in tables:
        download(table, force=args.force)


if __name__ == "__main__":
    main()
