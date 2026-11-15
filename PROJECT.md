# ECE Big Data Processing Project – IMDb

All project code lives in the [`Projet/`](Projet/) folder. Results and
methodology are in [`REPORT.md`](REPORT.md).

## Contributors

| Name | GitHub | Email | Main responsibilities |
|---|---|---|---|
| Mael Vaudin | `ChaseX9` | mael.vaudin@edu.ece.fr | Infrastructure, data ingestion |
| Hugo Riviere | `hugoriviere` | hugo.riviere@edu.ece.fr | Data exploration, open question |
| Thibault Rabbé | `ThibaultR14` | thibault.rabbe@edu.ece.fr | Data enrichment, streaming |

## Project layout

```text
Projet/
├── compose.yaml          # Kafka + Spark/Jupyter containers
├── .env.example          # Settings to copy into .env
├── requirements.txt      # Packages of the Kafka producer container
├── scripts/
│   ├── download_imdb.py  # Downloads the IMDb dumps into data/raw
│   ├── fetch_languages.py  # Original languages from Wikidata (exploration Q6)
│   ├── report_tables.py  # Writes the result tables into REPORT.md
│   ├── fetch_wikipedia.py  # Downloads Wikipedia articles (enrichment)
│   └── wiki_producer.py  # Wikipedia change feed -> Kafka (streaming)
├── jobs/
│   ├── ingest_imdb.py    # data/raw (TSV) -> data/bronze (Parquet)
│   ├── check_setup.py    # Checks Spark, tables and Kafka
│   ├── missing_values.py # Null / empty values per column
│   ├── quality_checks.py # Other data quality issues
│   ├── prepare_data.py   # data/bronze -> data/silver (cleaned tables)
│   ├── exploration.py    # Data exploration questions
│   ├── enrichment.py     # Enrichment targets and Spark processing
│   ├── streaming.py      # Structured Streaming lookup / query / report
│   └── open_question.py  # Open question
├── src/imdb_project/     # Python package: shared code (paths, schemas, Spark
│                         # session, I/O, definitions, Wikimedia client) and
│                         # one module per part
├── notebooks/            # One notebook per part (01 to 05): method and results
└── data/                 # Not committed: raw/, bronze/, silver/, reports/,
                          # results/, enrichment/, streaming/
```

## Running the project from scratch

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or
  Docker Engine with the Compose plugin), with **at least 8 GB of memory**
  allocated to Docker and about **20 GB of free disk space**.
- Git.
- An Internet connection (IMDb download, Wikipedia, Kafka connector).

Everything runs inside Docker; nothing else has to be installed. The same
commands work on Windows, macOS (Intel and Apple Silicon) and Linux: all the
images used are published for both `amd64` and `arm64`. On Windows, run them
in PowerShell or Git Bash; on macOS and Linux, in any terminal.

On Docker Desktop (Windows and macOS), the memory given to Docker is set in
*Settings → Resources*.

### 1. Clone the repository and configure

```bash
git clone https://github.com/ChaseX9/Lab-Project-Big-Data-processing.git
cd Lab-Project-Big-Data-processing/Projet
cp .env.example .env
```

Edit `.env` if needed (for instance lower `SPARK_DRIVER_MEMORY` on a machine
with little memory).

### 2. Start the containers

```bash
docker compose up -d
```

This starts two containers (you should see `Started` or `Running` for both):

- `projet_kafka`: Kafka broker, reachable at `localhost:9092` from the host
  and at `kafka:19092` from the Spark container.
- `projet_spark`: Spark 4.2 with JupyterLab. The `Projet/` folder is mounted
  at `/home/jovyan/projet`.

### 3. Download and ingest the IMDb dataset

```bash
docker compose exec spark python scripts/download_imdb.py
docker compose exec spark python jobs/ingest_imdb.py
```

The first command downloads the seven `*.tsv.gz` files (about 1.5 GB) into
`data/raw/`. It takes **about 10 minutes**, depending on the connection, and
prints `[done] <file>` for each file. The second converts them to Parquet in
`data/bronze/`, without changing their content, and prints
`[ingest] <table>: <n> rows` for each of the seven tables.

The download script only uses the Python standard library, so it can also
be run outside Docker with any Python 3.9+ (for instance on macOS,
`python3 scripts/download_imdb.py`, from the `Projet` folder). It skips the
files that are already downloaded; use `--force` to download them again.

### 4. Check the setup

```bash
docker compose exec spark python jobs/check_setup.py
```

Each line should start with `[ok  ]` (Spark, the seven bronze tables and
Kafka), and the script should end with `Setup OK`.

### 5. Data preparation

```bash
docker compose exec spark python jobs/missing_values.py
docker compose exec spark python jobs/quality_checks.py
docker compose exec spark python jobs/prepare_data.py
```

- `missing_values.py` writes the share of null / empty values per column to
  `data/reports/missing_values/`.
- `quality_checks.py` writes the other data quality issues to
  `data/reports/quality_issues/` and `data/reports/invalid_casts/`.
- `prepare_data.py` writes the cleaned tables to `data/silver/` and a summary
  to `data/reports/cleaning_summary/`. All later steps read `data/silver/`.

Each job takes about 5 to 10 minutes on a laptop. The first two end with
`Report written to ...` / `Reports written to ...`; `prepare_data.py` ends
with the `=== Cleaning summary ===` and `=== Flags per table ===` tables.

The notebook `notebooks/01_data_preparation.ipynb` explains each step and
shows the results (open it in JupyterLab, see step 11, after running the
three jobs).

### 6. Data exploration

```bash
docker compose exec spark python jobs/exploration.py q6        # Q6 targets
docker compose exec spark python scripts/fetch_languages.py    # Q6 languages
docker compose exec spark python jobs/exploration.py           # all questions
docker compose exec spark python jobs/exploration.py q1 q4     # some questions
```

IMDb has no original language, which Q6 needs: the first command writes the
list of successful films, and `fetch_languages.py` downloads their original
language from Wikidata (property P364, about 1 minute at one request per
second) into `data/enrichment/raw/languages.jsonl`.

Results are written to `data/results/<name>/` as Parquet and CSV. All
questions together take about 15 minutes; each question prints its result
table, and `data/results/` then contains one folder per question. The notebook
`notebooks/02_exploration.ipynb` explains the method of each question and
shows the results.

### 7. Data enrichment (Wikipedia)

```bash
docker compose exec spark python jobs/enrichment.py targets
docker compose exec spark python scripts/fetch_wikipedia.py
docker compose exec spark python jobs/enrichment.py build
```

1. `targets` selects the 10 best-rated feature films (among the films with
   at least 1,000 votes) per genre and year between 1980 and 2025, and their
   first 3 principals.
2. `fetch_wikipedia.py` downloads the matching English Wikipedia articles.
   IMDb ids are mapped to articles through Wikidata (property P345). It sends
   one request per second (several hours for all targets), can be stopped at
   any time and resumes where it stopped. Use `--limit 50` for a quick test.
3. `build` cleans the articles with Spark and writes
   `data/enrichment/enriched_films/` (Parquet and JSON): one row per film
   with its synopsis, its principals' biographies, the source URL of every
   text and a note when information is missing. It ends by printing the
   `=== Coverage ===` table, the notes for missing texts and a sample.

### 8. Streaming (Wikipedia changes through Kafka)

```bash
docker compose exec spark python jobs/streaming.py lookup
docker compose exec spark python jobs/streaming.py run --minutes 27 --starting-offsets latest --fresh   # terminal 1
docker compose run --rm producer --minutes 25                                                   # terminal 2
docker compose exec spark python jobs/streaming.py report
```

1. `lookup` lists the Wikipedia articles of the enriched films and
   principals (run it after the enrichment step).
2. `run` is the Spark Structured Streaming query. Start it first and wait
   for the first micro-batch (about one minute): with `latest`, it only
   reads the events sent after it started. It writes the relevant changes,
   the windowed counts and the streaming metrics to `data/streaming/`, and
   computes the counts of the last windows when it stops. The `--fresh`
   option deletes the checkpoints and outputs of the previous run (but keeps
   `lookup/`), so that each test starts from scratch.
3. The `producer` service reads the live English Wikipedia change feed and
   sends it to the Kafka topic `wikipedia-changes`.
4. `report` prints the summary tables in the terminal: relevant changes per
   entity type, most changed entities, windows and streaming metrics.

Keep the computer awake during the test: a sleeping machine stops the
containers and the live feed.

### 9. Open question

```bash
docker compose exec spark python jobs/open_question.py                         # about 30 min
docker compose exec spark python jobs/open_question.py path nm0000102 nm0000821   # one chain
```

The first command builds the co-star graph (actors and actresses linked by
the feature films with at least 1,000 votes) for all years and for each
decade since 1980, and computes its separation statistics and its centre.
It takes **about 30 minutes** and prints a `##### Graph: <period>` block
for each graph. Results are written to `data/results/open_*`. The second command prints the
shortest chain of actors and films between two people, given by their IMDb
ids (here Kevin Bacon and Amitabh Bachchan).

### 10. Result tables of `REPORT.md`

```bash
docker compose exec spark python scripts/report_tables.py
```

Writes the result tables of every part into `REPORT.md`, between the
`<!-- results:... -->` markers of each "Results" section, from the outputs
of the previous steps. It prints `[ok] <part>` for each part updated, or
`[skip] <part>` with the reason when the outputs of a part are missing. The
Spark container mounts the repository root at `/home/jovyan/repo` for this
purpose.

The notebooks `notebooks/03_enrichment.ipynb`, `04_streaming.ipynb` and
`05_open_question.ipynb` explain the method and show the results of steps 7
to 9.

### 11. Open JupyterLab

```bash
docker compose logs spark | grep "127.0.0.1:8888"
```

Open the printed URL (it contains the access token) and go to
`projet/notebooks/` (inside the container, the `Projet/` folder is mounted
as `projet/`). The Spark UI is at <http://localhost:4040> while a
session is running.

### 12. Stop

```bash
docker compose down
```

Data in `data/` is kept between runs.

## Use of AI / LLM tools

### How we worked with AI tools

AI tools were used as supervised assistants. **No AI tool took any decision
in this project.**

- **Decisions.** Every choice was made by the student in charge of the part:
  the definitions, thresholds and methods, the alternatives to test, and the
  trade-offs to keep. These choices are explained in `REPORT.md`.
- **Code.** The code was not generated by an AI tool in one go or without
  supervision. For each module and each question, the student in charge
  first described the question or what they wanted to do, step by step: the
  data to use, the method and the expected output. Claude Code/ Gemini then wrote
  the code following this description. The student ran it, checked the
  results and asked for changes until the code matched what they wanted.
- **Python style.** At the end, Claude Code reviewed and edited the whole
  code base so that it follows the Python style guides required by the
  course (layout, naming, docstrings and comments).
- **`REPORT.md`.** Only the result tables are written by a program
  (`Projet/scripts/report_tables.py`). All the text (methodology, reasons,
  alternatives, trade-offs and interpretation) was written by each student
  for their own part. At the end, Gemini was used only to format the
  Markdown so that the file follows the Markdown guidelines of the course.

The table below lists every use of an AI tool.

| Tool | Used by | Tasks |
|---|---|---|
| Claude Code (Anthropic) | Mael Vaudin | Final version of the infrastructure code: `Projet/compose.yaml`, `Projet/scripts/download_imdb.py`, `Projet/jobs/ingest_imdb.py`, `Projet/jobs/check_setup.py`, `Projet/src/imdb_project/` (`config.py`, `schemas.py`, `spark.py`, `io.py`, `definitions.py`, `wikimedia.py`) |
| Claude Code (Anthropic) | Mael Vaudin | Final version of the data preparation code: `Projet/src/imdb_project/quality.py`, `Projet/src/imdb_project/preparation.py`, `Projet/jobs/missing_values.py`, `Projet/jobs/quality_checks.py`, `Projet/jobs/prepare_data.py` |
| Claude Code (Anthropic) | Mael Vaudin | Final version of the data preparation notebook, code and explanatory text: `Projet/notebooks/01_data_preparation.ipynb` |
| Claude Code (Anthropic) | Mael Vaudin | First version of the data exploration code: `Projet/src/imdb_project/exploration.py`, `Projet/jobs/exploration.py`, `Projet/scripts/fetch_languages.py` |
| Claude Code (Anthropic) | Mael Vaudin | Final version of the data exploration notebook, code and explanatory text: `Projet/notebooks/02_exploration.ipynb` |
| Claude Code (Anthropic) | Thibault Rabbé | First version of the data enrichment code: `Projet/src/imdb_project/enrichment.py`, `Projet/scripts/fetch_wikipedia.py`, `Projet/jobs/enrichment.py` |
| Claude Code (Anthropic) | Thibault Rabbé | Final version of the streaming code: `Projet/scripts/wiki_producer.py`, `Projet/src/imdb_project/streaming.py`, `Projet/jobs/streaming.py`, `producer` service of `Projet/compose.yaml` |
| Claude Code (Anthropic) | Hugo Riviere | Final version of the open question code: `Projet/src/imdb_project/open_question.py`, `Projet/jobs/open_question.py` |
| Claude Code (Anthropic) | Hugo Riviere | Script that generates the result tables of `REPORT.md` from the job outputs: `Projet/scripts/report_tables.py` (the tables are program outputs; no explanation or reasoning in `REPORT.md` was written with an AI tool) |
| Claude Code (Anthropic) | Hugo Riviere | Creation of the `.gitattributes` file and subsequent updates to `compose.yaml` volume mounts. |
| Claude Code (Anthropic) | Each members | Final review and edit of the whole Python code base to follow the Python style guides of the course (layout, naming, docstrings and comments) |
| Gemini (Google) | All members | Final formatting of `REPORT.md` to follow the Markdown guidelines of the course (headings, lists, tables, spacing); the explanations and reasoning were not changed |



Apart from the Markdown formatting described above, no AI tool was used to
write the explanations or reasoning in `REPORT.md`.
