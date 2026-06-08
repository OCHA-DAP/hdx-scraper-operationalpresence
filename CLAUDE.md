# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-operationalpresence** retrieves operational presence data from HDX, processes it, and publishes org and 3W datasets back to HDX. It reads from Google Sheets and HDX resources, maps organisations and locations, then creates/updates HDX datasets.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.operationalpresence
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_operationalpresence.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline in `__main__.py`:

1. **`main`** — Reads configuration, authenticates with HDX, sets up Google Sheets access, then drives the pipeline.
2. **`Pipeline`** — Discovers datasets/resources on HDX, processes rows per country, and generates two output datasets (org and 3W).
3. **`Sheet`** — Handles Google Sheets auth and email notification.
4. **`org.py`** — Organisation lookup and acronym handling.
5. **`row.py`** — Per-row data processing.
6. **`date_processing.py`** — Date field normalisation.

### Key design points

- **Config directory**: `src/hdx/scraper/operationalpresence/config/` holds `project_configuration.yaml` and `hdx_dataset_static.yaml`.
- **Saved data**: Downloaded data can be cached in `saved_data/` via `--save`/`--use-saved` flags.

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-operationalpresence` entry.

Requires `GSHEET_AUTH` env var (JSON service account credentials) for Google Sheets access.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
