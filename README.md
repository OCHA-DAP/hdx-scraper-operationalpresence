# Operational Presence Pipeline

[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-operationalpresence/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-operationalpresence/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-operationalpresence/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-operationalpresence?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This pipeline retrieves operational presence (3W — Who, What, Where) data from
HDX, processes it via a Google Sheets configuration, and publishes an
organisations dataset and a 3W dataset back to HDX. It makes
HDX read calls (one search call for all datasets tagged "operational
presence" plus one resource download per country), a small number of Google
Sheets API calls to fetch per-country processing metadata, and 2 HDX writes.
Temporary per-country resource files (10 KB to 1 MB each in CSV, XLS, XLSX, or
ODS format) are downloaded and deleted after processing. Organisation names are
normalised and matched against a lookup database, free-text sector names are
mapped to standardised sector codes, location strings are fuzzy-matched to
admin-1 and admin-2 P-codes, and the results are aggregated into a global
organisations CSV and a global 3W CSV for the HAPI output.

## Data Pipeline

### API reads

- **HDX dataset search** (1 read): searches for all datasets tagged "operational
  presence" to discover per-country source datasets.
- **Per-country resource downloads** (~one read per country): downloads the
  operational presence resource for each country (CSV, XLS, XLSX, or ODS format,
  10 KB to 1 MB each).
- **Google Sheets API** (small number of reads): fetches per-country processing
  metadata (column mappings, sector lookups, org name corrections).

### API writes (2 calls per run)

- **Organisations dataset** (1 write): a global CSV listing all organisations and
  their attributes, normalised against a lookup database.
- **3W dataset** (1 write): a global CSV of Who, What, Where rows enriched with
  standardised sector codes and admin P-codes.

### Temporary files

- Per-country resource files (10 KB to 1 MB each, in CSV, XLS, XLSX, or ODS
  format), downloaded and deleted after processing.

### Uploaded files

- Global organisations CSV.
- Global 3W CSV.

### Transformations

1. **Organisation normalisation**: free-text organisation names are matched and
   normalised against a lookup database; acronyms are resolved.
2. **Sector mapping**: free-text sector names are mapped to standardised sector
   codes.
3. **P-code fuzzy matching**: location strings are fuzzy-matched to admin-1 and
   admin-2 P-codes using the COD admin boundary registry.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The script reads the key
 **hdx-scraper-operationalpresence** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

You will also need to set the `GSHEET_AUTH` environment variable with Google
Sheets service account credentials (JSON string).

To run, execute:

```shell
    uv run python -m hdx.scraper.operationalpresence
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
