# CAP Ecoscheme Application Processing

Pipeline for processing Spain's CAP (Common Agricultural Policy) land-use
application data for the 2023 and 2024 campaign years. It merges aid-line
records (AYUDAS) with parcel surface data (SUPERFICIES), classifies
ecoscheme rows, and computes per-row payment amounts using SIGPAC slope
data.

## Scope

This is a **specific, one-shot pipeline** for the 2023/2024 CAP campaign
years. It is not a general-purpose data processing framework. The input
files, classification tables, and ecoscheme range are all specific to this
dataset.

## Pipeline stages

```
merge → prefetch → process → sort
```

| Stage | What it does | Network? |
|-------|-------------|----------|
| **merge** | DuckDB left join AYUDAS + SUPERFICIES per year → `PAC_{year}_MERGED.csv` | No |
| **prefetch** | Download `pendiente_media` (slope) for every unique parcel referenced by LNA 510/511 rows → `data/sigpac_cache.jsonl` | **Yes** (rate-limited, 20 req/s global) |
| **process** | Classify ecoschemes + compute payments using the local cache → `PAC_{year}_PROCESSED.csv` | No |
| **sort** | DuckDB sort by `(SECUENCIAL, REC_CODIGO)` → `PAC_{year}_PROCESSED_SORTED.csv` | No |

Every stage is **idempotent and resumable**: re-running the same command
skips finished stages and continues where it left off.

## Input data

Place the following CSV files in `data/` (they are git-ignored):

- `data/PAC_2023_AYUDAS.csv`
- `data/PAC_2023_SUPERFICIES.csv`
- `data/PAC_2024_AYUDAS.csv`
- `data/PAC_2024_SUPERFICIES.csv`

Classification tables (tracked in the repo):

- `data/classification_2023.json`
- `data/classification_2024.json`

## Usage

```bash
# Install dependencies (uses uv)
uv sync

# Run the full pipeline for both years
.venv/bin/cap-process --years 2023 2024

# Run a single stage
.venv/bin/cap-process --years 2023 --stages process

# Limit to N rows (testing)
.venv/bin/cap-process --years 2023 --max-rows 1000

# Custom rate limit (default: 20 req/s)
.venv/bin/cap-process --years 2023 2024 --rate-limit 10
```

For long-running prefetches, use the launch script (survives SSH disconnect):

```bash
./launch_and_monitor.sh
```

## Output

Files are written to `out/` (git-ignored):

- `PAC_{year}_MERGED.csv` — merged AYUDAS + SUPERFICIES
- `PAC_{year}_PROCESSED.csv` — classified + computed payments
- `PAC_{year}_PROCESSED_SORTED.csv` — final sorted output
- `stage_state_{year}.json` — per-stage completion state (for resume)

The SIGPAC slope cache is at `data/sigpac_cache.jsonl` (git-ignored).
It is shared across years: a parcel's slope does not change between 2023
and 2024, so each unique parcel is fetched at most once.

## Ecoscheme classification

- **Range**: `500 <= LNA_CODIGO < 513`. Lines 513–516 are rice
  water-management aids, not ecoschemes.
- **API-dependent lines**: LNA 510 and 511 require SIGPAC slope data
  (`pendiente_media`) to determine the classification tier.
- **Status values** in the output `PROCESS_STATUS` column:
  - `OK` — classified and payment computed
  - `NOT_ECOSHEME` — outside the ecoscheme range
  - `PENDIENTE_CLASIFICACION` — no classification entry in the JSON
  - `API_DATA_MISSING` — parcel not in the SIGPAC cache
  - `FAILED` — processing error (e.g. NULL surface)

## Architecture

See [implementation_plan.md](implementation_plan.md) for the full
architecture document, data structures, and design decisions.

## Dependencies

- Python >= 3.13
- [uv](https://docs.astral.sh/uv/) for dependency management
- duckdb, pandas, pyarrow, structlog, sigpac-tools (git dev branch)

## Project structure

```
src/cap_application_processing/
├── cli.py                 # cap-process entry point (orchestrator)
├── constants.py           # Path constants (DATA_DIR, OUT_DIR, PLOTS_DIR)
├── prefetch_sigpac.py     # SIGPAC slope data prefetch (rate-limited, resumable)
├── process_cap_data.py    # Offline classification + payment computation
├── rate_limiter.py        # Async token-bucket rate limiter
├── utils.py               # DuckDB merge, CSV I/O, sort helpers
├── generate_dataset_plots.py  # (legacy) matplotlib plots
├── run_dataset_analysis.py    # (legacy) header analysis
└── cap_pandas_quick_analysis.py  # (legacy) quick analysis
data/
├── classification_2023.json   # Ecoscheme rates for 2023
├── classification_2024.json   # Ecoscheme rates for 2024
├── mapped_crop_codes.csv      # Crop code mapping reference
└── crop_codes_report.csv      # Crop code report reference
```
