# cap_application_processing

CAP ecoscheme application processing pipeline for Spain's 2023/2024
CAMPAIGN years. See the [project root README](../../README.md) for the
full usage guide.

## Module overview

| Module | Role |
|--------|------|
| `cli.py` | `cap-process` entry point. Orchestrates merge → prefetch → process → sort per year. |
| `prefetch_sigpac.py` | Rate-limited, resumable SIGPAC slope data download. The only stage that talks to the API. |
| `process_cap_data.py` | Offline classification + payment computation. Reads slopes from the JSONL cache. |
| `rate_limiter.py` | Async token-bucket rate limiter (shared across all worker tasks and years). |
| `utils.py` | DuckDB merge, CSV I/O helpers, quote-aware row counting, atomic sort. |
| `constants.py` | Path constants: `DATA_DIR`, `OUT_DIR`, `PLOTS_DIR`. |
| `generate_dataset_plots.py` | (Legacy) matplotlib distribution plots. |
| `run_dataset_analysis.py` | (Legacy) header/column analysis. |
| `cap_pandas_quick_analysis.py` | (Legacy) quick pandas analysis. |

## Data files

Tracked in the repo:

- `data/classification_{year}.json` — ecoscheme rates and conditions.
- `data/mapped_crop_codes.csv` — crop code mapping reference.
- `data/crop_codes_report.csv` — crop code report reference.

Git-ignored (large, generated at runtime):

- `data/PAC_{year}_AYUDAS.csv`, `data/PAC_{year}_SUPERFICIES.csv` — source inputs.
- `data/sigpac_cache.jsonl` — SIGPAC slope cache (shared across years).
- `data/pending_keys_{year}.txt` — parcel worklists for prefetch.
- `out/PAC_{year}_MERGED.csv`, `out/PAC_{year}_PROCESSED.csv`, `out/PAC_{year}_PROCESSED_SORTED.csv` — pipeline outputs.
- `out/stage_state_{year}.json` — per-stage completion state.
