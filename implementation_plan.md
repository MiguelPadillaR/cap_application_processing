# Implementation Plan

## Overview

Goal: make the CAP pipeline (merge AYUDAS+SUPERFICIES per year → classify ecoschemes → compute payments) **safe to run to completion on the full ~5.4M-row datasets** without DoS-ing the SIGPAC API, **idempotent/resumable at every stage**, and **parallelized** where it is safe.

Context and decisions taken with the user:

- The previous failure mode: the 510/511 rows (~1.3M/yr) each triggered a SIGPAC HTTP GET with 8 threads and no rate limit. Verified counts: **827,535 unique parcel refs (2023) + 808,825 (2024) ≈ 1.64M API calls** needed in total.
- **Architecture change (user-approved)**: split the pipeline into stages so that the API is only used in a dedicated, rate-limited, resumable **prefetch phase** that runs **before** row processing. The calculation phase becomes 100% offline.
  1. `merge` — DuckDB left join (unchanged logic, output format hardened).
  2. `prefetch` — download `pendiente_media` for every unique parcel referenced by 510/511 rows, **20 req/s global rate limit** (user-approved, configurable), persisted to disk, resumable.
  3. `process` — classify + compute payments using the local cache; no network.
  4. `sort` — final DuckDB sort by `SECUENCIAL, REC_CODIGO` (user-approved: replaces the fragile line-count idempotency; `REC_CODIGO` is unique per AYUDAS row, verified).
- **Orchestration (user-approved)**: a single command `cap-process --years 2023 2024` runs merge → prefetch → process → sort, years in parallel, with the **20 rps rate limit shared globally across both years**.
- **Ecoscheme range (user-approved)**: `500 <= LNA_CODIGO < 513` (was `500..520`). 513–516 are rice water-management aids, not ecoschemes → treated as normal non-ecoscheme rows.
- **506/507 (user-approved)**: they ARE ecoschemes (P1 variants: siega / islas de biodiversidad) but have no entry in `classification_{year}.json`. The user will supply the official rates; until then they are classified as **`PENDIENTE_CLASIFICACION`** (explicit, non-blocking). The code path is ready so that adding them to the JSON is a data-only change.
- `MAX_ROWS_TEST` is a testing aid; it stays as an optional CLI flag (`--max-rows`) and is **off by default** in the real run.

## Types

New data structures (all in `src/cap_application_processing/`):

- **Parcel key** (prefetch domain): tuple `(PRO_CODIGO, MSP_CODIGO, REC_POLIGONO, REC_PARCELA)` — all normalized to zero-padded strings (province 2, municipality 3, polygon 3, parcel 5) to match `build_cadastral_reference` expectations. This is the dedup unit: 1.64M unique keys total.
- **`sigpac_cache_{year}.jsonl`** (new persistent cache, one file per year at `data/`): one JSON object per line:
  ```json
  {"key": "14/048/001/00199", "status": "ok", "pendiente_media": 3.2, "fetched_at": "2026-11-17T10:00:00Z"}
  ```
  or `{"key": "...", "status": "error", "error": "<short message>", "attempts": 3}` for permanently failed keys (after retry budget). A key present in the file is **never re-fetched** (idempotency).
- **`pending_keys_{year}.txt`** (derived worklist at `data/`): one parcel key per line, produced by a DuckDB query over the merged file (510/511 rows joined to superficies). Regenerated cheaply on every prefetch start; the cache file is the source of truth for "done".
- **`stage_state_{year}.json`** (new, at `out/`): small JSON with `{"merge": "done", "prefetch": {"done": 827535, "total": 827535, "errors": 12}, "process": "done", "sort": "done"}` — written atomically (tmp+rename) after each stage; used by the orchestrator to skip finished stages.
- **Output CSV contract** (`PAC_{year}_PROCESSED.csv`):
  - UTF-8 (was latin1) with `quoting=csv.QUOTE_ALL` so the file is unambiguous.
  - New column `PROCESS_STATUS` with values: `OK`, `NOT_ECOSHEME` (outside 500..512), `PENDIENTE_CLASIFICACION` (506/507 or any in-range code with no classification), `API_DATA_MISSING` (510/511 whose parcel has no successful cache entry), `FAILED` (bad LNA_CODIGO or calc error). Replaces the only-on-failure `CORRECTLY_PROCESSED` column.
  - All other columns as today (`%_PENDIENTE_MEDIA`, `ECORREGIMEN_CODIGO`, `PENINSULAR_PAGO_BASE`, `PENINSULAR_PAGO_PLURI`, `INSULAR_PAGO_BASE`, `INSULAR_PAGO_PLURI`). For `NOT_ECOSHEME` rows `%_PENDIENTE_MEDIA` is empty (not a fake `0.0%`).
  - 1:1 row alignment with the merged file is preserved (every merged row produces exactly one output row), so the process stage can be verified by row count.
- **Classification JSON** (`data/classification_{year}.json`): schema unchanged; entries for `Land_Uses: [506]` and `[507]` to be **added by the user** with official P1 rates (see [Dependencies]/open items). The code must not require them to exist.

## Files

New files:

- `src/cap_application_processing/prefetch_sigpac.py` — the prefetch phase: builds the pending-keys worklist, runs the rate-limited fetch loop with retries/backoff, maintains `sigpac_cache_{year}.jsonl`, reports progress. Exposes `run_prefetch(year, rate_limit, max_workers) -> PrefetchStats`.
- `src/cap_application_processing/rate_limiter.py` — small async token-bucket rate limiter (shared instance across all worker tasks and both years).
- `src/cap_application_processing/cli.py` — `cap-process` entry point (argparse): `--years 2023 2024`, `--stages merge,prefetch,process,sort` (default all), `--rate-limit 20`, `--max-rows N` (testing), `--workers N`. Orchestrates stages per year with `ProcessPoolExecutor` for year-level parallelism; the prefetch stage runs both years' fetch loops inside **one** process sharing **one** rate limiter (this is what makes the 20 rps global).
- `tests/` (new dir, pytest):
  - `tests/test_classification.py` — `_select_classification` truth table (505 secano/riego, 508/509 secano/regadío/húmedo, 510/511 by slope, single-entry pass-through, ambiguous → None).
  - `tests/test_payments.py` — `_calculate_tiered_amount` / `_calculate_ecoscheme_payments` against hand-computed values for both 2023 and 2024 rate tables, incl. the `Threshold_ha: 'N/A'` (2024, 512) and flat-rate (dict vs number) cases.
  - `tests/test_prefetch.py` — with a mocked HTTP layer: rate limiting (measured over a burst), resume (pre-seeded cache file → no duplicate requests), retry/backoff on 5xx, error keys recorded and not retried after budget.
  - `tests/test_process_resume.py` — process stage: run with a fake cache, interrupt mid-chunk, re-run → output identical and no duplicate rows.

Modified files:

- `src/cap_application_processing/process_cap_data.py` — major refactor (see [Functions]): remove `MAX_ROWS_TEST`/in-line API calls/lru_cache; read slopes from the JSONL cache; explicit `PROCESS_STATUS`; chunked append with row-count checkpoint; 506/507 handling; 512 `'N/A'` threshold fix.
- `src/cap_application_processing/utils.py` — `get_merge_cap_data`: write with `QUOTE_ALL` + UTF-8 (DuckDB `COPY ... (HEADER, DELIMITER ',', QUOTE ALL, CODESET 'UTF-8')`); `iter_merged_data`: accept `max_rows=None` by default and read UTF-8; `save_processed_chunk`: UTF-8 + `QUOTE_ALL`; new `count_data_rows(path)` using `csv.reader` (quote-aware) instead of line counting; new `sort_processed_file(in_path, out_path)` (DuckDB `COPY (SELECT * FROM ... ORDER BY SECUENCIAL, REC_CODIGO)`).
- `src/cap_application_processing/main.py` — **deleted** (superseded by `cli.py`; had the `break  # Only do one year` bug).
- `src/cap_application_processing/main_parallel.py` — **deleted** (superseded; had the `merged_output_file=None` crash path when the merged file already existed).
- `pyproject.toml` — add `[project.scripts] cap-process = "cap_application_processing.cli:main"` (drop the existing `cap-application-processing` script, it points at a hello-world `__init__.main`); add `pytest` (dev group).
- `data/classification_2023.json`, `data/classification_2024.json` — **user action**: add `[506]` and `[507]` entries with official P1 rates. Until then the pipeline marks those rows `PENDIENTE_CLASIFICACION` and continues.
- `README.md` (package) — rewrite "Running the script" and "Data checks" sections to describe the new staged pipeline and CLI.

Deleted/retired:

- `out/PAC_2023_PROCESSED.csv`, `out/PAC_2024_PROCESSED.csv` (partial test outputs, latin1) — regenerated by the pipeline; one-time cleanup step before the real run.
- `src/cap_application_processing/processing.log` — move logging to stderr/`out/cap_process.log` via structlog file config; stop writing logs into `src/`.

## Functions

New:

- `run_prefetch(year: int, rate_limit: float, max_workers: int, cache_path: Path, worklist_path: Path) -> PrefetchStats` in `prefetch_sigpac.py` — builds/refreshes the worklist (DuckDB query: distinct parcel keys of 510/511 rows), loads done-keys from the cache, fetches the rest with the shared rate limiter, appends results to the JSONL cache (line-buffered, flushed per result → crash-safe), returns `PrefetchStats(done, total, errors, elapsed)`.
- `build_pending_worklist(merged_file: str, year: int, out_path: Path) -> int` in `prefetch_sigpac.py` — DuckDB: `SELECT DISTINCT PRO/MSP/POL/REC_PARCELA FROM merged WHERE LNA_CODIGO IN (510,511) AND superficies matched`, zero-padded, written to `worklist_path`.
- `fetch_parcel_slope(key: tuple) -> dict` in `prefetch_sigpac.py` — wraps `sigpac_tools.find.find_from_cadastral_registry` via `asyncio.to_thread`, extracts `pendiente_media` from `metadata["query"][-1]`, classifies slope bucket (≤6 Llanos / ≤12 Media / >12 Elevada) and stores both the raw value and the bucket.
- `RateLimiter(rate: float)` in `rate_limiter.py` — async `async def acquire()`; token bucket with capacity = rate (burst of one second's worth), shared across tasks.
- `main(argv: list[str] | None = None) -> int` in `cli.py` — parses args, creates `OUT_DIR`/`DATA_DIR`, runs stages per year: merge and process/sort via `ProcessPoolExecutor` (one worker per year), prefetch via a single asyncio loop handling both years under one `RateLimiter`; writes `stage_state_{year}.json` after each stage; skips stages already `done` (idempotent re-entry).
- `sort_processed_file(in_path: str, out_path: str) -> None` in `utils.py` — DuckDB sort by `SECUENCIAL, REC_CODIGO`, `QUOTE ALL`, UTF-8; writes to a temp file then `os.replace` (atomic).
- `count_data_rows(path: str) -> int` in `utils.py` — `csv.reader`-based count (quote/newline-aware) replacing `_get_already_processed_count`.

Modified:

- `process_cap_data(...)` in `process_cap_data.py` — new signature `process_cap_data(processed_output_file, merged_output_file, classification_file, cache_path, max_rows=None)`. Changes: (1) no network; (2) ecoscheme gate becomes `500 <= code < 513`; (3) 510/511 look up the parcel key in the in-memory dict loaded from `sigpac_cache_{year}.jsonl` (loaded once, ~1.6M small entries ≈ 200MB worst case — acceptable; if not, load per-chunk via a SQLite index — plan uses the dict, measured first); (4) missing/error cache entry → `API_DATA_MISSING` row (not FAILED, so a later cache completion + re-run fixes it without re-doing everything); (5) in-range code with no classification (506/507 today) → `PENDIENTE_CLASIFICACION` row with a warning log; (6) resume via `count_data_rows` on the output + the same chunk-skip logic (now safe because QUOTE_ALL makes row counting reliable); (7) `MAX_ROWS_TEST` removed from the module constant, replaced by the `max_rows` parameter.
- `_select_classification(...)` in `process_cap_data.py` — unchanged logic, but the `slope_status` parameter becomes required for 510/511 (it already is in practice); add explicit handling: if `matching_classifications` is empty for an in-range code → return a sentinel so the caller emits `PENDIENTE_CLASIFICACION` instead of the generic "Missing or ambiguous" warning path.
- `_calculate_tiered_amount(...)` in `process_cap_data.py` — handle `Threshold_ha: 'N/A'` (2024, code 512): treat as flat rate = `Tier_1` (2024's 512 entry already uses flat numeric rates; 2023 uses equal Tier_1/Tier_2 which is equivalent). Rule: if `Threshold_ha` is not a number → flat `Tier_1 * surface`.
- `get_merge_cap_data(year, output_file=None)` in `utils.py` — DuckDB `COPY` gains `QUOTE ALL` and explicit `CODESET 'UTF-8'`; input reads gain `CODESET 'UTF-8'` (verify source encoding first — see Testing; if sources are latin1, read with `CODESET 'LATIN1'` and write UTF-8).
- `iter_merged_data(...)` in `utils.py` — `max_rows: int | None = None` default (was forced by the caller), UTF-8 read.
- `save_processed_chunk(...)` in `utils.py` — `encoding="utf-8"`, `quoting=csv.QUOTE_ALL`.
- `_process_entry_api`, `_cached_fetch_sigpac_data`, `_fetch_parcel_crop_data` in `process_cap_data.py` — **removed** (replaced by the prefetch module); `check_cadastral_data` in `utils.py` stays (still used by prefetch to build refs).

Removed:

- `main.py` (file), `main_parallel.py` (file), `_process_year_wrapper` (function) — replaced by `cli.py`.
- `CORRECTLY_PROCESSED` output column — replaced by `PROCESS_STATUS`.

## Classes

No new classes beyond:

- `PrefetchStats` (dataclass in `prefetch_sigpac.py`): `done: int, total: int, errors: int, elapsed_s: float`.
- `RateLimiter` (in `rate_limiter.py`): token bucket, `async acquire()`, attributes `rate`, `tokens`.

No existing classes are modified (the codebase is module-level functions).

## Dependencies

- **No new runtime dependencies**: the prefetch reuses `sigpac_tools.find.find_from_cadastral_registry` (sync `requests`) wrapped in `asyncio.to_thread` with a bounded semaphore + the shared `RateLimiter`; this keeps the existing ref-validation logic and avoids adding an HTTP client.
- **Add (dev)**: `pytest` (uv dev group) for the new tests.
- **Unchanged**: `duckdb`, `pandas`, `pyarrow`, `structlog`, `sigpac-tools` (git dev rev).
- **User-provided data**: official rates for 506 (siega sostenible) and 507 (islas de biodiversidad en pastos) for 2023 and 2024, added to `classification_{year}.json` under `Land_Uses: [506]` / `[507]` following the existing schema (`Ecoscheme` string containing "Mediterráneo"/"Húmedo" for the pasture-regime matching, `Rates` with `Pluriannuality`, `Threshold_ha`, `Peninsular`, `Insular`).

## Testing

1. **Unit (offline, no API)**: classification truth table; payment math against both years' rate tables (hand-computed fixtures); threshold `'N/A'` case; quote-aware row counting on a CSV with embedded commas/newlines.
2. **Prefetch (mocked transport)**: monkeypatch `sigpac_tools.find.find_from_cadastral_registry`; verify (a) global rate ≤ configured rps over a 500-key burst, (b) pre-seeded cache → zero requests for those keys, (c) 3 transient 500s then success → 1 ok entry, (d) persistent failure → `status: error` after retry budget and no further attempts, (e) kill mid-run and re-run → exactly the remaining keys fetched.
3. **Process resume**: synthetic 25k-row merged CSV (all statuses represented) + fake cache; run to completion, truncate output at a random row, re-run → byte-identical final file.
4. **End-to-end smoke (real API, tiny)**: `cap-process --years 2023 --max-rows 40000 --rate-limit 1` against the real SIGPAC API; verify `PROCESS_STATUS` distribution, spot-check 10 parcels' `pendiente_media` against the web service, and confirm the merged/processed row counts equal the AYUDAS row count.
5. **Full-run validation queries (DuckDB, post-run)**: processed rows == AYUDAS rows per year; `COUNT(*) WHERE PROCESS_STATUS != 'OK' AND LNA_CODIGO BETWEEN 500 AND 512` reported as a summary; `API_DATA_MISSING` count == cache error count; no duplicate `(SECUENCIAL, REC_CODIGO, LNA_CODIGO)` in output.
6. Existing analysis scripts (`run_dataset_analysis.py`, `generate_dataset_plots.py`) keep working: they read `PAC_{year}_PROCESSED.csv` — update their `encoding="latin1"` to `utf-8` (small fix included).

## Implementation Order

1. **Fix the merge output contract** (`utils.py`): verify source CSV encoding (UTF-8 vs latin1 probe), `QUOTE ALL` + UTF-8 in `get_merge_cap_data`, `count_data_rows`, `sort_processed_file`. (No behavior change for callers yet.)
2. **Rate limiter + prefetch module** (`rate_limiter.py`, `prefetch_sigpac.py`): worklist builder, JSONL cache, retry/backoff (3 attempts, exponential 2s/10s/60s, then `status: error`), progress logging every 500 keys, `PrefetchStats`. Unit tests with mocked transport.
3. **Refactor `process_cap_data.py`**: drop API code paths, load cache into a dict, new ecoscheme gate `500..512`, `PROCESS_STATUS` column, `PENDIENTE_CLASIFICACION` for 506/507, `API_DATA_MISSING` for missing cache entries, flat-rate fix for `Threshold_ha: 'N/A'`, resume via `count_data_rows`, `max_rows` parameter (default `None`). Unit tests.
4. **CLI orchestrator** (`cli.py` + `pyproject.toml` script): staged per-year pipeline, `stage_state_{year}.json`, year-level `ProcessPoolExecutor` for merge/process/sort, single asyncio prefetch loop with one shared `RateLimiter` for all years; `--max-rows`, `--rate-limit`, `--stages`, `--workers`.
5. **Delete** `main.py`, `main_parallel.py`; remove `processing.log` from `src/`; update `generate_dataset_plots.py` / `run_dataset_analysis.py` encodings; rewrite package README.
6. **User supplies 506/507 rates** → added to both `classification_{year}.json` (data-only change; code already handles it).
7. **Clean `out/`** (remove partial latin1 test outputs), run the end-to-end smoke (`--max-rows 40000 --rate-limit 1`), review the `PROCESS_STATUS` summary.
8. **Full run**: `cap-process --years 2023 2024` (20 rps default). Expected: merge minutes; prefetch ≈ 23 h/year in parallel ≈ ~23–25 h wall clock total (shared 20 rps); process ≈ tens of minutes (offline); sort ≈ minutes. Interrupt at any point and re-run the same command to resume.
9. **Post-run validation** (step 5 queries) + generate plots/analysis reports.

## Open items (need the user)

- Official rates for **506 (Siega sostenible)** and **507 (Islas de biodiversidad en pastos)**, 2023 and 2024, to complete `classification_{year}.json`.
- Confirmation of the source CSV encoding (UTF-8 vs latin1) — the plan probes it in step 1 and adapts the DuckDB `CODESET` accordingly.



