"""Prefetch phase: download SIGPAC slope data for parcels before row processing.

This is the ONLY stage of the pipeline that talks to the SIGPAC API. It is
rate-limited (globally shared limiter), resumable (JSONL cache on disk) and
idempotent (a key present in the cache is never re-fetched).
"""

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import structlog

from cap_application_processing.constants import DATA_DIR, OUT_DIR
from cap_application_processing.rate_limiter import RateLimiter
from sigpac_tools.generate import build_cadastral_reference

logger = structlog.get_logger()

RETRY_DELAYS_S = (2.0, 10.0, 60.0)  # backoff between attempts
PROGRESS_EVERY = 500


@dataclass
class PrefetchStats:
    done: int
    total: int
    errors: int
    elapsed_s: float


def parcel_key(pro: str, msp: str, pol: str, par: str) -> str:
    """Normalize a parcel to its zero-padded canonical key (the dedup unit)."""
    return f"{str(pro).zfill(2)}/{str(msp).zfill(3)}/{str(pol).zfill(3)}/{str(par).zfill(5)}"


def cache_path(year: int | None = None) -> Path:
    """Shared SIGPAC slope cache.

    A single cache is used for ALL years: the pendiente_media of a parcel does
    not change between 2023 and 2024, and ~773k parcels are common to both
    worklists. Sharing the cache means each parcel is fetched at most once
    (862,847 unique instead of 1,636,360 per-year). `year` is accepted for
    backwards compatibility but ignored.
    """
    return DATA_DIR / "sigpac_cache.jsonl"


def worklist_path(year: int) -> Path:
    return DATA_DIR / f"pending_keys_{year}.txt"


def build_pending_worklist(year: int, out_path: Path | None = None) -> int:
    """Write one parcel key per line for every parcel referenced by 510/511 rows.

    The merged file must exist (run the merge stage first). Returns the number
    of unique keys written.
    """
    out_path = out_path or worklist_path(year)
    merged_file = OUT_DIR / f"PAC_{year}_MERGED.csv"
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT DISTINCT
                PRO_CODIGO, MSP_CODIGO, REC_POLIGONO, REC_PARCELA
            FROM read_csv_auto('{merged_file}', header=True, encoding='UTF-8')
            WHERE LNA_CODIGO IN (510, 511)
              AND PRO_CODIGO IS NOT NULL
              AND MSP_CODIGO IS NOT NULL
              AND REC_POLIGONO IS NOT NULL
              AND REC_PARCELA IS NOT NULL
            """
        ).fetchall()
    finally:
        con.close()

    keys = sorted({parcel_key(pro, msp, pol, par) for pro, msp, pol, par in rows})
    with open(out_path, "w", encoding="utf-8") as f:
        for key in keys:
            f.write(key + "\n")
    logger.info("Worklist built", year=year, unique_keys=len(keys), path=str(out_path))
    return len(keys)


def load_cache(cache_file: Path) -> dict[str, dict]:
    """Load the JSONL cache into {key: record}. Last record wins on duplicates."""
    cache: dict[str, dict] = {}
    if not cache_file.exists():
        return cache
    with open(cache_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                cache[rec["key"]] = rec
            except (json.JSONDecodeError, KeyError):
                logger.warning("Skipping malformed cache line", line=line[:200])
    return cache


def migrate_legacy_caches(years: list[int]) -> int:
    """Fold the old per-year caches (sigpac_cache_{year}.jsonl) into the shared
    cache (sigpac_cache.jsonl) so already-downloaded parcels are not re-fetched.

    Runs once: after merging, the per-year files are renamed to *.migrated so a
    re-run is a no-op. Returns the number of records folded in.
    """
    shared = cache_path()
    shared.parent.mkdir(parents=True, exist_ok=True)
    merged = load_cache(shared) if shared.exists() else {}
    added = 0
    for year in years:
        legacy = DATA_DIR / f"sigpac_cache_{year}.jsonl"
        if not legacy.exists():
            continue
        for rec in load_cache(legacy).values():
            key = rec["key"]
            if key not in merged:
                merged[key] = rec
                added += 1
        # Rename (not delete) so the data is recoverable and this won't re-run.
        legacy.rename(legacy.with_suffix(legacy.suffix + ".migrated"))
    if added:
        with open(shared, "a", encoding="utf-8") as f:
            for rec in list(merged.values())[-added:]:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info(
            "Legacy caches merged into shared cache",
            added=added,
            shared_total=len(merged),
            path=str(shared),
        )
    return added


def _fetch_one(key: str) -> dict:
    """Blocking fetch of one parcel's pendiente_media (runs in a thread).

    Returns a cache record with status 'ok' or 'error'.
    """
    from sigpac_tools.find import find_from_cadastral_registry

    pro, msp, pol, par = key.split("/")
    try:
        cad_ref = build_cadastral_reference(
            province=pro, municipality=msp, polygon=pol, parcel=par
        )
    except (NotImplementedError, ValueError) as e:
        return {"key": key, "status": "error", "error": f"bad reference: {e}"}
    try:
        __, metadata = find_from_cadastral_registry(cad_ref)
        query = metadata.get("query") or []
        if not query:
            return {"key": key, "status": "error", "error": "empty query result"}
        pendiente = query[-1].get("pendiente_media")
        if pendiente is None:
            return {"key": key, "status": "error", "error": "no pendiente_media"}
        pendiente = float(pendiente)
    except Exception as e:  # noqa: BLE001 - any API/validation failure is a retry
        return {"key": key, "status": "error", "error": f"{type(e).__name__}: {e}"[:300]}
    return {
        "key": key,
        "status": "ok",
        "pendiente_media": pendiente,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


async def _fetch_with_retry(key: str, limiter: RateLimiter) -> dict:
    """Fetch one key under the global rate limit, with retry/backoff."""
    last: dict = {"key": key, "status": "error", "error": "no attempt made"}
    for attempt, delay in enumerate((0.0, *RETRY_DELAYS_S), start=1):
        if delay:
            await asyncio.sleep(delay)
        await limiter.acquire()
        last = await asyncio.to_thread(_fetch_one, key)
        last["attempts"] = attempt
        if last["status"] == "ok":
            return last
        logger.debug(
            "Fetch attempt failed", key=key, attempt=attempt, error=last.get("error")
        )
    return last


async def run_prefetch(
    year: int,
    rate_limit: float,
    max_workers: int = 32,
    limiter: RateLimiter | None = None,
    cache_file: Path | None = None,
    worklist_file: Path | None = None,
) -> PrefetchStats:
    """Prefetch all pending parcels for `year` under a (shared) rate limit.

    `limiter` is shared across years by the orchestrator so the global rps
    holds. Resumable: keys already in the JSONL cache are skipped.
    """
    cache_file = cache_file or cache_path(year)
    worklist_file = worklist_file or worklist_path(year)
    limiter = limiter or RateLimiter(rate_limit)

    total = build_pending_worklist(year, worklist_file)
    cache = load_cache(cache_file)
    pending = [
        line.strip()
        for line in worklist_file.read_text(encoding="utf-8").splitlines()
        if line.strip() and line.strip() not in cache
    ]
    logger.info(
        "Prefetch starting",
        year=year,
        total=total,
        already_cached=len(cache),
        pending=len(pending),
        rate_limit=rate_limit,
    )

    if not pending:
        return PrefetchStats(
            done=total,
            total=total,
            errors=sum(1 for r in cache.values() if r.get("status") == "error"),
            elapsed_s=0.0,
        )

    sem = asyncio.Semaphore(max_workers)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    done = 0
    errors = 0

    # Append mode, flushed per result: a crash never loses more than the
    # in-flight requests, and resume simply skips cached keys.
    out = open(cache_file, "a", encoding="utf-8")

    async def worker(key: str) -> None:
        nonlocal done, errors
        async with sem:
            rec = await _fetch_with_retry(key, limiter)
        out.write(json.dumps(rec, ensure_ascii=False) + "\n")
        out.flush()
        done += 1
        if rec["status"] == "error":
            errors += 1
        if done % PROGRESS_EVERY == 0 or done == len(pending):
            elapsed = time.monotonic() - started
            rate = done / elapsed if elapsed > 0 else 0.0
            eta = (len(pending) - done) / rate if rate > 0 else 0.0
            logger.info(
                "Prefetch progress",
                year=year,
                done=done,
                pending=len(pending),
                errors=errors,
                rate=f"{rate:.1f}/s",
                eta=f"{eta / 60:.0f} min",
            )

    try:
        await asyncio.gather(*(worker(k) for k in pending))
    finally:
        out.close()

    elapsed = time.monotonic() - started
    # Count errors across the whole cache (pre-existing + this run).
    full_cache = load_cache(cache_file)
    total_errors = sum(1 for r in full_cache.values() if r.get("status") == "error")
    stats = PrefetchStats(
        done=total, total=total, errors=total_errors, elapsed_s=elapsed
    )
    logger.info("Prefetch finished", year=year, **vars(stats))
    return stats
