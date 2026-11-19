"""cap-process: single-command orchestrator for the CAP pipeline.

Stages per year: merge -> prefetch -> process -> sort.
- merge / process / sort run in parallel processes (one per year).
- prefetch runs in ONE asyncio loop for all years, sharing ONE global
  RateLimiter, so the configured rps is a hard global cap.
Every stage is idempotent and resumable; re-running the same command
continues where it left off.
"""

import argparse
import asyncio
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import structlog

from cap_application_processing.constants import DATA_DIR, OUT_DIR
from cap_application_processing.prefetch_sigpac import (
    PrefetchStats,
    cache_path,
    migrate_legacy_caches,
    run_prefetch,
)
from cap_application_processing.process_cap_data import process_cap_data
from cap_application_processing.rate_limiter import RateLimiter
from cap_application_processing.utils import (
    get_merge_cap_data,
    sort_processed_file,
)


def _configure_logging() -> None:
    """Keep our own logs at INFO but silence the noisy sigpac_tools library.

    sigpac_tools logs every single API request at DEBUG, which produced a
    ~440 MB log in 7.5 h. Both our code and the library use structlog, so we
    configure it once here: stdlib logging is the backend, our package logs at
    INFO, and the third-party library is clamped to WARNING.
    """
    import logging

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
        force=True,
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    # Our package: INFO. The SIGPAC library: WARNING (no per-request noise).
    logging.getLogger("cap_application_processing").setLevel(logging.INFO)
    logging.getLogger("sigpac_tools").setLevel(logging.WARNING)


_configure_logging()

logger = structlog.get_logger()

ALL_STAGES = ("merge", "prefetch", "process", "sort")


def _state_file(year: int) -> Path:
    return OUT_DIR / f"stage_state_{year}.json"


def _load_state(year: int) -> dict:
    path = _state_file(year)
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Corrupt stage state, ignoring", path=str(path))
    return {}


def _save_state(year: int, state: dict) -> None:
    """Atomic write (tmp + rename) so a crash never corrupts the state."""
    path = _state_file(year)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _stage_done(state: dict, stage: str) -> bool:
    return state.get(stage) == "done"


# --- Stage workers (run in their own process) -------------------------------


def _run_merge(year: int) -> None:
    merged_path = OUT_DIR / f"PAC_{year}_MERGED.csv"
    get_merge_cap_data(year, output_file=str(merged_path))


def _run_process(
    year: int,
    rate_limit: float,  # noqa: ARG001 - kept for signature symmetry
    max_rows: int | None,
) -> None:
    merged_path = str(OUT_DIR / f"PAC_{year}_MERGED.csv")
    processed_path = str(OUT_DIR / f"PAC_{year}_PROCESSED.csv")
    classification_file = str(DATA_DIR / f"classification_{year}.json")
    process_cap_data(
        processed_output_file=processed_path,
        merged_output_file=merged_path,
        classification_file=classification_file,
        cache_file=cache_path(year),
        max_rows=max_rows,
    )


def _run_sort(year: int) -> None:
    processed_path = OUT_DIR / f"PAC_{year}_PROCESSED.csv"
    sorted_path = OUT_DIR / f"PAC_{year}_PROCESSED_SORTED.csv"
    sort_processed_file(str(processed_path), str(sorted_path))


# --- Orchestration -----------------------------------------------------------


def _run_prefetch_all(
    years: list[int], rate_limit: float, max_workers: int
) -> dict[int, PrefetchStats]:
    """Prefetch every year under ONE shared global rate limiter."""
    limiter = RateLimiter(rate_limit)

    async def _all() -> dict[int, PrefetchStats]:
        return dict(
            zip(
                years,
                await asyncio.gather(
                    *(
                        run_prefetch(
                            year,
                            rate_limit,
                            max_workers=max_workers,
                            limiter=limiter,
                        )
                        for year in years
                    )
                ),
            )
        )

    return asyncio.run(_all())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cap-process",
        description="Merge, prefetch, process and sort CAP datasets per year.",
    )
    parser.add_argument(
        "--years", type=int, nargs="+", default=[2023, 2024], help="Years to process"
    )
    parser.add_argument(
        "--stages",
        default=",".join(ALL_STAGES),
        help="Comma-separated stages to run (default: all)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=20.0,
        help="Global SIGPAC req/s (default 20)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Limit merged rows per year (testing only; default: full file)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Concurrent prefetch fetchers (default 32)",
    )
    args = parser.parse_args(argv)

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    bad = [s for s in stages if s not in ALL_STAGES]
    if bad:
        parser.error(f"Unknown stage(s): {bad}. Valid: {ALL_STAGES}")

    OUT_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    years = args.years

    # 1) Merge (parallel per year)
    if "merge" in stages:
        todo = [y for y in years if not _stage_done(_load_state(y), "merge")]
        if todo:
            logger.info("Merge stage", years=todo)
            with ProcessPoolExecutor(max_workers=len(todo)) as ex:
                list(ex.map(_run_merge, todo))
            for y in todo:
                state = _load_state(y)
                state["merge"] = "done"
                _save_state(y, state)
        else:
            logger.info("Merge stage: all up to date", years=years)

    # 2) Prefetch (single loop, global rate limit shared across years)
    if "prefetch" in stages:
        # Fold any legacy per-year caches into the shared cache once, so
        # parcels already downloaded before the switch are not re-fetched.
        migrate_legacy_caches(years)
        todo = [y for y in years if not _stage_done(_load_state(y), "prefetch")]
        if todo:
            logger.info("Prefetch stage", years=todo, rate_limit=args.rate_limit)
            stats = _run_prefetch_all(todo, args.rate_limit, args.workers)
            for y in todo:
                state = _load_state(y)
                state["prefetch"] = "done"
                state["prefetch_stats"] = vars(stats[y])
                _save_state(y, state)
        else:
            logger.info("Prefetch stage: all up to date", years=years)

    # 3) Process (parallel per year, offline)
    if "process" in stages:
        todo = [y for y in years if not _stage_done(_load_state(y), "process")]
        if todo:
            logger.info("Process stage", years=todo, max_rows=args.max_rows)
            with ProcessPoolExecutor(max_workers=len(todo)) as ex:
                list(
                    ex.map(
                        _run_process,
                        todo,
                        [args.rate_limit] * len(todo),
                        [args.max_rows] * len(todo),
                    )
                )
            for y in todo:
                state = _load_state(y)
                state["process"] = "done"
                _save_state(y, state)
        else:
            logger.info("Process stage: all up to date", years=years)

    # 4) Sort (parallel per year)
    if "sort" in stages:
        todo = [y for y in years if not _stage_done(_load_state(y), "sort")]
        if todo:
            logger.info("Sort stage", years=todo)
            with ProcessPoolExecutor(max_workers=len(todo)) as ex:
                list(ex.map(_run_sort, todo))
            for y in todo:
                state = _load_state(y)
                state["sort"] = "done"
                _save_state(y, state)
        else:
            logger.info("Sort stage: all up to date", years=years)

    logger.info("Pipeline finished", years=years)
    return 0


if __name__ == "__main__":
    sys.exit(main())
