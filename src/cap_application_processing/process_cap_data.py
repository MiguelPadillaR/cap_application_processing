"""Offline processing stage: classify ecoschemes and compute payments.

This stage makes NO network calls. All SIGPAC data (pendiente_media) must
already be in the JSONL cache produced by the prefetch stage.
"""

import json
import os
from collections import defaultdict
from pathlib import Path

import structlog

from cap_application_processing.prefetch_sigpac import load_cache, parcel_key
from cap_application_processing.utils import (
    count_data_rows,
    iter_merged_data,
    save_processed_chunk,
)

logger = structlog.get_logger()

CHUNK_SIZE = 10_000

# Ecoscheme aid-line range (user decision): 513-516 are rice water-management
# aids, NOT ecoschemes.
ECOSHEME_MIN = 500
ECOSHEME_MAX_EXCLUSIVE = 513
# Lines whose classification depends on SIGPAC slope data.
API_LINES = (510, 511)

STATUS_OK = "OK"
STATUS_NOT_ECOSHEME = "NOT_ECOSHEME"
STATUS_PENDING_CLASSIFICATION = "PENDIENTE_CLASIFICACION"
STATUS_API_DATA_MISSING = "API_DATA_MISSING"
STATUS_FAILED = "FAILED"


def process_cap_data(
    processed_output_file: str,
    merged_output_file: str,
    classification_file: str,
    cache_file: str | Path,
    max_rows: int | None = None,
) -> None:
    """Process the merged file into the processed CSV, fully offline.

    Resumable: rows already present in `processed_output_file` (counted
    quote-aware) are skipped, so re-running after an interruption continues
    where it left off.
    """
    file_exists_and_nonempty = (
        os.path.exists(processed_output_file)
        and os.path.getsize(processed_output_file) > 0
    )
    already_processed_count = count_data_rows(processed_output_file)
    is_first_chunk = not file_exists_and_nonempty

    with open(classification_file, encoding="utf-8") as file:
        classification_data = json.load(file)["ES"]
    classification_index = _index_classifications(classification_data)

    slope_cache = load_cache(Path(cache_file))
    ok_entries = sum(1 for r in slope_cache.values() if r.get("status") == "ok")
    logger.info(
        "Slope cache loaded",
        cache=str(cache_file),
        entries=len(slope_cache),
        ok=ok_entries,
        errors=len(slope_cache) - ok_entries,
    )

    current_row_idx = 0
    for chunk in iter_merged_data(
        merged_output_file, batch_size=CHUNK_SIZE, max_rows=max_rows
    ):
        chunk_len = len(chunk)
        # IDEMPOTENCY GUARD: skip chunks fully written in previous runs
        if current_row_idx + chunk_len <= already_processed_count:
            current_row_idx += chunk_len
            continue

        # Partially written chunk: slice to the remaining entries
        if current_row_idx < already_processed_count < current_row_idx + chunk_len:
            skip_offset = already_processed_count - current_row_idx
            chunk = chunk[skip_offset:]
            current_row_idx = already_processed_count

        processed_entries = [
            _process_entry(entry, classification_index, slope_cache) for entry in chunk
        ]

        if processed_entries:
            save_processed_chunk(
                processed_entries, processed_output_file, is_first_chunk=is_first_chunk
            )
            is_first_chunk = False

        current_row_idx += chunk_len

    logger.info(f"Processing completed and saved to:\n\t{processed_output_file}")


def _process_entry(
    entry: dict,
    classification_index: dict,
    slope_cache: dict,
) -> dict:
    """Classify one merged row and compute its payments (no network)."""
    raw_aid_id = entry.get("LNA_CODIGO")
    try:
        aid_id = int(raw_aid_id)
    except (TypeError, ValueError):
        logger.warning("Invalid LNA_CODIGO; marking entry as failed: %s", entry)
        return _build_status_entry(entry, STATUS_FAILED)

    if not ECOSHEME_MIN <= aid_id < ECOSHEME_MAX_EXCLUSIVE:
        # Not an ecoscheme: keep the row, no slope, no payments.
        updated = dict(entry)
        updated["%_PENDIENTE_MEDIA"] = ""
        updated["ECORREGIMEN_CODIGO"] = ""
        updated.update(_empty_payments())
        updated["PROCESS_STATUS"] = STATUS_NOT_ECOSHEME
        return updated

    # Ecoscheme lines: determine water/pasture regime from the surface data.
    rainfed_status, pasture_status = (
        ("Secano", "Mediterráneo")
        if entry.get("SUP_SECANO_HA", 0)
        and entry.get("SUP_SECANO_HA", 0) > 0
        else ("Regadío", "Húmedo")
    )
    surface = (
        entry.get("SUP_SECANO_HA", 0)
        if rainfed_status == "Secano"
        else entry.get("SUP_RIEGO_HA", 0)
    )
    if surface is None:
        logger.warning(
            "Null surface for ecoscheme code %s; marking failed", aid_id
        )
        return _build_status_entry(entry, STATUS_FAILED)
    surface = float(surface)

    slope_status = None
    mean_slope = None
    if aid_id in API_LINES:
        key = parcel_key(
            entry.get("PRO_CODIGO"),
            entry.get("MSP_CODIGO"),
            entry.get("REC_POLIGONO"),
            entry.get("REC_PARCELA"),
        )
        rec = slope_cache.get(key)
        if rec is None or rec.get("status") != "ok":
            logger.debug("No slope data for parcel", key=key)
            return _build_status_entry(entry, STATUS_API_DATA_MISSING)
        mean_slope = rec["pendiente_media"]
        slope_status = (
            "Terrenos Llanos"
            if mean_slope <= 6.0
            else "Pendiente Media"
            if mean_slope <= 12.0
            else "Pendiente Elevada"
        )

    matching_classifications = classification_index.get(str(aid_id), [])
    if not matching_classifications:
        # In-range ecoscheme line without rates in the classification JSON
        # (e.g. 506/507 until the official rates are added).
        logger.warning("No classification for ecoscheme code %s; marking pending", aid_id)
        updated = dict(entry)
        updated["%_PENDIENTE_MEDIA"] = (
            f"{mean_slope}%" if mean_slope is not None else ""
        )
        updated["ECORREGIMEN_CODIGO"] = ""
        updated.update(_empty_payments())
        updated["PROCESS_STATUS"] = STATUS_PENDING_CLASSIFICATION
        return updated

    classification_entry = _select_classification(
        matching_classifications, rainfed_status, pasture_status, slope_status
    )
    if classification_entry is None:
        logger.warning("Ambiguous classification for code %s: %s", aid_id, entry)
        return _build_status_entry(entry, STATUS_FAILED)

    ecoscheme = classification_entry.get("Ecoscheme", "")
    ecoscheme_code = ecoscheme.split(" - ", maxsplit=1)[0]
    rates = classification_entry.get("Rates", {})
    try:
        payments = _calculate_ecoscheme_payments(surface, rates)
    except ValueError as ve:
        logger.error("Error calculating payments: %s", ve)
        return _build_status_entry(entry, STATUS_FAILED)

    updated = dict(entry)
    updated["%_PENDIENTE_MEDIA"] = (
        f"{mean_slope}%" if mean_slope is not None else ""
    )
    updated["ECORREGIMEN_CODIGO"] = ecoscheme_code
    updated.update(payments)
    updated["PROCESS_STATUS"] = STATUS_OK
    return updated


def _empty_payments() -> dict:
    return {
        "PENINSULAR_PAGO_BASE": "",
        "PENINSULAR_PAGO_PLURI": "",
        "INSULAR_PAGO_BASE": "",
        "INSULAR_PAGO_PLURI": "",
    }


def _build_status_entry(entry: dict, status: str) -> dict:
    """Standardize a row that could not be fully processed (1:1 alignment)."""
    failed_entry = dict(entry)
    failed_entry["%_PENDIENTE_MEDIA"] = ""
    failed_entry["ECORREGIMEN_CODIGO"] = ""
    failed_entry.update(_empty_payments())
    failed_entry["PROCESS_STATUS"] = status
    return failed_entry


def _index_classifications(classification_data):
    index = defaultdict(list)

    for item in classification_data:
        for land_use in item.get("Land_Uses", []):
            index[str(land_use)].append(item)

    return dict(index)


def _select_classification(
    matching_classifications: list[dict],
    rainfed_status: str,
    pasture_status: str,
    slope_status: str = None,
):
    if not matching_classifications:
        return None

    if len(matching_classifications) == 1:
        return matching_classifications[0]

    # 1. Check slope first (used by vegetal cover codes 510, 511)
    if slope_status:
        slope_matches = [
            item
            for item in matching_classifications
            if slope_status.lower() in item.get("Ecoscheme", "").lower()
        ]
        if len(slope_matches) == 1:
            return slope_matches[0]

    # 2. Check water regime (Secano / Regadío) before pasture regime
    elif rainfed_status:
        rainfed_matches = [
            item
            for item in matching_classifications
            if rainfed_status.lower() in item.get("Ecoscheme", "").lower()
        ]
        if len(rainfed_matches) == 1:
            return rainfed_matches[0]

    # 3. Check pasture regime (Húmedos / Mediterráneos)
    if pasture_status:
        pasture_matches = [
            item
            for item in matching_classifications
            if pasture_status.lower() in item.get("Ecoscheme", "").lower()
        ]
        if len(pasture_matches) == 1:
            return pasture_matches[0]

    return None


def _calculate_tiered_amount(
    surface: float,
    threshold: float,
    regional_rates: dict | int | float,
    supplement: float = 0.0,
) -> float:
    if isinstance(regional_rates, (int, float)):
        return (regional_rates + supplement) * surface
    elif not isinstance(regional_rates, dict):
        raise ValueError(f"Invalid regional_rates: {regional_rates}")
    else:
        tier_1_rate = regional_rates["Tier_1"]
        tier_2_rate = regional_rates["Tier_2"]

    if surface <= threshold:
        return (tier_1_rate + supplement) * surface

    threshold_amount = (tier_2_rate + supplement) * threshold
    excess_amount = (tier_1_rate + supplement) * (surface - threshold)

    return threshold_amount + excess_amount


def _calculate_ecoscheme_payments(
    surface: float,
    rates: dict,
) -> dict:
    threshold_raw = rates.get("Threshold_ha", 0.0)
    pluriannuality_applicable = rates.get("Pluriannuality") == "Applicable"
    supplement = 25.0 if pluriannuality_applicable else 0.0

    peninsular_rates = rates.get("Peninsular", {})
    insular_rates = rates.get("Insular", {})

    # Threshold_ha may be 'N/A' (e.g. 512 in 2024): treat as flat Tier_1 rate.
    if isinstance(threshold_raw, (int, float)):
        threshold = float(threshold_raw)
    else:
        threshold = 0.0
        peninsular_rates = (
            peninsular_rates["Tier_1"]
            if isinstance(peninsular_rates, dict)
            else peninsular_rates
        )
        insular_rates = (
            insular_rates["Tier_1"] if isinstance(insular_rates, dict) else insular_rates
        )

    peninsular_base = _calculate_tiered_amount(surface, threshold, peninsular_rates)
    insular_base = _calculate_tiered_amount(surface, threshold, insular_rates)

    peninsular_pluri = _calculate_tiered_amount(
        surface, threshold, peninsular_rates, supplement
    )
    insular_pluri = _calculate_tiered_amount(
        surface, threshold, insular_rates, supplement
    )

    return {
        "PENINSULAR_PAGO_BASE": round(peninsular_base, 2),
        "PENINSULAR_PAGO_PLURI": round(peninsular_pluri, 2),
        "INSULAR_PAGO_BASE": round(insular_base, 2),
        "INSULAR_PAGO_PLURI": round(insular_pluri, 2),
    }


