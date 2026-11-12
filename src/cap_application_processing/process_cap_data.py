from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import lru_cache
from collections import defaultdict
import json
import os
import structlog

from cap_application_processing.constants import DATA_DIR, OUT_DIR
from cap_application_processing.utils import (
    check_cadastral_data,
    iter_merged_data,
    save_processed_chunk,
    get_merge_cap_data,
)

from sigpac_tools.find import find_from_cadastral_registry

logger = structlog.getLogger(__name__)

CHUNK_SIZE = 10_000
MAX_ROWS_TEST = 40000  # Change to 10 or 100 for testing, None for full file
MAX_WORKERS = 8  # Concurrent network threads for API calls


def fetch_parcel_crop_data(cad_ref: str):
    """Worker helper to execute API lookup for a single cadastral reference."""
    __, metadata = find_from_cadastral_registry(cad_ref)
    return metadata.get("query").pop()


@lru_cache(maxsize=10_000)
def cached_fetch_sigpac_data(
    province: str, municipality: str, polygon: str, parcel_id: str
):
    """
    Fetches cadastral data with in-memory caching.
    Prevents redundant API calls for duplicate parcel references.
    """
    cad_ref = check_cadastral_data(
        None,
        province=province,
        municipality=municipality,
        polygon=polygon,
        parcel_id=parcel_id,
    )
    return fetch_parcel_crop_data(cad_ref)


def process_entry_api(entry: dict) -> tuple[dict, float, str, bool]:
    """Helper worker to fetch SIGPAC data for a single entry concurrently."""
    province = str(entry.get("PRO_CODIGO", "99999999"))
    municipality = str(entry.get("MSP_CODIGO", "99999999"))
    polygon = str(entry.get("REC_POLIGONO", "99999999"))
    parcel_id = str(entry.get("REC_PARCELA", "99999999"))

    try:
        sigpac_data = cached_fetch_sigpac_data(
            province, municipality, polygon, parcel_id
        )
        mean_slope = sigpac_data.get("pendiente_media", 0.0)
        slope_status = (
            "Terrenos Llanos"
            if mean_slope <= 6.0
            else "Pendiente Media"
            if mean_slope <= 12.0
            else "Pendiente Elevada"
        )
        return entry, mean_slope, slope_status, True
    except Exception as e:
        logger.error(f"Error checking cadastral data: {e}")
        return entry, 0.0, None, False


def get_already_processed_count(processed_output_file: str) -> int:
    """Returns number of data rows already processed (excluding header)."""
    if (
        not os.path.exists(processed_output_file)
        or os.path.getsize(processed_output_file) == 0
    ):
        return 0
    try:
        with open(processed_output_file, "r", encoding="latin1") as f:
            line_count = sum(1 for _ in f)
            return max(0, line_count - 1)  # Subtract 1 for CSV header row
    except Exception:
        return 0


def _build_unprocessed_entry(entry: dict) -> dict:
    """Helper to standardize failed/unprocessed entries for 1:1 CSV alignment."""
    failed_entry = dict(entry)
    failed_entry.pop("_requested_aid_id", None)
    failed_entry["%_PENDIENTE_MEDIA"] = ""
    failed_entry["ECORREGIMEN_CODIGO"] = ""
    failed_entry["PENINSULAR_PAGO_BASE"] = ""
    failed_entry["PENINSULAR_PAGO_PLURI"] = ""
    failed_entry["INSULAR_PAGO_BASE"] = ""
    failed_entry["INSULAR_PAGO_PLURI"] = ""
    failed_entry["CORRECTLY_PROCESSED"] = "N"
    return failed_entry


def process_cap_data(
    processed_output_file: str,
    merged_output_file: str,
    classification_file: str,
    use_parallel: bool = True,
    max_workers: int = MAX_WORKERS,
):
    # A file needs a header (is_first_chunk=True) ONLY if it doesn't exist or is 0 bytes
    file_exists_and_nonempty = (
        os.path.exists(processed_output_file)
        and os.path.getsize(processed_output_file) > 0
    )

    already_processed_count = get_already_processed_count(processed_output_file)
    is_first_chunk = not file_exists_and_nonempty
    with open(classification_file, encoding="utf-8") as file:
        classification_data = json.load(file)["ES"]

    classification_index = index_classifications(classification_data)
    current_row_idx = 0

    for chunk in iter_merged_data(
        merged_output_file, batch_size=CHUNK_SIZE, max_rows=MAX_ROWS_TEST
    ):
        chunk_len = len(chunk)
        # IDEMPOTENCY GUARD: Skip chunks that were fully written in previous runs
        if current_row_idx + chunk_len <= already_processed_count:
            current_row_idx += chunk_len
            continue

        # If chunk was partially written, slice to remaining entries
        if current_row_idx < already_processed_count < current_row_idx + chunk_len:
            skip_offset = already_processed_count - current_row_idx
            chunk = chunk[skip_offset:]
            current_row_idx = already_processed_count

        processed_entries = []
        ordered_chunk_tasks = []

        # Step 1: Prepare chunk entries while preserving original row sequence
        for entry in chunk:
            raw_aid_id = entry.get("LNA_CODIGO")
            try:
                requested_aid_id = int(raw_aid_id)
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid LNA_CODIGO; marking entry as unprocessed: %s", entry
                )
                ordered_chunk_tasks.append(("FAILED", entry))
                continue

            entry["_requested_aid_id"] = requested_aid_id

            if not 500 <= requested_aid_id <= 520:
                logger.debug("Skipping non-ecoscheme entry: %s", entry)
                ordered_chunk_tasks.append(("OK", entry, 0.0, None))
            elif requested_aid_id in [510, 511]:
                ordered_chunk_tasks.append("API_NEEDED")
            else:
                ordered_chunk_tasks.append(("OK", entry, 0.0, None))

        # Step 2: Execute API requests concurrently while strictly maintaining order
        api_indices = [
            idx for idx, task in enumerate(ordered_chunk_tasks) if task == "API_NEEDED"
        ]
        api_queue = [chunk[idx] for idx in api_indices]

        if api_queue:
            if use_parallel:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # executor.map guarantees results match api_queue order exactly
                    api_results = list(executor.map(process_entry_api, api_queue))
            else:
                api_results = [process_entry_api(entry) for entry in api_queue]

            for idx, (entry, mean_slope, slope_status, success) in zip(
                api_indices, api_results
            ):
                if success:
                    ordered_chunk_tasks[idx] = ("OK", entry, mean_slope, slope_status)
                else:
                    ordered_chunk_tasks[idx] = ("FAILED", entry)

        # Step 3: Compute payment classifications in exact original sequence
        for task in ordered_chunk_tasks:
            if task[0] == "FAILED":
                processed_entries.append(_build_unprocessed_entry(task[1]))
                continue

            _, entry, mean_slope, slope_status = task
            requested_aid_id = entry.pop("_requested_aid_id")

            if not 500 <= requested_aid_id <= 520:
                updated_entry = dict(entry)
                updated_entry["%_PENDIENTE_MEDIA"] = f"{mean_slope}%"
                updated_entry["ECORREGIMEN_CODIGO"] = ""
                processed_entries.append(updated_entry)
                continue

            matching_classifications = classification_index.get(
                str(requested_aid_id), []
            )

            rainfed_status, pasture_status = (
                ("Secano", "Mediterráneo")
                if entry.get("SUP_SECANO_HA", 0) > 0
                else ("Regadío", "Húmedo")
            )

            surface = (
                entry.get("SUP_SECANO_HA", 0)
                if rainfed_status == "Secano"
                else entry.get("SUP_RIEGO_HA", 0)
            )
            # logger.debug(f"Matching classifications for {requested_aid_id}: {matching_classifications}")
            classification_entry = select_classification(
                matching_classifications, rainfed_status, pasture_status, slope_status
            )

            if classification_entry is None:
                logger.warning(
                    "Missing or ambiguous classification for code %s: %s",
                    requested_aid_id,
                    entry,
                )
                processed_entries.append(_build_unprocessed_entry(entry))
                continue

            ecoscheme = classification_entry.get("Ecoscheme", "")
            ecoscheme_code = ecoscheme.split(" - ", maxsplit=1)[0]
            rates = classification_entry.get("Rates", {})
            # logger.debug(f"Detected matching classification for {requested_aid_id}: {classification_entry}")
            # logger.debug(f"Rates for {requested_aid_id}: {rates}")
            try:
                payments = calculate_ecoscheme_payments(surface, rates)
            except ValueError as ve:
                logger.error(f"Error calculating payments: {ve}")
                processed_entries.append(_build_unprocessed_entry(entry))
                continue

            updated_entry = dict(entry)
            updated_entry["%_PENDIENTE_MEDIA"] = f"{mean_slope}%"
            updated_entry["ECORREGIMEN_CODIGO"] = ecoscheme_code
            updated_entry.update(payments)
            processed_entries.append(updated_entry)

        # Save processed chunk to file incrementally
        if processed_entries:
            save_processed_chunk(
                processed_entries, processed_output_file, is_first_chunk=is_first_chunk
            )
            is_first_chunk = False

        current_row_idx += chunk_len

    logger.info(f"Processing completed and saved to:\n\t{processed_output_file}")
    return None


def index_classifications(classification_data):
    index = defaultdict(list)

    for item in classification_data:
        for land_use in item.get("Land_Uses", []):
            index[str(land_use)].append(item)

    return dict(index)


def select_classification(
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

    # 3. Fallback to pasture regime (Mediterráneo / Húmedo)
    elif pasture_status:
        pasture_matches = [
            item
            for item in matching_classifications
            if pasture_status.lower() in item.get("Ecoscheme", "").lower()
        ]
        if len(pasture_matches) == 1:
            return pasture_matches[0]

    return None


def calculate_tiered_amount(
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


def calculate_ecoscheme_payments(
    surface: float,
    rates: dict,
) -> dict:
    threshold = rates.get("Threshold_ha", 0.0)
    pluriannuality_applicable = rates.get("Pluriannuality") == "Applicable"
    supplement = 25.0 if pluriannuality_applicable else 0.0

    peninsular_rates = rates.get("Peninsular", {})
    insular_rates = rates.get("Insular", {})

    peninsular_base = calculate_tiered_amount(surface, threshold, peninsular_rates)
    insular_base = calculate_tiered_amount(surface, threshold, insular_rates)

    peninsular_pluri = calculate_tiered_amount(
        surface, threshold, peninsular_rates, supplement
    )
    insular_pluri = calculate_tiered_amount(
        surface, threshold, insular_rates, supplement
    )

    return {
        "PENINSULAR_PAGO_BASE": round(peninsular_base, 2),
        "PENINSULAR_PAGO_PLURI": round(peninsular_pluri, 2),
        "INSULAR_PAGO_BASE": round(insular_base, 2),
        "INSULAR_PAGO_PLURI": round(insular_pluri, 2),
    }


def process_year_wrapper(year: int):
    """Worker for running each year in parallel processes."""
    merged_output_file = str(OUT_DIR / f"PAC_{year}_MERGED.csv")
    processed_output_file = str(OUT_DIR / f"PAC_{year}_PROCESSED.csv")
    classification_file = str(DATA_DIR / f"classification_{year}.json")

    logger.info(f"Starting processing for year {year}...")
    print("ñ" * 50)
    logger.info(f"processed_output_file {processed_output_file}...")

    merged_output_file = get_merge_cap_data(year, output_file=merged_output_file)
    unprocessed_entries = process_cap_data(
        processed_output_file=processed_output_file,
        merged_output_file=merged_output_file,
        classification_file=classification_file,
        use_parallel=True,
        max_workers=MAX_WORKERS,
    )

    if unprocessed_entries:
        with open(
            DATA_DIR / f"unprocessed_entries_{year}.json", "w", encoding="utf-8"
        ) as f:
            json.dump(unprocessed_entries, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    from datetime import datetime

    start_time = datetime.now()
    years = [2023, 2024]
    OUT_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)

    with ProcessPoolExecutor(max_workers=len(years)) as executor:
        executor.map(process_year_wrapper, years)

    logger.info(f"Total execution time: {datetime.now() - start_time} seconds")
