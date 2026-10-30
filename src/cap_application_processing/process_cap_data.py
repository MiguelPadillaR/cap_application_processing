from collections import defaultdict
import json
import math
import structlog

from decimal import Decimal, InvalidOperation
from concurrent.futures import ThreadPoolExecutor

from .constants import DATA_DIR, OUT_DIR
from cap_application_processing.utils import (
    check_cadastral_data,
    classify_crop_code,
    iter_merged_data,
    save_processed_chunk,
    get_merge_cap_data,
    get_unique_crop_codes,
)

from sigpac_tools.find import find_from_cadastral_registry
logger = structlog.getLogger(__name__)

CHUNK_SIZE = 10_000
MAX_ROWS_TEST = 1000  # Change to 10 or 100 for testing, None for full file
MAX_WORKERS = 32  # Concurrent network threads for API calls


def fetch_parcel_crop_data(cad_ref: str):
    """Worker helper to execute API lookup for a single cadastral reference."""
    __, metadata = find_from_cadastral_registry(cad_ref)
    return metadata.get("query").pop()

def process_cap_data2(
    processed_output_file: str,
    merged_output_file: str,
    classification_file: str,
    use_parallel: bool = False,
    max_workers: int = MAX_WORKERS,
):
    out_files = []
    out_files.append(merged_output_file)

    is_first_chunk = True

    classification_data = None
    with open(classification_file, encoding="utf-8") as file:
        classification_data = json.load(file)["ES"]
    
    validate_classification_data(classification_data)
    classification_index = index_classifications(classification_data)
    
    unprocessed_entries = []
    cadastral_entries = []

    for chunk in iter_merged_data(
        merged_output_file, batch_size=CHUNK_SIZE, max_rows=MAX_ROWS_TEST
    ):
        cad_refs = []
        processed_entries = []

        # Step A: Filter non-ecoscheme entries and prepare cadastral keys
        for entry in chunk:
            raw_aid_id = entry.get("LNA_CODIGO")
            try:
                requested_aid_id = int(raw_aid_id)
            except (TypeError, ValueError):
                logger.warning("Invalid LNA_CODIGO; skipping entry: %s", entry)
                unprocessed_entries.append(entry)
                continue

            if not 500 <= requested_aid_id <= 520:
                logger.debug("Skipping non-ecoscheme entry: %s", entry)
                processed_entries.append(entry)
                continue

            elif requested_aid_id in [510, 511]:  # Vegetal Cover Ecoscheme detected
                # Prepare cadastral ref to query SIGAPC API
                province = str(entry.get("PRO_CODIGO", "99999999"))
                municipality = str(entry.get("MSP_CODIGO", "99999999"))
                polygon = str(entry.get("REC_POLIGONO", "99999999"))
                parcel_id = str(entry.get("REC_PARCELA", "99999999"))

                try:
                    cad_ref = check_cadastral_data(
                        None,
                        province=province,
                        municipality=municipality,
                        polygon=polygon,
                        parcel_id=parcel_id,
                    )
                    cadastral_entries.append((entry, cad_ref))
                    cad_refs.append(cad_ref)
                except Exception as e:
                    logger.error(f"Error checking cadastral data: {e}")
                    unprocessed_entries.append(entry)
                finally:
                    continue  # Move to next entry and process all cadastral references at the end

            # Determine surface and corresponding classification status.
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

            # Find all classifications matching the land-use code.
            matching_classifications = classification_index.get(str(requested_aid_id), [])

            # Select the classification matching the relevant conditions.
            classification_entry = select_classification(matching_classifications, rainfed_status, pasture_status)

            if classification_entry is None:
                logger.warning(
                    "Missing or ambiguous classification for code %s: %s",
                    requested_aid_id,
                    entry,
                )
                unprocessed_entries.append(entry)
                continue

            ecoscheme = classification_entry.get("Ecoscheme", "")
            ecoscheme_code = ecoscheme.split(" - ", maxsplit=1)[0]
            
            rates = classification_entry.get("Rates", {})

            payments = calculate_ecoscheme_payments(surface, rates)

            # Build updated entry
            updated_entry = dict(entry)
            updated_entry["%_PENDIENTE_MEDIA"] = "0.0%"
            updated_entry["ECORREGIMEN_CODIGO"] = ecoscheme_code
            updated_entry.update(payments)

            processed_entries.append(updated_entry)

        # TODO: Process cadastral refs entries

        # 4. Append chunk results incrementally to output file
        save_processed_chunk(
            processed_entries, processed_output_file, is_first_chunk=is_first_chunk
        )
        is_first_chunk = False

    return unprocessed_entries if len(unprocessed_entries) > 0 else None


def process_cap_data(
    processed_output_file: str,
    merged_output_file: str,
    classification_file: str,
    use_parallel: bool = False,
    max_workers: int = MAX_WORKERS,
):
    out_files = []
    eligible_schemes_by_land_use, non_eligible_uses = get_eligible_ecoschemes_data(
        rules_json_filepath=classification_file
    )

    out_files.append(merged_output_file)

    # 1. Quick search to inspect unique crop codes & descriptions
    get_unique_crop_codes(merged_output_file)

    # 2. Stream through merged data in chunks
    is_first_chunk = True

    for chunk in iter_merged_data(
        merged_output_file, batch_size=CHUNK_SIZE, max_rows=MAX_ROWS_TEST
    ):
        processed_entries = []
        unprocessed_entries = []
        valid_entries = []

        # Step A: Filter non-ecoscheme entries and prepare cadastral keys
        for entry in chunk:
            lna_code = entry.get("LNA_CODIGO")
            if lna_code is None or not (500 <= int(lna_code) <= 520):
                logger.debug(f"Skipping non-ecoscheme entry: {entry}")
                processed_entries.append(entry)
                continue

            province = str(entry.get("PRO_CODIGO", "0"))
            municipality = str(entry.get("MSP_CODIGO", "0"))
            polygon = str(entry.get("REC_POLIGONO", "0"))
            parcel_id = str(entry.get("REC_PARCELA", "0"))

            try:
                cad_ref = check_cadastral_data(
                    None,
                    province=province,
                    municipality=municipality,
                    polygon=polygon,
                    parcel_id=parcel_id,
                )
                valid_entries.append((entry, cad_ref))
            except Exception as e:
                logger.error(f"Error checking cadastral data: {e}")
                unprocessed_entries.append(entry)

        # Step B: Batch fetch crop data with deduplication & concurrency
        unique_cad_refs = list({cad_ref for _, cad_ref in valid_entries})

        crop_cache = process_sigpac_api(unique_cad_refs, use_parallel)

        # Step C: Process calculations with fetched metadata
        for entry, cad_ref in valid_entries:
            crop_data = crop_cache.get(cad_ref)

            if not crop_data:
                logger.info("Saving entry to unprocessed entry list")
                unprocessed_entries.append(entry)
                continue

            sup_total = entry.get("SUP_TOTAL_HA", 0)
            total_parcel_area = Decimal(str(sup_total if sup_total is not None else 0))

            crop_merged_id = entry.get("PRD_CODIGO")
            crop_merged_desc = entry.get("PRD_DESC")

            parsed_data = {
                crop_data.get(
                    "uso_sigpac", classify_crop_code(crop_merged_id, crop_merged_desc)
                ): {
                    "area": total_parcel_area,
                    "irrigation_coef": str(crop_data.get("coef_regadio") or "0.0")
                    + "%",
                    "slope_coef": str(crop_data.get("pendiente_media") or "0.0") + "%",
                }
            }

            land_use_assignments = get_exclusivity_land_uses(
                eligible_schemes_by_land_use, non_eligible_uses, parsed_data
            )

            estimated_payments = calculate_ecoscheme_payments(parsed_data, land_use_assignments)[0]

            updated_entry = dict(entry)
            updated_entry["%_PENDIENTE_MEDIA"] = str(
                crop_data.get("pendiente_media", "0.0")
            )

            if estimated_payments:
                scheme_data = estimated_payments[0]
                scheme_key = f"{scheme_data.get('Ecoscheme_ID', '')}: {scheme_data.get('Ecoscheme_Name', '')}"
                updated_entry["ECORREGIMEN_CODIGO"] = scheme_key

                is_pluri = scheme_data.get("pluriannuality_applicable", False)
                pluri_factor = Decimal("1.25") if is_pluri else Decimal("1.00")

                for rate_type in ["Peninsular", "Insular"]:
                    rate_info = scheme_data.get(rate_type, 0.0)

                    if isinstance(rate_info, dict):
                        raw_val = rate_info.get("Total_Base_Payment_EUR", 0.0)
                    else:
                        raw_val = rate_info

                    try:
                        base_payment = (
                            Decimal("0.0") if raw_val is None else Decimal(str(raw_val))
                        )
                    except (InvalidOperation, TypeError, ValueError):
                        base_payment = Decimal("0.0")

                    logger.debug(f"base_payment:\t{base_payment} for {rate_type}")
                    pluri_payment = base_payment * pluri_factor

                    prefix = rate_type.upper()
                    updated_entry[f"{prefix}_PAGO_BASE"] = float(base_payment)
                    updated_entry[f"{prefix}_PAGO_PLURI"] = float(pluri_payment)
            else:
                updated_entry["ECORREGIMEN_CODIGO"] = ""
                updated_entry["PENINSULAR_PAGO_BASE"] = 0.0
                updated_entry["PENINSULAR_PAGO_PLURI"] = 0.0
                updated_entry["INSULAR_PAGO_BASE"] = 0.0
                updated_entry["INSULAR_PAGO_PLURI"] = 0.0

            processed_entries.append(updated_entry)

        # 3. Append chunk results incrementally to output file
        save_processed_chunk(
            processed_entries, processed_output_file, is_first_chunk=is_first_chunk
        )
        is_first_chunk = False

    return unprocessed_entries if len(unprocessed_entries) > 0 else None

def process_sigpac_api(unique_cad_refs, use_parallel=False, max_workers=MAX_WORKERS):
    crop_cache = {}
    if use_parallel:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ref = {
                    executor.submit(fetch_parcel_crop_data, cad_ref): cad_ref
                    for cad_ref in unique_cad_refs
                }
            for future in future_to_ref:
                cad_ref = future_to_ref[future]
                try:
                    crop_cache[cad_ref] = future.result()
                except Exception as e:
                    if "No geometries found in the provided JSON data" in str(e):
                        logger.warning(str(e))
                    else:
                        logger.error(e)
                    crop_cache[cad_ref] = None
    else:
            # Fallback synchronous path
        for cad_ref in unique_cad_refs:
            try:
                crop_cache[cad_ref] = fetch_parcel_crop_data(cad_ref)
            except Exception as e:
                logger.error(e)
                crop_cache[cad_ref] = None

def calculate_tiered_amount(surface, threshold, regional_rates):
    if isinstance(regional_rates, (int, float)):
        return regional_rates * surface

    tier_1 = regional_rates["Tier_1"]
    tier_2 = regional_rates["Tier_2"]

    tier_1_surface = min(surface, threshold)
    tier_2_surface = max(surface - threshold, 0)

    return tier_1 * tier_1_surface + tier_2 * tier_2_surface

def validate_classification_data(classification_data):
    errors = []

    for index, item in enumerate(classification_data):
        prefix = f"classification[{index}]"

        for field in ("Ecoscheme", "Land_Uses", "Rates"):
            if field not in item:
                errors.append(f"{prefix}: missing {field}")

        if not all(field in item for field in ("Ecoscheme", "Land_Uses", "Rates")):
            continue

        if not isinstance(item["Ecoscheme"], str):
            errors.append(f"{prefix}: Ecoscheme must be a string")

        land_uses = item["Land_Uses"]
        if (
            not isinstance(land_uses, list)
            or not land_uses
            or any(type(code) is not int for code in land_uses)
        ):
            errors.append(
                f"{prefix}: Land_Uses must be a non-empty list of integers"
            )

        rates = item["Rates"]
        if not isinstance(rates, dict):
            errors.append(f"{prefix}: Rates must be an object")
            continue

        if rates.get("Pluriannuality") not in ("Applicable", "N/A"):
            errors.append(
                f"{prefix}: invalid or missing Pluriannuality"
            )

        threshold = rates.get("Threshold_ha")
        if threshold != "N/A" and (
            isinstance(threshold, bool)
            or not isinstance(threshold, (int, float))
            or not math.isfinite(threshold)
            or threshold < 0
        ):
            errors.append(f"{prefix}: invalid Threshold_ha")

        for region in ("Peninsular", "Insular"):
            if region not in rates:
                errors.append(f"{prefix}: missing {region}")
                continue

            regional_rates = rates[region]

            # A single rate is allowed for records such as P5.
            if isinstance(regional_rates, (int, float)) and not isinstance(
                regional_rates, bool
            ):
                if not math.isfinite(regional_rates) or regional_rates < 0:
                    errors.append(f"{prefix}: invalid {region} rate")
                continue

            if not isinstance(regional_rates, dict):
                errors.append(f"{prefix}: invalid {region} rate structure")
                continue

            for tier in ("Tier_1", "Tier_2"):
                rate = regional_rates.get(tier)

                if rate != "N/A" and (
                    isinstance(rate, bool)
                    or not isinstance(rate, (int, float))
                    or not math.isfinite(rate)
                    or rate < 0
                ):
                    errors.append(
                        f"{prefix}: invalid {region}.{tier}"
                    )

    if errors:
        raise ValueError(
            "Invalid classification data:\n- " + "\n- ".join(errors)
        )
    
def index_classifications(classification_data):
    index = defaultdict(list)

    for item in classification_data:
        for land_use in item.get("Land_Uses", []):
            index[str(land_use)].append(item)

    return dict(index)

def select_classification(
    matching_classifications,
    rainfed_status,
    pasture_status,
):
    if not matching_classifications:
        return None

    if len(matching_classifications) == 1:
        return matching_classifications[0]

    matches = [
        item
        for item in matching_classifications
        if (
            rainfed_status in item.get("Ecoscheme", "")
            or pasture_status in item.get("Ecoscheme", "")
        )
    ]

    if len(matches) != 1:
        return None

    return matches[0]

def calculate_tiered_amount(
    surface: float,
    threshold: float,
    regional_rates: dict | int | float,
    supplement: float = 0.0,
) -> float:
    """Calculate a payment using the existing tier semantics."""
    if isinstance(regional_rates, (int, float)):
        return (regional_rates + supplement) * surface

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

    peninsular_base = calculate_tiered_amount(
        surface, threshold, peninsular_rates
    )
    insular_base = calculate_tiered_amount(
        surface, threshold, insular_rates
    )

    peninsular_pluri = calculate_tiered_amount(
        surface, threshold, peninsular_rates, supplement
    )
    insular_pluri = calculate_tiered_amount(
        surface, threshold, insular_rates, supplement
    )

    return {
        "PENINSULAR_PAGO_BASE": peninsular_base,
        "PENINSULAR_PAGO_PLURI": peninsular_pluri,
        "INSULAR_PAGO_BASE": insular_base,
        "INSULAR_PAGO_PLURI": insular_pluri,
    }

if __name__ == "__main__":
    years = [2023, 2024]
    OUT_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    for year in years:
        merged_output_file = str(OUT_DIR / f"PAC_{year}_MERGED.csv")
        processed_output_file = str(OUT_DIR / f"PAC_{year}_PROCESSED.csv")
        classification_file = str(DATA_DIR / f"classification_{year}.json")

        merged_output_file = get_merge_cap_data(year, output_file=merged_output_file)
        process_cap_data(
            processed_output_file=processed_output_file,
            merged_output_file=merged_output_file,
            classification_file=classification_file,
        )

