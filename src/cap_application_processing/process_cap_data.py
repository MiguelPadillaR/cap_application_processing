from collections import defaultdict
import json
import math
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
MAX_ROWS_TEST = 500  # Change to 10 or 100 for testing, None for full file
MAX_WORKERS = 32  # Concurrent network threads for API calls


def fetch_parcel_crop_data(cad_ref: str):
    """Worker helper to execute API lookup for a single cadastral reference."""
    __, metadata = find_from_cadastral_registry(cad_ref)
    return metadata.get("query").pop()


def process_cap_data(
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

    # validate_classification_data(classification_data)
    classification_index = index_classifications(classification_data)

    unprocessed_entries = []

    for chunk in iter_merged_data(
        merged_output_file, batch_size=CHUNK_SIZE, max_rows=MAX_ROWS_TEST
    ):
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

            # Find all classifications matching the land-use code.
            matching_classifications = classification_index.get(
                str(requested_aid_id), []
            )
            mean_slope, slope_status = (0.0, None)
            rainfed_status, pasture_status = ("NaN", "NaN")

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
                    sigpac_data = fetch_parcel_crop_data(cad_ref)
                    print("sigpac_data", sigpac_data)
                    mean_slope = sigpac_data.get("pendiente_media")
                    slope_status = (
                        "Terrenos Llanos"
                        if mean_slope <= 6.0
                        else "Pendiente Media"
                        if mean_slope <= 12.0
                        else "Pendiente Elevada"
                    )
                except Exception as e:
                    logger.error(f"Error checking cadastral data: {e}")
                    unprocessed_entries.append(entry)
                finally:
                    continue  # Move to next entry and process all cadastral references at the end
            else:
                # Determine corresponding classification status.
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

            # Select the classification matching the relevant conditions.
            classification_entry = select_classification(
                matching_classifications, rainfed_status, pasture_status, slope_status
            )

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
            try:
                payments = calculate_ecoscheme_payments(surface, rates)
            except ValueError as ve:
                logger.error(f"Error calculating payments: {ve}")
                unprocessed_entries.append(entry)
                continue

            # Build updated entry
            updated_entry = dict(entry)
            updated_entry["%_PENDIENTE_MEDIA"] = str(mean_slope) + "%"
            updated_entry["ECORREGIMEN_CODIGO"] = ecoscheme_code
            updated_entry.update(payments)

            processed_entries.append(updated_entry)

        # Append chunk results incrementally to output file
        save_processed_chunk(
            processed_entries, processed_output_file, is_first_chunk=is_first_chunk
        )
        is_first_chunk = False

    return unprocessed_entries if len(unprocessed_entries) > 0 else None


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
            errors.append(f"{prefix}: Land_Uses must be a non-empty list of integers")

        rates = item["Rates"]
        if not isinstance(rates, dict):
            errors.append(f"{prefix}: Rates must be an object")
            continue

        if rates.get("Pluriannuality") not in ("Applicable", "N/A"):
            errors.append(f"{prefix}: invalid or missing Pluriannuality")

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
                    errors.append(f"{prefix}: invalid {region}.{tier}")

    if errors:
        logger.error(f"\n{classification_data}\n")
        raise ValueError("Invalid classification data:\n- " + "\n- ".join(errors))


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

    matches = [
        item
        for item in matching_classifications
        if any(
            status is not None and status in item.get("Ecoscheme", "")
            for status in (slope_status, rainfed_status, pasture_status)
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

    print(f"Regional rates: {regional_rates}")

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


if __name__ == "__main__":
    years = [2023, 2024]
    OUT_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    for year in years:
        merged_output_file = str(OUT_DIR / f"PAC_{year}_MERGED.csv")
        processed_output_file = str(OUT_DIR / f"PAC_{year}_PROCESSED.csv")
        classification_file = str(DATA_DIR / f"classification_{year}.json")

        merged_output_file = get_merge_cap_data(year, output_file=merged_output_file)
        unprocessed_entries = process_cap_data(
            processed_output_file=processed_output_file,
            merged_output_file=merged_output_file,
            classification_file=classification_file,
        )
        logger.info(
            f"Unprocessed entries for year {year}: {len(unprocessed_entries) if unprocessed_entries else 0}\n\n{unprocessed_entries if unprocessed_entries else 'None'}"
        )

        # Get unprocessed entries in JSON and CSV
        import json

        with open("unprocessed_entries.json", "w", encoding="utf-8") as f:
            json.dump(unprocessed_entries, f, indent=4, ensure_ascii=False)

        import csv

        with open(
            "unprocessed_entries.csv", "w", newline="", encoding="utf-8-sig"
        ) as f:
            writer = csv.DictWriter(
                f, fieldnames=unprocessed_entries[0].keys(), delimiter=";"
            )
            writer.writeheader()
            writer.writerows(unprocessed_entries)
