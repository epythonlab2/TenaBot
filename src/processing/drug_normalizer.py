import re

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


def split_combination_drug(name: str):
    """
    Split combination drugs into components.

    Example:
    'amoxicillin + clavulanic acid'
    -> ['amoxicillin', 'clavulanic acid']
    """

    if not isinstance(name, str) or not name.strip():
        return []

    parts = re.split(r"\s*\+\s*", name)

    return [p.strip() for p in parts if p.strip()]


def extract_dosage_forms(dosage: str):
    """
    Extract dosage forms from dosage string.

    Example:
    'tablet 10mg; injection 5mg/ml'
    -> ['tablet', 'injection']
    """

    if not isinstance(dosage, str) or not dosage.strip():
        return []

    dosage = dosage.lower()

    forms = []

    form_patterns = [
        "tablet",
        "capsule",
        "suspension",
        "injection",
        "cream",
        "ointment",
        "gel",
        "solution",
        "syrup",
        "drops",
        "powder",
    ]

    for form in form_patterns:
        if form in dosage:
            forms.append(form)

    return list(set(forms))


def normalize_drugs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add normalized drug metadata.
    """

    logger.info("[NORMALIZATION] Starting drug normalization")

    df = df.fillna("")

    normalized_rows = []

    for row in df.to_dict(orient="records"):

        base_drugs = split_combination_drug(row["generic_name"])
        dosage_forms = extract_dosage_forms(row["dosage"])

        normalized_rows.append(
            {
                **row,
                "base_drugs": base_drugs,
                "combination": len(base_drugs) > 1,
                "dosage_forms": dosage_forms,
            }
        )

    result = pd.DataFrame(normalized_rows)

    logger.info(f"[NORMALIZATION] Drugs processed: {len(result)}")

    return result
