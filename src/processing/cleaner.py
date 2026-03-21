import re

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


def clean_generic_name(name: str) -> str:
    """Normalize drug names"""

    if pd.isna(name):
        return ""

    name = str(name).lower()

    name = re.sub(r"\s+", " ", name)

    return name.strip()


def clean_dosage(dosage: str) -> str:
    """Normalize dosage formatting"""

    if pd.isna(dosage):
        return ""

    dosage = str(dosage)

    # normalize separators
    dosage = dosage.replace("|", ";")

    # normalize whitespace
    dosage = re.sub(r"\s+", " ", dosage)

    # normalize units
    dosage = dosage.replace(" mg", "mg")
    dosage = dosage.replace(" ml", "ml")

    # remove duplicate separators
    dosage = re.sub(r";\s*;", ";", dosage)

    return dosage.strip()


def split_category(category: str):
    """Split category code and name"""

    if pd.isna(category):
        return "", ""

    category = str(category)

    match = re.match(r"([A-Z]{2}\.\d{3})\s*(.*)", category)

    if match:
        return match.group(1), match.group(2).lower().strip()

    return category, ""


def generate_drug_id(category_code: str, s_no: str):
    """Generate unique drug id"""

    s_no = str(s_no).replace(".", "").strip()

    return f"{category_code}-{s_no.zfill(3)}"


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize extracted drug data"""

    logger.info("[CLEANING] Starting dataframe cleaning")

    cleaned_rows = []

    for row in df.itertuples(index=False):

        category_code, category_name = split_category(row.category)

        cleaned_rows.append(
            {
                "drug_id": generate_drug_id(category_code, row.s_no),
                "generic_name": clean_generic_name(row.generic_name),
                "dosage": clean_dosage(row.dosage),
                "category_code": category_code,
                "category_name": category_name,
            }
        )

    clean_df = pd.DataFrame(cleaned_rows)

    before = len(clean_df)

    clean_df = clean_df.drop_duplicates(subset=["generic_name", "dosage"])

    after = len(clean_df)

    clean_df = clean_df.reset_index(drop=True)

    logger.info(f"[CLEANING] Rows before deduplication: {before}")
    logger.info(f"[CLEANING] Rows after deduplication: {after}")
    logger.info(f"[CLEANING] Removed duplicates: {before - after}")

    return clean_df
