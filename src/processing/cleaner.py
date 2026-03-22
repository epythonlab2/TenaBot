import re
import unicodedata

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Move imports to top level for performance
# --------------------------------------------------
# Helpers
# --------------------------------------------------


def remove_garbage(text: str) -> str:
    """
    Remove encoding artifacts and strange unicode characters.
    """
    if pd.isna(text) or str(text).strip() == "":
        return ""

    text = str(text)

    # 1. Normalize Unicode (Fixes ligatures like 'ﬁ' to 'fi')
    text = unicodedata.normalize("NFKD", text)

    # 2. Comprehensive Cleaning
    # Added : ; % to the allowed list as they appear in dosages
    text = re.sub(r"[^a-zA-Z0-9\s.,/()+\-|:;%]", " ", text)

    # 3. Collapse multiple spaces/newlines
    text = re.sub(r"\s+", " ", text).strip()

    return text


def contains_garbage(text: str) -> bool:
    """
    Detect suspicious characters.
    Note: Run this BEFORE remove_garbage if you want to log original artifacts.
    """
    if pd.isna(text):
        return False
    # Check for characters outside basic printable ASCII
    return bool(re.search(r"[^\x20-\x7E]", str(text)))


# --------------------------------------------------
# Cleaning Functions
# --------------------------------------------------


def clean_generic_name(name: str) -> str:
    """Normalize drug generic names."""
    # remove_garbage already handles whitespace and case-normalization-prep
    name = remove_garbage(name).lower()
    return name


def clean_dosage(dosage: str) -> str:
    """Normalize dosage formatting."""
    dosage = remove_garbage(dosage)

    # Normalize separators: ensure consistency for downstream splitting
    dosage = dosage.replace("|", ";")

    # Normalize units: ensure no space between number and unit (e.g., 500mg)
    # Using regex to catch '500 mg', '500  mg', etc.
    dosage = re.sub(r"(\d+)\s+(mg|ml|mcg|g|l)", r"\1\2", dosage, flags=re.IGNORECASE)

    # Remove duplicate/trailing separators
    dosage = re.sub(r";\s*;", ";", dosage)
    return dosage.strip().strip(";")


# --------------------------------------------------
# Category Processing
# --------------------------------------------------


def split_category(category: str):
    """
    Split category code and category name.
    Example: AI.102 .Cephalosporins -> AI.102, cephalosporins
    """
    if pd.isna(category) or str(category).strip() == "":
        return "UNK.000", "unknown"

    category = str(category).strip()

    # Optimized regex for multi-level codes and ghost spaces
    match = re.match(r"^([A-Z]{2}(?:\s*\.\s*\d+)+)\s*\.?\s*(.*)", category)

    if match:
        code = re.sub(r"\s+", "", match.group(1))  # Clean 'AI . 102' -> 'AI.102'
        name = remove_garbage(match.group(2)).lower()
        return code, name

    return "UNK.000", category.lower()


# --------------------------------------------------
# Main DataFrame Cleaning
# --------------------------------------------------


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize extracted drug dataset.
    """
    logger.info("[CLEANING] Starting dataframe cleaning")
    cleaned_rows = []
    garbage_count = 0

    # itertuples is faster than iterrows, but we need to handle column access safely
    for row in df.itertuples(index=False):

        # Check for garbage before we clean it away
        if contains_garbage(row.generic_name) or contains_garbage(row.dosage):
            garbage_count += 1

        cat_code, cat_name = split_category(row.category)

        # Pre-clean generic and dosage
        gen_name = clean_generic_name(row.generic_name)
        dos_val = clean_dosage(row.dosage)

        # Skip rows with no generic name (invalid data)
        if not gen_name:
            continue

        # Generate unique ID
        # Strip dots from s_no to avoid '1.' becoming '001.'
        clean_sno = re.sub(r"\D", "", str(row.s_no))
        sno_str = clean_sno.zfill(3) if clean_sno else "000"
        drug_id = f"{cat_code}-{sno_str}"

        cleaned_rows.append(
            {
                "drug_id": drug_id,
                "generic_name": gen_name,
                "dosage": dos_val,
                "category_code": cat_code,
                "category_name": cat_name,
            }
        )

    if not cleaned_rows:
        logger.error("[CLEANING] No rows remained after cleaning!")
        return pd.DataFrame()

    clean_df = pd.DataFrame(cleaned_rows)

    # Deduplication
    before = len(clean_df)
    clean_df = clean_df.drop_duplicates(subset=["generic_name", "dosage"])
    after = len(clean_df)

    logger.info(
        f"[CLEANING] Success. Total: {after} | Removed: {before - after} | Garbage items fixed: {garbage_count}"
    )

    return clean_df.reset_index(drop=True)
