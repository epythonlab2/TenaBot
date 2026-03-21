import re
from pathlib import Path

import camelot
import pandas as pd

from src.utils.logger import get_logger

# ---------------------------------
# Logger
# ---------------------------------

logger = get_logger(__name__)


# ---------------------------------
# Paths
# ---------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_PDF_PATH = PROJECT_ROOT / "data/raw/ethiopia-2020.pdf"
OUTPUT_PATH = PROJECT_ROOT / "data/processed/processed_drugs.csv"


# ---------------------------------
# Config
# ---------------------------------

STOP_KEYWORDS = ["technical working group", "contributors", "annex", "index"]
HEADER_HINTS = ["generic", "dosage", "s.no"]


# ---------------------------------
# Table Extraction
# ---------------------------------


def extract_tables(pdf_path: Path) -> pd.DataFrame:

    if not pdf_path.exists():
        logger.error(f"[INGESTION] PDF not found: {pdf_path}")
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    logger.info("[INGESTION] Starting extraction from Ethiopia EML 2020")

    tables = camelot.read_pdf(
        str(pdf_path),
        pages="13-end",
        flavor="stream",
        row_tol=10,
        edge_tol=50,
    )

    logger.debug(f"[INGESTION] Tables detected: {len(tables)}")

    all_rows = []
    current_category = "General"
    found_table_start = False

    for table_idx, table in enumerate(tables, start=1):

        df = table.df

        if df.empty:
            logger.debug(f"[INGESTION] Table {table_idx} is empty, skipping")
            continue

        for row in df.itertuples(index=False):

            line = [str(v).strip() for v in row]
            row_text = " ".join(line).lower()

            if not any(line):
                continue

            first_cell = line[0] if len(line) > 0 else ""

            # ---------------------------------
            # Exit strategy
            # ---------------------------------

            if found_table_start and any(k in row_text for k in STOP_KEYWORDS):
                logger.info(
                    f"[INGESTION] End of drug list detected: {row_text[:50]}..."
                )
                return stitch_and_finalize(all_rows)

            # ---------------------------------
            # Category detection
            # ---------------------------------

            cat_match = re.search(r"^([A-Z]{2}\.\d{3}.*)", first_cell)

            if cat_match:
                current_category = cat_match.group(1)
                logger.info(f"[INGESTION] Category detected: {current_category}")
                continue

            # ---------------------------------
            # Header detection
            # ---------------------------------

            header_score = sum(h in row_text for h in HEADER_HINTS)

            if header_score >= 2:
                found_table_start = True
                logger.debug("[INGESTION] Table header detected")
                continue

            # ---------------------------------
            # Collect rows
            # ---------------------------------

            if found_table_start:

                if "page |" in row_text:
                    continue

                s_no = line[0] if len(line) > 0 else ""
                generic = line[1] if len(line) > 1 else ""
                dosage = " ".join(line[2:]) if len(line) > 2 else ""

                all_rows.append([s_no, generic, dosage, current_category])

    return stitch_and_finalize(all_rows)


# ---------------------------------
# Stitch multiline rows
# ---------------------------------


def stitch_and_finalize(rows) -> pd.DataFrame:

    if not rows:
        logger.warning("[PROCESSING] No rows to stitch")
        return pd.DataFrame()

    raw_df = pd.DataFrame(
        rows,
        columns=["s_no", "generic_name", "dosage", "category"],
    )

    logger.debug(f"[PROCESSING] Raw rows collected: {len(raw_df)}")

    final_data = []
    current_entry = None

    for row in raw_df.itertuples(index=False):

        s_no_clean = str(row.s_no).replace(".", "").strip()

        if s_no_clean.isdigit():

            if current_entry:
                final_data.append(current_entry)

            current_entry = {
                "s_no": row.s_no,
                "generic_name": row.generic_name,
                "dosage": row.dosage,
                "category": row.category,
            }

        else:

            if current_entry:

                if row.generic_name:
                    current_entry["generic_name"] += f" {row.generic_name}"

                if row.dosage:
                    current_entry["dosage"] += f" | {row.dosage}"

    if current_entry:
        final_data.append(current_entry)

    df = pd.DataFrame(final_data)

    df["generic_name"] = (
        df["generic_name"].str.replace(r"\s+", " ", regex=True).str.strip()
    )

    df["dosage"] = df["dosage"].str.replace(r"\s+", " ", regex=True).str.strip()

    logger.info(f"[PROCESSING] Final drug entries: {len(df)}")

    return df
